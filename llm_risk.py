"""Optional LLM news-risk check for market selection (analysis only).

The model can only veto or down-rank a candidate market. Sizing, orders,
entry/exit and risk limits never depend on it. Any error, timeout or bad JSON
fails open: the market keeps its deterministic rank.
"""
import json
import os
import time
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ENABLED = os.getenv("LLM_ENABLED", "0") == "1"
HOST = os.getenv("OLLAMA_HOST", "https://ollama.com").rstrip("/")
MODEL = os.getenv("LLM_MODEL", "beam")
TIMEOUT = float(os.getenv("LLM_TIMEOUT_SECONDS", "8"))
CACHE_SECONDS = float(os.getenv("LLM_CACHE_SECONDS", "3600"))
MAX_CALLS = int(os.getenv("LLM_MAX_CALLS_PER_SCAN", "8"))
SCAN_BUDGET_SECONDS = float(os.getenv("LLM_SCAN_BUDGET_SECONDS", "40"))
VETO_RISK = float(os.getenv("LLM_VETO_RISK", "0.8"))
HELD_VETO = os.getenv("LLM_HELD_VETO", "1") == "1"  # held market: exit via the bot's normal wind-down
PENALTY = float(os.getenv("LLM_PENALTY", "0.7"))  # rank multiplier at risk 1.0 = 1 - PENALTY
_cache = {}
CTX_TIMEOUT = float(os.getenv("LLM_CTX_TIMEOUT_SECONDS", "6"))
NEWS_MAX = int(os.getenv("LLM_NEWS_MAX", "6"))
CLOB = "https://clob.polymarket.com"
_STOP = set("will the a an be in of on by to for and or is are before after between next with at as than more less".split())


def _get(url, timeout=None):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 poly-hustle"})
    with urllib.request.urlopen(req, timeout=timeout or CTX_TIMEOUT) as resp:
        return resp.read()


def price_context(condition, fetch=_get):
    """Short text with 24h/7d moves of the Yes price, or a placeholder."""
    try:
        token = json.loads(fetch(f"{CLOB}/markets/{condition}"))["tokens"][0]["token_id"]
        now = int(time.time())
        out = []
        for label, secs, fid in (("24h", 86400, 15), ("7d", 7 * 86400, 120)):
            hist = json.loads(fetch(f"{CLOB}/prices-history?market={token}&startTs={now - secs}&fidelity={fid}"))
            prices = [float(p["p"]) for p in hist.get("history") or []]
            if len(prices) >= 2:
                steps = [abs(b - a) for a, b in zip(prices, prices[1:])]
                out.append(f"{label}: start {prices[0]:.3f} now {prices[-1]:.3f} change {prices[-1]-prices[0]:+.3f} "
                           f"range {max(prices)-min(prices):.3f} max step {max(steps):.3f} ({len(prices)} pts)")
        return "; ".join(out) or "no price history"
    except Exception:
        return "price history unavailable"


def news_query(question):
    words = re.findall(r"[A-Za-z0-9\-\.]+", question or "")
    keep = [w for w in words if w.lower() not in _STOP][:7]
    return " ".join(keep)


def news_context(question, fetch=_get):
    """Up to NEWS_MAX recent Google News headlines (last 7 days), or a placeholder."""
    try:
        q = urllib.parse.quote(news_query(question) + " when:7d")
        root = ET.fromstring(fetch(f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"))
        items = []
        for it in root.iter("item"):
            title = (it.findtext("title") or "").strip()[:140]
            date = (it.findtext("pubDate") or "")[5:22]
            if title:
                items.append(f"- [{date}] {title}")
            if len(items) >= NEWS_MAX:
                break
        return "\n".join(items) or "no headlines in the last 7 days"
    except Exception:
        return "news unavailable"

PROMPT = (
    "You assess Polymarket markets for a passive two-sided liquidity-rewards maker. "
    "Losses happen when the price jumps on news while our bids rest. Using the price history "
    "and headlines below, rate how likely the price moves >5 cents within hours over the next days. "
    "Use the full 0..1 scale: ~0.05 dormant market, no news, flat price; ~0.3 some coverage but stable; "
    "~0.6 active news flow or recent multi-cent moves; >=0.8 breaking news, imminent decision or big recent jumps. "
    "Answer ONLY JSON: {\"news_risk\": number 0..1, \"veto\": bool, \"reason\": short string}. "
    "Veto only for markets clearly driven by imminent or ongoing breaking news.\n"
    "Market: %s\nEnds: %s\nMid price: %s\nPrice history (Yes): %s\nRecent headlines:\n%s"
)


def _call(question, end, mid, condition=None):
    key = os.getenv("OLLAMA_API_KEY")
    if not key:
        return None
    body = json.dumps({"model": MODEL, "stream": False, "format": "json", "think": False,
                       "options": {"temperature": 0},
                       "messages": [{"role": "user", "content": PROMPT % (question, end, mid, price_context(condition) if condition else "n/a", news_context(question))}]}).encode()
    req = urllib.request.Request(f"{HOST}/api/chat", data=body, headers={
        "Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        content = json.load(resp)["message"]["content"]
    data = json.loads(content)
    risk = min(1.0, max(0.0, float(data.get("news_risk"))))
    return {"risk": risk, "veto": bool(data.get("veto")) or risk >= VETO_RISK,
            "reason": str(data.get("reason", ""))[:80]}


def assess(row, now=None, caller=_call):
    """Cached verdict dict or None (fail-open)."""
    now = time.time() if now is None else now
    cid = row.get("condition")
    hit = _cache.get(cid)
    if hit and now - hit[0] < CACHE_SECONDS:
        return hit[1]
    try:
        verdict = caller(row.get("q"), row.get("end"), row.get("mid"), row.get("condition"))
    except Exception:
        verdict = None
    if verdict is not None:
        _cache[cid] = (now, verdict)
    return verdict


def apply(rows, keep=(), caller=_call, log=print):
    """Annotate rows with llm_mult; drop vetoed non-held rows. Fail-open on any problem."""
    if not ENABLED or not rows:
        return rows
    keep = set(keep or ())
    started, calls, out = time.time(), 0, []
    for row in rows:
        verdict = None
        cached = _cache.get(row.get("condition"))
        fresh = cached and time.time() - cached[0] < CACHE_SECONDS
        if fresh or (calls < MAX_CALLS and time.time() - started < SCAN_BUDGET_SECONDS):
            if not fresh:
                calls += 1
            verdict = assess(row, caller=caller)
        if verdict:
            row["llm_risk"] = verdict["risk"]
            row["llm_mult"] = 1.0 - PENALTY * verdict["risk"]
            try:
                log(f"llm risk {row.get('slug')} {verdict['risk']:.2f} x{row['llm_mult']:.2f}")
            except Exception:
                pass
            if verdict["veto"] and (HELD_VETO or row.get("condition") not in keep):
                try:
                    log(f"llm veto {row.get('slug')} risk {verdict['risk']:.2f} | {verdict['reason']}")
                except Exception:
                    pass
                continue
        out.append(row)
    return out
