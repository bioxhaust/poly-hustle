"""Find Polymarket reward markets a small deposit can actually quote.

A market qualifies when the minimum size fits the budget, both sides already
rest inside the reward band with more size than we would post, the spread is
tight, and the market does not end within three days. The daily figure is the
planned order's share of the aggregated public book, after the maker's prices,
sizes and fee reserves. It is not the exchange's measurement of this account.
An empty band is skipped, and so is a book we would dominate.

Usage: python3 rewards_scan.py [budget_usd]
"""

import json
import os
import math
import sys
import threading
import time
import random
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

import quote_plan
import llm_risk

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
MIN_LIFE = timedelta(days=3)
MIN_POOL = 20.0
MAX_SHARE = 0.40
MAX_SPREAD = 0.04
MIN_MID = float(os.getenv("MIN_MID", "0.10"))
MAX_MID = float(os.getenv("MAX_MID", "0.90"))
DEPTH_MULT = 2.0
MIN_DAILY = 2.0
# Realized income ran ~0.4-0.67x the book estimate: calibrate, then require MIN_EST_DAY.
EST_CALIB = float(os.getenv("EST_CALIB", "0.5"))
MIN_EST_DAY = float(os.getenv("MIN_EST_DAY", "1.5"))
VOL1H_MAX = float(os.getenv("VOL1H_MAX", "0.03"))
VOL1H_CACHE_SECONDS = float(os.getenv("VOL1H_CACHE_SECONDS", "300"))
_vol1h_cache = {}
BLACKLIST_WHY = {}
REJECT_NOHISTORY = os.getenv("REJECT_NOHISTORY", "1") != "0"
CHECK = 250
# Volatility: midpoint range over the last VOL_HOURS from CLOB prices-history.
VOL_HOURS = float(os.getenv("VOL_HOURS", "6"))
VOL_MAX = float(os.getenv("VOL_MAX", "0.08"))      # range above this: skip the market
VOL_SCALE = float(os.getenv("VOL_SCALE", "0.03"))  # rank value /= 1 + range / VOL_SCALE
# Markets temporarily excluded (condition id -> unix time the ban ends).
BLACKLIST = {}
# Permanent bans from config (comma-separated condition ids).
for _cid in filter(None, (x.strip() for x in os.getenv("BLACKLIST_CIDS", "").split(","))):
    BLACKLIST[_cid] = float("inf")
VOL_CACHE_SECONDS = float(os.getenv("VOL_CACHE_SECONDS", "2400"))
_vol_cache = {}
# Stage-4 samples split here. Numbers below the cut are the fit; later ones
# are only a check. A same-time multiplier from the morning made the afternoon
# error worse, so a resting book's daily figure is not rescaled.
HOLDOUT_AT = "2026-10-27T11:00:00+00:00"
ENTRY_RATIO_LOW = 0.40371702571230017
ENTRY_RATIO_MID = 0.7225003992573809
ENTRY_RATIO_HIGH = 0.985882934508816
SAME_TIME_FACTOR_REJECTED = 0.924733
# Two finished exits of at least one share before the cut, losses $0 and $1.25,
# wall times 2203s and 3777s. Dust fills, open hedges, pair buys that were not
# sold, and markets with no reward rules are outside this. Activity rows had
# no fee field, so no measured fee is subtracted.
EXIT_LOSS = 0.625
EXIT_SECONDS = 2990.0
# Flat gaps that morning, from cancel or sell to the next quote: 2s, 157s, 161s.
DOWNTIME_SECONDS = 157.0
SWITCH_HORIZON_SECONDS = 6 * 3600
SELECTION_MIN_SAMPLES = 30
SELECTION_MIN_MARKETS = 6
SELECTION_MIN_EXITS = 8
_pace_lock = threading.Lock()
_pace_at = 0.0


def median(values):
    vals = sorted(values)
    n = len(vals)
    if not n:
        return None
    mid = n // 2
    if n % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


def selection_supported(stats):
    """Whether a later period is large enough to change which market is quoted.

    The book range and the exit cost stay in the shadow until this is true.
    Fees have to be present as fees. A missing fee is not treated as zero.
    """
    if not stats or stats.get("fees_known") is not True:
        return False
    if stats.get("holdout_samples", 0) < SELECTION_MIN_SAMPLES:
        return False
    if stats.get("holdout_markets", 0) < SELECTION_MIN_MARKETS:
        return False
    if stats.get("completed_exits", 0) < SELECTION_MIN_EXITS:
        return False
    raw, scaled = stats.get("raw_mae"), stats.get("scaled_mae")
    return raw is not None and scaled is not None and scaled < raw


# The 11:00–12:00 UTC check: 5 samples, 2 markets, fees unknown, and the
# morning scale increased the dollar error. Completed exits in the whole
# morning were 2, both before the cut.
STAGE4_HOLDOUT = {
    "holdout_samples": 5,
    "holdout_markets": 2,
    "fees_known": False,
    "completed_exits": 2,
    "raw_mae": 0.3753378000000002,
    "scaled_mae": 0.9854235349971997,
}
ECONOMIC_CHOICE = selection_supported(STAGE4_HOLDOUT)


SCAN_PACE = float(os.getenv("SCAN_PACE_SECONDS", "0.35"))
# Set by any 429 (scanner or trading loop): the scanner yields until then.
cooldown_until = [0.0]


def retry_after(exc, attempt, base=1.0, cap=30.0):
    """Seconds to wait after a 429: Retry-After if given, else exponential backoff with jitter."""
    try:
        value = float(exc.headers.get("Retry-After"))
        if value >= 0:
            return min(value, cap)
    except (AttributeError, TypeError, ValueError):
        pass
    return min(cap, base * 2 ** attempt) * (0.5 + random.random() / 2)


def note_429(delay):
    cooldown_until[0] = max(cooldown_until[0], time.monotonic() + delay)


def get(url, tries=4):
    """Scanner request: one at a time, paced, yielding to the trading loop after any 429."""
    global _pace_at
    for attempt in range(tries):
        with _pace_lock:
            wait = max(_pace_at, cooldown_until[0]) - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            _pace_at = time.monotonic() + SCAN_PACE
        req = urllib.request.Request(url, headers={"User-Agent": "poly-hustle"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < tries - 1:
                delay = retry_after(exc, attempt, base=5.0, cap=60.0)
                note_429(delay)
                continue
            raise


def reward_configs():
    out, cursor = [], ""
    while cursor != "LTE=":
        page = get(f"{CLOB}/rewards/markets/current?next_cursor={cursor}")
        out.extend(page["data"])
        cursor = page.get("next_cursor") or "LTE="
        if not page["data"]:
            break
    return out


def band_stats(levels, mid, band, min_size, bid):
    """Score and share-count of orders already inside the reward band."""
    score = size = 0.0
    for price, qty in levels:
        if qty < min_size:
            continue
        distance = (mid - price) if bid else (price - mid)
        if distance < 0 or distance >= band:
            continue
        score += ((band - distance) / band) ** 2 * qty
        size += qty
    return score, size


def touch_size(levels, price):
    return sum(qty for level, qty in levels if abs(level - price) < 1e-6)


def estimate_orders(market, bids, asks, orders, mid):
    """Approximate pool share for these exact orders, excluding them from competition.

    Public levels do not identify competitors' accounts, so their combined score
    remains an approximation even when our own prices and sizes are exact.
    """
    band, minimum = market["band"], market["min_size"]
    if band <= 0 or mid is None:
        return None
    bid_score, _ = band_stats(bids, mid, band, minimum, True)
    ask_score, _ = band_stats(asks, mid, band, minimum, False)
    own = {"bid": 0.0, "ask": 0.0}
    for (token, side), (price, size) in orders.items():
        book_side, book_price = quote_plan.yes_book_side(market, {
            "asset_id": token, "side": side, "price": price,
        })
        score, _ = band_stats([(book_price, size)], mid, band, minimum, book_side == "bid")
        own[book_side] += score
    ours, theirs = min(own.values()), min(bid_score, ask_score)
    if ours <= 0 or theirs <= 0:
        return None
    share = ours / (ours + theirs)
    # `share` is the aggregated book's score. `account_share` stays empty:
    # public levels do not say which account earns the pool.
    return {"share": share * 100, "est_daily": market["rate"] * share,
            "own_score": ours, "other_score": theirs, "account_share": None}


def evaluate(bids, asks, min_size, band, rate, budget, end, now, order_size=None,
             *, market=None, offset=0.02, cushion=20, own_orders=()):
    """Estimate the actual flat BUY plan on this snapshot, using the maker's policy."""
    if min_size <= 0 or band <= 0 or rate < MIN_POOL:
        return None, "pool"
    if end - now < MIN_LIFE:
        return None, "life"
    if not bids or not asks:
        return None, "book"
    best_bid = max(price for price, _ in bids)
    best_ask = min(price for price, _ in asks)
    market = dict(market or {"yes": "yes", "no": "no", "tick": 0.01,
                             "min_order": 5.0, "fee_rate": 0.0, "fee_exp": 0.0})
    min_size = max(min_size, market["min_order"])
    market.update({"min_size": min_size, "band": band, "rate": rate})
    book = {"bids": [{"price": p, "size": q} for p, q in bids],
            "asks": [{"price": p, "size": q} for p, q in asks]}
    bids, asks = quote_plan.others_book(book, own_orders, market)
    if not bids or not asks:
        return None, "book"
    spread = asks[0][0] - bids[0][0]
    if spread <= 0 or spread > MAX_SPREAD or spread / 2 >= band:
        return None, "spread"
    mid = quote_plan.midpoint(bids, asks, best_bid, best_ask, min_size)
    if not MIN_MID <= mid <= MAX_MID:
        return None, "mid"
    bid = quote_plan.pick(bids, mid, "bid", market, best_ask, offset, cushion)
    ask = quote_plan.pick(asks, mid, "ask", market, best_bid, offset, cushion)
    planned = quote_plan.opening_orders(market, bid, ask, budget, order_size if order_size is not None else math.inf)
    if not planned or min_size * HEDGE_MARGIN > budget:
        return None, "cash"
    size = planned[(market["yes"], "BUY")][1]
    _, bid_size = band_stats(bids, mid, band, min_size, True)
    _, ask_size = band_stats(asks, mid, band, min_size, False)
    thin = min(bid_size, ask_size)
    if thin < DEPTH_MULT * size:
        return None, "depth"
    bid_touch, ask_touch = touch_size(bids, bids[0][0]), touch_size(asks, asks[0][0])
    if min(bid_touch, ask_touch) < size:
        return None, "touch"
    estimate = estimate_orders(market, bids, asks, planned, mid)
    if estimate is None:
        return None, "book"
    if estimate["share"] / 100 > MAX_SHARE:
        return None, "share"
    if estimate["est_daily"] < MIN_DAILY or estimate["est_daily"] * EST_CALIB < MIN_EST_DAY:
        return None, "daily"
    no_bid = round(1 - ask, 4)
    estimate.update({
        "mid": mid, "spread_c": spread * 100, "band_c": band * 100,
        "min_size": min_size, "size": size, "rate": rate,
        "thin": thin, "touch_bid": bid_touch, "touch_ask": ask_touch,
        "yes_bid": bid, "no_bid": no_bid, "book_daily": estimate["est_daily"],
        "fee_rate": market.get("fee_rate"), "fee_exp": market.get("fee_exp"),
        "capital": quote_plan.reserve(market, bid, size) + quote_plan.reserve(market, no_bid, size),
    })
    return estimate, None


def skip_is_confirmed(reason, *, holding):
    """Whether a missing current market was shown to be unfit.

    `error` means the check failed. Cash shortfall while a position is open
    means the deposit is already in that market, not that the market stopped paying.
    """
    if reason == "error":
        return False
    if reason == "cash" and holding:
        return False
    return True


def occupied_capital(row):
    """Cash the planned buys lock, including the fee reserve when the row has it."""
    if not row:
        return None
    if row.get("capital") is not None:
        return float(row["capital"])
    if all(row.get(key) is not None for key in ("size", "yes_bid", "no_bid")):
        return float(row["size"]) * (float(row["yes_bid"]) + float(row["no_bid"]))
    return None


def scheduled_exit_fee(row):
    """Taker fee for crossing out once, from the published schedule.

    None when the schedule was not loaded. A missing schedule is not zero.
    A zero schedule is a real zero. Resting post-only quotes are not charged here.
    """
    if not row or row.get("fee_rate") is None or row.get("fee_exp") is None:
        return None
    rate = float(row["fee_rate"])
    if rate == 0:
        return 0.0
    price, size = row.get("mid"), row.get("size")
    if price is None or size is None:
        return None
    return float(size) * rate * (float(price) * (1 - float(price))) ** float(row["fee_exp"])


def _switch_cost(stay_reward, away_reward, *, holding, exit_fee):
    """One-time dollars to leave: exit time, flat downtime, observed exit loss, exit fee."""
    if stay_reward is None or away_reward is None:
        return None
    if holding and exit_fee is None:
        return None
    lost_now = stay_reward * (EXIT_SECONDS if holding else 0.0) / 86400
    lost_next = away_reward * DOWNTIME_SECONDS / 86400
    return lost_now + lost_next + (EXIT_LOSS if holding else 0.0) + (exit_fee if holding else 0.0)


def _covers(stay_reward, away_reward, *, holding, exit_fee):
    """Whether this away figure repays the switch inside the horizon."""
    if stay_reward is None or away_reward is None:
        return False
    advantage = away_reward - stay_reward
    if advantage <= 0:
        return False
    cost = _switch_cost(stay_reward, away_reward, holding=holding, exit_fee=exit_fee)
    if cost is None:
        return False
    return cost / advantage * 86400 <= SWITCH_HORIZON_SECONDS


def shadow_entry(rows):
    """Compare the economic order with the book order for a flat entry.

    One account quotes one market, and the entry band is a single factor, so
    it does not reorder book daily figures. Capital is reported, not used as a
    second score. The live leader stays row zero of the scan.
    """
    if not rows:
        return {"kind": "entry", "leader": None, "economic": None, "agrees": True,
                "reason": "empty", "choice_changed": False}
    # rows[0] is what a flat account enters. One entry factor does not reorder
    # book daily figures, so the economic leader matches a scan sorted that way.
    live = rows[0]
    economic = max(rows, key=lambda row: row.get("est_daily") or 0)
    agrees = live.get("slug") == economic.get("slug")
    return {
        "kind": "entry", "leader": live.get("slug"), "economic": economic.get("slug"),
        "agrees": agrees, "choice_changed": False,
        "capital": [occupied_capital(row) for row in rows],
        "reason": "one entry factor keeps the book order" if agrees else "first row is not the best book",
    }


def shadow_choice(current_slug, stay_daily, rows, *, stay_is_live, holding, live_target):
    """Score the best other book beside the live rule. Does not pick the target.

    A market we have not joined is an entry forecast: the morning band is
    applied, and only its low end can count as better. A market we are in uses
    the exchange share when that share exists. The raw book is kept when it
    does not, because scaling those books made the later hour worse.
    """
    other = max((row for row in rows if row.get("slug") != current_slug),
                key=lambda row: row.get("est_daily") or 0, default=None)
    payload = {
        "kind": "switch", "preferred": None, "live_target": live_target,
        "stay_kind": "live" if stay_is_live else ("book" if stay_daily is not None else "missing"),
        "stay_daily": stay_daily, "away_slug": None, "away_book": None,
        "away_low": None, "away_mid": None, "away_high": None,
        "switch_cost": None, "holding": holding, "capital_stay": None,
        "capital_away": None, "reason": "insufficient", "choice_changed": False,
        "agrees": None,
    }
    if other is None or stay_daily is None:
        return payload
    away_book = other.get("est_daily")
    if away_book is None:
        return payload
    stay_row = next((row for row in rows if row.get("slug") == current_slug), None)
    exit_fee = scheduled_exit_fee(stay_row) if holding else 0.0
    low = away_book * ENTRY_RATIO_LOW
    mid = away_book * ENTRY_RATIO_MID
    high = away_book * ENTRY_RATIO_HIGH
    shown_fee = None if holding and exit_fee is None else (exit_fee or 0.0)
    cost = None if shown_fee is None else _switch_cost(
        stay_daily, mid, holding=holding, exit_fee=shown_fee)
    payload.update({
        "away_slug": other.get("slug"), "away_book": away_book,
        "away_low": low, "away_mid": mid, "away_high": high, "switch_cost": cost,
        "capital_stay": occupied_capital(stay_row), "capital_away": occupied_capital(other),
    })
    fee_blocks = holding and exit_fee is None
    low_ok = _covers(stay_daily, low, holding=holding, exit_fee=0.0 if exit_fee is None else exit_fee)
    high_ok = _covers(stay_daily, high, holding=holding, exit_fee=0.0 if exit_fee is None else exit_fee)
    if fee_blocks and low_ok:
        payload["preferred"] = "uncertain"
        payload["reason"] = "fee schedule unknown"
    elif low_ok:
        payload["preferred"] = "away"
        payload["reason"] = "low end of the entry band covers the switch"
    elif high_ok:
        payload["preferred"] = "uncertain"
        payload["reason"] = "entry band crosses the current reward"
    else:
        payload["preferred"] = "stay"
        payload["reason"] = "entry band stays below the current reward"
    leaving = live_target not in (None, "")
    if payload["preferred"] == "away":
        payload["agrees"] = leaving
    elif payload["preferred"] in ("stay", "uncertain"):
        payload["agrees"] = not leaving
    return payload


def _money(value):
    return "unknown" if value is None else f"${value:.2f}"


def shadow_line(payload):
    """One journal line. The live target is unchanged while ECONOMIC_CHOICE is off."""
    if not payload:
        return "shadow selection | insufficient | choice unchanged"
    if payload.get("kind") == "entry":
        return (f"shadow entry | book leader {payload.get('leader')} | "
                f"economic leader {payload.get('economic')} | {payload.get('reason')} | "
                f"choice unchanged")
    away = payload.get("away_slug") or "none"
    band = "unknown"
    if payload.get("away_low") is not None:
        band = f"{_money(payload['away_low'])}-{_money(payload['away_high'])}"
    return (f"shadow selection | preferred {payload.get('preferred')} | "
            f"live {payload.get('live_target')} | stay {_money(payload.get('stay_daily'))} "
            f"({payload.get('stay_kind')}) | away {away} book {_money(payload.get('away_book'))} "
            f"band {band} | switch {_money(payload.get('switch_cost'))} | "
            f"capital stay {_money(payload.get('capital_stay'))} away {_money(payload.get('capital_away'))} | "
            f"{payload.get('reason')} | book is not an account share | choice unchanged")


# A hedged pair costs about $1 per share pair; keep room for a taker hedge on top.
HEDGE_MARGIN = quote_plan.HEDGE_MARGIN


def could_afford(cfg, budget):
    """Both reward-size legs plus a hedge margin must fit the balance.

    A pair of min_size Yes + min_size No bids costs close to min_size dollars, and
    an immediate hedge can cost up to MAX_PAIR_COST per pair. With $28, a 50-share
    minimum ($50+) never fits; the old bound let it in and quoted 25 unrewarded shares.
    """
    minimum = float(cfg.get("rewards_min_size") or 0)
    return minimum > 0 and minimum * HEDGE_MARGIN <= budget


def score(cfg, budget, order_size, offset=0.02, cushion=20, own_orders=(), keep=False):
    if not could_afford(cfg, budget):
        return None, "cash"
    try:
        market = get(f"{CLOB}/markets/{cfg['condition_id']}")
        if not market.get("accepting_orders") or market.get("closed"):
            return None, "closed"
        end_raw = market.get("end_date_iso")
        if not end_raw:
            return None, "no end date"
        end = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
        tokens = market["tokens"]
        if len(tokens) != 2:
            return None, "tokens"
        token = tokens[0]["token_id"]
        book = get(f"{CLOB}/book?token_id={token}")
        bids = [(float(level["price"]), float(level["size"])) for level in book.get("bids") or []]
        asks = [(float(level["price"]), float(level["size"])) for level in book.get("asks") or []]
        rules = {"yes": token, "no": tokens[1]["token_id"],
                 "tick": float(book["tick_size"]), "min_order": float(book["min_order_size"]),
                 "fee_rate": 0.0, "fee_exp": 0.0}
        ours = [o for o in own_orders if o.get("market") == cfg["condition_id"]]
        args = (bids, asks, cfg["rewards_min_size"], float(cfg["rewards_max_spread"]) / 100,
                cfg.get("total_daily_rate") or 0, budget, end, datetime.now(UTC), order_size)
        metrics, why = evaluate(*args, market=rules, offset=offset, cushion=cushion, own_orders=ours)
        # Avoid a metadata request for structurally invalid books. Fee reserves can
        # change size and thereby depth/share checks, so re-evaluate those candidates.
        if metrics is None and why not in ("depth", "touch", "share", "daily"):
            return None, why
        info = get(f"{CLOB}/clob-markets/{cfg['condition_id']}")
        gamma = get(f"{GAMMA}/markets?slug={quote(market['market_slug'])}")[0]
        configs = get(f"{CLOB}/rewards/markets/{cfg['condition_id']}")["data"]
        rules = quote_plan.market_rules(gamma, info, configs, market["market_slug"])
        if rules["yes"] != token or rules["no"] != tokens[1]["token_id"]:
            return None, "tokens"
        end = rules["end"]
        metrics, why = evaluate(
            bids, asks, rules["min_size"], rules["band"], rules["rate"], budget, end, datetime.now(UTC),
            order_size, market=rules, offset=offset, cushion=cushion, own_orders=ours)
    except (urllib.error.URLError, KeyError, IndexError, TimeoutError, TypeError, ValueError, json.JSONDecodeError):
        return None, "error"
    if metrics is None:
        return None, why
    vol = volatility(token)
    # The market we already quote has live share data: volatility only lowers its rank.
    # The current market (keep) is exempt from rejection; volatility only lowers its rank.
    if vol is None and REJECT_NOHISTORY and not keep:
        return None, "nohistory"
    if vol is not None and vol > VOL_MAX and not keep:
        return None, "volatile"
    if not keep:
        v1 = volatility1h(token)
        if v1 is None and REJECT_NOHISTORY:
            return None, "nohistory"
        if v1 is not None and v1 > VOL1H_MAX:
            return None, "volatile1h"
    metrics["vol"] = vol
    metrics.update({"q": market["question"][:72], "slug": market.get("market_slug"),
                    "condition": cfg["condition_id"], "end": end.date().isoformat()})
    return metrics, None


def volatility(token, now=None):
    """Midpoint high-low range over the last VOL_HOURS, or None when history is unavailable."""
    now = time.time() if now is None else now
    hit = _vol_cache.get(token)
    if hit and now - hit[0] < VOL_CACHE_SECONDS:
        return hit[1]
    try:
        data = get(f"{CLOB}/prices-history?market={token}&startTs={int(now - VOL_HOURS * 3600)}&fidelity=5")
        prices = [float(p["p"]) for p in (data.get("history") or [])]
    except (urllib.error.URLError, KeyError, TypeError, ValueError, TimeoutError, json.JSONDecodeError):
        return None
    value = max(prices) - min(prices) if len(prices) >= 2 else None
    _vol_cache[token] = (now, value)
    return value


def volatility1h(token, now=None, max_age=None):
    """Midpoint high-low range over the last hour (1-minute points), or None."""
    now = time.time() if now is None else now
    age = VOL1H_CACHE_SECONDS if max_age is None else max_age
    hit = _vol1h_cache.get(token)
    if hit and now - hit[0] < age:
        return hit[1]
    try:
        data = get(f"{CLOB}/prices-history?market={token}&startTs={int(now - 3600)}&fidelity=1")
        prices = [float(p["p"]) for p in (data.get("history") or [])]
    except (urllib.error.URLError, KeyError, TypeError, ValueError, TimeoutError, json.JSONDecodeError):
        return None
    value = max(prices) - min(prices) if len(prices) >= 2 else None
    _vol1h_cache[token] = (now, value)
    return value


def hourly_value(row):
    """Calibrated expected $/day for OUR size at the current competition, discounted for volatility."""
    vol = row.get("vol") or 0.0
    return float(row.get("est_daily") or 0) * EST_CALIB / (1 + vol / VOL_SCALE) * float(row.get("llm_mult", 1.0))


def rank_key(row):
    """Best expected reward for our size first (pool x our estimated share), volatility-penalised."""
    return (-hourly_value(row), row.get("slug") or "")


def blacklisted(cid, now=None):
    until = BLACKLIST.get(cid)
    if until is None:
        return False
    if (time.time() if now is None else now) >= until:
        BLACKLIST.pop(cid, None)
        return False
    return True


def _wanted_ids(always):
    """Condition ids that must be scored even when free cash cannot open them."""
    if not always:
        return []
    if isinstance(always, str):
        return [always]
    return [cid for cid in always if cid]


def search(budget, order_size=None, always=None, check=CHECK, *, offset=0.02, cushion=20, own_orders=()):
    """Markets the bot may enter, best book estimate first, plus skip counts.

    Every open condition is checked even when free cash cannot open a new pair.
    One open market keeps `counts["current_reason"]` as `ok`, a reject reason,
    `error`, or `absent`. Several open markets report the same per condition in
    `counts["reasons"]`. Skip totals themselves stay numeric.
    """
    listed = reward_configs()
    configs = [c for c in listed if could_afford(c, budget)]
    configs.sort(key=lambda c: -(c.get("total_daily_rate") or 0))
    chosen = configs[:check]
    wanted = _wanted_ids(always)
    wanted_set = set(wanted)
    reasons = {}
    for cid in wanted:
        if all(c.get("condition_id") != cid for c in chosen):
            extra = next((c for c in listed if c.get("condition_id") == cid), None)
            if extra:
                chosen.append(extra)
            else:
                reasons[cid] = "absent"
    if wanted_set:
        # Keep the open-order snapshot close to the markets we already quote.
        chosen.sort(key=lambda c: c.get("condition_id") not in wanted_set)
    counts = {}
    rows = []

    def run(cfg):
        row, why = score(cfg, budget, order_size, offset, cushion, own_orders,
                         keep=cfg.get("condition_id") in wanted_set)
        return cfg.get("condition_id"), row, why

    with ThreadPoolExecutor(2) as pool:
        found = list(pool.map(run, chosen))
    for cid, row, why in found:
        if row and blacklisted(cid):
            row, why = None, "blacklist"
        if cid in wanted_set:
            reasons[cid] = why or "ok"
        if row:
            rows.append(row)
        elif why:
            counts[why] = counts.get(why, 0) + 1
    if len(wanted) == 1:
        counts["current_reason"] = reasons.get(wanted[0], "absent")
    elif wanted:
        counts["reasons"] = {cid: reasons.get(cid, "absent") for cid in wanted}
    rows.sort(key=rank_key)
    try:
        before = {r.get("condition") for r in rows}
        rows = llm_risk.apply(rows, keep=wanted_set)
        after = {r.get("condition") for r in rows}
        vetoed = (before - after) & wanted_set
        if vetoed:
            for cid in vetoed:
                reasons[cid] = "llm_veto"
            if len(wanted) == 1:
                counts["current_reason"] = reasons.get(wanted[0], "absent")
            elif wanted:
                counts["reasons"] = {cid: reasons.get(cid, "absent") for cid in wanted}
    except Exception as exc:  # fail-open
        print(f"llm check skipped: {type(exc).__name__}")
    rows.sort(key=rank_key)
    return rows, counts


def main():
    budget = float(sys.argv[1]) if len(sys.argv) > 1 else 30.0
    rows, counts = search(budget)
    print(f"budget ${budget:.2f} | {len(rows)} markets qualify | skipped {counts}", file=sys.stderr)
    if not rows:
        print("nothing qualifies")
        return
    for row in rows[:15]:
        print(
            f"book ~${row['est_daily']:>6.2f}/day (not this account) | pool ${row['rate']:>4.0f} | "
            f"book share {row['share']:>4.1f}% | "
            f"{row['size']:g} shares | thin {row['thin']:.0f} | touch {row['touch_bid']:.0f}/{row['touch_ask']:.0f} | "
            f"mid {row['mid']:.3f} spread {row['spread_c']:.1f}c band ±{row['band_c']:.1f}c "
            f"quotes Yes {row['yes_bid']:g}/No {row['no_bid']:g} | min {row['min_size']} | ends {row['end']} | {row['q']}"
        )
        print(f"    https://polymarket.com/market/{row['slug']}")


if __name__ == "__main__":
    main()
