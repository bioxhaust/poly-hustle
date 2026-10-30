"""Liquidity-rewards maker for a Polymarket binary market.

Polymarket splits a daily pool between resting orders near the midpoint. An order
scores ((band - distance) / band)^2 * size; Yes bids and No asks count as one side
of the book, Yes asks and No bids as the other, and quoting only one side earns a
third. Every tick the bot reads the book, its position and free cash, and keeps:

- holding equal Yes+No: post-only SELL quotes on both outcomes, with no new BUYs;
- flat and cash free: post-only BUY quotes on both outcomes;
- any imbalance: cancel conflicting orders, verify their final fills, and buy
  the other outcome immediately for every share the book offers inside
  MAX_PAIR_COST, including fees. Selling the just-filled outcome is not a hedge.
  When that buy is not available, the excess stays: a post-only bid for the
  missing leg at the pair cap, and a post-only sale of the excess at entry or
  better. Nothing is sold into a bid below entry. Leaving a market still sells
  the whole remainder at the touch. The size is snapped so the exchange accepts
  the FAK amount. An excess with neither a pair nor a valid limit sends nothing
  and keeps fresh quotes off. Pairing cost, including fees, consumes the
  persistent LOSS_LIMIT_USD. The limit stops new quotes. A managed excess may
  keep its two limits while other markets are still quoted.
  A price/liquidity/budget failure disables fresh quotes and reports the remaining
  exposure. An ambiguous submission is persisted, matched to the book or to
  trades, and cannot be blindly retried. Resting orders and unresolved lookups
  are restored after a restart; a fill is charged from its saved size once.
  Each accounted fill, vanished order, and position gap is appended to
  trades.jsonl. state.json keeps the current book and is overwritten.

Orders sit OFFSET from the midpoint, or closer where at least CUSHION shares of
other makers rest at that price or better and would be filled first. They are
post-only except for risk-reducing hedges. New BUYs and paired SELLs require a
fresh qualifying spread/depth and a full episode loss reserve, plus a buy of
the other outcome inside MAX_PAIR_COST for their full size on that snapshot.
A sale of the same outcome does not qualify. While those quotes rest, every
fill poll reads the book again and cancels them the moment that pair is gone.
Ordinary quotes are paused when the
midpoint jumps; hedging confirmed executions takes priority over that pause.
Fills are checked between routine quote refreshes. Public market scans and
Telegram delivery run separately and cannot monopolize the execution loop.
Balances must confirm a completed hedge before reward quotes resume. Exchange
heartbeats expire if the bot dies, execution checks stall, or quote data goes stale.
It stops STOP_HOURS_BEFORE_END before the market ends, or at STOP_AT if earlier.
With AUTO_MARKET=1 the slug is ignored: every few minutes the bot picks one
market that fits the cash and where other makers already rest on both sides,
and leaves it when the pool, the minimum or the time left goes bad. It will
not leave while it still holds an unmatched position.

With DRY_RUN=1 (the default) nothing is sent; the bot prints what it would do.
"""

import html
import json
import math
import os
import queue
import shutil
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from collections import deque
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

from dotenv import load_dotenv
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    AssetType,
    BalanceAllowanceParams,
    OrderArgsV2,
    OrderMarketCancelParams,
    OrderType,
    TradeParams,
)
from py_clob_client_v2.exceptions import PolyApiException
from py_clob_client_v2.order_builder.constants import BUY, SELL

import quote_plan
import rewards_scan
import market_stats
import ws_feed
import hedge_plan
import merge

try:
    import dashboard_telemetry
except Exception:
    dashboard_telemetry = None

load_dotenv()

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
DATA = "https://data-api.polymarket.com"

SLUG = os.getenv("MARKET_SLUG", "")
AUTO = os.getenv("AUTO_MARKET", "0") != "0"
SCAN_SECONDS = float(os.getenv("SCAN_SECONDS", "600"))
# Discount an untested book and require a 25% gain on two consecutive scans.
SWITCH_DISCOUNT = 0.6
SWITCH_GAIN = float(os.getenv("SWITCH_GAIN", "1.3"))
DECISION_NOTICE = 1800
SIZE = float(os.getenv("SIZE", "25"))
# Reward score is quadratic in closeness to mid; 1c instead of 2c roughly doubles it.
OFFSET = float(os.getenv("OFFSET", "0.01"))
CUSHION = float(os.getenv("CUSHION", "20"))
POLL = float(os.getenv("POLL_SECONDS", "10"))
# Poll executions between ordinary quote refreshes; scans never own the maker.
FILL_POLL = 1.0
HEDGE_BAL_TOL = float(os.getenv("HEDGE_BAL_TOL", "0.5"))
# A live heartbeat is only useful while the owner of the orders makes progress.
LOOP_STALE_AFTER = 30.0
DATA_STALE_AFTER = max(30.0, 3 * POLL)
# Stale data must persist this long before the heartbeat is withheld (exchange cancels ~10s later).
STALE_GRACE = float(os.getenv("HEARTBEAT_STALE_GRACE", "15"))
STOP_HOURS = float(os.getenv("STOP_HOURS_BEFORE_END", "24"))
STOP_AT = os.getenv("STOP_AT", "")
MAX_PAIR = float(os.getenv("MAX_PAIR_COST", "1.02"))
REPAIR_HOURS = float(os.getenv("REPAIR_HOURS", "4"))
REPAIR_MAX = float(os.getenv("REPAIR_MAX_USD", "3"))
# The new immediate-hedge policy never spends more than $3 per imbalance episode.
HEDGE_MAX = min(3.0, max(0.0, REPAIR_MAX))
# A flat market whose quotes stay unhedgeable this long is skipped for BLACKLIST_SECONDS.
STATS_EVERY = float(os.getenv("STATS_EVERY", "1800"))
_stats_total = (None, 0.0)
_stats_logged = [0.0]
HEDGE_BLOCK_SECONDS = float(os.getenv("HEDGE_BLOCK_SECONDS", "120"))
BLACKLIST_SECONDS = float(os.getenv("BLACKLIST_SECONDS", "3600"))


def pair_reserve(size, markets=1):
    """Worst loss to complete one full fill inside the pair cap, per live market."""
    return max(0.0, MAX_PAIR - 1) * max(0.0, float(size)) * max(1, int(markets))
JUMP = float(os.getenv("JUMP", "0.03"))
JUMP_WINDOW = float(os.getenv("JUMP_WINDOW_SECONDS", "120"))
JUMP_PAUSE = float(os.getenv("JUMP_PAUSE_SECONDS", "300"))
SETTLE = float(os.getenv("SETTLE_SECONDS", "120"))
# Below this the account is flat: a switch is allowed, and the exchange will not
# accept an order. 0.01 share is a remainder the data API drops, not a position.
FLAT = 0.5
# Consecutive failed ticks (about one per second) before cancel-all and halt.
MAX_ERRORS = int(os.getenv("MAX_CONSECUTIVE_ERRORS", "60"))
# Largest loss accepted to sell an unpaired leg when leaving a market for a better one.
MAX_EXIT_LOSS = float(os.getenv("MAX_EXIT_LOSS_USD", "0.5"))
# Variant B: an unhedged leg may rest a capped repair bid at most this long; then the
# cheaper of buying the other leg or selling the excess fires (up to EMERGENCY_EXIT_USD)
# and the market is blacklisted for EMERGENCY_BLACKLIST_SECONDS.
REPAIR_MAX_SECONDS = float(os.getenv("REPAIR_MAX_SECONDS", "60"))
EMERGENCY_EXIT_USD = float(os.getenv("EMERGENCY_EXIT_USD", "0.5"))
# Pull quotes between full ticks when the live WS mid is this far from the mid we quoted at.
FAST_PULL_MOVE = float(os.getenv("FAST_PULL_MOVE", "0.015"))
EMERGENCY_BLACKLIST_SECONDS = float(os.getenv("EMERGENCY_BLACKLIST_SECONDS", "86400"))
# Live 1h-volatility check on active markets.
VOL1H_CHECK_SECONDS = float(os.getenv("VOL1H_CHECK_SECONDS", "60"))
# Switching: away must beat stay by SWITCH_GAIN and SWITCH_MIN_GAIN $/day, after MIN_DWELL.
SWITCH_MIN_GAIN = float(os.getenv("SWITCH_MIN_GAIN", "0.5"))
MIN_DWELL = float(os.getenv("MIN_DWELL", "21600"))
MERGER = merge.Merger()
# pull_quotes reasons that are deliberate waits, not data problems.
DELIBERATE_PULLS = {"position has not settled", "merge in progress", "hedge balances not confirmed",
                    "position or cost basis unavailable"}
LOSS_LIMIT = float(os.getenv("LOSS_LIMIT_USD", "16"))
if LOSS_LIMIT <= 0:  # 0 or negative disables the loss limit
    LOSS_LIMIT = float("inf")
# How many markets one loop can watch the way it watches one. Cash and the
# loss limit decide how many of these slots a balance actually opens.
MAX_MARKETS = int(os.getenv("MAX_MARKETS", "4"))
# Zero means "keep one quote's cash unposted". A set value is the reserve.
CASH_RESERVE = float(os.getenv("CASH_RESERVE", "0"))
# Share of cash never allocated to opening quotes, so any fill can be hedged.
HEDGE_RESERVE_PCT = float(os.getenv("HEDGE_RESERVE_PCT", "0.2"))
# Order size grows with allocated cash up to this many shares (fill risk per market).
MAX_ORDER_SIZE = float(os.getenv("MAX_ORDER_SIZE", "200"))
_last_cash = [0.0]
STATE_FILE = os.getenv("STATE_FILE", "state.json")
# Empty means "beside state.json". The tape is append-only; state.json is not.
TRADES_FILE = os.getenv("TRADES_FILE", "")
DRY_RUN = os.getenv("DRY_RUN", "1") != "0"
_recent_notices = {}
_decision_notices = {}
_telegram_queue = queue.SimpleQueue()
_telegram_lock = threading.Lock()
_telegram_thread = None
latest_share_line = ""
latest_earned_line = ""
_share_lines = {}
EARNINGS_EVERY = 120
EARNINGS_NOTICE = 1800
FORECAST_STABLE_SECONDS = 120
FORECAST_EVERY = 600
FORECAST_HISTORY = 200


def _dashboard(event, maker=None, value=None, makers=None):
    """Optional observations must never interrupt execution or recovery."""
    try:
        if dashboard_telemetry is None:
            return
        if event == "publish":
            dashboard_telemetry.publish(makers or [], DRY_RUN, LOSS_LIMIT, DATA_STALE_AFTER)
        else:
            dashboard_telemetry.observe(event, maker, value)
    except Exception:
        pass


def log(msg):
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def trades_path():
    if TRADES_FILE:
        return TRADES_FILE
    return os.path.join(os.path.dirname(os.path.abspath(STATE_FILE)), "trades.jsonl")


def tape_num(value):
    """A readable number for the tape. None stays unknown; it is not zero."""
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return round(number, 8)


def tape_g(value):
    number = tape_num(value)
    return f"{number:g}" if number is not None else "?"


def exchange_fee(payload):
    """Fee the exchange put on this payload. A missing field is unknown, not zero."""
    if not isinstance(payload, dict):
        return None
    for key in ("fee", "feeAmount", "fee_amount"):
        if key in payload and payload[key] not in (None, ""):
            return tape_num(payload[key])
    return None


def exchange_trade_ids(payload):
    if not isinstance(payload, dict):
        return None
    raw = payload.get("associate_trades")
    if raw is None:
        raw = payload.get("trades")
    if not isinstance(raw, list):
        return None
    found = []
    for item in raw:
        if isinstance(item, dict):
            item = item.get("id")
        if item:
            found.append(str(item))
    return found or None


def record_event(event):
    """Append one execution event. A write failure must not change trading."""
    row = {"at": datetime.now(UTC).isoformat()}
    row.update(event)
    try:
        with open(trades_path(), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
    except OSError as exc:
        log(f"could not record trade event: {short_err(exc)}")


def short_err(exc):
    """Exception class and HTTP status only. The body can be an HTML ban page."""
    code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    return f"{type(exc).__name__} {code}" if code else type(exc).__name__


def order_rejected(exc):
    """Definitive request rejection; timeouts, conflicts and server errors stay ambiguous."""
    if not isinstance(exc, PolyApiException):
        return False
    if exc.status_code in {400, 401, 403, 404, 422, 429}:
        return True
    # 503 {"error":"trading is disabled"} is the exchange refusing the order outright
    # (market/venue pause); nothing was placed. Kept ambiguous, it froze the book for hours.
    return exc.status_code == 503 and "trading is disabled" in str(exc.error_msg).lower()


def telegram_configured():
    return bool(os.getenv("TELEGRAM_BOT_TOKEN", "").strip() and os.getenv("TELEGRAM_CHAT_ID", "").strip())


def esc(value):
    return html.escape(str(value), quote=False)


def tg(title, *lines):
    """One Telegram card. The body sits in a quote, so it is inset under the title.
    An empty string is a blank line between groups."""
    body = ["" if line is None else line for line in lines]
    while body and not body[0]:
        body.pop(0)
    while body and not body[-1]:
        body.pop()
    text = f"<b>{title}</b>"
    if body:
        text += "\n\n<blockquote>" + "\n".join(body) + "</blockquote>"
    return text


def qmarket(question):
    return f"<i>«{esc(question)}»</i>"


def notify(text):
    """Send one Telegram message. Repeated texts are skipped for 5 minutes.
    A missing token or a delivery failure never stops the bot."""
    if DRY_RUN or not telegram_configured():
        return
    global _telegram_thread
    with _telegram_lock:
        now = time.time()
        if now - _recent_notices.get(text, 0) < 300:
            return
        _recent_notices[text] = now
        if _telegram_thread is None:
            _telegram_thread = threading.Thread(target=_telegram_worker, daemon=True, name="telegram")
            _telegram_thread.start()
        _telegram_queue.put(text)


def _telegram_worker():
    """A slow Telegram response must not delay handling an exchange execution."""
    while True:
        text = _telegram_queue.get()
        try:
            _send_telegram(text)
        except Exception as exc:
            log(f"telegram failed: {type(exc).__name__}")


def _telegram_chat_ids():
    ids = [os.environ.get("TELEGRAM_CHAT_ID", "").strip()]
    ids += [x.strip() for x in os.environ.get("TG_EXTRA_CHAT_IDS", "").split(",")]
    out = []
    for i in ids:
        if i and i not in out:
            out.append(i)
    return out


def _send_telegram(text):
    token = os.environ["TELEGRAM_BOT_TOKEN"].strip()
    for chat_id in _telegram_chat_ids():
        body = json.dumps({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=10).read()
        except Exception as exc:
            log(f"telegram failed for {chat_id}: {type(exc).__name__}")


def describe_share(rate, pct):
    """One line with the live reward share and what it pays per day at the current pool."""
    if pct <= 0:
        return ""
    return f"📈 Доля <b>{pct:.1f}%</b> · ~<b>${rate * pct / 100:.1f}</b>/день"


def describe_earnings(amount):
    """Today's reward accrual. Polymarket pays it at midnight UTC, 03:00 MSK."""
    if amount is None:
        return ""
    return f"💵 Сегодня <b>${amount:.2f}</b> · выплата <b>03:00</b> МСК"


def status_lines():
    shares = list(_share_lines.values()) or ([latest_share_line] if latest_share_line else [])
    return "\n".join(line for line in (*shares, latest_earned_line) if line)


def note(text):
    """notify, with the latest share and today's earnings appended once known."""
    extra = status_lines()
    if extra and text.endswith("</blockquote>"):
        text = text[:-len("</blockquote>")] + "\n\n" + extra + "</blockquote>"
    elif extra:
        text = f"{text}\n\n<blockquote>{extra}</blockquote>"
    notify(text)


def earnings_amount(rows):
    """USDC accrued in a get_total_earnings_for_user_for_day response."""
    if isinstance(rows, dict):
        rows = [rows] if "earnings" in rows else (rows.get("data") or [])
    if not isinstance(rows, list):
        return None
    return sum(float(row.get("earnings") or 0) for row in rows)


def day_earnings(client):
    """Official reward accrual for the current UTC day, or None if the read failed."""
    if not client:
        return None
    try:
        rows = client.get_total_earnings_for_user_for_day(datetime.now(UTC).date().isoformat())
    except Exception as exc:
        log(f"earnings lookup failed: {short_err(exc)}")
        return None
    return earnings_amount(rows)


REST_COUNT = [0]


def get(url, tries=3):
    """Trading-loop read. On 429: pause the scanner, honor Retry-After, short backoff with jitter."""
    for attempt in range(tries):
        REST_COUNT[0] += 1
        req = urllib.request.Request(url, headers={"User-Agent": "poly-hustle"})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt == tries - 1:
                raise
            rewards_scan.note_429(30.0)
            delay = rewards_scan.retry_after(exc, attempt, base=0.5, cap=4.0)
            log(f"429 on {url.split('?')[0]}; retry in {delay:.1f}s")
            time.sleep(delay)


def load_market(slug=None, condition=None):
    if condition and not slug:
        slug = get(f"{CLOB}/markets/{condition}")["market_slug"]
    slug = slug or SLUG
    if not slug:
        raise ValueError("no market slug")
    m = get(f"{GAMMA}/markets?slug={quote(slug)}")[0]
    info = get(f"{CLOB}/clob-markets/{m['conditionId']}")
    # A failed read must not look like a pool of $0. refresh_rules keeps the last rate.
    configs = get(f"{CLOB}/rewards/markets/{m['conditionId']}")["data"]
    return quote_plan.market_rules(m, info, configs, slug)



def make_client():
    client = ClobClient(
        CLOB,
        chain_id=137,
        key=os.environ["PRIVATE_KEY"],
        signature_type=int(os.getenv("SIGNATURE_TYPE", "1")),
        funder=os.environ["FUNDER_ADDRESS"],
    )
    # Derive the account's existing key first. Creating one fails with
    # "Could not create api key" when it already exists.
    try:
        creds = client.derive_api_key()
    except Exception as exc:
        log(f"derive api key failed ({short_err(exc)}); creating one")
        creds = client.create_api_key()
    if not creds or not getattr(creds, "api_key", None):
        raise RuntimeError("no API credentials: derive and create both failed")
    client.set_api_creds(creds)
    return client


class LoopHealth:
    """Share only progress and the current maker with the heartbeat thread."""
    def __init__(self):
        self._state = (time.monotonic(), None)
        self._stale_since = None

    def _data_reason(self, stale):
        """Stale data must persist DATA_STALE_AFTER before the heartbeat stops.

        Hedging and requotes briefly clear `data_at`; suspending on every such
        gap let the exchange cancel resting reward quotes again and again.
        """
        if not stale:
            self._stale_since = None
            return None
        now = time.monotonic()
        if self._stale_since is None:
            self._stale_since = now
        if now - self._stale_since >= STALE_GRACE:
            return "trading data unavailable or stale"
        return None

    def progress(self, maker):
        self._state = (time.monotonic(), maker)

    def reason(self):
        progressed_at, maker = self._state
        if isinstance(maker, (list, tuple)):
            return self._book_reason(progressed_at, maker)
        if time.monotonic() - progressed_at >= LOOP_STALE_AFTER:
            return "control loop stalled"
        if maker is not None and time.monotonic() - maker.pulse_at >= LOOP_STALE_AFTER:
            return "execution checks stalled"
        return self._data_reason(maker is not None and not maker.data_fresh())

    def _book_reason(self, progressed_at, makers):
        """One market keeps the strict halt. A book halts only when the loop or every market is blind."""
        makers = [item for item in makers if item is not None]
        if len(makers) <= 1:
            maker = makers[0] if makers else None
            if time.monotonic() - progressed_at >= LOOP_STALE_AFTER:
                return "control loop stalled"
            if maker is not None and time.monotonic() - maker.pulse_at >= LOOP_STALE_AFTER:
                return "execution checks stalled"
            if maker is not None and not maker.data_fresh():
                return "trading data unavailable or stale"
            return None
        if time.monotonic() - progressed_at >= LOOP_STALE_AFTER:
            return "control loop stalled"
        if any(time.monotonic() - item.pulse_at >= LOOP_STALE_AFTER for item in makers):
            return "execution checks stalled"
        return self._data_reason(not any(self._book_alive(item) for item in makers))

    def _book_alive(self, maker):
        """A market counts while its snapshot is fresh or its first check is still in flight.

        One hedging market clears its own orders. The heartbeat stays up for the others.
        """
        if maker.data_fresh():
            return True
        return maker.data_at is None and time.monotonic() - maker.pulse_at < DATA_STALE_AFTER


def keep_alive(client, stop, health):
    """Renew only healthy orders; missing heartbeats let the exchange cancel them."""
    beat = ""
    last_alert = 0.0
    first = True
    suspended = None
    while not stop.is_set():
        reason = health.reason()
        # Arm the exchange's cancellation timer even during a stalled startup.
        if reason and not first:
            if reason != suspended:
                log(f"heartbeat suspended: {reason}; exchange cancellation timer active")
                suspended = reason
            stop.wait(1)
            continue
        arming, first = first, False
        if suspended:
            log("heartbeat resumed: control loop and trading data fresh")
            suspended = None
        ok = False
        for attempt in range(2):
            if stop.is_set() or (attempt and not arming and health.reason()):
                break
            try:
                beat = (client.post_heartbeat(beat) or {}).get("heartbeat_id", "")
                ok = True
                break
            except PolyApiException as exc:
                expected = exc.error_msg.get("heartbeat_id") if isinstance(exc.error_msg, dict) else None
                if (expected is None or expected == beat) and not attempt:
                    # Invalid Heartbeat ID that names no new id (or echoes ours): start a fresh chain.
                    beat = ""
                    continue
                if expected is None or attempt:
                    log(f"heartbeat failed: {short_err(exc)}")
                    break
                beat = expected
            except Exception as exc:
                # ConnectionTerminated and similar: reconnect with a fresh chain.
                beat = ""
                if attempt:
                    log(f"heartbeat failed: {short_err(exc)}")
                    break
                stop.wait(0.5)
                continue
        if not ok and time.time() - last_alert > 300:
            last_alert = time.time()
            note(tg(
                "💓 Пульс",
                "Биржа не принимает пульс.",
                "",
                "Если так продлится больше <b>10 секунд</b>, она снимет заявки.",
            ))
        stop.wait(5 if ok else 1)


def reported_positions(market):
    """(size, average price) per outcome from the data API, which lags the exchange."""
    rows = get(f"{DATA}/positions?user={os.environ['FUNDER_ADDRESS']}&market={market['condition']}&sizeThreshold=0")
    held = {market["yes"]: (0.0, 0.0), market["no"]: (0.0, 0.0)}
    for r in rows:
        if r.get("asset") in held:
            held[r["asset"]] = (float(r.get("size") or 0), float(r.get("avgPrice") or 0))
    return held


def floor_tick(price, tick):
    return round(math.floor(price / tick + 1e-9) * tick, 4)


def ceil_tick(price, tick):
    return round(math.ceil(price / tick - 1e-9) * tick, 4)



def floor2(x):
    return quote_plan.floor2(x)


def reserve(market, price, size):
    return quote_plan.reserve(market, price, size)


def remaining(order):
    return float(order["original_size"]) - float(order.get("size_matched") or 0)


def name(market, token):
    return "Yes" if token == market["yes"] else "No"


def order_label(market, order):
    return f"{order['side']} {name(market, order['asset_id'])} {float(order['original_size']):g} @ {order['price']}"


def yes_book_side(market, order):
    """Where an order rests in the Yes book, which also lists No orders mirrored at 1 - price."""
    price = float(order["price"])
    if order["asset_id"] == market["yes"]:
        return ("bid" if order["side"] == BUY else "ask"), price
    return ("ask" if order["side"] == BUY else "bid"), round(1 - price, 4)


def others_book(book, ours, market):
    return quote_plan.others_book(book, ours, market)


BOOK_MAX_AGE = float(os.getenv("BOOK_MAX_AGE_SECONDS", "15"))


def book_fresh(book, fetched_at, token=None, now=None):
    """Whether a book WE fetched at monotonic `fetched_at` is still usable.

    The exchange `timestamp` is the book's last change, not our read time: a
    quiet but current book looked "stale" and pulled quotes for ~45s after
    entry. Freshness is the local age of our read plus basic sanity checks.
    """
    now = time.monotonic() if now is None else now
    if not isinstance(book, dict) or not isinstance(book.get("bids"), list) \
            or not isinstance(book.get("asks"), list):
        return False
    if token is not None and book.get("asset_id") not in (None, token):
        return False
    return 0.0 <= now - fetched_at <= BOOK_MAX_AGE


def fresh_book(token):
    """The websocket book when its feed is healthy and both sides are present, else REST /book."""
    feed = ws_feed.FEED
    book = feed.book(token) if feed is not None else None
    if book is not None and book["bids"] and book["asks"]:
        return book
    return get(f"{CLOB}/book?token_id={token}")


def live_mid(token):
    """Top-of-book midpoint from the websocket book (what /midpoint returns), or None."""
    feed = ws_feed.FEED
    book = feed.book(token) if feed is not None else None
    if not book or not book["bids"] or not book["asks"]:
        return None
    bid, ask = book_top(book)
    mid = round((bid + ask) / 2, 6)
    return mid if 0 < mid < 1 else None


def exchange_mid(token, fallback=None):
    """The exchange's own /midpoint. A self-computed mid from a thin book was wrong in the 28.10 incident."""
    try:
        value = float(get(f"{CLOB}/midpoint?token_id={token}")["mid"])
    except Exception:
        return fallback
    return value if 0 < value < 1 else fallback


def book_top(book):
    bids = [float(lvl["price"]) for lvl in book.get("bids") or []]
    asks = [float(lvl["price"]) for lvl in book.get("asks") or []]
    return (max(bids) if bids else None), (min(asks) if asks else None)


def depth_price(levels, size):
    return quote_plan.depth_price(levels, size)


def midpoint(bids, asks, best_bid, best_ask, min_size):
    return quote_plan.midpoint(bids, asks, best_bid, best_ask, min_size)


def pick(levels, mid, side, market, limit):
    return quote_plan.pick(levels, mid, side, market, limit, OFFSET, CUSHION)


def only_dust(sizes, min_order):
    """Flat, or holding only a one-sided partial-fill remainder too small to hedge alone."""
    values = list(sizes.values())
    return max(values) < FLAT or (min(values) < 0.01 and max(values) < min_order)


def wanted_orders(market, bid_px, ask_px, held, cash, repair, loss_left=None, allow_open=True,
                  exit_single=False, exit_bid=None, unmatched_basis=None):
    """Map (token_id, side) -> (price, size) of the order that should rest there.

    bid_px and ask_px are Yes-book prices: a Yes bid at p is also a No ask at 1 - p,
    a Yes ask at p a No bid at 1 - p. `repair` is the loss in USD this one exit may
    take. `loss_left` is what remains of the cumulative limit; it can force a cheaper
    price than MAX_PAIR_COST, down to completing the pair at $1. `allow_open` is
    false once this market's rewards no longer fit: balanced pairs and a one-sided
    repair can still be worked, down to the exchange minimum, but no fresh
    two-sided bids are added. While inventory is imbalanced, only the missing
    leg is bid; selling paired shares could increase or reverse the exposure.
    `unmatched_basis` maps an excess token to a conservative per-share basis of
    its unmatched lots, so cheap paired lots cannot dilute a repair's loss cap.
    `exit_single` replaces a sole leg's repair with a sale at entry or better;
    `exit_bid` is the best external bid for the held outcome."""
    min_size, tick = market["min_size"], market["tick"]
    yes, no = market["yes"], market["no"]
    (yes_sz, yes_avg), (no_sz, no_avg) = held[yes], held[no]
    net = yes_sz - no_sz
    pairs = floor2(min(yes_sz, no_sz))
    yes_bid = bid_px
    no_bid = round(1 - ask_px, 4) if ask_px else None
    wanted = {}
    floor = min_size if allow_open else market["min_order"]

    if exit_single and min(yes_sz, no_sz) < 0.01:
        token, size, avg = (yes, yes_sz, yes_avg) if net > 0 else (no, no_sz, no_avg)
        size = floor2(size)
        price = ceil_tick(avg, tick)
        if avg > 0 and tick <= price <= 1 - tick and size >= market["min_order"]:
            if exit_bid is not None and exit_bid >= price:
                price = exit_bid
            wanted[(token, SELL)] = (price, size)
        return wanted

    if pairs >= floor and abs(net) < FLAT:
        if ask_px:
            wanted[(yes, SELL)] = (ask_px, pairs)
        if bid_px:
            wanted[(no, SELL)] = (round(1 - bid_px, 4), pairs)
        return wanted
    if abs(net) >= FLAT:
        token, bid, avg = (no, no_bid, yes_avg) if net > 0 else (yes, yes_bid, no_avg)
        excess_token = yes if net > 0 else no
        if unmatched_basis is not None:
            avg = unmatched_basis.get(excess_token, avg)
        size = floor2(abs(net))
        if size < market["min_order"]:
            # Dust from a partial fill cannot be hedged on its own. Fold it into
            # the next opening BUY of the missing leg instead of freezing.
            if allow_open and yes_bid and no_bid and pairs < 0.01:
                opening = quote_plan.opening_orders(market, bid_px, ask_px, cash, market.get("order_size") or SIZE)
                key = (token, BUY)
                if key in opening:
                    price, base = opening[key]
                    extra = floor2(size)
                    if cash >= reserve(market, price, base + extra) + reserve(
                            market, opening[(excess_token, BUY)][0], base):
                        opening[key] = (price, floor2(base + extra))
                wanted.update(opening)
            return wanted
        max_pair = max(MAX_PAIR, 1 + repair / size)
        if loss_left is not None:
            max_pair = min(max_pair, 1 + max(0.0, loss_left) / size)
        cap = floor_tick(max_pair - avg, tick)
        if avg <= 0:
            log(f"no average price for the position yet; not bidding {name(market, token)}")
        elif not bid or cap < tick:
            log(f"no valid price to bid {name(market, token)} at; holding")
        else:
            price = min(bid, cap)
            size = floor2(min(size, cash / reserve(market, price, 1)))
            if loss_left is not None:
                while (price >= tick and size >= market["min_order"]
                       and (avg + price - 1) * size > loss_left + 1e-6):
                    price = round(price - tick, 4)
                    if price < tick:
                        break
                    size = floor2(min(floor2(abs(net)), cash / reserve(market, price, 1)))
            if price < bid and price >= tick:
                log(f"{name(market, token)} bid capped at {price}: pair cost limit {max_pair:.3f}")
            if price >= tick and size >= market["min_order"]:
                wanted[(token, BUY)] = (price, size)
            elif size < market["min_order"]:
                log(f"not enough cash to bid {name(market, token)} @ {price}; holding")
            else:
                log(f"no valid price to bid {name(market, token)} at; holding")
    elif allow_open and yes_bid and no_bid:
        wanted.update(quote_plan.opening_orders(market, bid_px, ask_px, cash, market.get("order_size") or SIZE))
    return wanted


class StateCorrupt(Exception):
    """The loss-limit file cannot be trusted. Callers must not replace it with zero."""


def _optional_float(value, label):
    if value in (None, ""):
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise StateCorrupt(f"{label} is damaged") from None
    if not math.isfinite(number):
        raise StateCorrupt(f"{label} is damaged")
    return number


def _read_json(path):
    """A dict, None when the file is absent, or False when it cannot be parsed."""
    try:
        with open(path) as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError, UnicodeError):
        return False
    if not isinstance(data, dict):
        return False
    return data


def read_state():
    """Last good state. A damaged file is not an empty loss history."""
    primary = _read_json(STATE_FILE)
    if isinstance(primary, dict):
        return primary
    backup = _read_json(STATE_FILE + ".bak")
    if isinstance(backup, dict):
        if primary is False:
            log("state file is damaged; using the last saved copy")
        return backup
    if primary is False or backup is False:
        raise StateCorrupt("state file is damaged")
    return {}


def _books(history):
    books = history.get("books")
    if isinstance(books, dict):
        return books
    if books not in (None,):
        raise StateCorrupt("books are damaged")
    if history.get("condition") and isinstance(history.get("lots"), dict):
        return {history["condition"]: {"lots": history["lots"], "known": True,
                                       "execution": history.get("execution") or {}}}
    return {}


def _restore_info(info):
    if not isinstance(info, dict) or not info.get("token") or not info.get("side"):
        raise StateCorrupt("order record is damaged")
    restored = dict(info)
    for key in ("matched", "price", "size"):
        restored[key] = _optional_float(info.get(key), key)
    seen = info.get("seen_trades") or []
    if not isinstance(seen, list):
        raise StateCorrupt("order record is damaged")
    if seen:
        restored["seen_trades"] = [str(item) for item in seen]
    return restored


def load_state(condition):
    """Global repair loss plus this market's lots, or None when nothing is saved yet.

    The spent total carries across markets. Lots are kept per condition, so a
    restart on a new market does not inherit another market's cost basis."""
    data = read_state()
    if not data:
        return None
    spent = _optional_float(data.get("spent"), "spent")
    if "books" in data:
        book = _books(data).get(condition)
        if not book:
            return {"spent": spent, "lots": {}, "known": False}
        return {"spent": spent, "lots": book.get("lots") or {}, "known": book.get("known", True)}
    if data.get("condition") != condition:
        return {"spent": spent, "lots": {}, "known": False}
    return {"spent": spent, "lots": data.get("lots") or {}, "known": True}


class Maker:
    def __init__(self, market, client):
        self.market, self.client = market, client
        self.mids = deque()
        self.paused_until = 0.0
        self.tracked = {}
        self.cancelled_ids = set()
        self.prev_sizes = None
        self.fill_base = None
        self.unsettled_since = None
        self.unresolved_orders = set()
        self.inventory_ready = False
        self.next_quote_at = 0.0
        self.data_at = None
        self.pulse_at = time.monotonic()
        self.hedge_active = False
        self.hedge_spent = 0.0
        self.pending_hedge = None
        self.share = 0.0
        self.share_pct = 0.0
        self.last_share_pct = None
        self.last_share_notice = 0.0
        self.live_share_pct = None
        self.observed_share_pct = None
        self.was_better = False
        self.exit_requested = False
        self.merge_job = None
        self.merge_failed_at = 0.0
        self.earned = None
        self.earned_at = 0.0
        self.last_earned_cents = None
        self.last_earned_notice = 0.0
        saved = load_state(market["condition"])
        self.spent = _optional_float(saved["spent"], "spent") if saved else 0.0
        self._spent_base = self.spent
        self.lots = {}
        self.lots_inited = bool(saved and saved["known"])
        if self.lots_inited:
            for token, lots in (saved.get("lots") or {}).items():
                self.lots[token] = [[float(size), float(price)] for size, price in lots]
        self.entry_rate = float(market.get("rate") or 0)
        self.entry_daily = float("inf")
        self.entered_at = time.time()
        self.misses = 0
        self.rewarding = True
        self.dead = None
        history = read_state()
        book = _books(history).get(market["condition"]) or {}
        self._restore_execution((book.get("execution") if isinstance(book, dict) else {}) or {})
        self.forecast_samples = list(history.get("forecast_samples") or [])[-FORECAST_HISTORY:]
        self.forecast_key = None
        self.forecast_since = self.forecast_at = 0.0
        self._restore_selection(book if isinstance(book, dict) else {}, history)
        _dashboard("market", self)

    def _restore_selection(self, book, history):
        """This market's confirmation. A second market must not inherit the first one's."""
        stored = book.get("selection") if isinstance(book.get("selection"), dict) else None
        if stored is None:
            top = history.get("selection") or {}
            if top.get("condition") == self.market["condition"] and top.get("slug") == self.market.get("slug"):
                stored = top
        if not isinstance(stored, dict):
            return
        self.live_share_pct = stored.get("live_share_pct")
        if self.live_share_pct is None and stored.get("live_daily") is not None and self.market["rate"] > 0:
            self.live_share_pct = stored["live_daily"] * 100 / self.market["rate"]
        estimate = stored.get("estimate_daily")
        self.entry_daily = estimate if estimate is not None else float("inf")
        self.entry_rate = stored.get("entry_rate", self.entry_rate)
        self.was_better = bool(stored.get("was_better"))
        self.misses = int(stored.get("misses") or 0)

    def _merged_spent(self, disk):
        """Global loss plus the fills this market has not written yet."""
        base = getattr(self, "_spent_base", self.spent)
        local = self.spent - base
        if local < 0:
            local = 0.0
        return float(disk) + local

    def _adopt_spent(self):
        """Pick up loss recorded by another market before this one decides to quote."""
        try:
            disk = _optional_float(read_state().get("spent"), "spent")
        except StateCorrupt:
            return
        self.spent = self._merged_spent(disk)
        self._spent_base = float(disk)

    def _restore_execution(self, execution):
        """Reload hedge progress and order baselines. Matched size is the part already charged."""
        execution = execution if isinstance(execution, dict) else {}
        self.hedge_active = bool(execution.get("active"))
        self.hedge_spent = _optional_float(execution.get("spent"), "hedge spent")
        started_at = execution.get("started_at")
        self.hedge_started_at = float(started_at) if isinstance(started_at, (int, float)) else None
        self.emergency = bool(execution.get("emergency"))
        raw_tracked = execution.get("tracked") or {}
        if not isinstance(raw_tracked, dict):
            raise StateCorrupt("tracked orders are damaged")
        tracked = {}
        for oid, info in raw_tracked.items():
            tracked[str(oid)] = _restore_info(info)
        pending = execution.get("pending")
        self.pending_hedge = None
        if pending:
            if not isinstance(pending, dict) or not isinstance(pending.get("info"), dict):
                raise StateCorrupt("pending order is damaged")
            info = _restore_info(pending["info"])
            oid = str(pending["id"]) if pending.get("id") else None
            if oid:
                prior = tracked.get(oid)
                if prior is not None:
                    info["matched"] = max(info["matched"], prior["matched"])
                tracked[oid] = info
            self.pending_hedge = {"id": oid, "info": info, "at": pending.get("at")}
        self.tracked = tracked
        raw_unresolved = execution.get("unresolved") or []
        if not isinstance(raw_unresolved, list):
            raise StateCorrupt("unresolved orders are damaged")
        self.unresolved_orders = {str(oid) for oid in raw_unresolved if oid}

    def inventory_net(self):
        return self._total(self.market["yes"]) - self._total(self.market["no"])

    def risk_busy(self):
        return bool(self.pending_hedge or self.unresolved_orders
                    or (self.inventory_ready and abs(self.inventory_net()) >= FLAT
                        and not self.dust_written_off()))

    def dust_written_off(self):
        """An excess below the exchange minimum left after the repair window expired.

        No order can trade it (hedge plans need min_order), so treating it as risk
        would block quoting and switching forever. It stays written off only while
        the excess does not grow and stays below the minimum in the same market."""
        mark = getattr(self, "dust_writeoff", None)
        if not mark or mark[0] != self.market["condition"]:
            return False
        net = abs(self.inventory_net())
        return net < self.market["min_order"] and net <= mark[1] + 1e-6

    def _order_snapshot(self):
        return (
            tuple(sorted(self.tracked)),
            tuple(sorted(self.unresolved_orders)),
            tuple(sorted((oid, float(info.get("matched") or 0)) for oid, info in self.tracked.items())),
        )

    def observe_orders(self, live):
        """Read fills before replacing baselines, and keep that result across a restart."""
        _dashboard("orders", self, live)
        if not isinstance(live, (list, tuple)):
            live = []
        self.reconcile_uncertain(live)
        ours = [o for o in live if isinstance(o, dict) and o.get("market") == self.market["condition"]]
        before = self._order_snapshot()
        filled = self.find_fills(ours)
        if filled:
            self.fill_base = self.prev_sizes
            self.unsettled_since = time.time()
            _dashboard("positions", self)
        self.track(ours)
        if self._order_snapshot() != before:
            self.save_state()
        return ours, filled

    def pulse(self):
        """Check fills often; retain the slower cadence for routine public reads."""
        self._adopt_spent()
        started = time.monotonic()
        if started - self.pulse_at >= LOOP_STALE_AFTER:
            self.data_at = None
        self.pulse_at = started
        try:
            live = self.client.get_open_orders() if self.client else []
            ours, filled = self.observe_orders(live)
            if time.monotonic() - started >= LOOP_STALE_AFTER:
                self.pull_quotes("order snapshot timed out", ours)
                return
            if self.pending_hedge or self.unresolved_orders or (self.inventory_ready and self.risk_busy()):
                self.data_at = None
                if filled or self.pending_hedge or self.unresolved_orders:
                    self._resting_snapshot = None
                if self.repair_merge_step(ours):
                    return
                self.hedge(live, ours, started=started)
                if not self.data_fresh():
                    self._resting_snapshot = None
                    self.next_quote_at = 0.0
                return
            if ours and self.fast_move():
                return
            quote_due = filled or time.time() >= self.next_quote_at or not self.data_fresh()
            if ours and not quote_due and self.resting_pair_gone(ours, live):
                self.pull_quotes("pair left the cap", ours)
                return
            if quote_due:
                self.next_quote_at = time.time() + POLL
                self.tick(live=live, observed=True, started=started)
        except Exception:
            # A failed quote tick must be retried in full, not hidden by fast polls.
            self.pull_quotes("pulse failed")
            raise

    def fast_move(self):
        """Every pulse (~1s, or at once on a WS mid move): pull if the live mid left the quoted mid."""
        quoted = getattr(self, "quoted_mid", None)
        if quoted is None or FAST_PULL_MOVE <= 0:
            return False
        mid = live_mid(self.market["yes"])
        if mid is None or abs(mid - quoted) < FAST_PULL_MOVE - 1e-9:
            return False
        now = time.time()
        self.paused_until = max(self.paused_until, now + JUMP_PAUSE)
        self.quoted_mid = None
        log(f"live mid {mid:.3f} moved {abs(mid - quoted) * 100:.1f}c from quoted {quoted:.3f}; "
            f"pulling orders for {JUMP_PAUSE:g}s")
        self.pull_quotes("fast mid move")
        return True

    def data_fresh(self):
        at = self.data_at
        return at is not None and time.monotonic() - at < DATA_STALE_AFTER

    def pull_quotes(self, reason, ours=None):
        """Invalidate data before I/O; preserve fill baselines even if cancel fails."""
        _dashboard("blocked", self, reason)
        self.data_at = None
        self.next_quote_at = 0.0
        log(f"pulling quotes: {reason}")
        try:
            if ours is None:
                self.cancelled_ids.update(cancel_all(self.client, self.market))
                refused = []
            elif ours:
                self.track(ours)
                refused = self.cancel(ours)
            else:
                refused = []
        except Exception as exc:
            log(f"quote cancellation failed: {short_err(exc)}; heartbeat remains suspended")
            return
        if not refused and reason in DELIBERATE_PULLS:
            # A deliberate wait (settling, hedging, merging) with nothing left resting is
            # not stale data; suspending the heartbeat here only cancels repair bids.
            self.data_at = time.monotonic()

    def tick(self, allow_open=True, live=None, observed=False, started=None):
        started = time.monotonic() if started is None else started
        m, now = self.market, time.time()
        if live is None:
            live = self.client.get_open_orders() if self.client else []
        ours = [o for o in live if o.get("market") == m["condition"]]
        if not observed:
            ours, _ = self.observe_orders(live)
        if self.risk_busy():
            self.data_at = None
            if self.repair_merge_step(ours):
                return
            self.hedge(live, ours, started=started)
            return
        reported = reported_positions(m)
        sizes = self.balances(reported)
        settled = self.settled(now, sizes, reported)
        self.prev_sizes = sizes
        if not settled:
            self.pull_quotes("position has not settled", ours)
            return

        if any(not position_ready(reported[t][0], reported[t][1], sizes[t]) for t in sizes):
            log("waiting for a consistent position and cost basis; new orders disabled")
            self.pull_quotes("position or cost basis unavailable", ours)
            return
        # Untracked sub-share dust (fee remainders, e.g. No 0.084 on rzp-zehut 29.10) is not an
        # unconfirmed hedge fill; a 0.01 tolerance blocked that market for good.
        if self.lots_inited and self.hedge_active and any(
                abs(self._total(t) - sizes[t]) >= HEDGE_BAL_TOL for t in sizes):
            log("waiting for balances to reflect confirmed hedge fills; new orders disabled")
            self.pull_quotes("hedge balances not confirmed", ours)
            if now - self.entered_at >= SETTLE:
                decision_note((m["condition"], "hedge-balance-mismatch"), tg(
                    "⚠️ Нужна сверка позиции", qmarket(m["question"]), "",
                    "Баланс расходится с подтверждёнными исполнениями. Новые котировки остановлены.",
                ))
            return

        held = {t: (sizes[t], reported[t][1]) for t in sizes}
        self.sync_lots(held)
        self.inventory_ready = True
        _dashboard("positions", self, sizes)
        if self.risk_busy():
            self.data_at = None
            if self.repair_merge_step(ours):
                return
            self.hedge(live, ours, started=started)
            return
        if self.hedge_active:
            self.hedge_active, self.hedge_spent = False, 0.0
            self.hedge_started_at, self.emergency = None, False
            self.save_state()
            log("hedge complete | inventory balanced; reward SELL quotes may resume")
        if self.merge_step(sizes, ours):
            return
        book_at = time.monotonic()
        # Live book when the feed is healthy; a REST read at least every WS_REST_CHECK s keeps it honest.
        rest_due = now - getattr(self, "rest_book_at", 0.0) >= WS_REST_CHECK
        book = ws_feed.FEED.book(m["yes"]) if ws_feed.FEED is not None and not rest_due else None
        from_ws = book is not None and bool(book["bids"]) and bool(book["asks"])
        if not from_ws:
            self.rest_book_at = now
            book = get(f"{CLOB}/book?token_id={m['yes']}")
        bids, asks = others_book(book, ours, m)
        best_bid, best_ask = book_top(book)
        if not book_fresh(book, book_at, m["yes"]):
            self.pull_quotes("order book snapshot is stale", ours)
            return
        if from_ws:
            # /midpoint is the full book's top-of-book mid; the live book gives the same number.
            mid = round((best_bid + best_ask) / 2, 6) if best_bid is not None and best_ask is not None else None
            if mid is None or not 0 < mid < 1:
                mid = exchange_mid(m["yes"], midpoint(bids, asks, best_bid, best_ask, m["min_size"]))
        else:
            mid = exchange_mid(m["yes"], midpoint(bids, asks, best_bid, best_ask, m["min_size"]))
        self.book_source = "ws" if from_ws else "rest"
        _dashboard("mid", self, mid)
        if not bids or not asks or mid is None:
            self.pull_quotes("book is empty on one side", ours)
            return
        self.check_vol1h(now)
        if self.jumped(now, mid) or now < self.paused_until:
            self.quoted_mid = None
            if ours:
                self.cancel(ours)
            self.track(ours)
            return
        self.quoted_mid = mid
        cash = self.free_cash(live)
        self.stats_losses()
        share = self.reward_share(ours)
        self.maybe_share(share, m["rate"])
        self.maybe_earnings()
        self.maybe_forecast(bids, asks, mid, ours, held, now)
        repair = min(REPAIR_MAX, m["rate"] * self.share * REPAIR_HOURS / 24)
        remaining = max(0.0, LOSS_LIMIT - self.spent)
        if remaining <= 1e-9:
            self.reconcile(ours, {})
            decision_note((m["condition"], "loss-limit"), tg(
                "🛑 Лимит расходов выбран", qmarket(m["question"]), "",
                "Новые котировки остановлены. Закрытие оставшегося перекоса допустимо только без дополнительного убытка.",
            ))
            return
        bid_px = pick(bids, mid, "bid", m, best_ask if best_ask is not None else 1.0) if mid is not None else None
        ask_px = pick(asks, mid, "ask", m, best_bid if best_bid is not None else 0.0) if mid is not None else None
        wanted = wanted_orders(m, bid_px, ask_px, held, cash, repair, remaining,
                               allow_open and self.rewarding and not self.exit_requested
                               and only_dust(sizes, m["min_order"]))
        blocked = self._quote_block_reason(wanted, bids, asks, mid, cash, live)
        self.note_hedge_block(blocked, sizes, now)
        _dashboard("blocked", self, blocked)
        if blocked:
            log(f"reward quotes blocked | {blocked}")
            decision_note((m["condition"], "quote-risk"), tg(
                "⏸ Котировки приостановлены", qmarket(m["question"]), "",
                "Свежий стакан или оставшийся лимит не позволяют закрыть исполнение целиком.",
                "Новые покупки и продажи пары ждут допустимых условий.",
            ))
            wanted = {}
        (ys, ya), (ns, na) = held[m["yes"]], held[m["no"]]
        earned = f" | today ${self.earned:.2f}" if self.earned is not None else ""
        mid_text = f"{mid:.3f}" if mid is not None else "unknown"
        log(f"mid {mid_text} | Yes {ys:g}@{ya:.3f} No {ns:g}@{na:.3f} | cash {cash:.2f} | "
            f"share {share:.1f}%{earned} | loss {self.spent:.2f}/{LOSS_LIMIT:.0f} | want "
            + (", ".join(f"{sd} {name(m, t)} {s:g}@{p}" for (t, sd), (p, s) in wanted.items()) or "nothing"))
        if wanted and not book_fresh(book, book_at, m["yes"]):
            # Checked again right before posting: a slow tick must not quote an old book.
            self.pull_quotes("order book snapshot is stale", ours)
            return
        if self.reconcile(ours, wanted, snapshot_at=started):
            # Publish only a completed decision, dated from its first request.
            self.data_at = started
        else:
            self.data_at = None
            self.next_quote_at = 0.0

    def repair_only(self):
        """Imbalanced, but no hedge order in flight: only a resting repair bid works."""
        return bool(self.risk_busy() and not self.pending_hedge and not self.unresolved_orders)

    def repair_merge_step(self, ours):
        """During a repair, merge the balanced min(Yes, No) pairs and keep the excess.

        The excess and its repair bid stay as they are (a BUY of the missing leg locks
        cash, not tokens). Merged pairs consume the oldest lots, so the excess keeps
        the newest lots that the repair basis is priced from. True while a merge owns
        this tick."""
        m = self.market
        if self.merge_job is None:
            if not self.repair_only() or not self.inventory_ready:
                return False
            if merge.merge_amount(self._total(m["yes"]), self._total(m["no"])) is None:
                return False
            if time.time() - self.merge_failed_at < 600 or not MERGER.available():
                return False
            sizes = self.balances(reported_positions(m))
            if any(abs(self._total(t) - sizes[t]) >= HEDGE_BAL_TOL for t in sizes):
                return False
            amount = merge.merge_amount(min(sizes[m["yes"]], self._total(m["yes"])),
                                        min(sizes[m["no"]], self._total(m["no"])))
            if amount is None:
                return False
            return self.merge_step(sizes, ours, partial=True, amount=amount)
        if not self.merge_job.get("partial"):
            return False
        sizes = self.balances(reported_positions(m))
        return self.merge_step(sizes, ours, partial=True)

    def merge_step(self, sizes, ours, partial=False, amount=None):
        """Free capital held in balanced pairs. True while a merge owns this tick.

        Merges Yes+No pairs to collateral through the Deposit Wallet relayer. When
        merge is unavailable, sells both legs at taker prices, but only when the two
        bids return at least 1 - PAIR_SELL_TOLERANCE per pair; otherwise the old
        post-only SELL quotes stay in charge.
        """
        m = self.market
        job = self.merge_job
        if job is not None:
            elapsed = time.time() - job["started"]
            if job["thread"].is_alive() and elapsed < merge.MERGE_WAIT_SECONDS:
                if not job.get("partial"):
                    self.pull_quotes("merge in progress", ours)
                return True
            self.merge_job = None
            result = dict(job["result"])
            if job["thread"].is_alive():
                result = {"ok": False, "error": f"no confirmation after {elapsed:.0f}s "
                                                f"(relayer tx {job['progress'].get('transaction_id')})"}
            if not result.get("ok") and not result.get("dry_run") \
                    and merged_by_balances(job["before"], sizes, job["shares"], m):
                # The relayer executed it even though the SDK wait failed or hung.
                result = {"ok": True, "tx": job["progress"].get("transaction_id") or "reconciled from balances"}
            self._merge_finished(job["shares"], result)
            return True
        yes_sz, no_sz = sizes[m["yes"]], sizes[m["no"]]
        if not partial:
            if abs(yes_sz - no_sz) >= FLAT:
                return False
            amount = merge.merge_amount(yes_sz, no_sz)
        if amount is None or time.time() - self.merge_failed_at < 600:
            return False
        if not MERGER.available():
            return False if partial else self._pair_sell_fallback(amount, ours)
        if ours and not partial:
            # Resting SELLs lock the tokens; clear them first, merge next tick.
            self.cancel(ours)
            self.track(ours)
            return True
        result, progress = {}, {}
        dry = DRY_RUN or not self.client
        neg_risk = bool(m.get("neg_risk"))

        def work():
            try:
                result.update(MERGER.merge(m["condition"], amount, dry_run=dry, neg_risk=neg_risk,
                                           progress=progress))
            except Exception as exc:
                result.update(ok=False, error=short_err(exc))

        log(f"{'[dry] ' if dry else ''}merging {amount:g} Yes+No pairs back to collateral"
            + (" (repair: excess and its bid kept)" if partial else ""))
        thread = threading.Thread(target=work, daemon=True)
        self.merge_job = {"thread": thread, "result": result, "shares": amount, "progress": progress,
                          "started": time.time(), "before": dict(sizes), "partial": partial}
        thread.start()
        self.data_at = None
        return True

    def _merge_finished(self, amount, result):
        m = self.market
        if not result.get("ok"):
            self.merge_failed_at = time.time()
            log(f"merge failed: {result.get('error') or 'no transaction hash'}")
            note(tg("⚠️ Merge не прошёл", qmarket(m["question"]), "",
                    "Пары остаются на руках, работают SELL-котировки. Повтор через 10 минут."))
            return
        dry = bool(result.get("dry_run"))
        if not dry:
            self._consume_oldest(m["yes"], amount)
            self._consume_oldest(m["no"], amount)
            self.save_state()
            refresh_balances(self.client, (m["yes"], m["no"]))
            # Balances are re-read before quoting; opening BUYs follow once flat.
            self.unsettled_since = time.time()
        log(f"{'[dry] ' if dry else ''}MERGED {amount:g} pairs -> ${amount:.2f} | tx {result.get('tx')}")
        record_event({"kind": "merge", "condition": m["condition"], "shares": tape_num(amount),
                      "tx": result.get("tx"), "dry_run": dry})
        if dry:
            # Paper mode cannot change balances; do not merge the same pairs every tick.
            self.merge_failed_at = time.time()

    def _pair_sell_fallback(self, amount, ours):
        """Sell both legs with FAK only when the pair returns >= 1 - tolerance."""
        m = self.market
        book = get(f"{CLOB}/book?token_id={m['yes']}")
        best_bid, best_ask = book_top(book)
        yes_bid = best_bid
        no_bid = round(1 - best_ask, 4) if best_ask is not None else None
        size = floor2(amount)
        if size < m["min_order"] or not merge.pair_sell_ok(yes_bid, no_bid):
            return False
        if ours:
            self.cancel(ours)
            self.track(ours)
            return True
        log(f"{'[dry] ' if DRY_RUN else ''}merge unavailable ({MERGER.reason}); selling {size:g} pairs "
            f"at Yes {yes_bid} + No {no_bid} = {yes_bid + no_bid:.3f}")
        if DRY_RUN or not self.client:
            return True
        for token, price in ((m["yes"], yes_bid), (m["no"], no_bid)):
            info = {"label": f"SELL {name(m, token)} {size:g} @ {price} | pair exit FAK", "matched": 0.0,
                    "token": token, "side": SELL, "price": price, "size": size, "hedge": True}
            try:
                response = self.client.create_and_post_order(
                    OrderArgsV2(token_id=token, price=price, size=size, side=SELL),
                    order_type=OrderType.FAK, post_only=False)
            except Exception as exc:
                # A one-sided fill becomes an imbalance; the hedge path repairs it.
                log(f"pair exit sell failed: {short_err(exc)}")
                break
            oid = (response or {}).get("orderID")
            if oid:
                self.tracked[oid] = info
                self._record_posted(oid, info)
        self.data_at = None
        return True

    def note_hedge_block(self, blocked, sizes, now):
        """Blacklist a flat market whose quotes stay blocked for lack of hedge depth."""
        if not blocked or not blocked.startswith("cannot hedge") or not only_dust(sizes, self.market["min_order"]):
            self.hedge_blocked_since = None
            return False
        since = getattr(self, "hedge_blocked_since", None)
        if since is None:
            self.hedge_blocked_since = now
            return False
        if now - since < HEDGE_BLOCK_SECONDS:
            return False
        cid = self.market["condition"]
        ban(cid, now + BLACKLIST_SECONDS, "cannot hedge a full fill")
        self.hedge_blocked_since = None
        self.rules_at = 0
        log(f"blacklisting {self.market.get('slug')} for {BLACKLIST_SECONDS / 60:.0f} min: "
            f"quotes blocked {now - since:.0f}s ({blocked})")
        return True

    def _quote_block_reason(self, wanted, bids, asks, mid, cash, live):
        """Do not create an imbalance without room to hedge the whole quoted size.

        A scan is only a candidate search. Its liquidity checks must also hold
        on the quote snapshot. The episode allowance is reserved before either
        an opening BUY or a paired SELL can fill; a few unused loss-limit cents
        are not permission to start another episode.
        """
        if not wanted:
            return None
        m = self.market
        elsewhere = {o.get("market") for o in live
                     if o.get("market") and o["market"] != m["condition"] and remaining(o) > 0}
        loss_left = max(0.0, LOSS_LIMIT - self.spent)
        size = max(amount for _price, amount in wanted.values())
        required = pair_reserve(size, 1 + len(elsewhere))
        if loss_left + 1e-6 < required:
            return f"hedge reserve ${required:.2f} exceeds remaining loss allowance ${loss_left:.4f}"
        spread = asks[0][0] - bids[0][0]
        if spread <= 0 or spread > rewards_scan.MAX_SPREAD + 1e-9 or spread / 2 >= m["band"]:
            return f"fresh spread {spread:.4f} is outside the entry limit"
        if mid is None or not rewards_scan.MIN_MID <= mid <= rewards_scan.MAX_MID:
            return "fresh midpoint is outside the entry limit"
        size = max(amount for _price, amount in wanted.values())
        if min(bids[0][1], asks[0][1]) + 1e-9 < size:
            return "fresh touch depth is smaller than the quote"
        _, bid_depth = rewards_scan.band_stats(bids, mid, m["band"], m["min_size"], True)
        _, ask_depth = rewards_scan.band_stats(asks, mid, m["band"], m["min_size"], False)
        if min(bid_depth, ask_depth) + 1e-9 < rewards_scan.DEPTH_MULT * size:
            return "fresh reward-band depth is too small"
        allowance = max(0.0, loss_left - pair_reserve(size, len(elsewhere)))
        for (token, side), (price, amount) in wanted.items():
            plan = self._pair_plan(token, side, price, amount, bids, asks, cash, allowance)
            if plan is None or amount - plan["size"] >= FLAT:
                return f"cannot hedge a full {name(m, token)} {side} fill of {amount:g} shares"
        return None

    def _pair_plan(self, token, side, price, amount, bids, asks, cash, allowance):
        """Buy of the other outcome that covers this fill inside the pair cap."""
        m = self.market
        # Selling one half of a pair leaves the other half at $1 minus proceeds.
        # Buying while flat creates an excess at the paid price.
        exposed = token if side == BUY else (m["no"] if token == m["yes"] else m["yes"])
        basis = price if side == BUY else 1 - price
        after_cash = cash - reserve(m, price, amount) if side == BUY else cash + price * amount
        return hedge_plan.plan(
            m, exposed, amount, basis, bids, asks, after_cash, allowance,
            pair_only=True, max_unit_loss=max(0.0, MAX_PAIR - 1))

    def resting_pair_gone(self, ours, live):
        """True when a resting quote can no longer be hedged inside the pair cap.

        A failed book read keeps the quotes. An empty book or a pair above the
        cap cancels them on this poll instead of waiting for the next reprice.
        """
        m = self.market
        sized = [o for o in ours if remaining(o) + 1e-9 >= m["min_order"]]
        if not sized:
            return False
        elsewhere = {o.get("market") for o in live
                     if o.get("market") and o["market"] != m["condition"] and remaining(o) > 0}
        loss_left = max(0.0, LOSS_LIMIT - self.spent)
        size = max(remaining(order) for order in sized)
        if loss_left + 1e-6 < pair_reserve(size, 1 + len(elsewhere)):
            return True
        try:
            book = fresh_book(m["yes"])
        except Exception as exc:
            log(f"could not recheck the pair: {short_err(exc)}")
            return False
        bids, asks = others_book(book, ours, m)
        if not bids or not asks:
            return True
        cash = self.free_cash(live)
        allowance = max(0.0, loss_left - pair_reserve(size, len(elsewhere)))
        for order in sized:
            amount = remaining(order)
            plan = self._pair_plan(
                order.get("asset_id"), order.get("side"), float(order["price"]), amount,
                bids, asks, cash, allowance)
            if plan is None or amount - plan["size"] >= FLAT:
                return True
        return False

    def _held_from_lots(self):
        def average(token):
            total = self._total(token)
            if total <= 1e-9:
                return 0.0
            return sum(size * price for size, price in self.lots.get(token, [])) / total

        return {token: (self._total(token), average(token))
                for token in (self.market["yes"], self.market["no"])}

    def _latch_emergency(self):
        """Repair window over: ban the market 24h, request exit, plan only emergency exits."""
        m = self.market
        self.emergency = True
        self.exit_requested = True
        self.rules_at = 0
        ban(m["condition"], time.time() + EMERGENCY_BLACKLIST_SECONDS, "repair timeout")
        log(f"repair window {REPAIR_MAX_SECONDS:g}s expired | emergency exit mode (cap ${EMERGENCY_EXIT_USD:g}/episode) | "
            f"blacklisting {m.get('slug')} for {EMERGENCY_BLACKLIST_SECONDS / 3600:g}h")
        self.save_state()

    def _emergency_exit(self, token, size, basis, bids, asks, cash):
        """Cheapest buy-pair or sell within what is left of EMERGENCY_EXIT_USD this episode."""
        allowance = max(0.0, EMERGENCY_EXIT_USD - self.hedge_spent)
        plan = hedge_plan.plan(self.market, token, size, basis, bids, asks, cash, allowance)
        if plan is not None:
            log(f"emergency {plan['side']} exit | loss bound ${plan['loss_bound']:.2f} | "
                f"episode ${self.hedge_spent:.2f}/{EMERGENCY_EXIT_USD:g}")
        return plan

    def check_vol1h(self, now):
        """Every VOL1H_CHECK_SECONDS: leave (1h ban) an active market whose 1h range exceeds VOL1H_MAX."""
        m = self.market
        result = getattr(self, "_vol1h_result", None)
        if result is not None and result[0] == m["yes"]:
            self._vol1h_result = None
            vol = result[1]
        else:
            vol = None
        if now - getattr(self, "vol1h_at", 0.0) >= VOL1H_CHECK_SECONDS:
            # Background read: never stall fill detection or hedging on a scanner request.
            self.vol1h_at = now
            token = m["yes"]

            def fetch():
                try:
                    self._vol1h_result = (token, rewards_scan.volatility1h(token, max_age=VOL1H_CHECK_SECONDS))
                except Exception:
                    pass
            threading.Thread(target=fetch, daemon=True).start()
        if vol is None or vol <= rewards_scan.VOL1H_MAX + 1e-9:
            return False
        ban(m["condition"], now + BLACKLIST_SECONDS, f"1h move {vol * 100:.1f}c")
        self.paused_until = max(self.paused_until, now + BLACKLIST_SECONDS)
        self.rules_at = 0
        log(f"1h range {vol * 100:.1f}c > {rewards_scan.VOL1H_MAX * 100:g}c | pulling and blacklisting "
            f"{m.get('slug')} for {BLACKLIST_SECONDS / 60:.0f} min")
        return True

    def _rest_open_leg(self, live, ours, bids, asks, started):
        """Rest a capped missing-leg bid while a marketable hedge is unavailable."""
        m = self.market
        snapshot = getattr(self, "_resting_snapshot", None)
        self._resting_snapshot = None
        if not bids or not asks:
            return False
        now = time.time()
        if (ours and now < self.next_quote_at and snapshot
                and time.monotonic() - snapshot[0] < min(DATA_STALE_AFTER, LOOP_STALE_AFTER)
                and abs(self.spent - snapshot[2]) < 1e-9
                and frozenset(o["id"] for o in ours) == snapshot[1]):
            # A fast fill poll does not renew the last full quote decision.
            self.data_at = snapshot[0]
            self._resting_snapshot = snapshot
            return True
        mid = live_mid(m["yes"]) or exchange_mid(
            m["yes"], midpoint(bids, asks, bids[0][0], asks[0][0], m["min_size"]))
        if mid is None:
            return False
        held = self._held_from_lots()
        signed_net = self.inventory_net()
        net = abs(signed_net)
        excess = m["yes"] if signed_net > 0 else m["no"]
        need, basis = net, 0.0
        for size, price in reversed(self.lots.get(excess, [])):
            if need <= 1e-9:
                break
            basis = max(basis, price)
            need -= min(need, size)
        remaining = max(0.0, LOSS_LIMIT - self.spent)
        loss_left = min(remaining, max(0.0, MAX_PAIR - 1) * max(net, 1.0))
        bid_px = pick(bids, mid, "bid", m, asks[0][0])
        ask_px = pick(asks, mid, "ask", m, bids[0][0])
        wanted = wanted_orders(m, bid_px, ask_px, held, self.free_cash(live), 0, loss_left,
                               allow_open=False, unmatched_basis={excess: basis})
        sell_px = ceil_tick(basis, m["tick"])
        reward = ask_px if excess == m["yes"] else (round(1 - bid_px, 4) if bid_px else None)
        if reward and reward > sell_px:
            sell_px = reward
        touch = bids[0][0] if excess == m["yes"] else round(1 - asks[0][0], 4)
        # A bid below entry is not an exit. The sale rests at entry or better.
        # Never rest a SELL of the excess next to a BUY of the missing leg: both can
        # fill and flip the exposure (28.10 14:37 UTC, Microsoft market).
        if (not wanted and m["tick"] <= sell_px <= 1 - m["tick"] and floor2(net) >= m["min_order"]
                and sell_px > touch + 1e-9):
            wanted[(excess, SELL)] = (sell_px, floor2(net))
        if not wanted:
            return False
        if not self.reconcile(ours, wanted, snapshot_at=started):
            self.next_quote_at = 0.0
            return True
        self.data_at = started
        self._resting_snapshot = (started, frozenset(self.tracked), self.spent)
        self.next_quote_at = now + POLL
        (ys, ya), (ns, na) = held[m["yes"]], held[m["no"]]
        repair_status = "repair bid planned" if DRY_RUN else "repair bid confirmed"
        log(f"{'[dry] ' if DRY_RUN else ''}mid {mid:.3f} | Yes {ys:g}@{ya:.3f} No {ns:g}@{na:.3f} | "
            f"hedge unresolved; {repair_status} | "
            + ", ".join(f"{sd} {name(m, t)} {s:g}@{p}" for (t, sd), (p, s) in wanted.items()))
        decision_note((m["condition"], "hedge-rest"), tg(
            "⚠️ Хедж пока не закрыт", qmarket(m["question"]),
            f"Yes <b>{ys:g}</b> · No <b>{ns:g}</b>.", "",
            f"Лимит пары <b>${MAX_PAIR:.2f}</b>; рыночное закрытие сейчас не проходит ограничения.",
            "Планирую лимитку на недостающую сторону." if DRY_RUN else
            "Биржа подтвердила лимитку на недостающую сторону. Жду исполнения; перекос остаётся.",
        ))
        return True

    def hedge(self, live, ours, started=None):
        """One risk-reducing FAK at a time, after conflicting orders are confirmed gone."""
        started = time.monotonic() if started is None else started
        m = self.market
        if self.pending_hedge or self.unresolved_orders:
            if ours:
                self.cancel(ours)
            if self.pending_hedge and not self.pending_hedge.get("id"):
                decision_note((m["condition"], "hedge-unknown"), tg(
                    "⚠️ Результат заявки неизвестен", qmarket(m["question"]), "",
                    "Биржа не подтвердила результат отправки. Новые заявки остановлены до сверки.",
                ))
            return
        if not self.inventory_ready:
            return
        net = self.inventory_net()
        if abs(net) < FLAT:
            return
        if not self.hedge_active:
            self.hedge_active, self.hedge_spent = True, 0.0
            self.hedge_started_at = time.time()
            self.save_state()
        if getattr(self, "hedge_started_at", None) is None:
            self.hedge_started_at = time.time()
        if not getattr(self, "emergency", False) and repair_expired(self.hedge_started_at):
            self._latch_emergency()
        token = m["yes"] if net > 0 else m["no"]
        size = floor2(abs(net))
        if size < m["min_order"] and repair_expired(self.hedge_started_at):
            # Untradable remainder (e.g. fee shares left after an exit): stop the hedge loop.
            if ours:
                self.cancel(ours)
                self.track(ours)
            self.dust_writeoff = (m["condition"], abs(net))
            log(f"hedge dust | {name(m, token)} {abs(net):g} below exchange minimum {m['min_order']:g} "
                f"after repair window | written off")
            return
        need, basis = size, 0.0
        for lot_size, lot_price in reversed(self.lots.get(token, [])):
            if need <= 1e-9:
                break
            basis = max(basis, lot_price)
            need -= min(need, lot_size)
        book = fresh_book(m["yes"])
        # A successful read during a hedge is fresh trading data: keep the heartbeat
        # alive so the exchange does not cancel repair bids mid-hedge.
        self.data_at = started
        bids, asks = others_book(book, ours, m)
        cash = self.free_cash(live)
        pair_cap = max(0.0, MAX_PAIR - 1)
        if getattr(self, "emergency", False):
            # Repair window expired: only the cheapest route within the remaining emergency allowance.
            plan = self._emergency_exit(token, size, basis, bids, asks, cash)
        elif self.exit_requested:
            # Leaving for a better market: complete the pair (then merge) if it fits
            # MAX_PAIR_COST, otherwise sell the excess with at most MAX_EXIT_LOSS_USD.
            # Pairing first only helps when the pair can be merged right after.
            plan = hedge_plan.plan(
                m, token, size, basis, bids, asks, cash, pair_cap * size + 1,
                pair_only=True, max_unit_loss=pair_cap) if MERGER.available() else None
            if plan is None:
                exit_room = max(0.0, MAX_EXIT_LOSS - self.hedge_spent)
                plan = hedge_plan.plan(
                    m, token, size, basis, bids, asks, cash, exit_room, sell_only=True)
        else:
            # Immediate hedge: buy the missing leg only if the pair stays within
            # MAX_PAIR_COST AND the whole hedge loses at most MAX_EXIT_LOSS_USD.
            # Per fill the pair cap binds (24 x 2c = $0.48 <= $0.50); the old OR-branch
            # that let a pair cost up to 1 + MAX_EXIT_LOSS/size is gone.
            room = min(pair_cap * size, max(pair_cap * size, MAX_EXIT_LOSS - self.hedge_spent))
            plan = hedge_plan.plan(
                m, token, size, basis, bids, asks, cash, room,
                pair_only=True, max_unit_loss=pair_cap)
        if time.monotonic() - started >= LOOP_STALE_AFTER:
            self.pull_quotes("hedge snapshot timed out", ours)
            return
        if plan is None:
            # Emergency mode with no exit inside EMERGENCY_EXIT_USD: hold and keep resting the repair bid.
            if ((not self.exit_requested or getattr(self, "emergency", False))
                    and self._rest_open_leg(live, ours, bids, asks, started)):
                return
            if ours:
                # Another market may keep the account heartbeat alive. Do not
                # rely on its expiry to clear this market's inadmissible quotes.
                self.cancel(ours)
                self.track(ours)
            log(f"hedge blocked | {name(m, token)} excess {size:g} | no pair inside ${MAX_PAIR:.2f} and no bid")
            decision_note((m["condition"], "hedge-blocked"), tg(
                "⚠️ Перекос пока не закрыт", qmarket(m["question"]),
                f"На руках лишние <b>{size:g} {name(m, token)}</b>.", "",
                "В стакане нет ни покупки другой стороны внутри лимита пары, ни ставки, по которой можно продать лишнее.",
                "Заявку вдали от рынка не ставлю. Новые котировки остановлены, пока позиция не закроется.",
            ))
            return
        if ours:
            refused = self.cancel(ours)
            self.track(ours)
            if refused or DRY_RUN or not self.client or getattr(self, "_hedge_retry", False):
                # Next pulse verifies final fills, including executions racing this cancel.
                return
            # Cancels confirmed: read their final fills now and hedge in this same tick
            # instead of waiting a pulse while the price runs away.
            self.find_fills([])
            if self.pending_hedge or self.unresolved_orders:
                return
            ids = {o.get("id") for o in ours}
            self._hedge_retry = True
            try:
                return self.hedge([o for o in live if o.get("id") not in ids], [], started)
            finally:
                self._hedge_retry = False
        asset, side, price, amount = (plan[k] for k in ("token", "side", "price", "size"))
        label = f"{side} {name(m, asset)} {amount:g} @ {price} | immediate hedge FAK"
        log(f"{'[dry] ' if DRY_RUN else ''}{label} | loss bound ${plan['loss_bound']:.2f}")
        if DRY_RUN or not self.client:
            return
        info = {"label": label, "matched": 0.0, "token": asset, "side": side,
                "price": price, "size": amount, "hedge": True,
                "fee_per_share": plan["fee_per_share"]}
        # Persist BEFORE submission: a crash/timeout must never blindly duplicate a hedge.
        self.pending_hedge = {"id": None, "info": info, "at": time.time()}
        self.save_state()
        try:
            response = self.client.create_and_post_order(
                OrderArgsV2(token_id=asset, price=price, size=amount, side=side),
                order_type=OrderType.FAK, post_only=False)
        except Exception as exc:
            if order_rejected(exc):
                self.pending_hedge = None
                self.save_state()
                log(f"hedge rejected by exchange: {short_err(exc)}")
                return
            log(f"hedge submission uncertain: {short_err(exc)}")
            return
        oid = (response or {}).get("orderID")
        if oid:
            self.pending_hedge["id"] = oid
            self.tracked[oid] = info
            self._record_posted(oid, info)
        elif response and (response.get("errorMsg") or response.get("success") is False):
            self.pending_hedge = None
            log("hedge rejected by exchange; will recompute from fresh orders and book")
        else:
            log("hedge submission uncertain: missing order ID")
        self.save_state()
        if oid:
            decision_note((m["condition"], "hedge-sent", asset, side), tg(
                "🛡 Закрываю перекос", self._order_line(side, asset, amount, price),
                f"Расход при этой цене не больше <b>${plan['loss_bound']:.2f}</b>, включая запас на комиссию.", "",
                "После подтверждения исполнения проверю остаток позиции.",
            ))

    def jumped(self, now, mid):
        """True while orders stay pulled after the midpoint moved too fast."""
        self.mids.append((now, mid))
        while self.mids[0][0] < now - JUMP_WINDOW:
            self.mids.popleft()
        swing = max(p for _, p in self.mids) - min(p for _, p in self.mids)
        if now >= self.paused_until and swing >= JUMP - 1e-9:
            self.paused_until = now + JUMP_PAUSE
            log(f"midpoint swung {swing * 100:.1f}c within {JUMP_WINDOW:g}s; pulling orders for {JUMP_PAUSE:g}s")
            note(tg(
                "📉 Сдвиг середины",
                f"<b>{swing * 100:.1f}¢</b> за <b>{JUMP_WINDOW / 60:g}</b> мин",
                "",
                f"Заявки сняты на <b>{JUMP_PAUSE / 60:g}</b> мин",
            ))
        elif now < self.paused_until:
            log(f"paused {self.paused_until - now:.0f}s more | mid {mid:.3f}")
        return now < self.paused_until

    def find_fills(self, ours):
        """Log the orders that traded since the last tick; True if any did."""
        unresolved_before = set(self.unresolved_orders)
        live = {o["id"]: o for o in ours if isinstance(o, dict) and o.get("id")}
        matched = {oid: float(order.get("size_matched") or 0) for oid, order in live.items()}
        # REST open orders can lag a fill by minutes; the user channel trade event does not.
        for oid in matched:
            seen = ws_feed.ws_matched(oid)
            if seen > matched[oid] + 1e-9:
                log(f"fill seen on user channel before REST | order {oid[:10]} matched {seen:g}")
                live[oid] = dict(live[oid], size_matched=str(seen))
                matched[oid] = seen
        filled = False
        for oid, info in self.tracked.items():
            label, before = info["label"], info["matched"]
            if oid in matched:
                self.unresolved_orders.discard(oid)
                done = matched[oid]
                if done > before + 1e-9:
                    filled = True
                    order = live[oid]
                    self._account_fill(
                        info, done, order_id=oid, status=str(order.get("status") or "LIVE"),
                        source="order", trade_ids=exchange_trade_ids(order),
                        exchange_fee=exchange_fee(order), partial=True)
                continue
            try:
                final = self.client.get_order(oid)
                if not isinstance(final, dict) or final.get("size_matched") is None:
                    # REST /data/order returns null for this account (seen 28.10 for live
                    # and cancelled orders alike). Use the user-channel order state instead.
                    final = ws_feed.order_state(oid)
                if not isinstance(final, dict) or final.get("size_matched") is None:
                    # A just-posted order can be missing from both lists for a moment.
                    # Not a fill and not a reason to cancel.
                    if time.time() - float(info.get("posted_at") or 0) < POSTED_GRACE:
                        continue
                    first = info.setdefault("lookup_failed_at", time.time())
                    if time.time() - first >= UNRESOLVED_GIVEUP:
                        # Gone from the open list, no REST record, no channel event for minutes:
                        # stop blocking quotes. Any fill still shows up in the position sync.
                        self.unresolved_orders.discard(oid)
                        info["gone_recorded"] = True
                        log(f"order {oid[:10]} untraceable for {UNRESOLVED_GIVEUP:.0f}s; "
                            "treating as gone, position sync will catch any fill")
                        continue
                    raise LookupError("order not found via REST or user channel")
                done = float(final["size_matched"])
            except Exception as exc:
                self.unresolved_orders.add(oid)
                if oid not in _unresolved_logged:
                    _unresolved_logged.add(oid)
                    log(f"order fill check unresolved: {short_err(exc)} | order {oid[:10]}\n"
                        + traceback.format_exc(limit=3).rstrip())
                continue
            status = str(final.get("status") or "")
            still_live = status.upper() in {"LIVE", "DELAYED", "UNMATCHED"}
            if still_live:
                self.unresolved_orders.add(oid)
            else:
                self.unresolved_orders.discard(oid)
            if done > before + 1e-9:
                filled = True
                self._account_fill(
                    info, done, order_id=oid, status=status or None, source="order",
                    trade_ids=exchange_trade_ids(final), exchange_fee=exchange_fee(final),
                    partial=still_live)
            elif not still_live and not info.get("gone_recorded"):
                info["gone_recorded"] = True
                self._record_order_gone(oid, info, status, done)
                if oid not in self.cancelled_ids and not info.get("hedge"):
                    log(f"order missing from open list; status {status or 'unknown'}: {label} | "
                        f"order {oid} | matched {done:g}")
                    reason = (f"Биржа ответила: <b>{esc(status)}</b>." if status
                              else "Причина неизвестна.")
                    note(tg(
                        "ℹ️ Заявка пропала из активных",
                        self._ru(info),
                        "",
                        reason,
                        "Верну, если она ещё нужна.",
                    ))
            if not still_live:
                self.cancelled_ids.discard(oid)
                if self.pending_hedge and self.pending_hedge.get("id") == oid:
                    self.pending_hedge = None
                    self.save_state()
        if self.unresolved_orders != unresolved_before:
            self.save_state()
        return filled

    def _account_fill(self, info, done, order_id=None, status=None, source="order",
                      trade_ids=None, exchange_fee=None, partial=False, trades=None):
        before = float(info.get("matched") or 0)
        if order_id:
            # REST can still list a fully matched order after find_fills charged it; track()
            # then re-adds it with matched 0 and the same fill is charged twice (anthropic-ipo
            # 30.10 01:39 UTC: No 22 booked as 44, hedge bought 18 Yes too many, bot froze).
            charged = getattr(self, "_charged", None)
            if charged is None:
                charged = self._charged = {}
            prior = float(charged.get(order_id) or 0)
            if prior > before + 1e-9:
                log(f"duplicate fill ignored | order {str(order_id)[:10]} already charged {prior:g}, seen {done:g}")
                before = prior
            charged[order_id] = max(prior, done)
            if len(charged) > 2000:
                for key in list(charged)[:1000]:
                    charged.pop(key, None)
        delta = done - before
        info["matched"] = max(done, before)
        if delta <= 1e-9:
            return
        if info.get("hedge"):
            loss = self.apply_hedge_fill(info, delta)
        else:
            loss = self.apply_fill(info["token"], info["side"], info["price"], delta)
        self._record_fill(info, delta, done, loss, order_id, status, source,
                          trade_ids, exchange_fee, partial, trades)
        note(self._fill_text(info, done, loss))

    def _record_posted(self, oid, info):
        info["posted_at"] = time.time()
        log(f"posted {info['label']} | order {oid} | {self.market.get('slug')}")
        record_event({
            "kind": "order_posted", "order_id": oid,
            "condition": self.market.get("condition"), "slug": self.market.get("slug"),
            "token": info.get("token"), "outcome": name(self.market, info["token"]),
            "side": info.get("side"), "price": tape_num(info.get("price")),
            "size": tape_num(info.get("size")), "hedge": bool(info.get("hedge")),
            "fee_bound_per_share": tape_num(info.get("fee_per_share")) if info.get("hedge") else None,
        })

    def _record_fill(self, info, delta, done, loss, order_id, status, source,
                     trade_ids, fee, partial, trades):
        """The number in the line is the new shares, not the original order size."""
        outcome = name(self.market, info["token"])
        slug = self.market.get("slug") or "unknown"
        order_size = info.get("size")
        fee_bound = info.get("fee_per_share") if info.get("hedge") else None
        word = "PARTLY FILLED" if partial else "FILLED"
        message = (
            f"{word} {info['side']} {outcome} {tape_g(delta)} @ {tape_g(info.get('price'))} | "
            f"order {order_id or '-'} | {slug} | matched {tape_g(done)}/{tape_g(order_size)} | "
            f"status {status or 'unknown'} | fee {'unknown' if fee is None else tape_g(fee)}"
        )
        if fee_bound is not None:
            message += f" | fee bound {tape_g(fee_bound)}/share"
        if trade_ids:
            message += " | trades " + ",".join(str(item) for item in trade_ids)
        log(message)
        event = {
            "kind": "fill", "source": source, "order_id": order_id, "trade_ids": trade_ids,
            "condition": self.market.get("condition"), "slug": self.market.get("slug"),
            "token": info.get("token"), "outcome": outcome, "side": info.get("side"),
            "price": tape_num(info.get("price")), "delta": tape_num(delta),
            "matched": tape_num(done), "order_size": tape_num(order_size),
            "fee": None if fee is None else tape_num(fee),
            "fee_bound_per_share": None if fee_bound is None else tape_num(fee_bound),
            "status": status or None, "hedge": bool(info.get("hedge")),
            "loss": tape_num(loss), "spent": tape_num(self.spent), "partial": bool(partial),
        }
        if trades is not None:
            event["trades"] = trades
        record_event(event)

    def _record_order_gone(self, oid, info, status, matched):
        record_event({
            "kind": "order_gone", "order_id": oid,
            "condition": self.market.get("condition"), "slug": self.market.get("slug"),
            "token": info.get("token"), "outcome": name(self.market, info["token"]),
            "side": info.get("side"), "price": tape_num(info.get("price")),
            "order_size": tape_num(info.get("size")), "matched": tape_num(matched),
            "status": status or None, "cancelled_by_us": oid in self.cancelled_ids,
            "hedge": bool(info.get("hedge")), "label": info.get("label"),
        })

    def _order_line(self, side, token, size, price):
        word = "🟢 покупка" if side == BUY else "🔴 продажа"
        return f"{word} <b>{esc(name(self.market, token))}</b> <code>{size:g} @ {price}</code>"

    def _ru(self, info):
        return self._order_line(info["side"], info["token"], info["size"], info["price"])

    def _fill_text(self, info, done, loss):
        side = "🟢 покупка" if info["side"] == BUY else "🔴 продажа"
        token = esc(name(self.market, info["token"]))
        if done is None:
            return tg(
                "✅ Исполнено",
                f"{side} <b>{token}</b> @ <code>{info['price']}</code>",
                "Сколько именно, биржа не ответила.",
            )
        if done + 1e-9 < info["size"]:
            title = "🟡 Частично исполнено"
            line = (
                f"{side} <b>{token}</b> "
                f"<code>{done:g}</code> из <code>{info['size']:g}</code> @ <code>{info['price']}</code>"
            )
        else:
            title = "✅ Исполнено"
            line = f"{side} <b>{token}</b> <code>{done:g} @ {info['price']}</code>"
        lines = [line]
        if loss > 1e-9:
            lines.append("")
            description = "Расход по верхней оценке" if info.get("hedge") else "Убыток"
            lines.append(
                f"💸 {description} <b>${loss:.2f}</b> · всего <b>${self.spent:.2f}</b> из <b>${LOSS_LIMIT:.0f}</b>"
            )
            if self.spent >= LOSS_LIMIT - 1e-9:
                lines.append("🛑 Лимит выбран: новые котировки остановлены.")
        return tg(title, *lines)

    def _total(self, token):
        return sum(size for size, _ in self.lots.get(token, []))

    def _tail_cost(self, token, size):
        """Cost of the newest `size` shares. Those are the ones a new buy completes."""
        need, cost, covered = size, 0.0, 0.0
        for lot_size, lot_price in reversed(self.lots.get(token, [])):
            if need <= 1e-9:
                break
            take = min(lot_size, need)
            cost += take * lot_price
            covered += take
            need -= take
        return covered, cost

    def _consume_oldest(self, token, size):
        left = size
        lots = self.lots.setdefault(token, [])
        while left > 1e-9 and lots:
            take = min(lots[0][0], left)
            lots[0][0] -= take
            left -= take
            if lots[0][0] <= 1e-9:
                lots.pop(0)

    def _consume_newest(self, token, size):
        lots = self.lots.setdefault(token, [])
        while size > 1e-9 and lots:
            take = min(lots[-1][0], size)
            lots[-1][0] -= take
            size -= take
            if lots[-1][0] <= 1e-9:
                lots.pop()

    def _rebase_range(self, token, start, size, basis):
        """Value newly unpaired shares against $1 minus proceeds from their sold mate."""
        rebuilt, at = [], 0.0
        end = start + size
        for quantity, price in self.lots.get(token, []):
            stop = at + quantity
            cuts = sorted({at, stop, max(at, min(stop, start)), max(at, min(stop, end))})
            for left, right in zip(cuts, cuts[1:]):
                if right - left > 1e-9:
                    rebuilt.append([right - left, basis if start <= left < end else price])
            at = stop
        self.lots[token] = rebuilt

    def _sell_inventory(self, token, price, size):
        other = self.market["no"] if token == self.market["yes"] else self.market["yes"]
        excess = max(0.0, self._total(token) - self._total(other))
        closing = min(size, excess)
        covered, cost = self._tail_cost(token, closing)
        loss = max(0.0, cost - price * covered)
        self._consume_newest(token, closing)
        unpairing = min(size - closing, self._total(token))
        if unpairing > 1e-9:
            paired = min(self._total(token), self._total(other))
            self._rebase_range(other, paired - unpairing, unpairing, 1 - price)
            self._consume_oldest(token, unpairing)
        return loss

    def apply_fill(self, token, side, price, size):
        """Record a fill. Returns the dollars lost completing pairs above $1.

        Shares bought while the other outcome was ahead of this one complete pairs.
        A sale from a pair rebases its remaining mate to $1 minus sale proceeds.
        This measures the next re-hedge from that sale rather than charging the
        pair's original locked loss again. Selling excess shares closes that risk."""
        if size <= 1e-9:
            return 0.0
        if side == SELL:
            loss = self._sell_inventory(token, price, size)
            self.spent += loss
            self._stats_loss(loss)
            if self.hedge_active:
                self.hedge_spent += loss
            self.save_state()
            return loss
        other = self.market["no"] if token == self.market["yes"] else self.market["yes"]
        matched = min(size, max(0.0, self._total(other) - self._total(token)))
        loss = 0.0
        if matched > 1e-9:
            covered, cost = self._tail_cost(other, matched)
            if covered > 1e-9:
                loss = max(0.0, cost + price * covered - covered)
        self.lots.setdefault(token, []).append([size, price])
        if loss > 1e-9:
            self.spent += loss
            self._stats_loss(loss)
            if self.hedge_active:
                self.hedge_spent += loss
        self.save_state()
        return loss

    def projected_loss(self, token, price, size):
        """What apply_fill would add if this buy traded, without changing lots."""
        if not self.lots_inited:
            return 0.0
        other = self.market["no"] if token == self.market["yes"] else self.market["yes"]
        matched = min(size, max(0.0, self._total(other) - self._total(token)))
        if matched <= 1e-9:
            return 0.0
        covered, cost = self._tail_cost(other, matched)
        if covered <= 1e-9:
            return 0.0
        return max(0.0, cost + price * covered - covered)

    def apply_hedge_fill(self, info, size):
        """Charge conservative execution cost once, including the maximum taker fee."""
        token, side, price = info["token"], info["side"], info["price"]
        fee = info.get("fee_per_share", 0.0)
        if side == BUY:
            other = self.market["no"] if token == self.market["yes"] else self.market["yes"]
            matched = min(size, max(0.0, self._total(other) - self._total(token)))
            covered, cost = self._tail_cost(other, matched)
            loss = max(0.0, cost + (price + fee) * covered - covered)
            self.lots.setdefault(token, []).append([size, price + fee])
        else:
            loss = self._sell_inventory(token, price - fee, size)
        self.spent += loss
        self._stats_loss(loss)
        self.hedge_spent += loss
        self.save_state()
        log(f"hedge accounted | cost upper bound ${loss:.4f} | episode ${self.hedge_spent:.4f}/{HEDGE_MAX:g}")
        return loss

    def sync_lots(self, held):
        """Seed the cost basis once the exchange and the data API agree.

        Later disagreements mean a fill happened while this process was down.
        The price of that fill is unknown, so its loss is not counted."""
        if not self.lots_inited:
            self.lots = {
                token: [[size, avg]] if size >= 0.01 and avg > 0 else []
                for token, (size, avg) in held.items()
            }
            self.lots_inited = True
            self.save_state()
            record_event({
                "kind": "position_seeded",
                "condition": self.market.get("condition"), "slug": self.market.get("slug"),
                "lots": {
                    token: ([[tape_num(size), tape_num(avg)]] if size >= 0.01 and avg > 0 else [])
                    for token, (size, avg) in held.items()
                },
            })
            return
        changed = False
        for token, (size, avg) in held.items():
            if abs(self._total(token) - size) <= 0.5:
                continue
            saved_size = self._total(token)
            saved_lots = [[tape_num(lot_size), tape_num(lot_price)]
                          for lot_size, lot_price in self.lots.get(token, [])]
            log(f"saved {name(self.market, token)} position {saved_size:g} differs from the "
                f"exchange {size:g} @ {avg:.4f}; saved lots {saved_lots}; "
                f"adopting the exchange average, that fill's loss is not counted")
            record_event({
                "kind": "position_diverged",
                "condition": self.market.get("condition"), "slug": self.market.get("slug"),
                "token": token, "outcome": name(self.market, token),
                "saved_size": tape_num(saved_size), "saved_lots": saved_lots,
                "exchange_size": tape_num(size), "exchange_avg": tape_num(avg),
                "loss_counted": False,
            })
            self.lots[token] = [[size, avg]] if size >= 0.01 and avg > 0 else []
            changed = True
        if changed:
            self.save_state()

    def save_state(self):
        if DRY_RUN or self.market is None:
            return
        try:
            data = read_state()
        except StateCorrupt:
            log("refusing to replace damaged state; loss limit history stays on disk")
            return
        books = data.get("books")
        if books is None:
            books = {}
        elif not isinstance(books, dict):
            log("refusing to replace damaged state; loss limit history stays on disk")
            return
        else:
            books = dict(books)
        if "books" not in data and data.get("condition"):
            books.setdefault(data["condition"], {"lots": data.get("lots") or {}})
        try:
            disk_spent = _optional_float(data.get("spent"), "spent")
        except StateCorrupt:
            log("refusing to replace damaged state; loss limit history stays on disk")
            return
        merged_spent = self._merged_spent(disk_spent)
        selection = {
            "slug": self.market.get("slug"), "condition": self.market["condition"],
            "live_daily": staying_daily(self.live_share_pct, self.market["rate"], None),
            "live_share_pct": self.live_share_pct,
            "estimate_daily": self.entry_daily if math.isfinite(self.entry_daily) else None,
            "entry_rate": self.entry_rate, "was_better": self.was_better, "misses": self.misses,
        }
        books[self.market["condition"]] = {
            "lots": self.lots, "known": self.lots_inited, "selection": selection,
            "execution": {"active": self.hedge_active, "spent": self.hedge_spent,
                          "started_at": getattr(self, "hedge_started_at", None),
                          "emergency": bool(getattr(self, "emergency", False)),
                          "pending": self.pending_hedge, "basis_model": "paired_value",
                          "tracked": self.tracked,
                          "unresolved": sorted(self.unresolved_orders)},
        }
        disk_samples = data.get("forecast_samples") if isinstance(data.get("forecast_samples"), list) else []
        samples = []
        for sample in disk_samples + list(self.forecast_samples or []):
            if sample not in samples:
                samples.append(sample)
        self.forecast_samples = samples[-FORECAST_HISTORY:]
        data.update({"spent": round(merged_spent, 4), "books": books,
                     "forecast_samples": self.forecast_samples, "selection": selection})
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(data, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, STATE_FILE)
        self.spent = merged_spent
        self._spent_base = merged_spent
        bak_tmp = STATE_FILE + ".bak.tmp"
        try:
            shutil.copyfile(STATE_FILE, bak_tmp)
            os.replace(bak_tmp, STATE_FILE + ".bak")
        except OSError as exc:
            log(f"could not refresh state backup: {short_err(exc)}")

    def retarget(self, market):
        """Point quoting at another market without resetting the global loss total."""
        if self.pending_hedge or self.unresolved_orders:
            raise RuntimeError("cannot retarget with an unresolved order")
        # Drop finished order ids before saving, or the next start would resume this market.
        self.tracked = {}
        self.unresolved_orders = set()
        self.pending_hedge = None
        self.save_state()
        self.market = market
        _dashboard("market", self)
        self.mids.clear()
        self.paused_until = 0.0
        self.tracked = {}
        self.cancelled_ids = set()
        self.prev_sizes = None
        self.fill_base = None
        self.unsettled_since = None
        self.unresolved_orders = set()
        self.inventory_ready = False
        self.next_quote_at = 0.0
        self.data_at = None
        self.hedge_active = False
        self.hedge_spent = 0.0
        self.pending_hedge = None
        self.share = 0.0
        self.share_pct = 0.0
        self.last_share_pct = None
        self.last_share_notice = 0.0
        self.live_share_pct = None
        self.observed_share_pct = None
        self.was_better = False
        self.exit_requested = False
        self.forecast_key = None
        self.forecast_since = self.forecast_at = 0.0
        saved = load_state(market["condition"])
        if saved:
            disk = float(saved["spent"])
            self.spent = self._merged_spent(disk)
            self._spent_base = disk
        self.lots = {}
        self.lots_inited = bool(saved and saved["known"])
        if self.lots_inited:
            for token, lots in (saved.get("lots") or {}).items():
                self.lots[token] = [[float(size), float(price)] for size, price in lots]
        book = _books(read_state()).get(market["condition"]) or {}
        self._restore_execution((book.get("execution") if isinstance(book, dict) else {}) or {})

    def balances(self, reported):
        """Shares held per outcome, as the exchange counts them."""
        if not self.client:
            return {t: s for t, (s, _) in reported.items()}
        return {
            t: float(self.client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=t))["balance"]) / 1e6
            for t in reported
        }

    def settled(self, now, sizes, reported):
        """Whether the position reflects our latest fills, so quoting from it is safe."""
        agree = all(position_ready(reported[t][0], reported[t][1], sizes[t]) for t in sizes)
        stale = self.fill_base is not None and all(abs(sizes[t] - self.fill_base[t]) < 0.01 for t in sizes)
        if agree and not stale:
            self.fill_base = self.unsettled_since = None
            return True
        self.unsettled_since = self.unsettled_since or now
        if now - self.unsettled_since < SETTLE:
            log("waiting for the exchange to report the new position")
            return False
        log(f"position not settled after {SETTLE:g}s; quoting from exchange balances")
        self.fill_base = self.unsettled_since = None
        return True

    def free_cash(self, live):
        """Cash for this market's bids: balance minus what our bids elsewhere hold."""
        if not self.client:
            return math.inf
        balance = float(self.client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))["balance"]) / 1e6
        _dashboard("cash", value=balance)
        elsewhere = sum(reserve(self.market, float(o["price"]), remaining(o)) for o in live
                        if o["side"] == BUY and o.get("market") != self.market["condition"])
        return balance - elsewhere

    def reward_share(self, ours):
        """Our share of this market's reward pool right now, in percent. The share earned
        while quoting both sides is kept, as that is what re-pairing gets back."""
        self.observed_share_pct = None
        if not self.client:
            return 0.0
        try:
            percentages = self.client.get_reward_percentages()
            value = percentages.get(self.market["condition"])
            observed = float(value) if value is not None else None
            pct = observed or 0.0
        except Exception:
            return self.live_share_pct if self.live_share_pct is not None else 0.0
        self.observed_share_pct = observed
        _dashboard("share", self, observed)
        # New orders may not have scored yet. After a live share was seen,
        # a successful zero response is real zero income, not a book fallback.
        if pct > 0 or self.live_share_pct is not None:
            changed = self.live_share_pct != pct
            self.live_share_pct = pct
            if changed:
                self.save_state()
        if len({yes_book_side(self.market, o)[0] for o in ours}) == 2:
            self.share = pct / 100
        return pct

    def maybe_share(self, pct, rate):
        """Remember the live share, and send it when it moves by a point or more."""
        global latest_share_line
        self.share_pct = pct
        line = describe_share(rate, pct)
        if line:
            line = f"{qmarket(self.market['question'])} · {line}"
            _share_lines[self.market["condition"]] = line
        else:
            _share_lines.pop(self.market["condition"], None)
        latest_share_line = line
        if pct <= 0:
            return
        now = time.time()
        if self.last_share_pct is None:
            notify(tg("📊 Сейчас", status_lines() or latest_share_line))
            self.last_share_pct = pct
            self.last_share_notice = now
            return
        if abs(pct - self.last_share_pct) >= 1 and now - self.last_share_notice >= 900:
            notify(tg("📊 Сейчас", status_lines() or latest_share_line))
            self.last_share_pct = pct
            self.last_share_notice = now

    def stats_rewards(self, amount, now):
        """Attribute growth of today's account accrual to this market; log a summary every 30 min."""
        global _stats_total
        day = datetime.now(UTC).date().isoformat()
        last_day, last = _stats_total
        if last_day == day and amount + 1e-9 >= last:
            delta = amount - last
        elif last_day is None:
            delta = 0.0  # first reading after start: earlier accruals are unattributed
        else:
            delta = amount if last_day != day else 0.0
        _stats_total = (day, amount)
        try:
            market_stats.record(self.market["condition"], self.market.get("slug"), rewards=delta)
            if now - _stats_logged[0] >= STATS_EVERY:
                _stats_logged[0] = now
                log(market_stats.summary())
        except Exception as exc:
            log(f"stats failed: {short_err(exc)}")

    def stats_losses(self):
        """Losses are recorded where they are accounted (_stats_loss); nothing to diff here."""
        return

    def _stats_loss(self, loss):
        """Book a hedge/exit loss against the market where the fill happened."""
        if not loss or loss <= 0:
            return
        try:
            market_stats.record(self.market["condition"], self.market.get("slug"), loss=float(loss))
        except Exception as exc:
            log(f"stats failed: {short_err(exc)}")

    def maybe_earnings(self):
        """Refresh today's accrual and tell Telegram when the cent amount moves."""
        global latest_earned_line
        now = time.time()
        if now - self.earned_at < EARNINGS_EVERY:
            return
        amount = day_earnings(self.client)
        self.earned_at = now
        if amount is None:
            return
        self.earned = amount
        self.stats_rewards(amount, now)
        _dashboard("earnings", value=amount)
        latest_earned_line = describe_earnings(amount)
        cents = round(amount, 2)
        if cents <= 0 or cents == self.last_earned_cents:
            return
        if self.last_earned_notice and now - self.last_earned_notice < EARNINGS_NOTICE:
            return
        notify(tg("💰 Начисление", latest_earned_line))
        self.last_earned_cents = cents
        self.last_earned_notice = now

    def maybe_forecast(self, bids, asks, mid, ours, held, now):
        """Compare exact stable resting BUYs with the exchange, without changing policy."""
        m = self.market
        valid = (mid is not None and all(size < 0.01 for size, _ in held.values())
                 and len(ours) == 2 and all(o["side"] == BUY for o in ours)
                 and {o["asset_id"] for o in ours} == {m["yes"], m["no"]}
                 and all(remaining(o) >= m["min_size"] for o in ours))
        if not valid:
            self.forecast_key = None
            self.forecast_since = 0.0
            return
        key = tuple(sorted((o["id"], o["asset_id"], float(o["price"]), remaining(o)) for o in ours))
        if key != self.forecast_key:
            self.forecast_key, self.forecast_since = key, now
            return
        if (self.observed_share_pct is None
                or now - self.forecast_since < FORECAST_STABLE_SECONDS
                or now - self.forecast_at < FORECAST_EVERY):
            return
        orders = {(o["asset_id"], o["side"]): (float(o["price"]), remaining(o)) for o in ours}
        estimate = rewards_scan.estimate_orders(m, bids, asks, orders, mid)
        if not estimate or estimate["est_daily"] <= 0:
            return
        live = staying_daily(self.observed_share_pct, m["rate"], None)
        sample = {"at": datetime.now(UTC).isoformat(), "slug": m.get("slug"),
                  "estimated_share": round(estimate["share"], 6), "live_share": self.observed_share_pct,
                  "estimated_daily": round(estimate["est_daily"], 6), "live_daily": round(live, 6),
                  "live_to_estimate": round(live / estimate["est_daily"], 6),
                  "pool": m["rate"], "mid": mid,
                  "yes_bid": orders[(m["yes"], BUY)][0], "no_bid": orders[(m["no"], BUY)][0],
                  "yes_size": orders[(m["yes"], BUY)][1], "no_size": orders[(m["no"], BUY)][1]}
        self.forecast_samples = (self.forecast_samples + [sample])[-FORECAST_HISTORY:]
        self.forecast_at = now
        self.save_state()
        log(f"forecast check | book ${estimate['est_daily']:.2f}/day | live ${live:.2f}/day | "
            f"live/book {sample['live_to_estimate']:.3f} | book is not an account share | "
            f"stable BUYs Yes {sample['yes_bid']:g} No {sample['no_bid']:g}")

    def reconcile_uncertain(self, live):
        """Attach an id or a trade to an ambiguous submit. Never posts a replacement."""
        pending = self.pending_hedge
        if DRY_RUN or not self.client or not pending or pending.get("id"):
            return
        info = pending.get("info") if isinstance(pending.get("info"), dict) else None
        if info is None:
            return
        try:
            size = float(info.get("size") or 0)
            matched = float(info.get("matched") or 0)
        except (TypeError, ValueError):
            return
        if size > 0 and matched + 1e-9 >= size:
            self.pending_hedge = None
            self.save_state()
            return
        if not isinstance(live, (list, tuple)):
            live = []
        matches = self._matching_open_orders(live, info)
        if len(matches) > 1:
            log("uncertain submission matches several open orders; not resubmitting")
            return
        if len(matches) == 1:
            order = matches[0]
            pending["id"] = order["id"]
            self.tracked[order["id"]] = info
            self.unresolved_orders.add(order["id"])
            if not info.get("hedge"):
                self.pending_hedge = None
            self.save_state()
            log(f"uncertain submission matches open order {str(order['id'])[:10]}; not resubmitting")
            return
        self._account_uncertain_trades(info)
        pending = self.pending_hedge
        if pending is None or pending.get("id"):
            return
        try:
            age = time.time() - float(pending.get("at"))
        except (TypeError, ValueError):
            return
        try:
            matched = float(info.get("matched") or 0)
        except (TypeError, ValueError):
            matched = 0.0
        # No open order and no trade after UNCERTAIN_GIVEUP s: the submit never landed.
        if age >= UNCERTAIN_GIVEUP and matched <= 1e-9:
            self.pending_hedge = None
            self.save_state()
            log(f"uncertain submission dropped after {age:.0f}s: no open order, no trades | {info.get('label')}")

    def _matching_open_orders(self, live, info):
        try:
            price, size = float(info["price"]), float(info["size"])
        except (KeyError, TypeError, ValueError):
            return []
        found = []
        for order in live:
            if not isinstance(order, dict) or not order.get("id") or order["id"] in self.tracked:
                continue
            if order.get("market") != self.market["condition"]:
                continue
            if order.get("asset_id") != info.get("token") or order.get("side") != info.get("side"):
                continue
            try:
                same = (abs(float(order.get("price")) - price) < 1e-9
                        and abs(float(order.get("original_size")) - size) < 0.01)
            except (TypeError, ValueError):
                continue
            if same:
                found.append(order)
        return found

    def _account_uncertain_trades(self, info):
        if any(other is not info and other.get("token") == info.get("token")
               and other.get("side") == info.get("side") for other in self.tracked.values()):
            log("uncertain submission left untouched; another order uses this outcome")
            return
        try:
            since = float(self.pending_hedge.get("at"))
        except (TypeError, ValueError):
            return
        try:
            rows = self.client.get_trades(
                TradeParams(market=self.market["condition"], asset_id=info.get("token"),
                            after=max(0, int(since) - 5)),
                only_first_page=True)
        except Exception as exc:
            log(f"uncertain submission trade lookup failed: {short_err(exc)}")
            return
        if not isinstance(rows, list):
            return
        seen = {str(item) for item in (info.get("seen_trades") or [])}
        fresh = []
        for trade in rows:
            if not isinstance(trade, dict):
                continue
            tid = str(trade.get("id") or "")
            if not tid or tid in seen or trade.get("asset_id") != info.get("token"):
                continue
            if trade.get("side") != info.get("side"):
                continue
            if str(trade.get("status", "")).upper() in {"FAILED", "RETRYING"}:
                continue
            try:
                price = float(trade.get("price"))
                size = float(trade.get("size") or 0)
                when = float(trade.get("match_time") or trade.get("last_update") or 0)
                limit = float(info["price"])
                room = float(info["size"]) - float(info.get("matched") or 0)
            except (KeyError, TypeError, ValueError):
                continue
            if size <= 1e-9 or when <= 0 or when + 5 < since:
                continue
            if info.get("side") == BUY and price > limit + 1e-9:
                continue
            if info.get("side") == SELL and price < limit - 1e-9:
                continue
            fresh.append((tid, size, room, exchange_fee(trade)))
        if not fresh:
            return
        room = fresh[0][2]
        take, used, taken = 0.0, [], []
        fee_parts, fee_known = [], True
        for tid, size, _room, trade_fee in fresh:
            if room - take <= 1e-9:
                break
            part = min(size, room - take)
            take += part
            used.append(tid)
            taken.append({"id": tid, "size": tape_num(size), "taken": tape_num(part), "fee": trade_fee})
            if trade_fee is None or abs(part - size) > 1e-9:
                fee_known = False
            else:
                fee_parts.append(trade_fee)
        if take <= 1e-9:
            return
        info["seen_trades"] = sorted(seen | set(used))
        done = float(info.get("matched") or 0) + take
        self._account_fill(
            info, done, status=None, source="trades", trade_ids=used,
            exchange_fee=sum(fee_parts) if fee_known else None,
            partial=done + 1e-9 < float(info["size"]), trades=taken)
        log(f"uncertain submission accounted {take:g} from trades; not resubmitting")
        if float(info["matched"]) + 1e-9 >= float(info["size"]):
            self.pending_hedge = None
            self.save_state()

    def reconcile(self, ours, wanted, taker=False, exit_single=False, snapshot_at=None):
        if snapshot_at is not None and time.monotonic() - snapshot_at >= min(DATA_STALE_AFTER, LOOP_STALE_AFTER):
            self.pull_quotes("quote snapshot timed out", ours)
            return False
        blocked = bool(self.pending_hedge) or bool(self.unresolved_orders)
        self.reconcile_uncertain(ours)
        if blocked or self.pending_hedge or self.unresolved_orders:
            return False
        m = self.market
        keep, kept = set(), set()
        for (token, side), (price, size) in wanted.items():
            match = next(
                (o for o in ours if o["id"] not in keep and o["asset_id"] == token and o["side"] == side
                 and abs(float(o["price"]) - price) < 1e-9
                 and quote_plan.resting_size_kept(
                     remaining(o), size, m["min_size"], exiting=exit_single)),
                None,
            )
            if match:
                keep.add(match["id"])
                kept.add((token, side))
        stale = [o for o in ours if o["id"] not in keep]
        if stale:
            self.cancel(stale)
            # A cancellation may race a fill. Keep its baseline until the next tick
            # reads the final order and refreshed balances before posting replacements.
            self.track(ours)
            return False
        self.track(ours)
        placed = bool(kept)
        for (token, side), (price, size) in wanted.items():
            if (token, side) in kept:
                continue
            if snapshot_at is not None and time.monotonic() - snapshot_at >= min(DATA_STALE_AFTER, LOOP_STALE_AFTER):
                # Earlier posts may have succeeded while this snapshot aged out.
                self.pull_quotes("quote snapshot timed out")
                return False
            post_only = not (exit_single and taker and side == SELL)
            label = f"{side} {name(m, token)} {size:g} @ {price}"
            if exit_single:
                label += " | single-leg exit " + ("post-only" if post_only else "taker limit")
            log(f"{'[dry] ' if DRY_RUN else ''}{label}")
            if DRY_RUN:
                placed = True
                continue
            info = {"label": label, "matched": 0.0, "token": token,
                    "side": side, "price": price, "size": size}
            self.pending_hedge = {"id": None, "info": info, "at": time.time()}
            self.save_state()
            try:
                resp = self.client.create_and_post_order(
                    OrderArgsV2(token_id=token, price=price, size=size, side=side), post_only=post_only)
            except Exception as exc:
                if order_rejected(exc):
                    self.pending_hedge = None
                    self.save_state()
                    log(f"order rejected: {label}: {short_err(exc)}")
                    note(tg("❌ Заявку отклонили", self._order_line(side, token, size, price)))
                    return False
                log(f"order submission uncertain: {label}: {short_err(exc)}")
                note(tg("⚠️ Результат заявки неизвестен", self._order_line(side, token, size, price)))
                return False
            if resp and resp.get("orderID"):
                self.pending_hedge = None
                placed = True
                self.tracked[resp["orderID"]] = info
                self._record_posted(resp["orderID"], info)
                self.save_state()
                if side == BUY:
                    risk = self.projected_loss(token, price, size)
                    if risk >= 0.05:
                        note(tg(
                            "🛒 Докупка",
                            self._order_line(side, token, size, price),
                            "",
                            f"Если исполнят, убыток около <b>${risk:.2f}</b>",
                            f"Потрачено <b>${self.spent:.2f}</b> из <b>${LOSS_LIMIT:.0f}</b>",
                        ))
            elif resp and (resp.get("errorMsg") or resp.get("success") is False):
                self.pending_hedge = None
                self.save_state()
                log(f"order rejected: {label}")
                note(tg("❌ Заявку отклонили", self._order_line(side, token, size, price)))
                return False
            else:
                log(f"order submission uncertain: {label}: missing order ID")
                return False
        if snapshot_at is not None and time.monotonic() - snapshot_at >= min(DATA_STALE_AFTER, LOOP_STALE_AFTER):
            self.pull_quotes("quote submission outlived snapshot")
            return False
        # Nothing wanted and nothing resting is a finished decision. Returning false
        # here made the next pulse run immediately and kept polling the exchange.
        return True

    def cancel(self, orders):
        """Cancel orders; returns the ones the exchange did not cancel."""
        ids = [o["id"] for o in orders if o.get("id")]
        log(f"cancel {len(orders)} order(s)" + (f" | {', '.join(ids)}" if ids else ""))
        if DRY_RUN or not self.client:
            return []
        response = self.client.cancel_orders(ids)
        cancelled = set(response.get("canceled") or [])
        self.cancelled_ids.update(cancelled)
        refused = response.get("not_canceled") or {}
        for oid, why in refused.items():
            log(f"could not cancel {oid[:10]}: {why}")
        record_event({
            "kind": "cancel", "condition": self.market.get("condition"),
            "slug": self.market.get("slug"), "order_ids": ids,
            "canceled": sorted(cancelled),
            "not_canceled": {str(oid): str(why) for oid, why in refused.items()},
        })
        return [o for o in orders if o["id"] not in cancelled]

    def track(self, orders):
        """Remember orders still awaiting a fill read. Do not advance the charged size."""
        if DRY_RUN:
            return
        previous = self.tracked
        pending_id = self.pending_hedge.get("id") if self.pending_hedge else None
        self.tracked = {oid: info for oid, info in previous.items()
                        if oid in self.unresolved_orders or oid == pending_id}
        for order in orders:
            prev = previous.get(order["id"])
            if prev:
                # find_fills is the only writer of `matched`. Copying the live
                # size here would hide a fill that has not been charged yet.
                self.tracked[order["id"]] = prev
                continue
            self.tracked[order["id"]] = {
                "label": order_label(self.market, order),
                "matched": float(order.get("size_matched") or 0),
                "token": order["asset_id"],
                "side": order["side"],
                "price": float(order["price"]),
                "size": float(order["original_size"]),
            }


def cancel_all(client, market):
    """Cancel this market's orders and return only exchange-confirmed cancellations."""
    cancelled = set()
    if client and not DRY_RUN:
        response = client.cancel_market_orders(OrderMarketCancelParams(market=market["condition"]))
        cancelled.update((response or {}).get("canceled") or [])
    log("cancelled all orders on this market")
    return cancelled


def staying_daily(live_share_pct, rate, estimate):
    """Use the observed share at today's pool, or the calibrated book estimate before it arrives."""
    if live_share_pct is not None:
        return rate * live_share_pct / 100
    return estimate * rewards_scan.EST_CALIB if estimate is not None and math.isfinite(estimate) else None


def ban(cid, until, why):
    """Blacklist a market; never shortens an existing longer ban."""
    rewards_scan.BLACKLIST[cid] = max(until, rewards_scan.BLACKLIST.get(cid, 0.0))
    rewards_scan.BLACKLIST_WHY[cid] = why


def repair_expired(started_at, now=None):
    if started_at is None or REPAIR_MAX_SECONDS <= 0:
        return False
    return (time.time() if now is None else now) - started_at >= REPAIR_MAX_SECONDS


def switch_target(current_slug, stay_daily, misses, dead, rows, was_better=False, better_override=None,
                  dwell=None):
    """One scan's decision. Confirmation follows the inequality, not a leader's slug.

    `better_override` replaces only the book-discount comparison. A dead market
    and two confirmed misses still leave on this scan. The shadow score passes
    an override only when `rewards_scan.ECONOMIC_CHOICE` is on.
    """
    other = max((r for r in rows if r["slug"] != current_slug),
                key=rewards_scan.hourly_value, default=None)
    away = rewards_scan.hourly_value(other) * SWITCH_DISCOUNT if other else None
    better = (away is not None and stay_daily is not None and away >= stay_daily * SWITCH_GAIN
              and away - stay_daily >= SWITCH_MIN_GAIN)
    if better_override is not None:
        better = bool(better_override)
    if dwell is not None and dwell < MIN_DWELL:
        better = False  # voluntary switches only after MIN_DWELL; dead/misses still leave
    target = None
    if dead or misses >= 2:
        target = other["slug"] if other else ""
        reason = "dead" if dead else "misses"
    elif better and was_better:
        target, reason = other["slug"], "confirmed"
    elif better:
        reason = "pending"
    else:
        reason = "threshold" if other and stay_daily is not None else "no comparison"
    return {"target": target, "other": other, "better": better, "stay_daily": stay_daily,
            "away_daily": away, "reason": reason}


def market_decision_card(maker, decision):
    """Four decision lines using only market labels, amounts and a fixed reason."""
    ours = qmarket(maker.market["question"])
    stay = decision["stay_daily"]
    if maker.live_share_pct is not None:
        ours += f" · живые <b>${stay:.2f}</b>/день"
    elif stay is not None:
        ours += f" · живой доли ещё нет, по стакану <b>${stay:.2f}</b>/день"
    else:
        ours += " · живой доли и прикидки ещё нет"
    other = decision["other"]
    theirs = (qmarket(other.get("q") or other["slug"])
              + f" · прикидка стакана <b>${other['est_daily']:.2f}</b>/день, не доля счёта"
              if other else "Другого подходящего рынка нет")
    discounted = (f"После скидки ×{SWITCH_DISCOUNT:g}: <b>${decision['away_daily']:.1f}</b>/день"
                  if other else "После скидки: —")
    why = {
        "pending": "Остаёмся: два подтверждённых скана ещё не собраны (1 из 2).",
        "threshold": f"Остаёмся: после скидки порог преимущества {round((SWITCH_GAIN - 1) * 100)}% не пройден.",
        "confirmed": f"Уходим: преимущество не меньше {round((SWITCH_GAIN - 1) * 100)}% подтверждено двумя сканами.",
        "dead": f"Уходим: {maker.dead}.",
        "misses": "Уходим: рынок не прошёл два скана подряд.",
        "no comparison": "Остаёмся: пока недостаточно данных для сравнения.",
    }[decision["reason"]]
    if decision.get("holding"):
        why += " Сначала закрываем позицию, затем меняем рынок."
    return tg("🚪 Решение: уходим" if decision["target"] is not None else "📍 Решение: остаёмся",
              ours, "", theirs, discounted, "", esc(why))


def decision_note(key, text):
    """Changing prices must not repeat a card with the same reason within 30 minutes."""
    now = time.time()
    if now - _decision_notices.get(key, -math.inf) < DECISION_NOTICE:
        return False
    _decision_notices[key] = now
    notify(text)
    return True


def collateral(client):
    """USDC balance, or None when there is no client to ask."""
    if not client:
        return None
    balance = float(client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))["balance"]) / 1e6
    _dashboard("cash", value=balance)
    return balance


def find_held():
    """Open positions of at least one share, largest first. Raises if the lookup fails."""
    rows = get(f"{DATA}/positions?user={os.environ['FUNDER_ADDRESS']}&sizeThreshold=0.5")
    if not isinstance(rows, list):
        raise ValueError("positions response was not a list")
    held = []
    for row in rows:
        size = float(row.get("size") or 0)
        cid = row.get("conditionId") or row.get("condition_id")
        if size >= 1 and cid:
            held.append((size, cid, row.get("title") or row.get("slug") or cid))
    held.sort(reverse=True)
    return held


def dead_reason(maker, cash, held_max):
    """Why this market's rewards are no longer worth quoting, or None."""
    market = maker.market
    if rewards_scan.blacklisted(market["condition"]):
        why = rewards_scan.BLACKLIST_WHY.get(market["condition"])
        return f"рынок на паузе ({why})" if why else "не могу захеджировать полное исполнение, рынок на паузе"
    if market["band"] <= 0:
        return "полоса награды пропала"
    if market["end"] - datetime.now(UTC) < rewards_scan.MIN_LIFE:
        return "до конца меньше 3 дней"
    if market["rate"] < rewards_scan.MIN_POOL or (maker.entry_rate and market["rate"] < maker.entry_rate * 0.5):
        return f"пул стал ${market['rate']:g} в день"
    # Free cash below the minimum, with nothing held, means we cannot quote.
    # A position already occupies that cash; it does not by itself make the market unfit.
    if held_max < 1 and cash is not None and cash < market["min_size"] * rewards_scan.HEDGE_MARGIN:
        return f"минимум {market['min_size']:g} акций уже не по карману"
    return None


_READ_COLLATERAL = object()


def refresh_rules(maker, cash=_READ_COLLATERAL):
    """Reload the pool, minimum and end date. Keeps the last rules if the read fails."""
    try:
        if cash is _READ_COLLATERAL:
            cash = collateral(maker.client)
        fresh = load_market(maker.market.get("slug"), maker.market["condition"])
        reported = reported_positions(maker.market)
        sizes = maker.balances(reported)
    except Exception as exc:
        maker.rules_failures = getattr(maker, "rules_failures", 0) + 1
        log(f"could not refresh market rules: {short_err(exc)}")
        if maker.rules_failures >= 3:
            # Stale reward rules could quote below a raised minimum; stop opening until a read succeeds.
            maker.rewarding = False
        return
    maker.rules_failures = 0
    for key in ("band", "min_size", "rate", "end", "tick", "fee_rate", "fee_exp", "question", "slug"):
        maker.market[key] = fresh[key]
    _dashboard("market", maker)
    held_max = max(sizes.values()) if sizes else 0.0
    reason = dead_reason(maker, cash, held_max)
    maker.rewarding = reason is None
    if reason != maker.dead:
        maker.dead = reason
        if reason:
            log(f"rewards no longer fit: {reason}")


def position_ready(reported_size, reported_avg, exchange_size):
    """True when this outcome can be quoted.

    A remainder below FLAT cannot be sold and is not a position. The data API
    drops it, so 0.01 on the exchange against a report of 0 is settled. A real
    size still has to match the report and carry an average price.
    """
    if reported_size < FLAT and exchange_size < FLAT:
        return True
    if abs(reported_size - exchange_size) >= 0.01:
        return False
    if exchange_size >= 0.01 and reported_avg <= 0:
        return False
    return True


def merged_by_balances(before, after, shares, market, tol=0.02):
    """Both legs fell by at least the merged amount: the merge executed."""
    try:
        return all(float(before[t]) - float(after[t]) >= shares - tol for t in (market["yes"], market["no"]))
    except (KeyError, TypeError, ValueError):
        return False


def refresh_balances(client, tokens=()):
    """Make the exchange re-read on-chain balances; its cache lags a merge."""
    if not client:
        return
    params = [BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)]
    params += [BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=t) for t in tokens]
    for p in params:
        try:
            client.update_balance_allowance(p)
        except Exception as exc:
            log(f"balance refresh failed: {short_err(exc)}")


def is_flat(maker):
    # A merge in flight owns this market until it is confirmed or reconciled;
    # otherwise the scanner releases the maker and the merge result is never read.
    if getattr(maker, "merge_job", None) is not None:
        return False
    reported = reported_positions(maker.market)
    sizes = maker.balances(reported)
    return all(size < FLAT for size in sizes.values())


def cancel_accepted(response):
    """True when the exchange named the cancel result and refused nothing."""
    if not isinstance(response, dict) or "canceled" not in response:
        return False
    refused = response.get("not_canceled") or {}
    if refused or response.get("errorMsg") or response.get("success") is False:
        return False
    return True


def _keep_ids(condition):
    if condition is None:
        return set()
    if isinstance(condition, str):
        return {condition}
    return {cid for cid in condition if cid}


def cancel_foreign(client, condition):
    """Cancel markets outside `condition`. True only when a fresh read agrees."""
    keep = _keep_ids(condition)
    if not client or DRY_RUN:
        return True
    try:
        foreign = [o for o in client.get_open_orders() if o.get("market") not in keep and o.get("id")]
    except Exception as exc:
        log(f"could not list orders on other markets: {short_err(exc)}")
        return False
    if not foreign:
        return True
    try:
        response = client.cancel_orders([o["id"] for o in foreign])
    except Exception as exc:
        log(f"could not cancel orders on other markets: {short_err(exc)}")
        return False
    if not cancel_accepted(response):
        log("cancel of other markets was not confirmed; orders may remain")
        return False
    try:
        left = [o for o in client.get_open_orders() if o.get("market") not in keep and o.get("id")]
    except Exception as exc:
        log(f"could not verify other markets have no orders: {short_err(exc)}")
        return False
    if left:
        log(f"{len(left)} order(s) still live on other markets")
        return False
    log(f"cancelled {len(foreign)} orders on other markets")
    return True


def wipe_orders(client):
    """Cancel every resting order. True only when a fresh read shows none left."""
    if not client or DRY_RUN:
        return True
    try:
        response = client.cancel_all()
    except Exception as exc:
        log(f"could not cancel resting orders: {short_err(exc)}")
        return False
    if not cancel_accepted(response):
        log("cancel-all was not confirmed; orders may remain")
        return False
    try:
        left = client.get_open_orders()
    except Exception as exc:
        log(f"could not verify resting orders were removed: {short_err(exc)}")
        return False
    if left:
        log(f"{len(left)} order(s) still live after cancel-all")
        return False
    return True


def _cancel_owned(client, owned_market, maker):
    """Confirm one market's cancel. Raises on transport errors."""
    response = client.cancel_market_orders(OrderMarketCancelParams(market=owned_market["condition"]))
    if not isinstance(response, dict) or "canceled" not in response:
        log("shutdown cancellation unconfirmed: missing exchange acknowledgement")
        return False
    cancelled = set(response.get("canceled") or [])
    if maker is not None:
        maker.cancelled_ids.update(cancelled)
    refused = response.get("not_canceled") or {}
    if refused or response.get("errorMsg") or response.get("success") is False:
        log(f"shutdown cancellation incomplete: {len(cancelled)} confirmed, "
            f"{len(refused)} refused; orders may remain")
        return False
    log(f"shutdown cancellation confirmed: {len(cancelled)} order(s)")
    return True


def shutdown(client, stop, maker=None, market=None, makers=None):
    """Stop heartbeats first, then report only confirmed cleanup of owned orders."""
    stop.set()
    if not client or DRY_RUN:
        return True
    book = []
    if makers is not None and not isinstance(makers, (list, tuple)):
        makers = [makers]
    for item in list(makers or []) + ([maker] if maker is not None else []):
        if item is not None and all(item is not other for other in book):
            book.append(item)
    try:
        if book:
            for item in book:
                if not _cancel_owned(client, item.market, item):
                    return False
            return True
        if market is not None:
            return _cancel_owned(client, market, None)
        # Startup can fail before an existing order's market is discovered.
        response = client.cancel_all()
        if not isinstance(response, dict) or "canceled" not in response:
            log("shutdown cancellation unconfirmed: missing exchange acknowledgement")
            return False
        cancelled = set(response.get("canceled") or [])
        refused = response.get("not_canceled") or {}
        if refused or response.get("errorMsg") or response.get("success") is False:
            log(f"shutdown cancellation incomplete: {len(cancelled)} confirmed, "
                f"{len(refused)} refused; orders may remain")
            return False
        log(f"shutdown cancellation confirmed: {len(cancelled)} order(s)")
        return True
    except Exception as exc:
        log(f"could not cancel orders during shutdown: {short_err(exc)}; orders may remain")
        return False


def saved_spent():
    """Spent loss limit. A damaged file raises instead of looking like a fresh zero."""
    return _optional_float(read_state().get("spent"), "spent")


def _needs_order_resume(book):
    if not isinstance(book, dict):
        return False
    execution = book.get("execution") or {}
    if not isinstance(execution, dict):
        raise StateCorrupt("execution record is damaged")
    if execution.get("pending") or execution.get("unresolved"):
        return True
    tracked = execution.get("tracked") or {}
    return isinstance(tracked, dict) and bool(tracked)


def _known_tokens(book):
    if not isinstance(book, dict):
        return []
    tokens = list((book.get("lots") or {}).keys())
    execution = book.get("execution") or {}
    if not isinstance(execution, dict):
        return tokens
    tracked = execution.get("tracked") or {}
    if isinstance(tracked, dict):
        tokens.extend(info.get("token") for info in tracked.values()
                      if isinstance(info, dict) and info.get("token"))
    pending = execution.get("pending") or {}
    info = pending.get("info") if isinstance(pending, dict) else None
    if isinstance(info, dict) and info.get("token"):
        tokens.append(info["token"])
    return tokens


def exchange_holdings(client, history):
    """Exchange balances of at least one share for tokens this state already knows."""
    if not client:
        return []
    found, seen = [], set()
    for cid, book in _books(history).items():
        for token in _known_tokens(book):
            if token in seen:
                continue
            seen.add(token)
            size = float(client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token))["balance"]) / 1e6
            if size >= 1:
                found.append((size, cid))
    return found


def exchange_holding(client, history):
    """Largest exchange balance among tokens this state already knows, or None."""
    found = exchange_holdings(client, history)
    return max(found) if found else None


def _finish_startup(maker, client):
    """Account restored orders before the first quote. Fills use the saved baseline."""
    live = client.get_open_orders() if client else []
    ours, _ = maker.observe_orders(live)
    maker.reward_share(ours)
    maker.save_state()


def _scan_budget(client):
    refresh_balances(client)
    try:
        budget = collateral(client)
    except Exception as exc:
        log(f"could not read cash: {short_err(exc)}")
        budget = None
    if budget is None:
        budget = float(os.getenv("SCAN_BUDGET", "30"))
    else:
        _last_cash[0] = float(budget)
    return budget


def reserve_for(budget):
    """Hedge reserve for a given cash balance."""
    try:
        return max(CASH_RESERVE, HEDGE_RESERVE_PCT * float(budget))
    except (TypeError, ValueError):
        return reserve_cash()


def reserve_cash():
    """Cash kept out of opening quotes so a fill can be hedged: HEDGE_RESERVE_PCT of the
    last cash read (at least CASH_RESERVE). Falls back to SIZE before any cash read."""
    if _last_cash[0] > 0:
        return max(CASH_RESERVE, HEDGE_RESERVE_PCT * _last_cash[0])
    if CASH_RESERVE > 0:
        return CASH_RESERVE
    return SIZE


def market_slots(budget):
    """How many markets this balance can watch, each at the same size as one market now.

    A balance that cannot fund a second quote stays on one market. The cap is
    MAX_MARKETS so the loop can still check every book.
    """
    try:
        budget = float(budget)
    except (TypeError, ValueError):
        return 1
    room = max(0.0, budget - reserve_for(budget))
    opened = int(room // max(float(SIZE), 1.0))
    return max(1, min(MAX_MARKETS, opened if opened else 1))


def quote_capital(row):
    """Dollars the planned two-sided quote locks. A missing plan uses the share size."""
    need = rewards_scan.occupied_capital(row)
    if need is not None:
        return need
    try:
        if row.get("size") is not None:
            return float(row["size"])
    except (TypeError, ValueError):
        pass
    return float(SIZE)


def locked_bids(orders):
    """Cash already posted in BUY orders. The collateral balance still includes it."""
    total = 0.0
    for order in orders or []:
        if not isinstance(order, dict) or order.get("side") != BUY:
            continue
        try:
            total += float(order["price"]) * remaining(order)
        except (TypeError, ValueError, KeyError):
            continue
    return total


def pack_entries(rows, budget, held_ids, locked, repairing=()):
    """Best books that still fit after the reserve and the quotes already working.

    A market only resting a repair bid keeps its cash locked (counted in `locked`)
    but does not take a quoting slot."""
    slots = market_slots(budget) - len(set(held_ids) - set(repairing))
    if slots <= 0:
        return []
    left = float(budget) - reserve_for(budget) - float(locked)
    picked, seen = [], set(held_ids)
    for row in rows:
        cid = row.get("condition")
        if not cid or cid in seen:
            continue
        need = quote_capital(row)
        if need > left + 1e-9:
            continue
        picked.append(row)
        seen.add(cid)
        left -= need
        slots -= 1
        if slots <= 0:
            break
    allocate_sizes(picked, left)
    return picked


def allocate_sizes(picked, spare):
    """Split spare cash over the picked books by expected $/day and turn it into order size.

    Each row keeps at least its planned size. Extra size stays inside the book's own
    depth limits (thin / DEPTH_MULT and the touch on both sides) and MAX_ORDER_SIZE.
    """
    weights = [max(rewards_scan.hourly_value(row), 0.0) for row in picked]
    total = sum(weights)
    for row, weight in zip(picked, weights):
        base = float(row.get("size") or SIZE)
        capital = quote_capital(row)
        per_share = capital / base if base > 0 and capital > 0 else 1.0
        extra = (max(0.0, spare) * weight / total) if total > 0 else 0.0
        size = base + extra / quote_plan.HEDGE_MARGIN / per_share
        limits = [MAX_ORDER_SIZE]
        if row.get("thin"):
            limits.append(float(row["thin"]) / rewards_scan.DEPTH_MULT)
        for key in ("touch_bid", "touch_ask"):
            if row.get(key):
                limits.append(float(row[key]))
        row["order_size"] = float(math.floor(max(base, min([size] + limits)) + 1e-9))


def startup(client):
    """Resume every open market before quoting. A damaged limit file stops this.

    Returns (makers, ready). Stray orders that belong to no resumed market must
    clear, or the loop retries instead of quoting beside them.
    """
    history = read_state()
    _optional_float(history.get("spent"), "spent")
    _dashboard("spent", value=history.get("spent", 0.0))
    books = _books(history)
    wanted = []

    def add(cid):
        if cid and cid not in wanted:
            wanted.append(cid)

    for cid, book in books.items():
        if _needs_order_resume(book):
            add(cid)
    # An unresolved order has to resume even when the position API is down.
    blocked = []
    for cid, book in books.items():
        execution = book.get("execution") if isinstance(book, dict) else None
        if isinstance(execution, dict) and (execution.get("pending") or execution.get("unresolved")):
            blocked.append(cid)
    held, balances = [], []
    if not blocked:
        held = find_held()
        for _size, cid, _title in held:
            add(cid)
        balances = exchange_holdings(client, history)
        for _size, cid in balances:
            add(cid)
    live = client.get_open_orders() if client else []
    for order in live:
        if isinstance(order, dict):
            add(order.get("market"))
    makers = []
    for cid in wanted:
        market = load_market(condition=cid)
        if market.get("condition") != cid:
            log(f"loaded {market.get('slug')} does not match open market {cid}")
            continue
        makers.append(Maker(market, client))
    keep = {maker.market["condition"] for maker in makers}
    if not cancel_foreign(client, keep):
        log("other-market orders remain; startup will retry")
        return [], False
    if not makers:
        live = client.get_open_orders() if client else []
    if makers:
        for maker in makers:
            _finish_startup(maker, client)
            resting = [o for o in live if isinstance(o, dict) and o.get("market") == maker.market["condition"]]
            log(f"staying on {maker.market['slug']} at startup | keeping {len(resting)} resting orders")
        _announce_resume(makers, held, balances)
        return makers, True
    if live and not wipe_orders(client):
        return [], False
    budget = _scan_budget(client)
    slots = market_slots(budget)
    spent = saved_spent()
    if slots > 1:
        note(tg(
            "🟢 Бот запущен",
            f"Касса <b>${budget:.0f}</b> · открою до <b>{slots}</b> рынков.",
            "На каждом те же заявки и тот же контроль, что на одном.",
            f"Резерв <b>${reserve_cash():.0f}</b> не ставлю. Размер заявки <b>{SIZE:g}</b>.",
            "",
            f"🛡 Лимит <b>${LOSS_LIMIT:.0f}</b> · потрачено <b>${spent:.2f}</b>",
        ))
        log(f"flat, slots {slots}, reserve ${reserve_cash():.0f}, "
            f"loss limit ${LOSS_LIMIT:.0f}, spent ${spent:.2f}")
    else:
        note(tg(
            "🟢 Бот запущен",
            "Сам ищет рынок.",
            "",
            f"🛡 Лимит <b>${LOSS_LIMIT:.0f}</b> · потрачено <b>${spent:.2f}</b>",
        ))
        log(f"flat, loss limit ${LOSS_LIMIT:.0f}, spent ${spent:.2f}")
    return [], True


def _announce_resume(makers, held, balances):
    """Tell Telegram which markets the restart picked up."""
    if len(makers) == 1 and (held or balances):
        maker = makers[0]
        size = next((item[0] for item in held if item[1] == maker.market["condition"]), None)
        if size is None:
            size = next((item[0] for item in balances if item[1] == maker.market["condition"]), 0)
        if held:
            log(f"holding {size:g} shares on {maker.market['question']}; watching it with the book")
            lines = [qmarket(maker.market["question"]), "", "Позицию веду."]
            if market_slots(_scan_budget(maker.client)) > 1:
                lines.append("Свободная касса может открыть другие рынки.")
        else:
            log(f"holding {size:g} shares via exchange balance on {maker.market['question']}; data api empty")
            lines = [qmarket(maker.market["question"]), "",
                     "Биржа показывает акции. Журнал позиций ещё пуст. Веду этот рынок."]
        missed = [title for _size, cid, title in held if cid not in {item.market["condition"] for item in makers}]
        if missed:
            lines.append(f"Не открыл {qmarket(missed[0])}: рынок не загрузился.")
        note(tg("📦 На руках позиция", *lines))
        return
    if len(makers) > 1:
        lines = [qmarket(maker.market["question"]) for maker in makers]
        note(tg(
            "📦 Слежу за несколькими рынками",
            *lines,
            "",
            "Каждый веду так же, как один: заявки, хедж и выход.",
        ))


def enter(row, client, maker):
    """Start quoting the chosen market. The loss total is kept."""
    global latest_share_line
    market = load_market(row["slug"])
    if row.get("order_size"):
        market["order_size"] = float(row["order_size"])
    if maker is None:
        maker = Maker(market, client)
    elif maker.market["condition"] != market["condition"]:
        maker.retarget(market)
    else:
        maker.market = market
    maker.entry_rate = market["rate"]
    maker.entry_daily = row["est_daily"]
    maker.entered_at = time.time()
    maker.misses = 0
    maker.rewarding = True
    maker.dead = None
    maker.live_share_pct = None
    maker.was_better = False
    maker.exit_requested = False
    maker.save_state()
    latest_share_line = f"📖 По стакану <b>{row['share']:.1f}%</b> · ~<b>${row['est_daily']:.1f}</b>/день"
    _share_lines[market["condition"]] = f"{qmarket(market['question'])} · {latest_share_line}"
    maker.share_pct = row["share"]
    maker.last_share_pct = row["share"]
    maker.last_share_notice = time.time()
    leave = market["end"] - rewards_scan.MIN_LIFE
    log(f"quoting {market['question']} | pool ${market['rate']:g} | "
        f"book share {row['share']:.1f}% ~${row['est_daily']:.2f}/day | size {row.get('order_size') or row['size']} | "
        f"leaves {leave:%Y-%m-%d %H:%M} UTC")
    note(tg(
        "🎯 Беру рынок",
        qmarket(market["question"]),
        "",
        f"💰 Пул <b>${market['rate']:g}</b>/день",
        f"📏 Минимум <b>{market['min_size']:g}</b>",
        "",
        "🚪 Уйду, если пул срежут, минимум станет не по карману или до конца останется меньше 3 дней.",
        f"⏰ Стоп <b>{(leave + timedelta(hours=3)):%d.%m %H:%M}</b> МСК",
        "",
        f"🛡 Лимит <b>${LOSS_LIMIT:.0f}</b> · потрачено <b>${maker.spent:.2f}</b>",
    ))
    return maker


class ScanJob:
    """A public-data worker with immutable inputs; all trading stays on the main loop."""
    def __init__(self, budget, conditions, orders):
        self.budget = budget
        if isinstance(conditions, str):
            conditions = (conditions,)
        self.conditions = tuple(cid for cid in (conditions or ()) if cid)
        self.condition = self.conditions[0] if len(self.conditions) == 1 else None
        self.results = queue.SimpleQueue()
        snapshot = tuple(dict(o) for o in orders)
        always = self.conditions[0] if len(self.conditions) == 1 else (self.conditions or None)
        def work():
            try:
                rows, counts = rewards_scan.search(
                    budget, order_size=SIZE, always=always,
                    offset=OFFSET, cushion=CUSHION, own_orders=snapshot)
                self.results.put((rows, counts, None))
            except Exception as exc:
                self.results.put((None, None, short_err(exc)))
        self.thread = threading.Thread(target=work, daemon=True, name="market-scan")
        self.thread.start()

    def poll(self):
        try:
            return self.results.get_nowait()
        except queue.Empty:
            return None


def _position_open(maker):
    """True when at least one share is held, False when flat, None if the read fails."""
    try:
        return not is_flat(maker)
    except Exception as exc:
        log(f"could not read position: {short_err(exc)}")
        return None


def choose(maker, client, scan=None):
    """Enter the best row when free; use two-scan comparisons for an occupied market."""
    if maker and (maker.pending_hedge or maker.unresolved_orders):
        maker.was_better = False
        maker.save_state()
        return maker
    if scan is not None:
        budget, rows, counts = scan
    else:
        budget = collateral(client)
        if budget is None:
            budget = float(os.getenv("SCAN_BUDGET", "30"))
        if maker:
            refresh_rules(maker, budget)
        try:
            scan_orders = client.get_open_orders() if client else []
            if maker:
                maker.observe_orders(scan_orders)
            rows, counts = rewards_scan.search(
                budget, order_size=SIZE, always=maker.market["condition"] if maker else None,
                offset=OFFSET, cushion=CUSHION, own_orders=scan_orders)
        except Exception as exc:
            log(f"scan failed: {short_err(exc)}")
            if maker:
                maker.was_better = False
                maker.save_state()
            return maker
    current_reason = counts.get("current_reason") if isinstance(counts, dict) else None
    skipped = {key: value for key, value in counts.items() if key != "current_reason"} if isinstance(counts, dict) else counts
    log(f"scan ${budget:.2f}: {len(rows)} fit | skip {skipped}"
        + (f" | current {current_reason}" if current_reason else ""))
    for row in rows[:3]:
        plan = (f" | plan Yes {row['yes_bid']:g}/No {row['no_bid']:g} x{row['size']:g}"
                if "yes_bid" in row else "")
        log(f"  book ${row['est_daily']:.2f}/day (not account share) {row['share']:.0f}% thin {row['thin']:.0f} "
            f"touch {row['touch_bid']:.0f}/{row['touch_ask']:.0f} {row['slug']}{plan}")
    if maker is None:
        if rows:
            log(rewards_scan.shadow_line(rewards_scan.shadow_entry(rows)))
        return enter(rows[0], client, None) if rows else None
    mine = next((row for row in rows if row["slug"] == maker.market.get("slug")), None)
    if mine:
        maker.entry_daily = mine["est_daily"]
    holding = _position_open(maker) if mine is None and current_reason == "cash" else False
    if maker.dead or (mine is None and rewards_scan.skip_is_confirmed(current_reason, holding=bool(holding))):
        maker.misses += 1
        if current_reason == "llm_veto":
            maker.misses = max(maker.misses, 2)  # leave now; wind-down keeps repair/hedge until flat
        log(f"current market miss {maker.misses}" + (f": {maker.dead}" if maker.dead else ""))
    elif mine is None:
        log(f"current market check inconclusive | {current_reason} | holding {holding}")
    else:
        maker.misses = 0
    # Orders may fill during the slow scan; read both payout and fills before cancelling.
    live = client.get_open_orders() if client else []
    ours = [o for o in live if o.get("market") == maker.market["condition"]]
    filled = maker.find_fills(ours)
    if filled:
        maker.fill_base = maker.prev_sizes
        maker.unsettled_since = time.time()
    maker.track(ours)
    # A remembered slug is not an occupied market. After the last leg is sold,
    # pick from this scan immediately instead of reopening the previous market.
    if (not live and not filled and not maker.risk_busy()
            and maker.unsettled_since is None and maker.fill_base is None
            and is_flat(maker)):
        maker.was_better = False
        maker.exit_requested = False
        maker.save_state()
        if not rows:
            log("flat with no resting orders; nothing qualifies, waiting")
            return None
        log(rewards_scan.shadow_line(rewards_scan.shadow_entry(rows)))
        log(f"flat with no resting orders; entering best scan row {rows[0]['slug']}")
        return enter(rows[0], client, maker)
    maker.reward_share(ours)
    previous_better = maker.was_better
    stay_daily = staying_daily(maker.live_share_pct, maker.market["rate"], maker.entry_daily)
    decision = switch_target(
        maker.market.get("slug"), stay_daily, maker.misses, maker.dead, rows, previous_better,
        dwell=time.time() - maker.entered_at)
    open_position = _position_open(maker)
    shadow = rewards_scan.shadow_choice(
        maker.market.get("slug"), stay_daily, rows,
        stay_is_live=maker.live_share_pct is not None,
        holding=open_position is not False, live_target=decision["target"])
    if rewards_scan.ECONOMIC_CHOICE and shadow.get("preferred") in ("away", "stay", "uncertain"):
        decision = switch_target(
            maker.market.get("slug"), stay_daily, maker.misses, maker.dead, rows, previous_better,
            better_override=shadow["preferred"] == "away", dwell=time.time() - maker.entered_at)
    log(rewards_scan.shadow_line(shadow))
    maker.was_better = decision["better"]
    target = decision["target"]
    maker.exit_requested = target is not None
    maker.save_state()
    decision["holding"] = target is not None and (filled or maker.risk_busy()
                                                or maker.unsettled_since is not None or not is_flat(maker))
    stay_text = f"${decision['stay_daily']:.2f}" if decision["stay_daily"] is not None else "unknown"
    away_text = f"${decision['away_daily']:.2f}" if decision["away_daily"] is not None else "none"
    log(f"market decision | stay {stay_text}/day ({'live' if maker.live_share_pct is not None else 'book'}) | "
        f"away {away_text}/day after x{SWITCH_DISCOUNT:g} | {decision['reason']}")
    other = decision["other"]
    if target is not None or (other and (decision["stay_daily"] is None or other["est_daily"] > decision["stay_daily"])):
        decision_note((maker.market["condition"], "market", decision["reason"]), market_decision_card(maker, decision))
    if target is None:
        log(f"staying on {maker.market.get('slug')}")
        return maker
    if decision["holding"]:
        log(f"staying on {maker.market.get('slug')} until flat | {decision['reason']}")
        return maker
    # Record our own cancellations before checking for fills that raced the request.
    maker.cancelled_ids.update(cancel_all(client, maker.market))
    live = [o for o in client.get_open_orders() if o.get("market") == maker.market["condition"]] if client else []
    filled = maker.find_fills(live)
    if filled:
        maker.fill_base = maker.prev_sizes
        maker.unsettled_since = time.time()
    maker.track(live)
    if live or filled or maker.risk_busy() or not is_flat(maker):
        log(f"staying on {maker.market.get('slug')} until cancellations and fills settle")
        return maker
    if target == "":
        why = maker.dead or "книга больше не подходит"
        note(tg(
            "🧹 Снял заявки",
            qmarket(maker.market["question"]),
            "",
            f"{esc(why)}.",
            "",
            "⏳ Подходящего рынка нет, жду.",
        ))
        log("leaving, nothing else qualifies")
        return None
    row = next(row for row in rows if row["slug"] == target)
    return enter(row, client, maker)


def _market_reasons(counts, makers):
    if not isinstance(counts, dict):
        return {}
    reasons = counts.get("reasons")
    if isinstance(reasons, dict):
        return reasons
    if len(makers) == 1 and "current_reason" in counts:
        return {makers[0].market["condition"]: counts.get("current_reason")}
    return {}


def _ours(live, maker):
    return [o for o in live if isinstance(o, dict) and o.get("market") == maker.market["condition"]]


def _idle_flat(maker, ours, filled):
    if ours or filled or maker.risk_busy() or maker.unsettled_since is not None or maker.fill_base is not None:
        return False
    return is_flat(maker)


def _account_scan(maker, rows, reason):
    mine = next((row for row in rows if row.get("slug") == maker.market.get("slug")), None)
    if mine:
        maker.entry_daily = mine["est_daily"]
    holding = _position_open(maker) if mine is None and reason == "cash" else False
    if maker.dead or (mine is None and rewards_scan.skip_is_confirmed(reason, holding=bool(holding))):
        maker.misses += 1
        if reason == "llm_veto":
            maker.misses = max(maker.misses, 2)  # leave now; wind-down keeps repair/hedge until flat
        log(f"current market miss {maker.misses}" + (f": {maker.dead}" if maker.dead else ""))
    elif mine is None:
        log(f"current market check inconclusive | {reason} | holding {holding}")
    else:
        maker.misses = 0
    return mine


def _release(maker, client):
    """Cancel this market. True only when it is flat and nothing is still resting."""
    maker.cancelled_ids.update(cancel_all(client, maker.market))
    live = client.get_open_orders() if client else []
    ours = _ours(live, maker)
    filled = maker.find_fills(ours)
    if filled:
        maker.fill_base = maker.prev_sizes
        maker.unsettled_since = time.time()
    maker.track(ours)
    if ours or filled or maker.risk_busy() or not is_flat(maker):
        log(f"staying on {maker.market.get('slug')} until cancellations and fills settle")
        return False
    maker.was_better = False
    maker.exit_requested = False
    maker.save_state()
    return True


def _skip_counts(counts):
    if not isinstance(counts, dict):
        return counts
    return {key: value for key, value in counts.items() if key not in ("current_reason", "reasons")}


def settle_book(makers, client, scan):
    """Keep each open market on its own rules, and fill empty slots from the same scan.

    One slot is the old single-market chooser. Extra cash opens more markets
    instead of pulling a healthy one to chase a second book.
    """
    budget, rows, counts = scan
    makers = [maker for maker in (makers or []) if maker is not None]
    if len(makers) <= 1 and market_slots(budget) <= 1 and not any(m.repair_only() for m in makers):
        nxt = choose(makers[0] if makers else None, client, scan)
        return [nxt] if nxt else []
    reasons = _market_reasons(counts, makers)
    current = reasons if len(reasons) != 1 else next(iter(reasons.values()))
    log(f"scan ${float(budget):.2f}: {len(rows)} fit | skip {_skip_counts(counts)}"
        + (f" | current {current}" if current else ""))
    for row in rows[:3]:
        plan = (f" | plan Yes {row['yes_bid']:g}/No {row['no_bid']:g} x{row['size']:g}"
                if "yes_bid" in row else "")
        log(f"  book ${row['est_daily']:.2f}/day (not account share) {row['share']:.0f}% thin {row['thin']:.0f} "
            f"touch {row['touch_bid']:.0f}/{row['touch_ask']:.0f} {row['slug']}{plan}")
    live = client.get_open_orders() if client else []
    occupied = []
    for maker in makers:
        if maker.pending_hedge or maker.unresolved_orders or maker.risk_busy():
            if maker.pending_hedge or maker.unresolved_orders:
                maker.was_better = False
                maker.save_state()
            occupied.append(maker)
            continue
        ours = _ours(live, maker)
        filled = maker.find_fills(ours)
        if filled:
            maker.fill_base = maker.prev_sizes
            maker.unsettled_since = time.time()
        maker.track(ours)
        if _idle_flat(maker, ours, filled):
            log(f"flat with no resting orders on {maker.market.get('slug')}; slot is free")
            maker.was_better = False
            maker.exit_requested = False
            maker.save_state()
            continue
        _account_scan(maker, rows, reasons.get(maker.market["condition"]))
        maker.reward_share(ours)
        occupied.append(maker)
    slots = market_slots(budget)
    room = len(occupied) < slots
    active = []
    voluntary = []
    held_ids = {maker.market["condition"] for maker in occupied}
    outsiders = [row for row in rows if row.get("condition") not in held_ids]
    for maker in occupied:
        if maker.pending_hedge or maker.unresolved_orders or maker.risk_busy():
            active.append(maker)
            continue
        stay_daily = staying_daily(maker.live_share_pct, maker.market["rate"], maker.entry_daily)
        decision = switch_target(
            maker.market.get("slug"), stay_daily, maker.misses, maker.dead, outsiders, maker.was_better,
            dwell=time.time() - maker.entered_at)
        shadow = rewards_scan.shadow_choice(
            maker.market.get("slug"), stay_daily, rows,
            stay_is_live=maker.live_share_pct is not None,
            holding=False, live_target=decision["target"])
        if rewards_scan.ECONOMIC_CHOICE and shadow.get("preferred") in ("away", "stay", "uncertain"):
            decision = switch_target(
                maker.market.get("slug"), stay_daily, maker.misses, maker.dead, outsiders, maker.was_better,
                better_override=shadow["preferred"] == "away", dwell=time.time() - maker.entered_at)
        if decision["target"] and decision["reason"] == "confirmed" and room:
            log(f"keeping {maker.market.get('slug')}; {decision['target']} fits in an open slot")
            decision = dict(decision, target=None, reason="room")
            maker.was_better = False
        else:
            maker.was_better = decision["better"]
        maker.exit_requested = decision["target"] is not None
        maker.save_state()
        log(rewards_scan.shadow_line(shadow))
        stay_text = f"${decision['stay_daily']:.2f}" if decision["stay_daily"] is not None else "unknown"
        away_text = f"${decision['away_daily']:.2f}" if decision["away_daily"] is not None else "none"
        log(f"market decision | {maker.market.get('slug')} | stay {stay_text}/day | "
            f"away {away_text}/day after x{SWITCH_DISCOUNT:g} | {decision['reason']}")
        if decision["reason"] in ("pending", "threshold", "confirmed", "dead", "misses", "no comparison"):
            other = decision["other"]
            if decision["target"] is not None or (
                    other and (decision["stay_daily"] is None or other["est_daily"] > decision["stay_daily"])):
                decision_note(
                    (maker.market["condition"], "market", decision["reason"]),
                    market_decision_card(maker, decision))
        if decision["target"] and decision["reason"] == "confirmed":
            voluntary.append((decision["stay_daily"] if decision["stay_daily"] is not None else 0, maker, decision))
            continue
        if decision["target"] is not None and decision["reason"] in ("dead", "misses"):
            if _release(maker, client):
                why = maker.dead or "книга больше не подходит"
                note(tg(
                    "🧹 Снял заявки",
                    qmarket(maker.market["question"]),
                    "",
                    f"{esc(why)}.",
                    "",
                    "Слот свободен.",
                ))
                log(f"leaving {maker.market.get('slug')}")
                continue
            active.append(maker)
            continue
        log(f"staying on {maker.market.get('slug')}")
        active.append(maker)
    if voluntary:
        voluntary.sort(key=lambda item: item[0])
        _stay, maker, decision = voluntary[0]
        target = next((row for row in rows if row.get("slug") == decision["target"]), None)
        freed = locked_bids(_ours(live, maker))
        fits = target is not None and quote_capital(target) <= (
            float(budget) - reserve_cash() - locked_bids(live) + freed + 1e-9)
        if fits and _release(maker, client):
            log(f"leaving {maker.market.get('slug')} for a stronger book")
        else:
            maker.exit_requested = False
            maker.save_state()
            log(f"staying on {maker.market.get('slug')}")
            active.append(maker)
        for _stay, maker, _decision in voluntary[1:]:
            maker.exit_requested = False
            maker.save_state()
            log(f"staying on {maker.market.get('slug')}")
            active.append(maker)
    live = client.get_open_orders() if client else []
    picked = pack_entries(rows, budget, {maker.market["condition"] for maker in active}, locked_bids(live),
                          repairing={maker.market["condition"] for maker in active if maker.repair_only()})
    log(f"portfolio ${float(budget):.2f} | slots {slots} | open {len(active)} | "
        f"adding {len(picked)} | reserve ${reserve_cash():.0f}")
    if picked and rows:
        log(rewards_scan.shadow_line(rewards_scan.shadow_entry(rows)))
    for row in picked:
        active.append(enter(row, client, None))
    return active


def _as_book(started):
    """startup may return one maker from an older caller, or the whole book."""
    if started is None:
        return []
    if isinstance(started, (list, tuple)):
        return [maker for maker in started if maker is not None]
    return [started]


def _book_blocked(makers):
    """No scan while a hedge order is in flight; a market that only rests a repair bid
    does not hold the rest of the book hostage."""
    if any(maker.pending_hedge or maker.unresolved_orders for maker in makers):
        return True
    return bool(makers) and all(maker.risk_busy() and not maker.repair_only() for maker in makers)


def _flat_orders(maker):
    return (maker.exit_requested and maker.prev_sizes is not None
            and not maker.risk_busy() and maker.unsettled_since is None
            and all(size < FLAT for size in maker.prev_sizes.values()))


WS_REPORT_EVERY = 600.0
POSTED_GRACE = 30.0
_unresolved_logged = set()
UNRESOLVED_GIVEUP = float(os.getenv("UNRESOLVED_GIVEUP_SECONDS", "120"))
UNCERTAIN_GIVEUP = float(os.getenv("UNCERTAIN_GIVEUP_SECONDS", "300"))
RULES_EVERY = float(os.getenv("RULES_REFRESH_SECONDS", "180"))
NEW_MARKET_RESCAN = float(os.getenv("NEW_MARKET_RESCAN_SECONDS", "1200"))
_new_market_seen = [0.0, 0.0]
WS_REST_CHECK = float(os.getenv("WS_REST_CHECK_SECONDS", "60"))
_ws_report = [time.monotonic(), 0]


def ws_follow(makers):
    """Point the feeds at the markets we quote; log REST rate and feed health every 10 min."""
    feed = ws_feed.FEED
    if feed is None:
        return
    feed.follow({m.market["yes"] for m in makers if m}, {m.market["condition"] for m in makers if m})
    now = time.monotonic()
    if now - _ws_report[0] >= WS_REPORT_EVERY:
        rate = (REST_COUNT[0] - _ws_report[1]) / ((now - _ws_report[0]) / 60)
        log(f"ws status | market {'up' if feed.fresh('market') else 'down'} {feed.updates} msgs | "
            f"user {'up' if feed.fresh('user') else 'down'} {feed.user_events} events/{feed.user_raw} msgs | "
            f"reconnects {feed.reconnects} | public REST {rate:.1f}/min")
        _ws_report[0], _ws_report[1] = now, REST_COUNT[0]


def run_auto():
    log(f"AUTO: up to {MAX_MARKETS} markets, each watched like one, "
        "only where others already quote both sides")
    log("DRY RUN: no orders will be sent" if DRY_RUN else "LIVE: orders will be sent")
    if not DRY_RUN and not os.getenv("PRIVATE_KEY"):
        sys.exit("PRIVATE_KEY is required when DRY_RUN=0")
    if not DRY_RUN and not telegram_configured():
        log("telegram is not configured")
    client = make_client() if os.getenv("PRIVATE_KEY") else None
    if ws_feed.ENABLED and client is not None and ws_feed.FEED is None:
        ws_feed.FEED = ws_feed.Feed(creds=getattr(client, "creds", None), log=log)
        ws_feed.FEED.start()
        log("ws feeds started (market book + user fills); REST stays the fallback")
    stop = threading.Event()
    health = LoopHealth()
    makers = []
    ready = False
    next_scan = 0.0
    last_wait = 0.0
    streak = 0
    scan_job = None
    try:
        if client and not DRY_RUN:
            threading.Thread(target=keep_alive, args=(client, stop, health), daemon=True).start()
            log(f"order watchdog active | loop {LOOP_STALE_AFTER:g}s | data {DATA_STALE_AFTER:g}s")
        while True:
            _dashboard("publish", makers=makers)
            if not ready:
                try:
                    makers, ready = startup(client)
                    makers = _as_book(makers)
                except Exception as exc:
                    log(f"startup failed: {short_err(exc)}")
                    time.sleep(POLL)
                    continue
                if not ready:
                    time.sleep(POLL)
                    continue
                health.progress(makers)
            now = time.time()
            failed = False
            for maker in list(makers):
                try:
                    maker.pulse()
                except Exception as exc:
                    failed = True
                    streak += 1
                    log(f"tick failed: {short_err(exc)}")
                    if streak == 1 or streak % 30 == 0:
                        note(tg("🔴 Цикл не проходит", f"<code>{esc(short_err(exc))}</code>"))
                else:
                    if _flat_orders(maker):
                        next_scan = 0.0
            if failed and streak >= MAX_ERRORS:
                log(f"{streak} consecutive failed ticks; cancelling all orders and halting")
                note(tg("🛑 Остановка", f"{streak} ошибок подряд. Заявки сняты, бот остановлен."))
                break
            if failed:
                health.progress(makers)
                time.sleep(FILL_POLL)
                continue
            streak = 0
            for maker in makers:
                raw_rules = getattr(maker, "rules_at", 0)
                try:
                    rules_due = now >= float(raw_rules)
                except (TypeError, ValueError):
                    rules_due = True
                if maker.risk_busy() or not rules_due:
                    continue
                maker.rules_at = now + RULES_EVERY
                was = maker.dead
                refresh_rules(maker)
                if maker.dead and maker.dead != was:
                    try:
                        flat_now = is_flat(maker)
                    except Exception:
                        flat_now = False
                    if flat_now:
                        next_scan = now
                    else:
                        note(tg(
                            "⚠️ Награда больше не подходит",
                            qmarket(maker.market["question"]),
                            "",
                            f"{esc(maker.dead)}.",
                            "",
                            "Позицию не бросаю.",
                        ))
            blocked = _book_blocked(makers)
            if scan_job is not None and not blocked:
                result = scan_job.poll()
                if result is not None:
                    job, scan_job = scan_job, None
                    rows, counts, error = result
                    open_ids = {maker.market["condition"] for maker in makers}
                    if set(job.conditions) != open_ids:
                        log("discarding scan for a previous market")
                    elif error:
                        log(f"scan failed: {error}")
                        for maker in makers:
                            maker.was_better = False
                            maker.save_state()
                    else:
                        try:
                            makers = settle_book(makers, client, (job.budget, rows, counts))
                        except Exception as exc:
                            log(f"switch failed: {short_err(exc)}")
                            note(tg("❌ Не смог сменить рынок", f"<code>{esc(short_err(exc))}</code>"))
            # A new market on the feed may carry a fresh reward pool: rescan early (REST stays the
            # source of reward params), at most once per NEW_MARKET_RESCAN seconds.
            seen = ws_feed.lifecycle["new_market_at"]
            if seen > _new_market_seen[0] and time.monotonic() - _new_market_seen[1] >= NEW_MARKET_RESCAN:
                _new_market_seen[0], _new_market_seen[1] = seen, time.monotonic()
                if scan_job is None and next_scan > now:
                    log("new market announced on ws; rescanning early")
                    next_scan = now
            for maker in makers:
                if maker.market["condition"] in ws_feed.lifecycle["resolved"]:
                    maker.rules_at = 0
            if now >= next_scan and scan_job is None and not blocked:
                next_scan = now + SCAN_SECONDS
                try:
                    budget = _scan_budget(client)
                    orders = client.get_open_orders() if client else []
                    conditions = [maker.market["condition"] for maker in makers]
                    scan_target = conditions[0] if len(conditions) == 1 else (conditions or None)
                    scan_job = ScanJob(budget, scan_target, orders)
                    log("scan started in background; execution checks continue")
                except Exception as exc:
                    log(f"could not start scan: {short_err(exc)}")
            if not makers and scan_job is None and now - last_wait > 3600:
                last_wait = now
                note(tg("⏳ Подходящего рынка нет", "В стакане пусто или мы были бы там почти одни.", "", "Жду."))
            health.progress(makers)
            ws_follow(makers)
            if ws_feed.FEED is None:
                time.sleep(FILL_POLL)
            else:
                # A user-channel order/trade event ends the wait at once: fill check + hedge run now.
                ws_feed.wake.wait(FILL_POLL)
                ws_feed.wake.clear()
    except KeyboardInterrupt:
        pass
    finally:
        cleaned = shutdown(client, stop, makers=makers)
        note(tg("⛔ Бот остановлен", "Заявки сняты." if cleaned else "Снятие заявок не подтверждено."))


def main():
    if AUTO:
        run_auto()
        return
    market = load_market()
    if not market["band"]:
        sys.exit("this market pays no liquidity rewards")
    stop_at = market["end"] - timedelta(hours=STOP_HOURS)
    if STOP_AT:
        at = datetime.fromisoformat(STOP_AT.replace("Z", "+00:00"))
        stop_at = min(stop_at, at if at.tzinfo else at.replace(tzinfo=UTC))
    log(f"{market['question']} | band ±{market['band'] * 100:.1f}c | min {market['min_size']:g} | "
        f"pool ${market['rate']:g}/day | stops {stop_at:%Y-%m-%d %H:%M} UTC")
    log("DRY RUN: no orders will be sent" if DRY_RUN else "LIVE: orders will be sent")
    if not DRY_RUN and not os.getenv("PRIVATE_KEY"):
        sys.exit("PRIVATE_KEY is required when DRY_RUN=0")
    client = make_client() if os.getenv("PRIVATE_KEY") else None
    stop = threading.Event()
    health = LoopHealth()
    maker = None
    try:
        if client and not DRY_RUN:
            threading.Thread(target=keep_alive, args=(client, stop, health), daemon=True).start()
            log(f"order watchdog active | loop {LOOP_STALE_AFTER:g}s | data {DATA_STALE_AFTER:g}s")
        maker = Maker(market, client)
        log(f"loss limit ${LOSS_LIMIT:.0f}, spent so far ${maker.spent:.2f}")
        if not DRY_RUN and not telegram_configured():
            log("telegram is not configured")
        msk = stop_at + timedelta(hours=3)
        pct = 0.0
        if client:
            try:
                pct = float(client.get_reward_percentages().get(market["condition"]) or 0)
            except Exception:
                pct = 0.0
        maker.share_pct = pct
        if pct > 0:
            maker.last_share_pct = pct
            maker.last_share_notice = time.time()
            global latest_share_line
            latest_share_line = describe_share(market["rate"], pct)
        note(tg(
            "🟢 Бот запущен",
            qmarket(market["question"]),
            "",
            f"💰 Пул <b>${market['rate']:g}</b>/день",
            f"⏰ Стоп <b>{msk:%d.%m %H:%M}</b> МСК",
            "",
            f"🛡 Лимит <b>${LOSS_LIMIT:.0f}</b> · потрачено <b>${maker.spent:.2f}</b>",
        ))
        streak = 0
        while datetime.now(UTC) < stop_at:
            try:
                maker.pulse()
            except Exception as exc:
                streak += 1
                log(f"tick failed: {short_err(exc)}")
                if streak == 1 or streak % 30 == 0:
                    note(tg("🔴 Цикл не проходит", f"<code>{esc(short_err(exc))}</code>"))
            else:
                streak = 0
                health.progress(maker)
            _dashboard("publish", makers=[maker])
            time.sleep(FILL_POLL)
        log(f"stop time {stop_at:%Y-%m-%d %H:%M} UTC reached, stopping")
    except KeyboardInterrupt:
        pass
    finally:
        cleaned = shutdown(client, stop, maker=maker, market=market)
        note(tg("⛔ Бот остановлен", "Заявки на этом рынке сняты." if cleaned
                else "Снятие заявок не подтверждено."))


if __name__ == "__main__":
    sys.exit(main())
