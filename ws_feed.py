"""Polymarket CLOB websocket feeds, used as a fast path next to REST.

Market channel: live Yes-token books for the markets we quote (book snapshots
plus price_change deltas). User channel: our order/trade events, which only
wake the main loop so the REST fill check (and the FAK hedge) runs at once.
REST stays the source of truth whenever a feed is disconnected or stale.
"""

import asyncio
import json
import os
import random
import threading
import time

WS_BASE = os.getenv("WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws")
ENABLED = os.getenv("WS_ENABLED", "1") != "0"
PING_EVERY = 10.0
# A connection with no message or PONG for this long is treated as dead.
STALE_AFTER = float(os.getenv("WS_STALE_SECONDS", "25"))

wake = threading.Event()


def _levels(rows):
    out = {}
    for row in rows or []:
        try:
            price, size = round(float(row["price"]), 4), float(row["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if size > 0:
            out[price] = size
    return out


class Books:
    """Thread-safe local books keyed by asset id."""

    def __init__(self):
        self.lock = threading.Lock()
        self.books = {}

    def snapshot(self, asset, bids, asks):
        with self.lock:
            self.books[asset] = {"bid": _levels(bids), "ask": _levels(asks), "at": time.monotonic()}

    def change(self, asset, side, price, size):
        with self.lock:
            book = self.books.get(asset)
            if book is None:
                return False  # no snapshot yet: ignore deltas
            key = "bid" if str(side).upper() == "BUY" else "ask"
            price = round(float(price), 4)
            if float(size) <= 0:
                book[key].pop(price, None)
            else:
                book[key][price] = float(size)
            book["at"] = time.monotonic()
            return True

    def drop_all(self):
        with self.lock:
            self.books.clear()

    def get(self, asset):
        """REST-shaped book dict (bids ascending, asks descending like /book), or None."""
        with self.lock:
            book = self.books.get(asset)
            if book is None:
                return None
            bids = sorted(book["bid"].items())
            asks = sorted(book["ask"].items(), reverse=True)
        return {"asset_id": asset,
                "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
                "asks": [{"price": str(p), "size": str(s)} for p, s in asks]}


# Lifecycle events (custom_feature_enabled): monotonic time of the latest new_market,
# and condition ids seen in market_resolved.
lifecycle = {"new_market_at": 0.0, "new_markets": 0, "resolved": set()}


def handle_market(books, payload):
    """Apply one market-channel message (a dict or a list of them)."""
    items = payload if isinstance(payload, list) else [payload]
    for msg in items:
        if not isinstance(msg, dict):
            continue
        kind = msg.get("event_type")
        if kind == "new_market":
            lifecycle["new_market_at"] = time.monotonic()
            lifecycle["new_markets"] += 1
        elif kind == "market_resolved":
            if msg.get("market"):
                lifecycle["resolved"].add(msg["market"])
        elif kind == "book":
            books.snapshot(msg.get("asset_id"), msg.get("bids") or msg.get("buys"),
                           msg.get("asks") or msg.get("sells"))
        elif kind == "price_change":
            changes = msg.get("price_changes") or msg.get("changes") or []
            for ch in changes:
                asset = ch.get("asset_id") or msg.get("asset_id")
                try:
                    books.change(asset, ch["side"], ch["price"], ch["size"])
                except (KeyError, TypeError, ValueError):
                    continue
        _wake_on_move(books, msg.get("asset_id") or next(
            (c.get("asset_id") for c in (msg.get("price_changes") or msg.get("changes") or [])
             if isinstance(c, dict)), None))


WAKE_MOVE = 0.01
_wake_mid = {}


def _wake_on_move(books, asset):
    """A >=1c top-of-book mid move wakes the main loop so its fast-move guard runs now."""
    if not asset:
        return
    try:
        book = books.get(asset)
        if not book or not book["bids"] or not book["asks"]:
            return
        mid = (float(book["bids"][-1]["price"]) + float(book["asks"][-1]["price"])) / 2
    except (KeyError, TypeError, ValueError, IndexError):
        return
    last = _wake_mid.get(asset)
    if last is None or abs(mid - last) >= WAKE_MOVE - 1e-9:
        _wake_mid[asset] = mid
        if last is not None:
            wake.set()


def describe(payload):
    """Shape of a user message without ids, owners or keys."""
    items = payload if isinstance(payload, list) else [payload]
    parts = []
    for msg in items[:3]:
        if isinstance(msg, dict):
            parts.append(f"{msg.get('event_type')}/{msg.get('type')}/{msg.get('status')}")
        else:
            parts.append(type(msg).__name__)
    return ", ".join(parts)


# Latest order state seen on the user channel: id -> {"status", "size_matched", ...}.
orders = {}
_orders_lock = threading.Lock()


def order_state(order_id):
    """Order dict shaped like REST get_order (status, size_matched), from user events, or None."""
    with _orders_lock:
        state = orders.get(order_id)
        return dict(state) if state else None


def _remember_order(msg):
    oid = msg.get("id")
    if not oid:
        return
    status = str(msg.get("status") or "").upper()
    if str(msg.get("type") or "").upper() == "CANCELLATION" and not status:
        status = "CANCELED"
    with _orders_lock:
        prev = orders.get(oid, {})
        try:
            matched = max(float(msg.get("size_matched") or 0), float(prev.get("size_matched") or 0))
        except (TypeError, ValueError):
            matched = float(prev.get("size_matched") or 0)
        orders[oid] = {"id": oid, "status": status or prev.get("status") or "",
                       "size_matched": str(matched),
                       "original_size": msg.get("original_size") or prev.get("original_size"),
                       "associate_trades": msg.get("associate_trades") or prev.get("associate_trades") or []}
        if len(orders) > 2000:
            for key in list(orders)[:500]:
                orders.pop(key, None)


_trade_fills = {}  # (order_id, trade_id) -> matched amount, so MATCHED/MINED/CONFIRMED count once


def _remember_trade(msg):
    """A trade event carries our matched amount before REST open orders show it (seen 29.10:
    REST lagged ~2 min). Fold it into the order state so find_fills hedges at once."""
    tid = msg.get("id") or msg.get("trade_id")
    if not tid:
        return
    status = str(msg.get("status") or "").upper()
    if status in ("FAILED", "RETRYING"):
        return
    legs = []
    for maker in msg.get("maker_orders") or []:
        if isinstance(maker, dict) and maker.get("order_id"):
            legs.append((maker["order_id"], maker.get("matched_amount")))
    if msg.get("taker_order_id"):
        legs.append((msg["taker_order_id"], msg.get("size")))
    with _orders_lock:
        for oid, amount in legs:
            try:
                amount = float(amount or 0)
            except (TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            _trade_fills[(oid, tid)] = amount
            total = sum(v for (o, _), v in _trade_fills.items() if o == oid)
            prev = orders.get(oid, {})
            try:
                known = float(prev.get("size_matched") or 0)
            except (TypeError, ValueError):
                known = 0.0
            trades = list(prev.get("associate_trades") or [])
            if tid not in trades:
                trades.append(tid)
            orders[oid] = {"id": oid, "status": prev.get("status") or "MATCHED",
                           "size_matched": str(max(known, total)),
                           "original_size": prev.get("original_size"),
                           "associate_trades": trades}
        if len(_trade_fills) > 4000:
            for key in list(_trade_fills)[:1000]:
                _trade_fills.pop(key, None)


def ws_matched(order_id):
    """size_matched seen on the user channel for an order, or 0."""
    state = order_state(order_id)
    try:
        return float(state.get("size_matched") or 0) if state else 0.0
    except (TypeError, ValueError):
        return 0.0


def handle_user(payload, on_event=None):
    """Any order/trade event for our account wakes the main loop."""
    items = payload if isinstance(payload, list) else [payload]
    hit = False
    for msg in items:
        if isinstance(msg, dict) and msg.get("event_type") in ("trade", "order"):
            hit = True
            if msg.get("event_type") == "order":
                _remember_order(msg)
            else:
                _remember_trade(msg)
            if on_event:
                on_event(msg)
    if hit:
        wake.set()
    return hit


class Feed:
    """One background thread with an asyncio loop running both channels."""

    def __init__(self, creds=None, log=print, connect=None):
        self.creds = creds
        self.log = log
        self.books = Books()
        self.assets = frozenset()
        self.markets = frozenset()
        self.alive = {"market": 0.0, "user": 0.0}
        self.connected = {"market": False, "user": False}
        self.updates = 0
        self.user_events = 0
        self.user_raw = 0
        self._samples = 0
        self.reconnects = 0
        self._stop = threading.Event()
        self._changed = {"market": threading.Event(), "user": threading.Event()}
        self._connect = connect
        self.thread = None

    # ---- public API used by the bot ----
    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="ws-feed", daemon=True)
            self.thread.start()

    def stop(self):
        self._stop.set()

    def follow(self, assets, markets):
        assets, markets = frozenset(a for a in assets if a), frozenset(m for m in markets if m)
        if assets != self.assets:
            self.assets = assets
            self._changed["market"].set()
        if markets != self.markets:
            self.markets = markets
            self._changed["user"].set()

    def fresh(self, channel="market", now=None):
        now = time.monotonic() if now is None else now
        return self.connected[channel] and now - self.alive[channel] <= STALE_AFTER

    def book(self, asset, now=None):
        """Live book for `asset` when the market feed is healthy, else None (use REST)."""
        if asset not in self.assets or not self.fresh("market", now):
            return None
        return self.books.get(asset)

    # ---- internals ----
    def _run(self):
        asyncio.run(self._main())

    async def _main(self):
        tasks = [asyncio.create_task(self._channel("market"))]
        if self.creds is not None:
            tasks.append(asyncio.create_task(self._channel("user")))
        await asyncio.gather(*tasks)

    def _subscription(self, channel):
        if channel == "market":
            return {"assets_ids": sorted(self.assets), "type": "market", "custom_feature_enabled": True}
        c = self.creds
        # Whole account, no market filter: the bot filters by condition itself.
        return {"auth": {"apiKey": c.api_key, "secret": c.api_secret, "passphrase": c.api_passphrase},
                "type": "user"}

    async def _open(self, url):
        if self._connect is not None:
            return await self._connect(url)
        import websockets
        return await websockets.connect(url, ping_interval=None, open_timeout=15, max_size=2 ** 23)

    async def _channel(self, channel):
        attempt = 0
        while not self._stop.is_set():
            wanted = self.assets if channel == "market" else self.markets
            if not wanted:
                await asyncio.sleep(1)
                continue
            self._changed[channel].clear()
            try:
                ws = await self._open(f"{WS_BASE}/{channel}")
                try:
                    await ws.send(json.dumps(self._subscription(channel)))
                    self.connected[channel] = True
                    self.alive[channel] = time.monotonic()
                    self.log(f"ws {channel} connected | {len(wanted)} subscribed")
                    attempt = 0
                    await self._pump(ws, channel)
                finally:
                    self.connected[channel] = False
                    if channel == "market":
                        self.books.drop_all()
                    try:
                        await ws.close()
                    except Exception:
                        pass
            except Exception as exc:
                self.log(f"ws {channel} error: {type(exc).__name__}")
            if self._stop.is_set():
                break
            if self._changed[channel].is_set():
                continue  # resubscribe at once for a new market set
            self.reconnects += 1
            delay = min(60.0, 2 ** attempt) * (0.5 + random.random() / 2)
            attempt = min(attempt + 1, 6)
            await asyncio.sleep(delay)

    async def _pump(self, ws, channel):
        last_ping = 0.0
        while not self._stop.is_set() and not self._changed[channel].is_set():
            now = time.monotonic()
            if now - last_ping >= PING_EVERY:
                await ws.send("PING")
                last_ping = now
            if now - self.alive[channel] > STALE_AFTER:
                raise TimeoutError("stale")
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            self.alive[channel] = time.monotonic()
            if raw in ("PONG", b"PONG") or not raw:
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if channel == "market":
                handle_market(self.books, payload)
                self.updates += 1
            else:
                self.user_raw += 1
                if self._samples < 3:
                    self._samples += 1
                    self.log(f"ws user message: {describe(payload)}")
                if handle_user(payload):
                    self.user_events += 1


FEED = None
