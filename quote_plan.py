"""Pure quote planning shared by the scanner and the live maker. No I/O or orders."""

import math
import json
from datetime import datetime


def market_rules(market, info, configs, slug=None):
    """Use identical Gamma/CLOB reward rules for scanning and live quoting."""
    yes, no = json.loads(market["clobTokenIds"])
    fee = info.get("fd") or {}
    rate = reward_min = 0.0
    band = (market.get("rewardsMaxSpread") or 0) / 100
    for cfg in configs:
        reward_min = max(reward_min, float(cfg.get("rewards_min_size") or 0))
        if cfg.get("rewards_max_spread"):
            band = float(cfg["rewards_max_spread"]) / 100
        rate += sum(float(r.get("rate_per_day") or 0) for r in cfg.get("rewards_config") or [])
    min_order = float(info.get("mos") or 5)
    return {
        "question": market["question"], "slug": slug or market["slug"],
        "condition": market["conditionId"], "yes": yes, "no": no, "band": band,
        "min_size": max(market.get("rewardsMinSize") or 0, reward_min, min_order),
        "min_order": min_order, "tick": float(info["mts"]),
        "fee_rate": float(fee.get("r") or 0), "fee_exp": float(fee.get("e") or 0), "rate": rate,
        "neg_risk": bool(info.get("nr") or market.get("negRisk")),
        "end": datetime.fromisoformat(market["endDate"].replace("Z", "+00:00")),
    }


def floor2(x):
    return math.floor(x * 100 + 1e-9) / 100


def reserve(market, price, size):
    """Cash the exchange locks for a BUY: its cost plus the taker fee it could pay."""
    return size * (price + market["fee_rate"] * (price * (1 - price)) ** market["fee_exp"])


def remaining(order):
    return float(order["original_size"]) - float(order.get("size_matched") or 0)


def resting_size_kept(left, target, min_size, *, exiting):
    """Whether a resting order still matches the size that should be on the book.

    Ordinary reward quotes have to stay at or above the market's minimum size.
    A 10% band would keep 18 shares of a 20-share quote, and an order below the
    minimum scores nothing. Closing a position is different: the size is the
    inventory, matched to the cent, and it may sit below that reward minimum.
    """
    closing = bool(exiting) or float(target) + 1e-9 < float(min_size)
    if closing:
        return abs(float(left) - float(target)) < 0.01
    if float(left) + 1e-9 < float(min_size):
        return False
    return 0.9 * float(target) - 1e-9 <= float(left) <= 1.1 * float(target) + 1e-9


def scoring_sides(orders, mid, band, min_size):
    """Yes-book sides that currently score. None when the rule inputs are missing.

    `orders` are (book_side, price, size). A side scores only for size at or
    above the reward minimum and strictly inside the max spread. The spread
    boundary itself does not score, matching the book scanner.
    """
    if mid is None or band is None or min_size is None or band <= 0:
        return None
    sides = set()
    for book_side, price, size in orders:
        if size + 1e-9 < min_size:
            continue
        distance = (mid - price) if book_side == "bid" else (price - mid)
        if distance < -1e-9 or distance >= band - 1e-12:
            continue
        sides.add(book_side)
    return sides


def reward_eligible(mid, band, min_size, orders):
    """Whether these orders earn a reward sample under the current program.

    Both sides score at any midpoint. One side scores only while the midpoint
    is inside [0.10, 0.90]; outside that range a single side earns nothing.
    Returns None when the midpoint or the market's reward rules are unknown.
    """
    sides = scoring_sides(orders, mid, band, min_size)
    if sides is None:
        return None
    if not sides:
        return False
    if sides >= {"bid", "ask"}:
        return True
    return 0.10 - 1e-12 <= mid <= 0.90 + 1e-12


def yes_book_side(market, order):
    """Where an order rests in the Yes book, which also lists No orders mirrored at 1 - price."""
    price = float(order["price"])
    if order["asset_id"] == market["yes"]:
        return ("bid" if order["side"] == "BUY" else "ask"), price
    return ("ask" if order["side"] == "BUY" else "bid"), round(1 - price, 4)


def others_book(book, ours, market):
    """Yes-book levels without our own orders, best price first on each side."""
    levels = {"bid": {}, "ask": {}}
    for side, key in (("bid", "bids"), ("ask", "asks")):
        for lvl in book.get(key) or []:
            price = round(float(lvl["price"]), 4)
            levels[side][price] = levels[side].get(price, 0.0) + float(lvl["size"])
    for o in ours:
        side, price = yes_book_side(market, o)
        if price in levels[side]:
            levels[side][price] -= remaining(o)
    bids = sorted(((p, s) for p, s in levels["bid"].items() if s > 1e-6), reverse=True)
    asks = sorted((p, s) for p, s in levels["ask"].items() if s > 1e-6)
    return bids, asks


def depth_price(levels, size):
    total = 0.0
    for price, s in levels:
        total += s
        if total >= size:
            return price
    return None


def midpoint(bids, asks, best_bid, best_ask, min_size):
    """Midpoint between the prices where other makers' bids and asks reach min_size of depth,
    or of the whole book's top when they are thinner than that."""
    bid, ask = depth_price(bids, min_size), depth_price(asks, min_size)
    if bid is None or ask is None:
        bid, ask = best_bid, best_ask
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2


def pick(levels, mid, side, market, limit, offset=0.02, cushion=20):
    """Yes-book price for our bid or ask: the closest to mid with at least CUSHION shares of
    other makers resting at it or better, otherwise OFFSET from mid. Never at or past
    `limit`, the best price on the other side of the book."""
    tick = market["tick"]
    off = min(offset, market["band"] - tick)
    if side == "bid":
        ks = range(math.ceil(mid / tick - 1e-9) - 1, math.floor((mid - off) / tick + 1e-9) - 1, -1)
    else:
        ks = range(math.floor(mid / tick + 1e-9) + 1, math.ceil((mid + off) / tick - 1e-9) + 1)
    prices = [round(k * tick, 4) for k in ks if 0 < k < round(1 / tick)]
    prices = [p for p in prices if (p < limit if side == "bid" else p > limit)]
    for p in prices:
        ahead = sum(s for q, s in levels if (q >= p if side == "bid" else q <= p))
        if ahead >= cushion:
            return p
    return prices[-1] if prices else None



# Cash kept free beyond the two opening bids, so a fill can be hedged at a worse price.
HEDGE_MARGIN = 1.15


def opening_orders(market, bid_px, ask_px, cash, requested_size):
    """The actual two BUY orders a flat maker can fund at its chosen prices, leaving hedge margin."""
    if not bid_px or not ask_px:
        return {}
    no_bid = round(1 - ask_px, 4)
    if no_bid <= 0:
        return {}
    per_pair = reserve(market, bid_px, 1) + reserve(market, no_bid, 1)
    size = floor2(min(max(requested_size, market["min_size"]), cash / HEDGE_MARGIN / per_pair))
    # Whole shares: a fractional size like 24.76 cannot be fully hedged by a FAK at most
    # prices (USDC amounts are whole cents), which blocked quoting and hopped markets (29.10).
    if math.floor(size + 1e-9) >= market["min_size"]:
        size = float(math.floor(size + 1e-9))
    # Never post an opening quote below the reward minimum: it earns nothing and can still fill.
    if size < market["min_size"] or not market.get("min_size"):
        return {}
    return {(market["yes"], "BUY"): (bid_px, size), (market["no"], "BUY"): (no_bid, size)}
