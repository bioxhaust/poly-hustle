"""Pure plans for reducing an inventory imbalance. Never sends orders."""

import math

from quote_plan import floor2


def fee_bound(market, price, side):
    """Upper bound per filled share, including price improvement toward 0.5."""
    rate, exponent = market["fee_rate"], market["fee_exp"]
    if rate <= 0:
        return 0.0
    peak = min(price, 0.5) if side == "BUY" else max(price, 0.5)
    return rate * (peak * (1 - peak)) ** exponent


def marketable_size(price, size, side):
    """Largest 0.01-share size at or below `size` that a FAK will be accepted at.

    A market buy pays USDC to 2 decimals and receives shares to 4. A market sell
    gives shares to 2 decimals and receives USDC to 4. The order builder also
    floors the share size to 2 decimals, so a size such as 13.04 at 0.62 is
    rejected before it can hedge anything.
    """
    hundredths = math.floor(float(size) * 100 + 1e-9)
    price_units = int(round(float(price) * 10000))
    if price_units <= 0 or hundredths <= 0:
        return 0.0
    if side == "BUY":
        step = 10000 // math.gcd(price_units, 10000)
    else:
        step = 100 // math.gcd(price_units, 100)
    hundredths -= hundredths % step
    return hundredths / 100


def plan(market, token, size, basis, bids, asks, cash, allowance, prefer_exit=False,
         max_unit_loss=None, pair_only=False, sell_only=False):
    """Choose the greatest affordable reduction, then its cheapest execution.

    `basis` bounds the cost of every unmatched share. A limit order's worst
    price and fee must fit the allowance even if the entire order fills there.
    Equal-cost choices build a pair, or flatten when leaving the market.
    `max_unit_loss` drops any level whose loss per share is above that cap.
    `pair_only` considers buying the other outcome. `sell_only` considers
    selling the excess. The live bot uses one of those, not both.
    """
    size = floor2(size)
    minimum, tick = market["min_order"], market["tick"]
    if size < minimum or basis <= 0 or not math.isfinite(basis):
        return None
    other = market["no"] if token == market["yes"] else market["yes"]
    if token == market["yes"]:
        buy_levels = [(round(1 - p, 4), q) for p, q in bids]
        sell_levels = bids
    else:
        buy_levels = asks
        sell_levels = [(round(1 - p, 4), q) for p, q in asks]
    choices = []
    routes = []
    if not sell_only:
        routes.append(("BUY", other, buy_levels))
    if not pair_only:
        routes.append(("SELL", token, sell_levels))
    for side, asset, levels in routes:
        depth = 0.0
        for price, quantity in sorted(levels, reverse=side == "SELL"):
            if not (tick <= price <= 1 - tick) or quantity <= 0:
                continue
            depth += quantity
            fee = fee_bound(market, price, side)
            unit_cost = basis + price + fee - 1 if side == "BUY" else basis - price + fee
            unit_loss = max(0.0, unit_cost)
            if max_unit_loss is not None and unit_loss > max_unit_loss + 1e-6:
                continue
            allowed_size = min(size, depth)
            if unit_loss > 1e-12:
                allowed_size = min(allowed_size, max(0.0, allowance) / unit_loss)
            if side == "BUY":
                allowed_size = min(allowed_size, max(0.0, cash) / (price + fee))
            allowed_size = marketable_size(price, floor2(allowed_size), side)
            if allowed_size < minimum:
                continue
            choices.append({"token": asset, "side": side, "price": price, "size": allowed_size,
                            "fee_per_share": fee, "loss_bound": unit_loss * allowed_size,
                            "cost_bound": unit_cost * allowed_size})
            if depth >= size:
                break
    if not choices:
        return None
    preferred = "SELL" if prefer_exit else "BUY"
    return min(choices, key=lambda p: (-p["size"], round(p["cost_bound"], 9), p["side"] != preferred))
