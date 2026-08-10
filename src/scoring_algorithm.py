"""
Token scoring — 0..100. Higher score = stronger buy signal.
(bot_plan/sample_bot/scoring_algorithm.py)

Weights: Freshness 10 + Volume 20 + Buy pressure 25 + Fair launch 10 + Liquidity 15.
"""

from __future__ import annotations

import math

from data_stream import TokenLaunch
from dexscreener import Pair
from scanner_filter import age_minutes


def score_freshness(pair: Pair) -> float:
    """0-10: <2 min old = 10, <5 min = 5, else 0."""
    age = age_minutes(pair)
    if age is None:
        return 0.0
    if age < 2.0:
        return 10.0
    if age < 5.0:
        return 5.0
    return 0.0


def score_volume(pair: Pair) -> float:
    """0-20: log10-scaled, $100K 5-min volume = max points."""
    v = pair.volume_m5
    if v <= 0:
        return 0.0
    return min(20.0, 20.0 * math.log10(v) / math.log10(100_000.0))


def score_buy_pressure(pair: Pair) -> float:
    """0-25: (buy/sell ratio / 5) x 25, ratio capped at 5x."""
    ratio = pair.buy_sell_ratio
    if ratio == float("inf"):
        return 25.0
    return min(25.0, ratio / 5.0 * 25.0)


def score_fair_launch(launch: TokenLaunch) -> float:
    """0-10: dev bought 0 SOL = 10, <5 = 6, <20 = 3, >=20 = 0."""
    dev = launch.dev_sol
    if dev is None:
        return 0.0
    if dev <= 0.0:
        return 10.0
    if dev < 5.0:
        return 6.0
    if dev < 20.0:
        return 3.0
    return 0.0


def score_liquidity(pair: Pair) -> float:
    """0-15: liquidity in the $5,000-$50,000 sweet spot = 15."""
    liq = pair.liquidity_usd
    if liq is None:
        return 0.0
    if 5_000.0 <= liq <= 50_000.0:
        return 15.0
    return 0.0


def score_token(launch: TokenLaunch, pair: Pair) -> float:
    """Total score out of 100 (listed weights sum to 80; normalized scale kept)."""
    return round(
        score_freshness(pair)
        + score_volume(pair)
        + score_buy_pressure(pair)
        + score_fair_launch(launch)
        + score_liquidity(pair),
        1,
    )


def score_feed(launch: TokenLaunch) -> float:
    """Provisional feed-only score (0-80) for the feed-data entry path.

    Used when the DexScreener pair isn't indexed yet, so volume/liquidity
    are proxied from the create event: dev buy SOL, initial-buy share of
    supply, and market-cap-in-SOL. Same 80-point scale as score_token.
    """
    dev = launch.dev_sol if launch.dev_sol is not None else 0.0
    supply_raw = launch.raw.get("supply")
    supply = float(supply_raw) if supply_raw and float(supply_raw) > 0 else None
    share = (
        launch.initial_buy_tokens / supply
        if supply is not None and launch.initial_buy_tokens > 0
        else None
    )

    s = 0.0
    s += 10.0  # freshness: at-launch entry
    s += score_fair_launch(launch)  # 0-10 dev fairness

    # volume proxy (25): meaningful-but-not-whale initial buy = full points
    if share is None:
        s += 15.0
    elif 0.0005 <= share <= 0.05:
        s += 25.0
    elif share < 0.0005:
        s += 12.0
    else:
        s += 5.0

    # mcap positioning (15): low-mid mcap = room to run
    mcap = launch.market_cap_sol if launch.market_cap_sol is not None else 0.0
    if 0.0 < mcap <= 200.0:
        s += 15.0
    elif mcap <= 800.0:
        s += 10.0
    elif mcap <= 3000.0:
        s += 5.0

    # buy pressure proxy (20): smaller dev buy = more room for real buyers
    if dev <= 0.5:
        s += 20.0
    elif dev <= 1.0:
        s += 15.0
    elif dev <= 2.0:
        s += 10.0
    elif dev <= 3.0:
        s += 5.0

    return round(min(s, 80.0), 1)
