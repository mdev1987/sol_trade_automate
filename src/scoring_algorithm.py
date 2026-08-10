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
