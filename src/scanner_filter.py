"""
Filter thresholds — tokens must pass all of these after rug checks.
(bot_plan/sample_bot/scanner_filter.py)
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from data_stream import TokenLaunch
from dexscreener import Pair


@dataclass
class Thresholds:
    """DexScreener-pair thresholds for the (legacy) pair-based filter."""
    max_age_minutes: float = 5.0  # older = easy gains gone
    min_liquidity_usd: float = 2_000.0
    max_liquidity_usd: float = 500_000.0
    min_volume_5m: float = 500.0
    min_txns_5m: int = 12
    min_buys_5m: int = 8
    min_buy_sell_ratio: float = 1.3
    max_market_cap: float = 10_000_000.0


@dataclass
class FeedThresholds:
    """Entry gates evaluated on PumpAPI create-event data only (no pair yet).

    Used by the feed-data entry path — the bot buys the moment a launch is
    validated instead of waiting for DexScreener to index the pair.
    """

    max_dev_sol: float = 3.0  # dev bought > 3 SOL = dump risk
    require_initial_buy: bool = True  # a dev with no buy = no skin in game
    max_mcap_sol: float = 50_000.0  # loose cap (~$7.5M @ $150/SOL)


def passes_feed_filters(
    launch: TokenLaunch, t: FeedThresholds | None = None
) -> tuple[bool, list[str]]:
    """Feed-only validation: dev dump cap, initial buy, metadata, mcap cap."""
    t = t or FeedThresholds()
    failures: list[str] = []

    if launch.dev_sol is None:
        failures.append("no_dev_sol")
    elif launch.dev_sol > t.max_dev_sol:
        failures.append(f"dev={launch.dev_sol:.1f}SOL>max{t.max_dev_sol:.0f}")

    if t.require_initial_buy and launch.initial_buy_tokens <= 0:
        failures.append("no_initial_buy")

    if not launch.symbol or not launch.name:
        failures.append("empty_meta")

    mcap = launch.market_cap_sol
    if mcap is not None and mcap > t.max_mcap_sol:
        failures.append(f"mcap={mcap:.0f}SOL>max{t.max_mcap_sol:.0f}")

    return (not failures, failures)


def age_minutes(pair: Pair) -> float | None:
    """Pair age in minutes from pairCreatedAt (ms epoch)."""
    if not pair.pair_created_at:
        return None
    return (time.time() * 1000 - pair.pair_created_at) / 60_000.0


def passes_filters(
    launch: TokenLaunch, pair: Pair, t: Thresholds | None = None
) -> tuple[bool, list[str]]:
    """Return (passed, failed_reasons). Fails on the first unmet threshold."""
    t = t or Thresholds()
    failures: list[str] = []

    age = age_minutes(pair)
    if age is not None and age > t.max_age_minutes:
        failures.append(f"age={age:.1f}m>max{t.max_age_minutes}m")

    if pair.liquidity_usd is None:
        failures.append("no_liquidity_data")
    else:
        if pair.liquidity_usd < t.min_liquidity_usd:
            failures.append(f"liq={pair.liquidity_usd:.0f}<min{t.min_liquidity_usd:.0f}")
        if pair.liquidity_usd > t.max_liquidity_usd:
            failures.append(f"liq={pair.liquidity_usd:.0f}>max{t.max_liquidity_usd:.0f}")

    if pair.volume_m5 < t.min_volume_5m:
        failures.append(f"vol5m={pair.volume_m5:.0f}<min{t.min_volume_5m:.0f}")
    if pair.txns_m5 < t.min_txns_5m:
        failures.append(f"txns5m={pair.txns_m5}<min{t.min_txns_5m}")
    if pair.txns_m5_buys < t.min_buys_5m:
        failures.append(f"buys5m={pair.txns_m5_buys}<min{t.min_buys_5m}")
    if pair.buy_sell_ratio < t.min_buy_sell_ratio:
        failures.append(f"b/s={pair.buy_sell_ratio:.2f}<min{t.min_buy_sell_ratio}")

    mcap = pair.market_cap if pair.market_cap is not None else pair.fdv
    if mcap is not None and mcap > t.max_market_cap:
        failures.append(f"mcap={mcap:.0f}>max{t.max_market_cap:.0f}")

    return (not failures, failures)
