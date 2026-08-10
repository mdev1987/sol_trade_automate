"""
Rug detection — any flag means the token is skipped.

Course checks (bot_plan/sample_bot/rug_detection.py) + extra on-chain
authority checks available from the feed (bot_plan/docs/pumpapi_stream_doc.md).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from data_stream import TokenLaunch
from dexscreener import Pair

# Scam name keywords (course + sample-code extensions)
SCAM_KEYWORDS = (
    "guaranteed",
    "100x",
    "airdrop",
    "giveaway",
    "presale",
    "safemoon",
)

# Dev bought more than this many SOL at launch → loaded bag, likely dumps
MAX_DEV_SOL = 20.0

# Honeypot: this many buys with fewer than this many sells → can't sell
HONEYPOT_MIN_BUYS = 50
HONEYPOT_MAX_SELLS = 2

# Wash trading: 5-min volume this many times liquidity → fake activity
WASH_TRADE_VOLUME_MULT = 50.0

# Post-migration pools: burned liquidity below this % = rug risk (PumpAPI field)
MIN_BURNED_LIQUIDITY_PCT = 30.0


@dataclass
class RugReport:
    passed: bool
    flags: list[str] = field(default_factory=list)


def check_scam_name(launch: TokenLaunch) -> list[str]:
    flags = []
    text = f"{launch.name} {launch.symbol}".lower()
    for kw in SCAM_KEYWORDS:
        if kw in text:
            flags.append(f"scam_name:'{kw}'")
    return flags


def check_dev_dump(launch: TokenLaunch) -> list[str]:
    if launch.dev_sol is not None and launch.dev_sol > MAX_DEV_SOL:
        return [f"dev_dump:{launch.dev_sol:.2f}SOL"]
    return []


def check_honeypot(pair: Pair | None) -> list[str]:
    if pair is None:
        return []
    if pair.txns_m5_buys >= HONEYPOT_MIN_BUYS and pair.txns_m5_sells < HONEYPOT_MAX_SELLS:
        return [f"honeypot:{pair.txns_m5_buys}b/{pair.txns_m5_sells}s"]
    return []


def check_wash_trading(pair: Pair | None) -> list[str]:
    if pair is None or not pair.liquidity_usd:
        return []
    if pair.liquidity_usd > 0 and pair.volume_m5 > WASH_TRADE_VOLUME_MULT * pair.liquidity_usd:
        return [f"wash_trading:vol={pair.volume_m5:.0f}/liq={pair.liquidity_usd:.0f}"]
    return []


def check_mayhem(launch: TokenLaunch) -> list[str]:
    if launch.is_mayhem_mode:
        return ["mayhem_mode"]  # AI agent gets 1B tokens — can dump before us
    return []


def check_authorities(raw: dict) -> list[str]:
    """On-chain authority checks available in PumpAPI-style events."""
    flags = []
    if raw.get("mintAuthority"):
        flags.append("mint_authority_set")  # can mint unlimited supply
    if raw.get("freezeAuthority"):
        flags.append("freeze_authority_set")  # can freeze = honeypot
    burned = raw.get("burnedLiquidity")
    if burned is not None:
        try:
            pct = float(str(burned).replace("%", ""))
            if pct < MIN_BURNED_LIQUIDITY_PCT:
                flags.append(f"low_burned_liquidity:{pct}%")
        except ValueError:
            pass
    fee = raw.get("poolFeeRate")
    if fee is not None:
        try:
            if float(fee) > 0.10:  # > 10% fee per trade = scam pool
                flags.append(f"high_pool_fee:{fee}")
        except (TypeError, ValueError):
            pass
    return flags


def rug_check(launch: TokenLaunch, pair: Pair | None, raw: dict | None = None) -> RugReport:
    """Run all rug checks. Any flag → token skipped."""
    flags: list[str] = []
    flags += check_scam_name(launch)
    flags += check_dev_dump(launch)
    flags += check_honeypot(pair)
    flags += check_wash_trading(pair)
    flags += check_mayhem(launch)
    if raw:
        flags += check_authorities(raw)
    return RugReport(passed=not flags, flags=flags)
