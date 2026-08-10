"""
token_scanner.py — detect launches, wait 25s for DexScreener, validate, score,
and hand qualifying tokens to the bot via an asyncio queue.

Loop: WebSocket (<1s) → wait 25s (DexScreener indexing) → rug check → filters
      → scoring → queue (scan window: 15 min, then new cycle).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone

from config import settings
from control import TradeGate
from data_stream import LaunchFeedRouter, TokenLaunch
from dexscreener import DexScreenerClient, Pair
from monitoring import append_journal
from rug_detection import rug_check
from scanner_filter import passes_filters
from scoring_algorithm import score_token

log = logging.getLogger("sniper_bot.scanner")

DEXSCREENER_INDEX_WAIT_S = 25  # course timing: wait for the pair to be indexed


@dataclass
class Candidate:
    """A token that passed validation — queued for the bot to buy."""
    launch: TokenLaunch
    pair: Pair
    score: float
    scanned_at: str


async def fetch_pair_with_retry(ds: DexScreenerClient, mint: str, attempts: int = 6) -> Pair | None:
    """Fetch the DexScreener pair, retrying every 5s (pair may not be indexed yet)."""
    for attempt in range(attempts):
        pairs = await ds.token_pairs(mint)
        pair = ds.pick_pair(pairs)
        if pair is not None and pair.liquidity_usd is not None:
            return pair
        log.info("Pair not indexed yet (attempt %d/%d) — retrying in 5s", attempt + 1, attempts)
        await asyncio.sleep(5)
    return None


async def process_launch(launch: TokenLaunch, ds: DexScreenerClient) -> Candidate | None:
    """Validate one launch: rug → filters → score. Returns a Candidate or None."""
    log.info("NEW LAUNCH: %s ($%s) mint=%s dev=%sSOL",
             launch.symbol, launch.name[:30], launch.mint[:8], launch.dev_sol)

    # 1) wait for DexScreener to index the pair (liquidity/volume data)
    await asyncio.sleep(DEXSCREENER_INDEX_WAIT_S)
    pair = await fetch_pair_with_retry(ds, launch.mint)
    if pair is None:
        log.info("SKIP %s — DexScreener never indexed the pair", launch.symbol)
        return None

    # 2) rug detection (any flag = skip)
    report = rug_check(launch, pair, launch.raw)
    if not report.passed:
        log.info("RUG-FLAG SKIP %s — %s", launch.symbol, ", ".join(report.flags))
        return None

    # 3) filter thresholds
    passed, failures = passes_filters(launch, pair)
    if not passed:
        log.info("FILTER SKIP %s — %s", launch.symbol, "; ".join(failures))
        return None

    # 4) scoring
    score = score_token(launch, pair)
    candidate = Candidate(
        launch=launch, pair=pair, score=score,
        scanned_at=datetime.now(timezone.utc).isoformat(),
    )
    log.info("QUALIFIED %s — score=%.1f liq=$%.0f vol5m=$%.0f b/s=%.2f price=$%s",
             launch.symbol, score, pair.liquidity_usd or 0, pair.volume_m5,
             pair.buy_sell_ratio, pair.price_usd)
    append_journal({"type": "scan", "mint": launch.mint, "symbol": launch.symbol,
                    "name": launch.name, "score": score, "pair": pair.pair_address,
                    "price_usd": pair.price_usd, "liquidity_usd": pair.liquidity_usd,
                    "volume_m5": pair.volume_m5})
    return candidate


async def scan_loop(queue: asyncio.Queue[Candidate],
                   gate: Optional[TradeGate] = None) -> None:
    """Run the scanner forever. One scan window = MAX_SCAN_WINDOW minutes.

    Pauses when the trade gate is closed (telegram /stop or signal).
    """
    ds = DexScreenerClient()
    stream = LaunchFeedRouter()  # primary pumpapi, fallback pumpdev
    try:
        while True:
            if gate is not None:
                await gate.wait()
                if gate.shutdown:
                    log.info("Gate shutdown — scanner exiting")
                    break
            cycle_start = time.monotonic()
            log.info("=== New scan cycle started (window %d min) ===", settings.max_scan_window_min)
            async for launch in stream.launches():
                if gate is not None and not gate.is_started():
                    log.info("Gate closed — scanner paused")
                    break
                try:
                    candidate = await process_launch(launch, ds)
                    if candidate is not None:
                        await queue.put(candidate)
                except Exception:  # noqa: BLE001 — never kill the scanner
                    log.exception("Error processing launch %s", launch.mint)
                # scan window expired? restart cycle
                if time.monotonic() - cycle_start > settings.max_scan_window_min * 60:
                    log.info("Scan window expired — starting new cycle")
                    break
    finally:
        await ds.close()


if __name__ == "__main__":
    from monitoring import setup_logging
    setup_logging()
    settings.validate()
    q: asyncio.Queue[Candidate] = asyncio.Queue()
    asyncio.run(scan_loop(q))
