"""
token_scanner.py — detect launches, validate from the feed itself, score, and
hand qualifying tokens to the bot via an asyncio queue.

Loop: WebSocket (<1s) → rug check (feed data) → feed filters → feed score
      → queue. No DexScreener wait: the pair is often not indexed for 30-60s,
      and the bot buys on validated feed data instead (entry speed = edge).
      DexScreener becomes a monitoring source inside the trade.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from config import settings
from control import TradeGate
from data_stream import LaunchFeedRouter, TokenLaunch
from dexscreener import Pair
from monitoring import append_journal
from rug_detection import rug_check
from scanner_filter import passes_feed_filters
from scoring_algorithm import score_feed

log = logging.getLogger("sniper_bot.scanner")


@dataclass
class Candidate:
    """A token that passed validation — queued for the bot to buy.

    pair may be None: entry decisions use feed data only; DexScreener is
    fetched (best-effort) inside the trade for the monitor/cards.
    """

    launch: TokenLaunch
    pair: Optional[Pair]
    score: float
    scanned_at: str


async def process_launch(launch: TokenLaunch) -> Candidate | None:
    """Validate one launch from feed data only: rug → filters → score."""
    log.info(
        "NEW LAUNCH: %s ($%s) mint=%s dev=%sSOL mcap=%sSOL",
        launch.symbol,
        launch.name[:30],
        launch.mint[:8],
        launch.dev_sol,
        launch.market_cap_sol,
    )

    # 1) rug detection (any flag = skip) — feed-only; pair checks are skipped
    report = rug_check(launch, None, launch.raw)
    if not report.passed:
        log.info("RUG-FLAG SKIP %s — %s", launch.symbol, ", ".join(report.flags))
        return None

    # 2) feed-only filter thresholds (dev dump cap, initial buy, mcap cap)
    passed, failures = passes_feed_filters(launch)
    if not passed:
        log.info("FEED-FILTER SKIP %s — %s", launch.symbol, "; ".join(failures))
        return None

    # 3) feed scoring (provisional — DexScreener pair may not exist yet)
    score = score_feed(launch)
    if score < settings.min_score:
        log.info("SCORE SKIP %s — score=%.1f < min %.1f", launch.symbol, score, settings.min_score)
        return None

    candidate = Candidate(
        launch=launch,
        pair=None,
        score=score,
        scanned_at=datetime.now(timezone.utc).isoformat(),
    )
    log.info(
        "QUALIFIED %s (feed entry) — score=%.1f dev=%sSOL mcap=%sSOL",
        launch.symbol,
        score,
        launch.dev_sol,
        launch.market_cap_sol,
    )
    append_journal(
        {
            "type": "scan",
            "mint": launch.mint,
            "symbol": launch.symbol,
            "name": launch.name,
            "score": score,
            "pair": None,
            "price_sol": launch.raw.get("price"),
            "dev_sol": launch.dev_sol,
            "mcap_sol": launch.market_cap_sol,
        }
    )
    return candidate


async def scan_loop(
    queue: asyncio.Queue[Candidate],
    gate: Optional[TradeGate] = None,
    router: Optional[LaunchFeedRouter] = None,
) -> None:
    """Run the scanner forever. One scan window = MAX_SCAN_WINDOW minutes.

    Pauses when the trade gate is closed (telegram /stop or signal).
    `router` is shared with the LivePriceFeed (PumpAPI = 1 connection);
    if None, the scanner owns its own router.
    """
    own_router = router is None
    stream = router or LaunchFeedRouter()  # primary pumpapi, fallback pumpdev
    try:
        if own_router:
            await stream.start()
        while True:
            if gate is not None:
                await gate.wait()
                if gate.shutdown:
                    log.info("Gate shutdown — scanner exiting")
                    break
            cycle_start = time.monotonic()
            stats = {"launches": 0, "qualified": 0, "rug_skips": 0,
                     "filter_skips": 0, "score_skips": 0}
            log.info("=== New scan cycle started (window %d min) ===", settings.max_scan_window_min)
            async for launch in stream.launches():
                if gate is not None and (not gate.is_started() or gate.shutdown):
                    log.info("Gate closed — scanner paused")
                    break
                stats["launches"] += 1
                try:
                    candidate = await process_launch(launch)
                    if candidate is not None:
                        stats["qualified"] += 1
                        await queue.put(candidate)
                    else:
                        # classify the skip for the cycle report
                        report = rug_check(launch, None, launch.raw)
                        if not report.passed:
                            stats["rug_skips"] += 1
                        else:
                            passed, _ = passes_feed_filters(launch)
                            stats["filter_skips" if not passed else "score_skips"] += 1
                except Exception:  # noqa: BLE001 — never kill the scanner
                    log.exception("Error processing launch %s", launch.mint)
                # scan window expired? restart cycle
                if time.monotonic() - cycle_start > settings.max_scan_window_min * 60:
                    log.info("Scan window expired — starting new cycle")
                    break
            log.info("=== Cycle report: %d launches | %d qualified | %d rug | %d filter | %d score ===",
                     stats["launches"], stats["qualified"], stats["rug_skips"],
                     stats["filter_skips"], stats["score_skips"])
    finally:
        if own_router:
            await stream.stop()
        log.info("Scanner stopped")


if __name__ == "__main__":
    from monitoring import setup_logging

    setup_logging()
    settings.validate()
    q: asyncio.Queue[Candidate] = asyncio.Queue()
    asyncio.run(scan_loop(q))
