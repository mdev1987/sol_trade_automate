"""
main.py — run scanner + bot + telegram command bot together (the full system).

    Detect → validate → score → buy → monitor → sell → compound. Repeat 24/7.

Hardening (v2):
  - Single-instance lock: a second `main.py` exits immediately (flock).
  - LivePriceFeed: PumpAPI buy/sell stream for sub-second TP/SL triggers.
  - Heartbeat: periodic /status card every STATUS_INTERVAL_MIN (watchdog).
  - Feed-data entry: scanner no longer waits for DexScreener to index.

Control:
  - SIGINT/SIGTERM or Telegram /stop → graceful shutdown: the gate closes,
    the in-flight trade finishes, clients close, process exits 0.
  - Telegram /start → reopen the gate (resume trading).
  - Telegram /status → balance, winrate, PnL, active position (markdown).
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time as _t

from bot import trade_loop
from config import settings
from control import TradeGate
from data_stream import LaunchFeedRouter
from live_feed import LivePriceFeed
from monitoring import setup_logging
from singleton import SingleInstanceLock
from stats import TradeStats
from telegram_bot import build_telegram_bot
from token_scanner import Candidate, scan_loop

log = setup_logging()

# how long we wait for an in-flight trade to finish before hard-cancelling
GRACEFUL_TRADE_TIMEOUT_S = 90


async def heartbeat_loop(notifier, stats, t0) -> None:
    """Periodic /status card — if this goes silent, the bot is stuck/dead."""
    interval = settings.status_interval_min * 60
    if interval <= 0:
        return
    while True:
        await asyncio.sleep(interval)
        try:
            await notifier.send_status(
                _t.monotonic() - t0, stats.trades, stats.winrate * 100.0,
                stats.realized_pnl_usd, stats.balance_usd, stats.exit_counts,
                quotes=stats.quote_gate,
            )
            log.info("Heartbeat status card sent")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — a failed card must not kill main
            log.exception("Heartbeat card failed")


async def main() -> None:
    settings.validate()

    queue: asyncio.Queue[Candidate] = asyncio.Queue()
    gate = TradeGate(auto_start=settings.auto_start)
    stats = TradeStats(dry_run=settings.dry_run)
    t0 = _t.monotonic()
    stop = asyncio.Event()

    def _shutdown(*_) -> None:  # noqa: ANN002 — SIGINT/SIGTERM handler
        if not stop.is_set():  # signals can arrive twice (timeout/uv forwarding)
            log.info("Shutdown signal received — closing the gate")
            stop.set()

    async def _telegram_stop() -> None:
        # called by /stop after the gate closes: same graceful path as signals
        stop.set()

    notifier = build_telegram_bot(gate, stats, on_stop=_telegram_stop)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:  # Windows
            pass

    # gate starts closed? then telegram /start opens it — log the hint
    if not gate.is_started():
        log.info("AUTO_START=false — trading paused; send /start to the bot to begin")

    # telegram: verify credentials + start command polling (PTB background task)
    await notifier.test()
    await notifier.start_polling()

    def _log_task_error(task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception() is not None:
            log.error("Task %s died: %s", task.get_name(), task.exception())

    # ONE PumpAPI connection, shared: scanner (creates) + live feed (trades)
    router = LaunchFeedRouter()
    await router.start()

    # live price feed (buy/sell stream on the shared hub → sub-second TP/SL)
    live_feed = LivePriceFeed(trades=router.trades())
    await live_feed.start()

    scanner_task = asyncio.create_task(scan_loop(queue, gate, router), name="scanner")
    bot_task = asyncio.create_task(
        trade_loop(queue, gate, stats, notifier, live_feed), name="bot")
    heartbeat_task = asyncio.create_task(
        heartbeat_loop(notifier, stats, t0), name="heartbeat")
    for t in (scanner_task, bot_task, heartbeat_task):
        t.add_done_callback(_log_task_error)

    await notifier.send_startup(
        f"AUTO_START=`{settings.auto_start}` · DRY_RUN=`{settings.dry_run}` "
        f"· play `${settings.starting_amount:g}` USDC · TP `{settings.take_profit:g}x` "
        f"SL `{settings.stop_loss:g}x` · daily cap `-${settings.daily_loss_limit:g}`"
    )

    await stop.wait()
    log.info("Graceful shutdown: finishing in-flight trade (≤ %ds)…", GRACEFUL_TRADE_TIMEOUT_S)
    await gate.stop()          # no new trades
    gate.request_shutdown()    # wake idle loops so they exit immediately
    try:
        await asyncio.wait_for(bot_task, timeout=GRACEFUL_TRADE_TIMEOUT_S)
    except asyncio.TimeoutError:
        log.warning("In-flight trade exceeded %ds — cancelling", GRACEFUL_TRADE_TIMEOUT_S)
    for task in (scanner_task, bot_task, heartbeat_task):
        task.cancel()
    await asyncio.gather(scanner_task, bot_task, heartbeat_task, return_exceptions=True)
    await live_feed.stop()
    await router.stop()
    await notifier.send_stopped(
        _t.monotonic() - t0, stats.trades, stats.winrate * 100.0,
        stats.realized_pnl_usd, stats.balance_usd, stats.exit_counts,
        skips="", quotes=stats.quote_gate,
    )
    await notifier.close()
    log.info("Bye. (final stats: %s trades, %d wins, pnl $%.2f, balance $%.2f)",
             stats.trades, stats.wins, stats.realized_pnl_usd, stats.balance_usd)


if __name__ == "__main__":
    lock = SingleInstanceLock()
    if not lock.acquire():
        log.error("Another bot instance is already running (%s) — exiting", lock.path)
        sys.exit(1)
    try:
        asyncio.run(main())
    finally:
        lock.release()
