"""
bot.py — consume validated candidates, buy via Jupiter, monitor price, sell,
then apply the compounding split and risk management.

Flow: buy (Jupiter /order + /execute via jupiter_swap.JupiterSwap) → monitor
      (live PumpAPI price + 8s DexScreener liquidity poll) → sell (2x TP /
      0.82x SL / dead pool / max hold, slippage escalation 200→300→500→1000)
      → 60/40 split → update TradeStats.

Entry: feed-data path — the buy is attempted the moment a launch passes feed
validation (no 25s DexScreener wait). The entry price is the real fill price
when token decimals are known, else DexScreener / Jupiter / live-feed price.

Dry-run: the quote gate still runs against Jupiter's real /order API
(paper quoting — no taker, so Jupiter returns a verified route but never a
transaction), but nothing is ever signed or executed.
"""

from __future__ import annotations

import asyncio
import logging
import time as _t
from typing import Optional

from config import USDC_DECIMALS, settings
from control import TradeGate
from dexscreener import DexScreenerClient
from jupiter_swap import JupiterSwap, SwapResult
from live_feed import LivePriceFeed
from monitoring import append_journal, append_trade_log
from price_monitor import PriceMonitor
from risk_management import RiskManager
from stats import TradeStats
from telegram_bot import TelegramNotifier
from token_scanner import Candidate

log = logging.getLogger("sniper_bot.bot")

PAPER_QUOTE_SENTINEL = "paper quote: no transaction to execute"

# simulated exit proceeds multipliers for dry-run (no real fill prices).
# Unknown reasons (max_hold / shutdown) sell at the last known price instead.
_DRY_PROCEEDS = {
    "take_profit": lambda s, _sig: s.take_profit,
    "stop_loss": lambda s, _sig: s.stop_loss,
    "dead_pool": lambda s, _sig: 0.0,
}


async def _estimate_entry_price(
    mint: str,
    launch,
    jupiter: JupiterSwap,
    ds: DexScreenerClient,
    live_feed: LivePriceFeed | None,
    pair,
) -> tuple[float | None, float]:
    """Best available USD price before/without a fill: pair > jupiter > live > feed-SOL.

    Returns (price_usd, liquidity_usd).
    """
    price = pair.price_usd if pair else None
    liquidity = (pair.liquidity_usd or 0.0) if pair else 0.0
    if price is None:
        price = await jupiter.price_usd(mint)
    if price is None and live_feed is not None:
        price = await live_feed.price_usd(mint)
    if price is None:
        raw_price = launch.raw.get("price")
        if raw_price and live_feed is not None:
            sol = await live_feed.sol_usd()
            if sol:
                price = float(raw_price) * sol
    return price, liquidity


async def _quick_pair(ds: DexScreenerClient, mint: str):
    """One best-effort DexScreener lookup (pair may not be indexed yet)."""
    try:
        pairs = await ds.token_pairs(mint)
        return ds.pick_pair(pairs)
    except Exception:  # noqa: BLE001 — never block the entry on this
        return None


async def execute_trade(
    candidate: Candidate,
    risk: RiskManager,
    jupiter: JupiterSwap,
    ds: DexScreenerClient,
    notifier: TelegramNotifier,
    stats: Optional[TradeStats] = None,
    live_feed: Optional[LivePriceFeed] = None,
    stop_check=None,
) -> tuple[bool, str]:
    """stop_check: callable()->bool — monitor exits "shutdown" when True so a
    graceful stop sells the open position instead of hard-cancelling it."""
    """One full trade cycle. Returns (won, exit_reason)."""
    amount = risk.play_amount
    mint = candidate.launch.mint

    log.info(
        "=== TRADE %s — buying $%.2f of %s (score %.1f) ===",
        candidate.launch.symbol,
        amount,
        mint[:8],
        candidate.score,
    )

    # --- entry price estimate BEFORE the buy (fast, one DexScreener call) ---
    pair = await _quick_pair(ds, mint)
    entry_price, liquidity_usd = await _estimate_entry_price(
        mint, candidate.launch, jupiter, ds, live_feed, pair
    )
    if entry_price is None or entry_price <= 0:
        log.error("No entry price for %s — aborting trade", candidate.launch.symbol)
        append_journal(
            {
                "type": "trade",
                "mint": mint,
                "symbol": candidate.launch.symbol,
                "side": "buy",
                "status": "failed",
                "error": "no_entry_price",
            }
        )
        if stats:
            stats.record_buy_failure(candidate.launch.symbol, "no_entry_price")
        return False, "no_entry_price"

    # 1) BUY via Jupiter (quote gate + managed execute)
    buy_result: SwapResult = await jupiter.buy(mint, amount, liquidity_usd)
    simulated = False
    if not buy_result.success:
        if settings.dry_run and buy_result.error == PAPER_QUOTE_SENTINEL:
            # Paper quoting verified a real route but assembled no transaction —
            # that IS the dry-run buy. Simulate the token amount from price.
            log.info("DRY-RUN: quote verified for %s @ $%.6f (no execute)", mint, entry_price)
            tokens_raw = int(amount / entry_price * 1_000_000)  # simulate 6-dec token
            simulated = True
        else:
            log.error("BUY FAILED for %s: %s", candidate.launch.symbol, buy_result.error)
            append_journal(
                {
                    "type": "trade",
                    "mint": mint,
                    "symbol": candidate.launch.symbol,
                    "side": "buy",
                    "status": "failed",
                    "error": buy_result.error,
                }
            )
            if stats:
                stats.record_buy_failure(candidate.launch.symbol, buy_result.error)
            return False, "buy_failed"
    else:
        tokens_raw = buy_result.output_amount
        # refine entry price from the REAL fill when token decimals are known
        decimals = candidate.launch.raw.get("decimals")
        if decimals is not None and tokens_raw > 0:
            fill_price = (buy_result.input_amount / 10**USDC_DECIMALS) / (
                tokens_raw / 10 ** int(decimals)
            )
            if fill_price > 0:
                entry_price = fill_price
                log.info("Fill-based entry price: $%.8f", entry_price)

    # 2) MONITOR until exit (live feed for sub-second TP/SL, DexScreener liq)
    monitor = PriceMonitor(
        ds,
        jupiter,
        entry_price,
        mint,
        live_feed=live_feed if settings.live_feed_exit else None,
        stop_check=stop_check,
    )
    log.info(
        "Entry $%.8f — TP $%.8f / SL $%.8f — monitoring (live feed %s)",
        entry_price,
        monitor.take_profit_price,
        monitor.stop_loss_price,
        "on" if monitor.live_feed else "off",
    )
    if stats:
        stats.record_buy(
            mint,
            candidate.launch.symbol,
            amount,
            entry_price,
            monitor.take_profit_price,
            monitor.stop_loss_price,
        )
        age_s = _t.time() - candidate.launch.created_at if candidate.launch.created_at else 0.0
        await notifier.send_buy(
            mint,
            candidate.launch.symbol,
            candidate.score,
            entry_price,
            liquidity_usd,
            pair.volume_m5 if pair else 0.0,
            pair.buy_sell_ratio if pair else 0.0,
            age_s,
            amount,
            stats.balance_usd,
        )
    signal = await monitor.run_until_exit()
    log.info("EXIT SIGNAL: %s @ $%s", signal.reason, signal.price_usd)

    # 3) SELL via Jupiter (slippage escalation 200→300→500→1000)
    if settings.dry_run and simulated:
        sell_result = SwapResult(True, "dry-run-sig", 0, 0, "")
    else:
        try:
            sell_result = await jupiter.sell(mint, tokens_raw)
        except Exception as exc:  # noqa: BLE001
            log.exception("SELL FAILED for %s", candidate.launch.symbol)
            append_journal(
                {
                    "type": "trade",
                    "mint": mint,
                    "symbol": candidate.launch.symbol,
                    "side": "sell",
                    "status": "failed",
                    "error": str(exc),
                }
            )
            if stats:
                stats.record_sell_failure(candidate.launch.symbol, str(exc))
            try:
                await notifier.send_alert("SELL FAILED", f"{candidate.launch.symbol} — {exc}")
            except Exception:  # noqa: BLE001
                pass
            return False, "sell_failed"

    exit_reason = signal.reason

    # proceeds + PnL (real fill amounts, or simulated in dry-run)
    if not simulated:
        proceeds_usd = sell_result.output_amount / (10**USDC_DECIMALS)
    else:
        factor_fn = _DRY_PROCEEDS.get(exit_reason)
        if factor_fn is None:
            # max_hold / shutdown: sell at the last known price
            factor = (signal.price_usd / entry_price) if signal.price_usd else 0.0
        else:
            factor = factor_fn(settings, signal)
        proceeds_usd = amount * factor
    pnl_usd = proceeds_usd - amount
    won = sell_result.success and proceeds_usd > amount

    # 4) compounding + risk update
    next_amount = risk.record_result(won)
    log.info(
        "RESULT: %s (%s) — next trade $%.2f", "WIN" if won else "LOSS", exit_reason, next_amount
    )
    hold_s = 0.0
    if stats:
        opened_at = stats.active_position.get("opened_at", 0.0)
        hold_s = _t.monotonic() - opened_at if opened_at else 0.0
        stats.record_exit(
            won, pnl_usd, proceeds_usd, exit_reason, signal.price_usd or 0.0, sell_result.signature
        )
        roi_pct = (pnl_usd / amount * 100.0) if amount else 0.0
        await notifier.send_sell(
            mint,
            candidate.launch.symbol,
            exit_reason,
            pnl_usd,
            roi_pct,
            amount,
            proceeds_usd,
            hold_s,
            stats.balance_usd,
        )

    append_trade_log(
        {
            "mint": mint,
            "symbol": candidate.launch.symbol,
            "action": "buy+sell",
            "side": "close",
            "amount_usd": amount,
            "entry_price": entry_price,
            "exit_price": signal.price_usd,
            "pnl_usd": round(pnl_usd, 6),
            "exit_reason": exit_reason,
            "score": candidate.score,
            "signature": sell_result.signature,
            "dry_run": settings.dry_run,
        }
    )
    append_journal(
        {
            "type": "trade",
            "mint": mint,
            "symbol": candidate.launch.symbol,
            "status": "closed",
            "won": won,
            "exit_reason": exit_reason,
            "entry_price": entry_price,
            "exit_price": signal.price_usd,
            "play_amount": amount,
            "next_amount": next_amount,
            "pnl_usd": round(pnl_usd, 6),
            "dry_run": settings.dry_run,
        }
    )
    return won, exit_reason


async def _wait_daily_reset(stats: TradeStats, gate: TradeGate, notifier: TelegramNotifier) -> None:
    """Daily-loss kill switch: alert once, then idle until UTC midnight."""
    log.warning(
        "DAILY LOSS LIMIT HIT (today %.2f) — halted until UTC midnight", stats.daily_pnl_usd
    )
    try:
        await notifier.send_alert(
            "DAILY LOSS LIMIT", f"Today {stats.daily_pnl_usd:+.2f} — halted until UTC midnight"
        )
    except Exception:  # noqa: BLE001
        pass
    while not gate.shutdown:
        wait = stats.next_day_reset_seconds()
        if wait <= 0:
            return
        try:
            await asyncio.wait_for(asyncio.sleep(wait), timeout=60.0)
        except asyncio.TimeoutError:
            continue  # re-check shutdown + recalculate
    if gate.shutdown:
        log.info("Gate shutdown during daily halt — exiting")


async def trade_loop(
    queue: asyncio.Queue[Candidate],
    gate: TradeGate,
    stats: Optional[TradeStats] = None,
    notifier: Optional[TelegramNotifier] = None,
    live_feed: Optional[LivePriceFeed] = None,
) -> None:
    """Consume validated candidates forever, one trade at a time.

    - pauses when the gate is closed (telegram /stop or signal);
    - an in-flight trade always runs to completion (graceful);
    - loss-pause circuit breaker and daily-loss kill switch apply.
    """
    jupiter = JupiterSwap()
    ds = DexScreenerClient()
    risk = RiskManager()  # persistent: pause + play amount
    if notifier is None:
        from telegram_bot import build_telegram_bot

        notifier = build_telegram_bot(gate, stats or TradeStats(dry_run=settings.dry_run))
    try:
        while True:
            await gate.wait()  # paused until /start, auto-start, or shutdown
            if gate.shutdown:
                log.info("Gate shutdown — trade loop exiting")
                break
            if stats is not None and stats.daily_loss_limit_hit():
                await _wait_daily_reset(stats, gate, notifier)
                if gate.shutdown:
                    break
            await _wait_risk_paused(risk, gate)  # loss-pause breaker, shutdown-aware
            candidate = await _queue_get(queue, gate)
            if candidate is None:
                break  # shutdown while waiting for a candidate
            # queue-backlog aging: a candidate this old has long lost its edge
            if candidate.launch.created_at:
                age_s = _t.time() - candidate.launch.created_at
                if age_s > settings.max_candidate_age_min * 60:
                    if stats is not None:
                        stats.aged_out += 1
                    log.info("SKIP %s — candidate %.0fs old > %.0fs max (queue backlog)",
                             candidate.launch.symbol, age_s,
                             settings.max_candidate_age_min * 60)
                    append_journal({"type": "scan", "mint": candidate.launch.mint,
                                    "symbol": candidate.launch.symbol,
                                    "status": "aged_out", "age_s": round(age_s, 1)})
                    continue
            try:
                await execute_trade(
                    candidate,
                    risk,
                    jupiter,
                    ds,
                    notifier,
                    stats,
                    live_feed,
                    stop_check=lambda: gate.shutdown,
                )
                if stats is not None:
                    stats.quote_gate = jupiter.quote_summary()
            except Exception:  # noqa: BLE001 — one bad trade must not kill the loop
                log.exception("Trade cycle failed for %s", candidate.launch.mint)
    finally:
        await jupiter.close()
        await ds.close()


async def _queue_get(queue: asyncio.Queue, gate: TradeGate):
    """Get a candidate, waking every second to notice shutdown (queue may be idle)."""
    while True:
        try:
            return await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            if gate.shutdown:
                return None


async def _wait_risk_paused(risk: RiskManager, gate: TradeGate) -> None:
    """Wait out a loss pause, waking every second to notice shutdown."""
    while True:
        try:
            await asyncio.wait_for(risk.wait_if_paused(), timeout=1.0)
            return
        except asyncio.TimeoutError:
            if gate.shutdown:
                return


if __name__ == "__main__":
    from monitoring import setup_logging

    setup_logging()
    settings.validate()
    q: asyncio.Queue[Candidate] = asyncio.Queue()
    gate = TradeGate(auto_start=settings.auto_start)
    stats = TradeStats(dry_run=settings.dry_run)
    asyncio.run(trade_loop(q, gate, stats))
