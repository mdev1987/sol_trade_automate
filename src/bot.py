"""
bot.py — consume validated candidates, buy via Jupiter, monitor price, sell,
then apply the compounding split and risk management.

Flow: buy (Jupiter /order + /execute via jupiter_swap.JupiterSwap) → monitor
      (8s DexScreener poll) → sell (2x TP / 0.82x SL / dead pool, slippage
      escalation 200→300→500→1000) → 60/40 split → update TradeStats.

Dry-run: the quote gate still runs against Jupiter's real /order API
(paper quoting — no taker, so Jupiter returns a verified route but never a
transaction), but nothing is ever signed or executed.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from config import settings, USDC_DECIMALS
from control import TradeGate
from dexscreener import DexScreenerClient
from jupiter_swap import JupiterSwap, SwapResult
from monitoring import append_journal, append_trade_log
from price_monitor import PriceMonitor
from risk_management import RiskManager
from stats import TradeStats
from telegram_bot import TelegramCommandBot
from token_scanner import Candidate

log = logging.getLogger("sniper_bot.bot")

PAPER_QUOTE_SENTINEL = "paper quote: no transaction to execute"

# simulated exit proceeds multipliers for dry-run (no real fill prices)
_DRY_PROCEEDS = {"take_profit": lambda s: s.take_profit,
                 "stop_loss": lambda s: s.stop_loss,
                 "dead_pool": lambda s: 0.0}


async def execute_trade(candidate: Candidate, risk: RiskManager,
                        jupiter: JupiterSwap, ds: DexScreenerClient,
                        notifier: TelegramCommandBot,
                        stats: Optional[TradeStats] = None) -> tuple[bool, str]:
    """One full trade cycle. Returns (won, exit_reason)."""
    amount = risk.play_amount
    mint = candidate.launch.mint

    log.info("=== TRADE %s — buying $%.2f of %s (score %.1f) ===",
             candidate.launch.symbol, amount, mint[:8], candidate.score)

    # entry price + liquidity before the buy — liquidity drives the quote
    # gate's dynamic slippage tier.
    pair = ds.pick_pair(await ds.token_pairs(mint))
    entry_price = pair.price_usd if pair else await jupiter.price_usd(mint)
    if entry_price is None:
        log.error("No entry price for %s — aborting trade", candidate.launch.symbol)
        append_journal({"type": "trade", "mint": mint, "symbol": candidate.launch.symbol,
                        "side": "buy", "status": "failed", "error": "no_entry_price"})
        if stats:
            stats.record_buy_failure(candidate.launch.symbol, "no_entry_price")
        return False, "no_entry_price"
    liquidity_usd = pair.liquidity_usd if pair else 0.0

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
            append_journal({"type": "trade", "mint": mint, "symbol": candidate.launch.symbol,
                            "side": "buy", "status": "failed", "error": buy_result.error})
            if stats:
                stats.record_buy_failure(candidate.launch.symbol, buy_result.error)
            return False, "buy_failed"
    else:
        tokens_raw = buy_result.output_amount

    # 2) MONITOR until exit
    monitor = PriceMonitor(ds, jupiter, entry_price, mint)
    log.info("Entry $%.8f — TP $%.8f / SL $%.8f — monitoring every %ds",
             entry_price, monitor.take_profit_price, monitor.stop_loss_price,
             settings.poll_interval)
    if stats:
        stats.record_buy(mint, candidate.launch.symbol, amount, entry_price,
                         monitor.take_profit_price, monitor.stop_loss_price)
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
            append_journal({"type": "trade", "mint": mint, "symbol": candidate.launch.symbol,
                            "side": "sell", "status": "failed", "error": str(exc)})
            if stats:
                stats.record_sell_failure(candidate.launch.symbol, str(exc))
            return False, "sell_failed"

    won = signal.reason == "take_profit" and sell_result.success
    exit_reason = signal.reason

    # proceeds + PnL (real fill amounts, or simulated in dry-run)
    if not simulated:
        proceeds_usd = sell_result.output_amount / (10**USDC_DECIMALS)
    else:
        factor = _DRY_PROCEEDS.get(exit_reason, lambda s: 0.0)(settings)
        proceeds_usd = amount * factor
    pnl_usd = proceeds_usd - amount

    # 4) compounding + risk update
    next_amount = risk.record_result(won)
    log.info("RESULT: %s (%s) — next trade $%.2f", "WIN" if won else "LOSS",
             exit_reason, next_amount)
    if stats:
        stats.record_exit(won, pnl_usd, proceeds_usd, exit_reason,
                          signal.price_usd or 0.0, sell_result.signature)

    append_trade_log({
        "mint": mint, "symbol": candidate.launch.symbol, "action": "buy+sell",
        "side": "close", "amount_usd": amount, "entry_price": entry_price,
        "exit_price": signal.price_usd, "pnl_usd": round(pnl_usd, 6),
        "exit_reason": exit_reason, "score": candidate.score,
        "signature": sell_result.signature, "dry_run": settings.dry_run,
    })
    append_journal({"type": "trade", "mint": mint, "symbol": candidate.launch.symbol,
                    "status": "closed", "won": won, "exit_reason": exit_reason,
                    "entry_price": entry_price, "exit_price": signal.price_usd,
                    "play_amount": amount, "next_amount": next_amount,
                    "pnl_usd": round(pnl_usd, 6), "dry_run": settings.dry_run})
    await notifier.notify(
        f"**{'✅ WIN' if won else '❌ LOSS'}** {candidate.launch.symbol}\n"
        f"{exit_reason} @ ${signal.price_usd}\n"
        f"PnL `{pnl_usd:+.2f}` · play `${amount:.2f}` → next `${next_amount:.2f}`"
    )
    return won, exit_reason


async def trade_loop(queue: asyncio.Queue[Candidate], gate: TradeGate,
                     stats: Optional[TradeStats] = None,
                     notifier: Optional[TelegramCommandBot] = None) -> None:
    """Consume validated candidates forever, one trade at a time.

    - pauses when the gate is closed (telegram /stop or signal);
    - an in-flight trade always runs to completion (graceful);
    - loss-pause circuit breaker from RiskManager still applies.
    """
    jupiter = JupiterSwap()
    ds = DexScreenerClient()
    risk = RiskManager()                     # persistent: pause + play amount
    if notifier is None:
        from telegram_bot import build_telegram_bot
        notifier = build_telegram_bot(gate, stats or TradeStats(dry_run=settings.dry_run))
    try:
        while True:
            await gate.wait()                # paused until /start or auto-start
            await risk.wait_if_paused()      # loss-pause circuit breaker
            candidate = await queue.get()
            try:
                await execute_trade(candidate, risk, jupiter, ds, notifier, stats)
            except Exception:  # noqa: BLE001 — one bad trade must not kill the loop
                log.exception("Trade cycle failed for %s", candidate.launch.mint)
    finally:
        await jupiter.close()
        await ds.close()


if __name__ == "__main__":
    from monitoring import setup_logging
    setup_logging()
    settings.validate()
    q: asyncio.Queue[Candidate] = asyncio.Queue()
    gate = TradeGate(auto_start=settings.auto_start)
    stats = TradeStats(dry_run=settings.dry_run)
    asyncio.run(trade_loop(q, gate, stats))
