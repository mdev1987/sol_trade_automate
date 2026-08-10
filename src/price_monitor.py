"""
Price monitoring & exit strategy — exit at 2x / 0.82x / dead pool / max hold.

Price sources (fastest first):
  1. LivePriceFeed  — PumpAPI buy/sell stream, sub-second (fresh <= 10s)
  2. DexScreener    — also supplies liquidity for the dead-pool check
  3. Jupiter Price API fallback

When a fresh live price is available the loop ticks every 1s; DexScreener is
still polled at POLL_INTERVAL for liquidity. MAX_HOLD_MIN force-exits a
position that never hits TP/SL/dead-pool (stuck-position watchdog).
(bot_plan/sample_bot/price_monitory_exit_strategy.py)
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from config import settings
from dexscreener import DexScreenerClient, Pair
from jupiter_swap import JupiterSwap

log = logging.getLogger("sniper_bot.monitor")

DEAD_POOL_LIQUIDITY_USD = 25.0  # pool is effectively dead below this
LIVE_TICK_S = 1.0               # fast loop when a fresh live price exists


@dataclass
class ExitSignal:
    exit: bool
    reason: str          # "take_profit" | "stop_loss" | "dead_pool" | "max_hold" | "none"
    price_usd: float | None
    liquidity_usd: float | None


class PriceMonitor:
    def __init__(self, dexscreener: DexScreenerClient, jupiter: JupiterSwap,
                 entry_price_usd: float, mint: str, live_feed=None):
        self.ds = dexscreener
        self.jupiter = jupiter
        self.entry_price_usd = entry_price_usd
        self.mint = mint
        self.live_feed = live_feed
        self.take_profit_price = entry_price_usd * settings.take_profit
        self.stop_loss_price = entry_price_usd * settings.stop_loss
        self.started_at = time.monotonic()
        self.max_hold_s = settings.max_hold_min * 60.0
        self._last_ds = 0.0          # DexScreener throttle (poll_interval cadence)
        self._cached_liq: float | None = None

    async def current_pair(self) -> Pair | None:
        pairs = await self.ds.token_pairs(self.mint)
        return self.ds.pick_pair(pairs)

    def _evaluate(self, price_usd: float, liquidity_usd: float | None) -> ExitSignal:
        """TP / SL / dead-pool / max-hold evaluation on the best known price."""
        if price_usd >= self.take_profit_price:
            return ExitSignal(True, "take_profit", price_usd, liquidity_usd)
        if price_usd <= self.stop_loss_price:
            return ExitSignal(True, "stop_loss", price_usd, liquidity_usd)
        if liquidity_usd is not None and liquidity_usd < DEAD_POOL_LIQUIDITY_USD:
            return ExitSignal(True, "dead_pool", price_usd, liquidity_usd)
        if time.monotonic() - self.started_at >= self.max_hold_s:
            return ExitSignal(True, "max_hold", price_usd, liquidity_usd)
        return ExitSignal(False, "none", price_usd, liquidity_usd)

    async def check(self) -> ExitSignal:
        """One cycle: fresh live price > DexScreener (throttled) > Jupiter."""
        price_usd = liquidity = None
        if self.live_feed is not None:
            price_usd = await self.live_feed.price_usd(self.mint)
        # DexScreener at most once per POLL_INTERVAL (rate limit ~60/min)
        if time.monotonic() - self._last_ds >= settings.poll_interval:
            pair = await self.current_pair()
            self._last_ds = time.monotonic()
            if pair is not None:
                self._cached_liq = pair.liquidity_usd
                if price_usd is None:
                    price_usd = pair.price_usd
        liquidity = self._cached_liq
        if price_usd is None:
            price_usd = await self.jupiter.price_usd(self.mint)
            if price_usd is not None:
                log.info("DexScreener/live miss — Jupiter fallback price: %s", price_usd)
        if price_usd is None:
            return ExitSignal(False, "none", None, liquidity)
        return self._evaluate(price_usd, liquidity)

    async def run_until_exit(self, on_price=None) -> ExitSignal:
        """Poll until an exit signal fires.

        Ticks every 1s while a fresh live price exists (DexScreener still
        polled at POLL_INTERVAL for liquidity), else every POLL_INTERVAL.
        """
        while True:
            signal = await self.check()
            if on_price:
                await on_price(signal)
            if signal.exit:
                return signal
            fast = self.live_feed is not None and await self.live_feed.price_usd(self.mint) is not None
            await asyncio.sleep(LIVE_TICK_S if fast else settings.poll_interval)
