"""
Price monitoring & exit strategy — poll every 8s, sell at 2x / 0.82x / dead pool.

Primary price source: DexScreener (also gives liquidity).
Fallback: Jupiter Price API.
(bot_plan/sample_bot/price_monitory_exit_strategy.py)
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from config import settings
from dexscreener import DexScreenerClient, Pair
from jupiter_swap import JupiterSwap

log = logging.getLogger("sniper_bot.monitor")

DEAD_POOL_LIQUIDITY_USD = 25.0  # pool is effectively dead below this


@dataclass
class ExitSignal:
    exit: bool
    reason: str          # "take_profit" | "stop_loss" | "dead_pool" | "none"
    price_usd: float | None
    liquidity_usd: float | None


class PriceMonitor:
    def __init__(self, dexscreener: DexScreenerClient, jupiter: JupiterSwap,
                 entry_price_usd: float, mint: str):
        self.ds = dexscreener
        self.jupiter = jupiter
        self.entry_price_usd = entry_price_usd
        self.mint = mint
        self.take_profit_price = entry_price_usd * settings.take_profit
        self.stop_loss_price = entry_price_usd * settings.stop_loss

    async def current_pair(self) -> Pair | None:
        pairs = await self.ds.token_pairs(self.mint)
        return self.ds.pick_pair(pairs)

    async def check(self) -> ExitSignal:
        """One poll cycle: DexScreener first, Jupiter price as fallback."""
        pair = await self.current_pair()
        price = pair.price_usd if pair else None
        liquidity = pair.liquidity_usd if pair else None

        if price is None:
            price = await self.jupiter.price_usd(self.mint)
            log.info("DexScreener miss — Jupiter fallback price: %s", price)

        if price is None:
            return ExitSignal(False, "none", None, liquidity)

        if price >= self.take_profit_price:
            return ExitSignal(True, "take_profit", price, liquidity)
        if price <= self.stop_loss_price:
            return ExitSignal(True, "stop_loss", price, liquidity)
        if liquidity is not None and liquidity < DEAD_POOL_LIQUIDITY_USD:
            return ExitSignal(True, "dead_pool", price, liquidity)
        return ExitSignal(False, "none", price, liquidity)

    async def run_until_exit(self, on_price=None) -> ExitSignal:
        """Poll every POLL_INTERVAL seconds until an exit signal fires."""
        while True:
            signal = await self.check()
            if on_price:
                await on_price(signal)
            if signal.exit:
                return signal
            await asyncio.sleep(settings.poll_interval)
