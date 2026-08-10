"""Live price feed — subscribes to PumpAPI buy/sell events and tracks the
latest SOL-denominated price per mint.

This gives the exit monitor sub-second TP/SL triggers instead of waiting up
to POLL_INTERVAL seconds for the DexScreener poll (the single biggest edge
improvement after entry speed). DexScreener remains the liquidity/dead-pool
source, polled at its own cadence.

Also caches a SOL->USD conversion (Jupiter /price/v3, 60s TTL) so the
SOL-denominated pump prices can be compared against USD entry/exit levels.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

import httpx
import websockets

from config import SOL_MINT, settings

log = logging.getLogger("sniper_bot.live_feed")

# events we accept: buy/sell on the pump pool carry {"price": <SOL per token>}
TRADE_ACTIONS = ("buy", "sell")
PRICE_MAX_AGE_S = 10.0      # a price older than this is stale
SOL_USD_TTL_S = 60.0
RECONNECT_BASE_S = 1.0
RECONNECT_MAX_S = 30.0


class LivePriceFeed:
    """Background task: keep {mint: (price_sol, ts)} fresh from PumpAPI."""

    def __init__(self, url: str | None = None) -> None:
        self.url = url or settings.pumpapi_ws_url
        self._prices: dict[str, tuple[float, float]] = {}
        self._sol_usd: float | None = None
        self._sol_usd_ts = 0.0
        self._task: asyncio.Task | None = None
        self._client = httpx.AsyncClient(timeout=15.0)

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="live-feed")
            log.info("LivePriceFeed started (%s)", self.url)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._client.aclose()

    # ------------------------------------------------------------------ loop
    async def _run(self) -> None:
        delay = RECONNECT_BASE_S
        while True:
            try:
                async with websockets.connect(self.url, open_timeout=15) as ws:
                    delay = RECONNECT_BASE_S
                    async for message in ws:
                        try:
                            event = json.loads(message)
                        except (ValueError, TypeError):
                            continue
                        if event.get("action") not in TRADE_ACTIONS:
                            continue
                        if event.get("pool") != "pump":
                            continue
                        mint = event.get("mint")
                        price = event.get("price")
                        if mint and isinstance(price, (int, float)) and price > 0:
                            self._prices[mint] = (float(price), time.monotonic())
            except (websockets.ConnectionClosed, OSError) as exc:
                log.warning("LivePriceFeed disconnected (%s) — reconnect in %.0fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_S)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — keep the feed alive
                log.exception("LivePriceFeed error")
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_S)

    # --------------------------------------------------------------- queries
    def price_sol(self, mint: str, max_age_s: float = PRICE_MAX_AGE_S) -> float | None:
        """Latest fresh SOL-per-token price for mint, or None if stale/absent."""
        hit = self._prices.get(mint)
        if hit is None or time.monotonic() - hit[1] > max_age_s:
            return None
        return hit[0]

    async def sol_usd(self) -> float | None:
        """SOL price in USD (cached 60s; None on failure)."""
        if self._sol_usd is not None and time.monotonic() - self._sol_usd_ts < SOL_USD_TTL_S:
            return self._sol_usd
        try:
            r = await self._client.get(
                settings.jupiter_price_base, params={"ids": SOL_MINT}
            )
            r.raise_for_status()
            data = r.json()
            self._sol_usd = float(data[SOL_MINT]["usdPrice"])
            self._sol_usd_ts = time.monotonic()
        except Exception:  # noqa: BLE001
            log.debug("sol_usd fetch failed")
            return self._sol_usd  # possibly stale value
        return self._sol_usd

    async def price_usd(self, mint: str, max_age_s: float = PRICE_MAX_AGE_S) -> float | None:
        """Fresh live price converted to USD, or None."""
        price = self.price_sol(mint, max_age_s)
        if price is None:
            return None
        sol = await self.sol_usd()
        return price * sol if sol else None
