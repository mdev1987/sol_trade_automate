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
PRICE_MAX_AGE_S = 10.0  # a price older than this is stale
SOL_USD_TTL_S = 60.0
# per-source cap so one hung oracle can't stall the chain (5s x up to 4 sources)
SOL_USD_SOURCE_TIMEOUT_S = 5.0
RECONNECT_BASE_S = 1.0
RECONNECT_MAX_S = 30.0


class LivePriceFeed:
    """Background task: keep {mint: (price_sol, ts)} fresh from buy/sell events.

    Consumes the shared PumpEventHub trades stream by default (PumpAPI allows
    only ONE websocket per client — the scanner owns it). A standalone
    `url` starts its own connection instead (for scripts/tests).
    """

    def __init__(self, url: str | None = None, trades=None) -> None:
        """Create the feed; consumes the shared hub trades when trades given."""
        self.url = url or settings.pumpapi_ws_url
        self._trades = trades  # async iterator of (mint, price_sol)
        self._prices: dict[str, tuple[float, float]] = {}
        # mint -> (quoteInPool SOL, monotonic ts) — on-chain pool liquidity
        self._liqs: dict[str, tuple[float, float]] = {}
        # per-mint asyncio.Event fired on every trade update — lets the exit
        # monitor wake on sub-second websocket ticks instead of polling 1s
        self._events: dict[str, asyncio.Event] = {}
        self._sol_usd: float | None = None
        self._sol_usd_ts = 0.0
        self._task: asyncio.Task | None = None
        self._client = httpx.AsyncClient(timeout=15.0)
        self._connected_at = 0.0  # monotonic ts of the current stream connection

    # ------------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Start the background feed task if it isn't already running."""
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="live-feed")
            log.info("LivePriceFeed started (shared hub=%s)", self._trades is not None)

    async def stop(self) -> None:
        """Cancel the feed task and close the httpx client."""
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
        """Dispatch to hub-consumption or standalone-connection mode."""
        if self._trades is not None:
            await self._consume(self._trades)
        else:
            await self._run_own_connection()

    async def _consume(self, trades) -> None:
        """Consume (mint, price_sol, quoteInPool_sol) tuples from the hub."""
        delay = RECONNECT_BASE_S
        while True:
            try:
                self._connected_at = time.monotonic()
                async for mint, price, liq in trades:
                    now = time.monotonic()
                    self._prices[mint] = (float(price), now)
                    if liq is not None:
                        self._liqs[mint] = (float(liq), now)
                    self._wake(mint)
                    self._prune()
                delay = RECONNECT_BASE_S
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — keep the feed alive
                log.warning("LivePriceFeed stream error (%s) — retry in %.0fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_S)

    async def _run_own_connection(self) -> None:
        """Standalone mode: own websocket to PumpAPI (only for scripts/tests)."""
        delay = RECONNECT_BASE_S
        while True:
            try:
                async with websockets.connect(self.url, open_timeout=15) as ws:
                    delay = RECONNECT_BASE_S
                    self._connected_at = time.monotonic()
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
                        liq = event.get("quoteInPool")
                        if mint and isinstance(price, (int, float)) and price > 0:
                            now = time.monotonic()
                            self._prices[mint] = (float(price), now)
                            if isinstance(liq, (int, float)) and liq > 0:
                                self._liqs[mint] = (float(liq), now)
                            self._wake(mint)
            except (websockets.ConnectionClosed, OSError) as exc:
                log.warning("LivePriceFeed disconnected (%s) — reconnect in %.0fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_S)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("LivePriceFeed error")
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_S)

    def _prune(self, max_age_s: float = 120.0) -> None:
        """Drop stale mints so the dict doesn't grow forever (~250/min live)."""
        if len(self._prices) < 500:
            return
        cutoff = time.monotonic() - max_age_s
        self._prices = {m: v for m, v in self._prices.items() if v[1] >= cutoff}
        self._liqs = {m: v for m, v in self._liqs.items() if v[1] >= cutoff}

    def _wake(self, mint: str) -> None:
        """Set the per-mint event (fire-and-forget) on a fresh trade tick."""
        ev = self._events.get(mint)
        if ev is not None:
            ev.set()

    def _event_for(self, mint: str) -> asyncio.Event:
        """Lazily create the per-mint wake event."""
        ev = self._events.get(mint)
        if ev is None:
            ev = asyncio.Event()
            self._events[mint] = ev
        return ev

    async def wait_trade(self, mint: str, timeout_s: float) -> None:
        """Wait up to timeout_s for the next trade tick on mint, else timeout.

        Returns immediately (even on timeout) so callers just re-check the
        price — this is the event-driven heartbeat for the exit monitor:
        sub-second TP/SL reaction on real websocket ticks, with a 1s backstop
        so a quiet mint still gets a regular check.
        """
        ev = self._event_for(mint)
        ev.clear()
        try:
            await asyncio.wait_for(ev.wait(), timeout_s)
        except asyncio.TimeoutError:
            pass

    # --------------------------------------------------------------- queries
    def price_sol(self, mint: str, max_age_s: float = PRICE_MAX_AGE_S) -> float | None:
        """Latest fresh SOL-per-token price for mint, or None if stale/absent."""
        hit = self._prices.get(mint)
        if hit is None or time.monotonic() - hit[1] > max_age_s:
            return None
        return hit[0]

    def last_trade_age(self, mint: str) -> float | None:
        """Seconds since the last buy/sell event for mint (None = never seen)."""
        hit = self._prices.get(mint)
        return (time.monotonic() - hit[1]) if hit else None

    def feed_age(self) -> float | None:
        """Seconds since the current stream connection was established."""
        return time.monotonic() - self._connected_at if self._connected_at else None

    async def _sol_usd_dexscreener(self) -> float | None:
        """Primary SOL/USD oracle — DexScreener (measured 270ms, 3/3).

        Picks the highest-liquidity SOL pair's priceUsd from /tokens/{mint}.
        None on any failure (fail-open; the chain moves to the next source).
        """
        try:
            url = f"{settings.dexscreener_base}/latest/dex/tokens/{SOL_MINT}"
            r = await self._client.get(url, timeout=SOL_USD_SOURCE_TIMEOUT_S)
            r.raise_for_status()
            pairs = r.json().get("pairs") or []
            if not pairs:
                return None
            best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
            usd = float(best["priceUsd"])
            return usd if usd > 0 else None
        except Exception:  # noqa: BLE001 — fail-open
            log.debug("sol_usd dexscreener failed")
            return None

    async def _sol_usd_jupiter(self) -> float | None:
        """SOL/USD via Jupiter /price/v3. None on any failure."""
        try:
            r = await self._client.get(
                settings.jupiter_price_base, params={"ids": SOL_MINT},
                timeout=SOL_USD_SOURCE_TIMEOUT_S,
            )
            r.raise_for_status()
            usd = float(r.json()[SOL_MINT]["usdPrice"])
            return usd if usd > 0 else None
        except Exception:  # noqa: BLE001
            log.debug("sol_usd jupiter fetch failed")
            return None

    async def _sol_usd_pumpcoins(self) -> float | None:
        """SOL/USD via pumpcoins.net ({"asOf":..., "usd":..., "change24h":...})."""
        try:
            r = await self._client.get(
                settings.pumpcoins_sol_price_url, timeout=SOL_USD_SOURCE_TIMEOUT_S
            )
            r.raise_for_status()
            usd = float(r.json()["usd"])
            return usd if usd > 0 else None
        except Exception:  # noqa: BLE001 — fail-open
            log.debug("sol_usd pumpcoins fallback failed")
            return None

    async def _sol_usd_coingecko(self) -> float | None:
        """SOL/USD via CoinGecko simple/price (free tier, 429-prone — last)."""
        try:
            r = await self._client.get(
                settings.coingecko_sol_price_url, timeout=SOL_USD_SOURCE_TIMEOUT_S
            )
            r.raise_for_status()
            usd = float(r.json()["solana"]["usd"])
            return usd if usd > 0 else None
        except Exception:  # noqa: BLE001
            log.debug("sol_usd coingecko failed")
            return None

    async def sol_usd(self) -> float | None:
        """SOL price in USD (cached SOL_USD_TTL_S; None only if never fetched).

        Source order (measured): DexScreener (270ms) -> Jupiter (307ms) ->
        pumpcoins.net (308ms) -> CoinGecko (461ms, 429-prone). Every source is
        fail-open and time-boxed; on total failure the last known (possibly
        stale) value is returned so callers never crash.
        """
        if self._sol_usd is not None and time.monotonic() - self._sol_usd_ts < SOL_USD_TTL_S:
            return self._sol_usd
        usd: float | None = await self._sol_usd_dexscreener()
        if usd is None:
            usd = await self._sol_usd_jupiter()
        if usd is None:
            usd = await self._sol_usd_pumpcoins()
        if usd is None:
            usd = await self._sol_usd_coingecko()
        if usd is not None:
            self._sol_usd = usd
            self._sol_usd_ts = time.monotonic()
        return self._sol_usd

    async def price_usd(self, mint: str, max_age_s: float = PRICE_MAX_AGE_S) -> float | None:
        """Fresh live price converted to USD, or None."""
        price = self.price_sol(mint, max_age_s)
        if price is None:
            return None
        sol = await self.sol_usd()
        return price * sol if sol else None

    def pool_liquidity_sol(self, mint: str, max_age_s: float = 60.0) -> float | None:
        """Latest on-chain pool quoteInPool (SOL) for this mint, or None.

        Only buy/sell events carry it, so a brand-new launch may not have an
        entry yet — the caller polls until the confirmation window expires.
        """
        hit = self._liqs.get(mint)
        if hit is None:
            return None
        liq_sol, ts = hit
        if time.monotonic() - ts > max_age_s:
            return None
        return liq_sol

    def pool_liquidity_usd(self, mint: str, sol_usd: float = 150.0, max_age_s: float = 60.0) -> float | None:
        """Pool liquidity in USD ≈ 2 x quoteInPool x SOL (bonding-curve model,
        same as the replay backtest). None when unknown/stale."""
        liq_sol = self.pool_liquidity_sol(mint, max_age_s)
        if liq_sol is None:
            return None
        return 2.0 * liq_sol * sol_usd
