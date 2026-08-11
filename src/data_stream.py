"""
Pump.fun launch feed — PRIMARY: PumpAPI (stream.pumpapi.io), FALLBACK: PumpDev.

- PumpAPI: server pushes ALL events; we filter to `create` events with
  pool == "pump" (docs: bot_plan/docs/pumpapi_stream_doc.md). PumpAPI allows
  ONE websocket connection per client (1008 policy violation otherwise), so
  the PumpEventHub owns that single connection and fans raw events out to
  subscribers: create events -> scanner, buy/sell events -> LivePriceFeed.
- PumpDev: subscribeNewToken; txType == "create"
  (docs: bot_plan/docs/pumpdev_stream_doc.md).

Both are free. The hub falls back to PumpDev when PumpAPI keeps failing and
periodically tries to revive the primary.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

import websockets

from config import settings

log = logging.getLogger("sniper_bot.feed")

SILENCE_TIMEOUT = 180  # seconds without an event before failing over
REVIVE_INTERVAL = 300  # seconds of fallback before reviving the primary


@dataclass
class TokenLaunch:
    """A new Pump.fun token launch event (create)."""

    mint: str
    name: str
    symbol: str
    uri: str
    creator: str
    signature: str
    initial_buy_tokens: float
    dev_sol: Optional[float]  # SOL the developer bought at launch
    market_cap_sol: Optional[float]
    quote_mint: str
    is_mayhem_mode: bool
    is_cashback_enabled: bool
    source: str  # "pumpapi" | "pumpdev"
    created_at: float = 0.0  # epoch seconds (0 = unknown)
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_event(cls, ev: dict, source: str = "pumpapi") -> "TokenLaunch":
        """Parse either a PumpAPI event (action=create) or PumpDev event (txType=create)."""
        action = ev.get("action") or ev.get("txType")
        if action not in ("create",):
            raise ValueError(f"not a create event: {action}")

        # dev SOL bought at launch — both feeds expose it under different names
        dev_sol = ev.get("initialQuoteAmount") or ev.get("quoteAmount") or ev.get("solAmount")
        # market cap — pumpapi quotes in quote token (SOL for pump), pumpdev has marketCapSol
        mcap = (
            ev.get("marketCapQuote")
            if ev.get("quoteMint")
            in (
                "So11111111111111111111111111111111111111112",
                "So11111111111111111111111111111111111111111",
            )
            else ev.get("marketCapSol")
        )
        # launch time — pumpapi `timestamp` (epoch s; ms if > 1e12), pumpdev best-effort
        ts = ev.get("timestamp") or ev.get("createdAt") or ev.get("blockTime") or 0
        try:
            ts = float(ts)
            if ts > 1e12:
                ts /= 1000.0
        except (TypeError, ValueError):
            ts = 0.0
        return cls(
            mint=ev.get("mint", ""),
            name=ev.get("name", ""),
            symbol=ev.get("symbol", ""),
            uri=ev.get("uri", ""),
            creator=ev.get("traderPublicKey") or ev.get("txSigner") or "",
            signature=ev.get("signature", ""),
            initial_buy_tokens=float(ev.get("initialBuy") or 0),
            dev_sol=float(dev_sol) if dev_sol is not None else None,
            market_cap_sol=float(mcap) if mcap is not None else None,
            quote_mint=ev.get("quoteMint", ""),
            is_mayhem_mode=bool(ev.get("isMayhemMode", ev.get("mayhemMode", False))),
            is_cashback_enabled=bool(ev.get("isCashbackEnabled", ev.get("cashbackEnabled", False))),
            source=source,
            created_at=ts,
            raw=ev,
        )


class PumpApiStream:
    """Primary feed — wss://stream.pumpapi.io/ (raw events, no filtering)."""

    def __init__(self, url: str = ""):
        self.url = url or settings.pumpapi_ws_url

    async def events(self) -> AsyncIterator[dict]:
        """Yield every raw event (create + buy/sell + control)."""
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    log.info("PumpAPI connected: %s", self.url)
                    async for message in ws:
                        try:
                            yield json.loads(message)
                        except json.JSONDecodeError:
                            continue
            except (websockets.ConnectionClosed, OSError) as exc:
                log.warning("PumpAPI disconnected (%s)", exc)
                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                raise


class PumpDevStream:
    """Fallback feed — wss://pumpdev.io/ws with subscribeNewToken + backoff."""

    def __init__(self, url: str = "", api_key: str = ""):
        self.url = url or settings.pumpdev_ws_url
        self.api_key = api_key or settings.pump_api_key

    async def events(self) -> AsyncIterator[dict]:
        """Yield raw trade events (control messages filtered out)."""
        delay = 1.0
        while True:
            try:
                extra = {"x-api-key": self.api_key} if self.api_key else {}
                async with websockets.connect(self.url, extra_headers=extra) as ws:
                    log.info("PumpDev connected: %s", self.url)
                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    delay = 1.0
                    async for message in ws:
                        try:
                            ev = json.loads(message)
                        except json.JSONDecodeError:
                            continue
                        if ev.get("type") in ("connected", "subscribed", "error", "notice"):
                            log.info("PumpDev control: %s", ev)
                            continue
                        yield ev
            except (websockets.ConnectionClosed, OSError) as exc:
                log.warning("PumpDev disconnected (%s) — retrying in %.0fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
            except asyncio.CancelledError:
                raise


class PumpEventHub:
    """
    Owns the ONE allowed PumpAPI connection and fans raw events out to
    subscribers via per-type queues (create -> scanner, buy/sell -> prices).

    PRIMARY -> FALLBACK failover with silence detection, same as before:
      - if no event arrives in SILENCE_TIMEOUT seconds, switch to PumpDev;
      - fallback runs at most REVIVE_INTERVAL seconds, then revive primary;
      - a successful primary event switches back immediately.
    Subscribers that lag simply drop events (put_nowait) — a stalled scanner
    must never wedge the feed.
    """

    def __init__(self, creates_size: int = 256, trades_size: int = 8192):
        self.primary = PumpApiStream()
        self.fallback = PumpDevStream()
        self._creates: asyncio.Queue[dict] = asyncio.Queue(creates_size)
        self._trades: asyncio.Queue[dict] = asyncio.Queue(trades_size)
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="feed-hub")
            log.info("PumpEventHub started")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ------------------------------------------------------------- dispatch
    def _dispatch(self, ev: dict) -> None:
        action = ev.get("action") or ev.get("txType")
        if action == "create":
            try:
                self._creates.put_nowait(ev)
            except asyncio.QueueFull:
                log.warning("creates queue full — dropping create event")
        elif action in ("buy", "sell") and ev.get("pool") == "pump":
            try:
                self._trades.put_nowait(ev)
            except asyncio.QueueFull:
                pass  # price ticks are disposable; never block the feed

    async def _run(self) -> None:
        while True:
            # --- primary (pumpapi) -----------------------------------------
            try:
                primary = self.primary.events()
                while True:
                    ev = await asyncio.wait_for(primary.__anext__(), timeout=SILENCE_TIMEOUT)
                    self._dispatch(ev)
            except asyncio.TimeoutError:
                log.warning(
                    "PumpAPI silent for %ds — switching to PumpDev fallback", SILENCE_TIMEOUT
                )
            except StopAsyncIteration:
                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("PumpAPI stream error: %s", exc)
                await asyncio.sleep(2.0)

            # --- fallback (pumpdev) until primary can be revived ------------
            try:
                fallback = self.fallback.events()
                while True:
                    ev = await asyncio.wait_for(fallback.__anext__(), timeout=REVIVE_INTERVAL)
                    self._dispatch(ev)
            except asyncio.TimeoutError:
                log.info("Revive check — trying PumpAPI primary again")
            except StopAsyncIteration:
                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("PumpDev stream error: %s", exc)
                await asyncio.sleep(2.0)

    # ---------------------------------------------------------- subscribers
    async def creates(self) -> AsyncIterator[TokenLaunch]:
        """Token creations for the scanner (one consumer expected)."""
        while True:
            ev = await self._creates.get()
            source = "pumpapi" if ev.get("action") == "create" else "pumpdev"
            yield TokenLaunch.from_event(ev, source=source)

    async def trades(self) -> AsyncIterator[tuple[str, float, float | None]]:
        """(mint, price_sol, quoteInPool_sol) buy/sell events for the live
        price feed. quoteInPool lets the feed track on-chain pool liquidity
        (entry floor / dead-pool sanity); None when the event lacks it."""
        while True:
            ev = await self._trades.get()
            mint = ev.get("mint")
            price = ev.get("price")
            liq = ev.get("quoteInPool")
            if mint and isinstance(price, (int, float)) and price > 0:
                q = float(liq) if isinstance(liq, (int, float)) and liq > 0 else None
                yield mint, float(price), q


class LaunchFeedRouter:
    """Backwards-compatible facade: one PumpEventHub shared by all consumers.

    The scanner and the LivePriceFeed must share the SAME router (PumpAPI
    allows only one connection per client).
    """

    def __init__(self):
        self.hub = PumpEventHub()

    async def start(self) -> None:
        await self.hub.start()

    async def stop(self) -> None:
        await self.hub.stop()

    def launches(self) -> AsyncIterator[TokenLaunch]:
        return self.hub.creates()

    def trades(self) -> AsyncIterator[tuple[str, float]]:
        return self.hub.trades()


# backwards-compatible alias used by the scanner
def create_feed() -> LaunchFeedRouter:
    return LaunchFeedRouter()
