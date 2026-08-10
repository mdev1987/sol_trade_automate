"""
Pump.fun launch feed — PRIMARY: PumpAPI (stream.pumpapi.io), FALLBACK: PumpDev.

- PumpAPI: server pushes ALL events; we filter to `create` events with
  pool == "pump" (docs: bot_plan/docs/pumpapi_stream_doc.md).
- PumpDev: subscribeNewToken; txType == "create"
  (docs: bot_plan/docs/pumpdev_stream_doc.md).

Both are free. The router falls back to PumpDev when PumpAPI keeps failing
and periodically tries to revive the primary.
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
    dev_sol: Optional[float]          # SOL the developer bought at launch
    market_cap_sol: Optional[float]
    quote_mint: str
    is_mayhem_mode: bool
    is_cashback_enabled: bool
    source: str                       # "pumpapi" | "pumpdev"
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_event(cls, ev: dict, source: str = "pumpapi") -> "TokenLaunch":
        """Parse either a PumpAPI event (action=create) or PumpDev event (txType=create)."""
        action = ev.get("action") or ev.get("txType")
        if action not in ("create",):
            raise ValueError(f"not a create event: {action}")

        # dev SOL bought at launch — both feeds expose it under different names
        dev_sol = (
            ev.get("initialQuoteAmount")
            or ev.get("quoteAmount")
            or ev.get("solAmount")
        )
        # market cap — pumpapi quotes in quote token (SOL for pump), pumpdev has marketCapSol
        mcap = (
            ev.get("marketCapQuote")
            if ev.get("quoteMint") in ("So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111")
            else ev.get("marketCapSol")
        )
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
            raw=ev,
        )


class PumpApiStream:
    """Primary feed — wss://stream.pumpapi.io/ (all events, filter client-side)."""

    def __init__(self, url: str = ""):
        self.url = url or settings.pumpapi_ws_url

    async def launches(self) -> AsyncIterator[TokenLaunch]:
        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    log.info("PumpAPI connected: %s", self.url)
                    async for message in ws:
                        try:
                            ev = json.loads(message)
                        except json.JSONDecodeError:
                            continue
                        # filter to new Pump.fun token creations only
                        if ev.get("action") == "create" and ev.get("pool") == "pump":
                            yield TokenLaunch.from_event(ev, source="pumpapi")
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

    async def launches(self) -> AsyncIterator[TokenLaunch]:
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
                        if ev.get("txType") == "create":
                            yield TokenLaunch.from_event(ev, source="pumpdev")
            except (websockets.ConnectionClosed, OSError) as exc:
                log.warning("PumpDev disconnected (%s) — retrying in %.0fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
            except asyncio.CancelledError:
                raise


class LaunchFeedRouter:
    """
    PRIMARY PumpAPI -> FALLBACK PumpDev, with silence detection.

    - `wait_for(anext())` on the primary: if no event arrives within
      SILENCE_TIMEOUT seconds (server silent or connection wedged), we switch
      to PumpDev.
    - Fallback runs for at most REVIVE_INTERVAL seconds, then we try to bring
      the primary back. Successful primary event switches back immediately.
    - Both streams handle their own reconnects internally.
    """

    SILENCE_TIMEOUT = 180   # seconds without an event before failing over
    REVIVE_INTERVAL = 300   # seconds of fallback before reviving the primary

    def __init__(self):
        self.primary = PumpApiStream()
        self.fallback = PumpDevStream()

    async def launches(self) -> AsyncIterator[TokenLaunch]:
        while True:
            # --- primary (pumpapi) -----------------------------------------
            try:
                primary = self.primary.launches()
                while True:
                    launch = await asyncio.wait_for(
                        primary.__anext__(), timeout=self.SILENCE_TIMEOUT
                    )
                    yield launch
            except asyncio.TimeoutError:
                log.warning("PumpAPI silent for %ds — switching to PumpDev fallback",
                            self.SILENCE_TIMEOUT)
            except StopAsyncIteration:
                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("PumpAPI stream error: %s", exc)
                await asyncio.sleep(2.0)

            # --- fallback (pumpdev) until primary can be revived ------------
            try:
                fallback = self.fallback.launches()
                while True:
                    launch = await asyncio.wait_for(
                        fallback.__anext__(), timeout=self.REVIVE_INTERVAL
                    )
                    yield launch
            except asyncio.TimeoutError:
                log.info("Revive check — trying PumpAPI primary again")
            except StopAsyncIteration:
                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("PumpDev stream error: %s", exc)
                await asyncio.sleep(2.0)


# backwards-compatible alias used by the scanner
def create_feed() -> LaunchFeedRouter:
    return LaunchFeedRouter()
