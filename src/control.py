"""Trade gate — start/stop control shared by the scanner, the trade loop and
the Telegram command bot. Clearing the gate pauses trading gracefully:
no new trades start; an in-flight trade runs to completion.
"""
from __future__ import annotations

import asyncio


class TradeGate:
    """Asyncio-based start/stop switch."""

    def __init__(self, auto_start: bool = True) -> None:
        self._event = asyncio.Event()
        if auto_start:
            self._event.set()

    async def start(self) -> None:
        """Open the gate (resume trading)."""
        self._event.set()

    async def stop(self) -> None:
        """Close the gate (pause trading after the current trade)."""
        self._event.clear()

    def is_started(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        """Block until the gate is open."""
        await self._event.wait()
