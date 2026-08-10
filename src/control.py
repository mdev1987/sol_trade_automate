"""Trade gate — start/stop control shared by the scanner, the trade loop and
the Telegram command bot. Clearing the gate pauses trading gracefully:
no new trades start; an in-flight trade runs to completion.

`request_shutdown()` also wakes idle loops blocked on `wait()` so a graceful
shutdown exits immediately instead of waiting for the gate to reopen.
"""
from __future__ import annotations

import asyncio


class TradeGate:
    """Asyncio-based start/stop switch with a shutdown latch."""

    def __init__(self, auto_start: bool = True) -> None:
        self._event = asyncio.Event()
        self._shutdown = False
        if auto_start:
            self._event.set()

    async def start(self) -> None:
        """Open the gate (resume trading)."""
        self._event.set()

    async def stop(self) -> None:
        """Close the gate (pause trading after the current trade)."""
        self._event.clear()

    def request_shutdown(self) -> None:
        """Latch shutdown and wake any loop blocked on wait()."""
        self._shutdown = True
        self._event.set()

    @property
    def shutdown(self) -> bool:
        return self._shutdown

    def is_started(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        """Block until the gate is open or shutdown is requested."""
        await self._event.wait()
