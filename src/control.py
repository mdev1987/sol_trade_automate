"""Trade gate — start/stop control shared by the scanner, the trade loop and
the Telegram command bot. Clearing the gate pauses trading gracefully:
no new trades start; an in-flight trade runs to completion.

`request_shutdown()` latches shutdown and wakes idle loops blocked on `wait()`
(without touching the started flag, so a paused gate stays paused).
"""

from __future__ import annotations

import asyncio


class TradeGate:
    """Asyncio-based start/stop switch with a shutdown latch."""

    def __init__(self, auto_start: bool = True) -> None:
        """Create the gate; open it immediately when auto_start is True."""
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
        """Latch shutdown and wake any loop blocked on wait() within 1s."""
        self._shutdown = True

    @property
    def shutdown(self) -> bool:
        """True once request_shutdown() has latched the shutdown flag."""
        return self._shutdown

    def is_started(self) -> bool:
        """True when the gate is open (trading allowed)."""
        return self._event.is_set()

    async def wait(self) -> None:
        """Block until the gate is open or shutdown is requested (≤1s wake)."""
        while not self._shutdown and not self._event.is_set():
            try:
                await asyncio.wait_for(self._event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass  # re-check shutdown latch
