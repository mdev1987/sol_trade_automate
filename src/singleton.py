"""Single-instance lock — flock on a lockfile so two bots can never run at once
(duplicate buys + Telegram getUpdates conflicts). The OS releases the lock
automatically if the process dies, so crashes don't leave a stale lock.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger("sniper_bot.singleton")


class SingleInstanceLock:
    def __init__(self, path: str = ".sniper-bot.lock") -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> bool:
        """Try to take the lock. False = another instance is running."""
        import fcntl

        try:
            self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(self._fd, 0)
            os.write(self._fd, str(os.getpid()).encode())
            return True
        except (OSError, BlockingIOError):
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            return False

    def release(self) -> None:
        if self._fd is None:
            return
        import fcntl

        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(self._fd)
        self._fd = None
