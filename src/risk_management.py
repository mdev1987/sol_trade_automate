"""
Risk management — play floor, loss pause, dead-pool backoff.
(bot_plan/sample_bot/risk_management.py)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from config import settings

log = logging.getLogger("sniper_bot.risk")


@dataclass
class RiskManager:
    play_amount: float = field(default_factory=lambda: settings.starting_amount)
    consecutive_losses: int = 0
    paused_until: float = 0.0  # monotonic time when pause ends

    @property
    def paused(self) -> bool:
        return time.monotonic() < self.paused_until

    async def wait_if_paused(self) -> None:
        """Sleep until the loss-pause cooldown expires, if any."""
        if not self.paused:
            return
        wait = self.paused_until - time.monotonic()
        log.info("Loss pause active — sleeping %.0fs", wait)
        await asyncio.sleep(wait)

    def record_result(self, won: bool) -> float:
        """
        Update state after a trade and return the next play amount.

        Play floor: if the play amount would drop below PLAY_FLOOR, reset to
        STARTING_AMOUNT (prevents the death spiral).
        Loss pause: after LOSS_PAUSE_TRIGGER consecutive losses, pause 5 min.
        """
        from compounding import next_play_amount

        if won:
            self.consecutive_losses = 0
            self.play_amount = next_play_amount(
                self.play_amount, won=True, exit_reason="take_profit"
            )
        else:
            self.consecutive_losses += 1
            self.play_amount = next_play_amount(self.play_amount, won=False, exit_reason="loss")
            if self.consecutive_losses >= settings.loss_pause_trigger:
                self.paused_until = time.monotonic() + settings.loss_pause_minutes * 60
                log.warning(
                    "%d consecutive losses — pausing %d min",
                    self.consecutive_losses,
                    settings.loss_pause_minutes,
                )
                self.consecutive_losses = 0
        log.info("Play amount for next trade: $%.2f", self.play_amount)
        return self.play_amount
