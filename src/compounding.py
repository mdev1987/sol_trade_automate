"""
Compounding — 60% reinvested, 40% saved on every win.
(bot_plan/sample_bot/compunding_strategy.py)
"""
from __future__ import annotations

from config import settings


def split_proceeds(proceeds_usd: float) -> tuple[float, float]:
    """Return (reinvest_amount, saved_amount)."""
    reinvest = proceeds_usd * settings.reinvest_ratio
    saved = proceeds_usd - reinvest
    return reinvest, saved


def next_play_amount(current: float, won: bool, exit_reason: str) -> float:
    """
    Compute the next trade size after a completed cycle.

    - Win (take_profit): reinvest 60% of proceeds (current was ~starting amount,
      so 60% of 2x = 1.2x current).
    - Loss (stop_loss / dead_pool): play floor resets to STARTING_AMOUNT if
      the bankroll would drop below PLAY_FLOOR.
    """
    if won:
        proceeds = current * settings.take_profit  # token doubled
        reinvest, _ = split_proceeds(proceeds)
        return round(reinvest, 2)
    # loss
    remaining = current * settings.stop_loss
    if remaining < settings.play_floor:
        return round(settings.starting_amount, 2)
    return round(remaining, 2)
