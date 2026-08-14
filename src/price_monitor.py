"""
Price monitoring & exit strategy — TP / trailing stop / SL / dead pool / max hold.

Exit ladder (checked on every fresh price, fastest source first):
  1. take_profit  — price >= entry * TAKE_PROFIT (2.0x)
  2. trail_stop   — price <= running_peak * (1 - TRAIL_EXIT_PCT); the running
     peak is the highest price seen since entry. Live analysis (DexPaprika 1m
     OHLCV, 5 real fills) showed entries land at/near the launch-pump ATH and
     then gap 75-85% in the same minute — the fixed SL booked ~-70% while a
     15% trail converts that to ~-15% and locks gains on positions that pump.
  3. stop_loss    — price <= entry * STOP_LOSS (0.82x, hard floor / last resort)
  4. dead_pool    — liquidity dried up
  5. max_hold     — stuck-position watchdog

Price sources (fastest first):
  1. LivePriceFeed  — PumpAPI buy/sell stream, sub-second (fresh <= 10s)
  2. DexScreener    — also supplies liquidity for the dead-pool check
  3. Jupiter Price API fallback

When a fresh live price is available the loop ticks every 1s; DexScreener is
still polled at POLL_INTERVAL for liquidity. MAX_HOLD_MIN force-exits a
position that never hits TP/SL/dead-pool (stuck-position watchdog).
(bot_plan/sample_bot/price_monitory_exit_strategy.py)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from config import settings
from dexscreener import DexScreenerClient, Pair
from jupiter_swap import JupiterSwap

log = logging.getLogger("sniper_bot.monitor")

DEAD_POOL_LIQUIDITY_USD = 25.0  # pool is effectively dead below this
LIVE_TICK_S = 1.0  # fast loop when a fresh live price exists


@dataclass
class ExitSignal:
    """Result of one monitor evaluation; exit=True triggers a sell."""
    exit: bool
    reason: str  # "take_profit"|"trail_stop"|"stop_loss"|"dead_pool"|"no_trades"|"max_hold"|"shutdown"|"none"
    price_usd: float | None
    liquidity_usd: float | None


class PriceMonitor:
    """Watch one open position and signal when to exit (TP/SL/dead/max-hold)."""

    def __init__(
        self,
        dexscreener: DexScreenerClient,
        jupiter: JupiterSwap,
        entry_price_usd: float,
        mint: str,
        live_feed=None,
        stop_check=None,
    ):
        """Create a monitor for one position: TP/SL/dead/max-hold exit rules."""
        self.ds = dexscreener
        self.jupiter = jupiter
        self.entry_price_usd = entry_price_usd
        self.mint = mint
        self.live_feed = live_feed
        self.stop_check = stop_check  # callable -> bool; exit "shutdown" when True
        self.take_profit_price = entry_price_usd * settings.take_profit
        self.stop_loss_price = entry_price_usd * settings.stop_loss
        # trailing-stop state: the highest price seen since entry. Exits when
        # the price falls TRAIL_EXIT_PCT below this peak, so a launch-burst
        # dump (~-70% past the 0.82 SL) becomes a ~-15% exit instead.
        self.trailing_peak = entry_price_usd
        self.started_at = time.monotonic()
        self.max_hold_s = settings.max_hold_min * 60.0
        self._last_ds = 0.0  # DexScreener throttle (poll_interval cadence)
        self._cached_liq: float | None = None
        self._seen_pair = False  # a DexScreener pair ever appeared?

    async def current_pair(self) -> Pair | None:
        """Best-effort DexScreener lookup — a network failure returns None."""
        try:
            pairs = await self.ds.token_pairs(self.mint)
            return self.ds.pick_pair(pairs)
        except Exception as exc:  # noqa: BLE001 — monitor must survive outages
            log.warning("DexScreener lookup failed for %s: %s", self.mint[:8], exc)
            return None

    def _evaluate(self, price_usd: float, liquidity_usd: float | None) -> ExitSignal:
        """TP / trailing-stop / SL / dead-pool / max-hold evaluation."""
        if price_usd >= self.take_profit_price:
            return ExitSignal(True, "take_profit", price_usd, liquidity_usd)
        # trailing stop: a position that peaked and rolled over exits here,
        # BEFORE the fixed SL (TRAIL_EXIT_PCT below the running peak is tighter
        # than STOP_LOSS on a flat/dumping entry) — this is the sell-on-first-
        # red rule that turns launch-burst dumps into small losses.
        if settings.trail_exit_pct > 0 and price_usd <= self.trailing_peak * (1.0 - settings.trail_exit_pct):
            return ExitSignal(True, "trail_stop", price_usd, liquidity_usd)
        if price_usd <= self.stop_loss_price:
            return ExitSignal(True, "stop_loss", price_usd, liquidity_usd)
        if liquidity_usd is not None and liquidity_usd < DEAD_POOL_LIQUIDITY_USD:
            return ExitSignal(True, "dead_pool", price_usd, liquidity_usd)
        if time.monotonic() - self.started_at >= self.max_hold_s:
            return ExitSignal(True, "max_hold", price_usd, liquidity_usd)
        return ExitSignal(False, "none", price_usd, liquidity_usd)

    async def check(self) -> ExitSignal:
        """One cycle: fresh live price > DexScreener (throttled) > Jupiter."""
        price_usd = liquidity = None
        if self.live_feed is not None:
            price_usd = await self.live_feed.price_usd(self.mint)
        # DexScreener at most once per POLL_INTERVAL (rate limit ~60/min)
        if time.monotonic() - self._last_ds >= settings.poll_interval:
            pair = await self.current_pair()
            self._last_ds = time.monotonic()
            if pair is not None:
                self._seen_pair = True
                self._cached_liq = pair.liquidity_usd
                if price_usd is None:
                    price_usd = pair.price_usd
        liquidity = self._cached_liq
        if price_usd is None:
            try:
                price_usd = await self.jupiter.price_usd(self.mint)
                if price_usd is not None:
                    log.info("DexScreener/live miss — Jupiter fallback price: %s", price_usd)
            except Exception as exc:  # noqa: BLE001 — never crash the monitor
                log.warning("Jupiter price lookup failed for %s: %s", self.mint[:8], exc)
        if price_usd is not None and price_usd > self.trailing_peak:
            self.trailing_peak = price_usd
        if self._stale_dead():
            log.info("No live trades for %s — treating as dead (exit)", self.mint[:8])
            return ExitSignal(True, "no_trades", price_usd, liquidity)
        if price_usd is None:
            return ExitSignal(False, "none", None, liquidity)
        return self._evaluate(price_usd, liquidity)

    def _stale_dead(self) -> bool:
        """Dead-token exit: no live trades for STALE_EXIT_SEC and no indexed pair.

        Frees the single position slot — most pump launches never get a
        DexScreener pair and stop trading within a minute. A token with an
        indexed pair is handled by the dead_pool check instead. Disabled when
        the live feed is off.
        """
        if self.live_feed is None:
            return False
        if time.monotonic() - self.started_at < settings.stale_exit_grace_sec:
            return False
        if self._seen_pair:
            return False  # indexed pair → dead_pool/max_hold handle it
        age = self.live_feed.last_trade_age(self.mint)
        if age is None:
            # never saw a trade for this mint — exit only if the feed has been
            # listening the whole time (no recent reconnect that could have
            # missed events)
            feed_age = self.live_feed.feed_age()
            if feed_age is None:
                return False
            return feed_age > settings.stale_exit_grace_sec + settings.stale_exit_sec
        return age > settings.stale_exit_sec

    def _shutdown_pending(self) -> bool:
        """True when the shutdown callback fires (exit the position)."""
        return bool(self.stop_check and self.stop_check())

    async def run_until_exit(self, on_price=None) -> ExitSignal:
        """Poll until an exit signal fires (or shutdown is requested).

        Ticks every 1s while a fresh live price exists (DexScreener still
        polled at POLL_INTERVAL for liquidity), else every POLL_INTERVAL.
        On shutdown, returns "shutdown" so the caller can sell the position
        instead of being hard-cancelled with a stuck position.
        """
        while True:
            if self._shutdown_pending():
                last_price = None
                if self.live_feed is not None:
                    last_price = await self.live_feed.price_usd(self.mint)
                if last_price is None:
                    try:
                        last_price = await self.jupiter.price_usd(self.mint)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("Jupiter price failed at shutdown: %s", exc)
                if last_price is None:
                    pair = await self.current_pair()  # last resort: DexScreener
                    if pair is not None:
                        last_price = pair.price_usd
                log.info("Shutdown requested — exiting position at $%s", last_price)
                return ExitSignal(True, "shutdown", last_price, self._cached_liq)
            signal = await self.check()
            if on_price:
                await on_price(signal)
            if signal.exit:
                return signal
            # Event-driven: wake on the next websocket trade tick for this mint
            # (sub-second TP/SL reaction), with a 1s backstop so a quiet mint
            # still gets a regular check. This replaces the old fixed 1s sleep
            # that let prices gap 40-72% past the stop between polls.
            if self.live_feed is not None:
                await self.live_feed.wait_trade(self.mint, LIVE_TICK_S)
            else:
                await asyncio.sleep(settings.poll_interval)
