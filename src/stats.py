"""Trade statistics — balance, winrate, realized PnL, active position.

`TradeStats` is updated by the trade loop and rendered as Telegram markdown
(consumed by telegramify-markdown, so emoji + **bold** + `code` are safe).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from config import settings


def _utc_day() -> str:
    """Today's UTC date as YYYY-MM-DD (daily-loss tracking key)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


@dataclass
class TradeStats:
    """Running trade statistics: balance, PnL, winrate, active position."""
    dry_run: bool
    started_at: float = field(default_factory=time.monotonic)
    balance_usd: float = 0.0

    trades: int = 0
    wins: int = 0
    losses: int = 0
    realized_pnl_usd: float = 0.0
    buy_failures: int = 0
    sell_failures: int = 0
    dev_vetoes: int = 0  # launches blocked by the Helius dev-reputation veto
    thin_pools: int = 0  # entries skipped: pool liquidity below the floor
    last_trade: dict = field(default_factory=dict)
    active_position: dict = field(default_factory=dict)
    exit_counts: dict = field(default_factory=dict)  # reason -> count
    quote_gate: str = ""  # jupiter quote summary
    aged_out: int = 0  # queued candidates dropped for being too old

    # daily PnL tracking (UTC day) for the daily-loss kill switch
    day_key: str = field(default_factory=_utc_day)
    daily_pnl_usd: float = 0.0

    # ------------------------------------------------------------------ state
    def __post_init__(self) -> None:
        """Seed the bankroll from settings when not explicitly given."""
        if not self.balance_usd:
            self.balance_usd = settings.starting_balance

    @property
    def winrate(self) -> float:
        """Wins / total trades (0 when no trades yet)."""
        return (self.wins / self.trades) if self.trades else 0.0

    @property
    def in_trade(self) -> bool:
        """True while a position is open."""
        return bool(self.active_position)

    def record_buy(
        self,
        mint: str,
        symbol: str,
        amount_usd: float,
        entry_price: float,
        tp_price: float,
        sl_price: float,
    ) -> None:
        """Debit the bankroll and remember the open position."""
        self.balance_usd -= amount_usd
        self.active_position = {
            "mint": mint,
            "symbol": symbol,
            "amount_usd": amount_usd,
            "entry_price": entry_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "opened_at": time.monotonic(),
        }

    def record_exit(
        self,
        won: bool,
        pnl_usd: float,
        proceeds_usd: float,
        exit_reason: str,
        exit_price: float,
        signature: str = "",
    ) -> None:
        """Credit proceeds, update win/loss counters and the PnL total."""
        today = _utc_day()
        if today != self.day_key:
            self.day_key = today
            self.daily_pnl_usd = 0.0
        self.trades += 1
        if won:
            self.wins += 1
        else:
            self.losses += 1
        self.realized_pnl_usd += pnl_usd
        self.daily_pnl_usd += pnl_usd
        self.balance_usd += proceeds_usd
        self.exit_counts[exit_reason] = self.exit_counts.get(exit_reason, 0) + 1
        self.last_trade = {
            "symbol": self.active_position.get("symbol", ""),
            "won": won,
            "pnl_usd": pnl_usd,
            "exit_reason": exit_reason,
            "exit_price": exit_price,
            "signature": signature,
        }
        self.active_position = {}

    def record_buy_failure(self, symbol: str, error: str) -> None:
        """Count a failed buy attempt and record the error."""
        self.buy_failures += 1
        self.last_trade = {"symbol": symbol, "won": False, "error": error}

    def record_dev_veto(self, symbol: str, reason: str) -> None:
        """A launch was blocked by the dev-reputation check (no buy attempted)."""
        self.dev_vetoes += 1
        self.last_trade = {"symbol": symbol, "won": False, "error": f"dev_veto:{reason}"}

    def record_thin_pool(self, symbol: str) -> None:
        """A launch was skipped because its pool never proved liquid enough."""
        self.thin_pools += 1
        self.last_trade = {"symbol": symbol, "won": False, "error": "thin_liquidity"}

    def record_sell_failure(self, symbol: str, error: str) -> None:
        """Count a failed sell attempt and record the error."""
        self.sell_failures += 1
        self.last_trade = {"symbol": symbol, "won": False, "error": error}

    # ------------------------------------------------- daily-loss kill switch
    def daily_loss_limit_hit(self) -> bool:
        """True when the daily realized PnL <= -DAILY_LOSS_LIMIT (0 = disabled)."""
        limit = getattr(settings, "daily_loss_limit", 0.0)
        return limit > 0 and self.daily_pnl_usd <= -abs(limit)

    def next_day_reset_seconds(self) -> float:
        """Seconds until UTC midnight (day rollover that resets daily PnL)."""
        now = datetime.now(timezone.utc)
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return max(0.0, (tomorrow - now).total_seconds())

    # ---------------------------------------------------------------- helpers
    def _fmt_usd(self, v: float) -> str:
        """Format a USD value as a markdown code string."""
        return f"${v:,.2f}"

    def _uptime(self) -> str:
        """Format elapsed uptime as HH:MM:SS."""
        s = int(time.monotonic() - self.started_at)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    # ---------------------------------------------------------------- markdown
    def markdown(self) -> str:
        """Render the /status summary (telegramify-markdown friendly)."""
        mode = "**DRY RUN** 🧪" if self.dry_run else "**LIVE** 🔴"
        lines = [
            "📊 **Sniper Bot — Status**",
            "━━━━━━━━━━━━━━━",
            f"Mode: {mode}",
            f"💰 Balance: `{self._fmt_usd(self.balance_usd)}`",
            f"📈 Trades: `{self.trades}`  (✅ `{self.wins}` / ❌ `{self.losses}`)",
            f"🎯 Winrate: `{self.winrate * 100:.1f}%`",
            f"🚫 Dev vetoes: `{self.dev_vetoes}`",
            f"🩸 Thin pools: `{self.thin_pools}`",
            f"💵 Realized PnL: `{self._fmt_usd(self.realized_pnl_usd)}`",
            f"📅 Today: `{self._fmt_usd(self.daily_pnl_usd)}`",
            f"⚠️ Buy fails: `{self.buy_failures}` · Sell fails: `{self.sell_failures}`",
            f"⏳ Aged out: `{self.aged_out}`",
            f"⏱ Uptime: `{self._uptime()}`",
        ]
        if self.active_position:
            a = self.active_position
            lines += [
                "",
                "🎯 **Active position**",
                f"  • Token: `{a['symbol']}` (`{a['mint'][:8]}…`)",
                f"  • Play: `{self._fmt_usd(a['amount_usd'])}` @ entry `{a['entry_price']:.10g}`",
                f"  • TP `{a['tp_price']:.10g}` / SL `{a['sl_price']:.10g}`",
            ]
        if self.last_trade and not self.active_position:
            lt = self.last_trade
            icon = "✅" if lt.get("won") else "❌"
            pnl = self._fmt_usd(lt.get("pnl_usd", 0.0)) if lt.get("pnl_usd") is not None else "—"
            lines += [
                "",
                f"{icon} **Last trade**",
                f"  • {lt.get('symbol', '?')} — {lt.get('exit_reason', lt.get('error', ''))}",
                f"  • PnL: `{pnl}`",
            ]
        return "\n".join(lines)
