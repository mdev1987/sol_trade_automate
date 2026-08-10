"""Trade statistics — balance, winrate, realized PnL, active position.

`TradeStats` is updated by the trade loop and rendered as Telegram markdown
(consumed by telegramify-markdown, so emoji + **bold** + `code` are safe).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from config import settings


@dataclass
class TradeStats:
    dry_run: bool
    started_at: float = field(default_factory=time.monotonic)
    balance_usd: float = 0.0

    trades: int = 0
    wins: int = 0
    losses: int = 0
    realized_pnl_usd: float = 0.0
    buy_failures: int = 0
    sell_failures: int = 0
    last_trade: dict = field(default_factory=dict)
    active_position: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.balance_usd:
            self.balance_usd = settings.starting_amount

    # ------------------------------------------------------------------ state
    @property
    def winrate(self) -> float:
        return (self.wins / self.trades) if self.trades else 0.0

    @property
    def in_trade(self) -> bool:
        return bool(self.active_position)

    def record_buy(self, mint: str, symbol: str, amount_usd: float,
                   entry_price: float, tp_price: float, sl_price: float) -> None:
        """Debit the bankroll and remember the open position."""
        self.balance_usd -= amount_usd
        self.active_position = {
            "mint": mint, "symbol": symbol, "amount_usd": amount_usd,
            "entry_price": entry_price, "tp_price": tp_price, "sl_price": sl_price,
            "opened_at": time.monotonic(),
        }

    def record_exit(self, won: bool, pnl_usd: float, proceeds_usd: float,
                    exit_reason: str, exit_price: float, signature: str = "") -> None:
        """Credit proceeds, update win/loss counters and the PnL total."""
        self.trades += 1
        if won:
            self.wins += 1
        else:
            self.losses += 1
        self.realized_pnl_usd += pnl_usd
        self.balance_usd += proceeds_usd
        self.last_trade = {
            "symbol": self.active_position.get("symbol", ""),
            "won": won, "pnl_usd": pnl_usd, "exit_reason": exit_reason,
            "exit_price": exit_price, "signature": signature,
        }
        self.active_position = {}

    def record_buy_failure(self, symbol: str, error: str) -> None:
        self.buy_failures += 1
        self.last_trade = {"symbol": symbol, "won": False, "error": error}

    def record_sell_failure(self, symbol: str, error: str) -> None:
        self.sell_failures += 1
        self.last_trade = {"symbol": symbol, "won": False, "error": error}

    # ---------------------------------------------------------------- helpers
    def _fmt_usd(self, v: float) -> str:
        return f"${v:,.2f}"

    def _uptime(self) -> str:
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
            f"💵 Realized PnL: `{self._fmt_usd(self.realized_pnl_usd)}`",
            f"⚠️ Buy fails: `{self.buy_failures}` · Sell fails: `{self.sell_failures}`",
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
