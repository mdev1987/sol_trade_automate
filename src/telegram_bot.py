"""
Telegram notifier + command bot — python-telegram-bot + telegramify-markdown.

Card design ported from bot_plan/sample_telegram_code.txt:
  - ICONS per event type, `•` separators, compact _f()/_sign() formatting
  - send_buy / send_sell / send_signal / send_status / send_stopped cards
  - messages go out as text + entities (no parse_mode) via
    telegramify_markdown.convert(text, latex_escape=False)
  - test() verifies credentials against the API

Command handling uses PTB Application + CommandHandler (docs:
bot_plan/docs/telegram_bot_docs/python_telegram_bot.md):
  /start   — open the trade gate (resume trading)
  /stop    — graceful shutdown: gate closes, in-flight trade finishes, exit 0
  /status  — balance, winrate, PnL, active position, quote-gate stats
  /help    — command list
Commands are only answered for the configured CHAT_ID.
"""

from __future__ import annotations

import asyncio

import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

import telegramify_markdown
from telegram import Bot, Update
from telegram import MessageEntity as TGMessageEntity
from telegram.ext import Application, CommandHandler, ContextTypes

log = logging.getLogger("sniper_bot.telegram")

# Inline separator between label/value pairs on a card line.
SEP = "•"

# Card emoji per event type (from sample_telegram_code.txt).
ICONS = {
    "start": "🚀",
    "signal": "🟢",
    "buy": "🟢",
    "sell_win": "💰",
    "sell_loss": "🔻",
    "tp": "🎯",
    "sl": "🛑",
    "trailing": "📈",
    "trail": "📈",
    "ttl": "⏱️",
    "dead": "💀",
    "status": "📊",
    "alert": "⚠️",
    "stop": "🏁",
}


class TelegramNotifier:
    """Sends markdown trade cards and answers /start /stop /status commands."""

    def __init__(
        self,
        bot_token: str = "",
        chat_id: str = "",
        gate=None,
        stats=None,
        on_stop: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        token = (bot_token or "").strip()
        self._chat_id = str(chat_id or "").strip()
        self._enabled = bool(token and self._chat_id)
        self._bot: Optional[Bot] = Bot(token) if self._enabled else None
        self._gate = gate
        self._stats = stats
        self._on_stop = on_stop
        self._application: Optional[Application] = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ------------------------------------------------------------------- send
    async def _bot_ready(self) -> bool:
        """Make sure the PTB bot is initialized before direct sends."""
        if self._bot is None:
            return False
        if not getattr(self._bot, "_initialized", False):
            try:
                await self._bot.initialize()
            except Exception as exc:  # noqa: BLE001
                log.warning("bot initialize failed: %s", exc)
                return False
        return True

    async def _send(self, text: str) -> None:
        """Send one message with resolved markdown entities (best-effort)."""
        if not self._enabled or self._bot is None:
            return
        if not await self._bot_ready():
            return
        try:
            rendered, entities = telegramify_markdown.convert(text, latex_escape=False)
            tg_entities = self._to_tg_entities(entities)
            await self._bot.send_message(
                chat_id=self._chat_id, text=rendered, entities=tg_entities or None
            )
        except Exception as exc:  # noqa: BLE001 — never let alerts break trading
            log.warning("telegram send failed: %s", exc)

    @staticmethod
    def _to_tg_entities(items) -> list[TGMessageEntity]:
        """Translate telegramify-markdown MessageEntity objects to PTB ones."""
        result = []
        for item in items or []:
            kwargs = {"type": item.type, "offset": item.offset, "length": item.length}
            url = getattr(item, "url", None)
            if url:
                kwargs["url"] = url
            result.append(TGMessageEntity(**kwargs))
        return result

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _f(value, dp: int = 2) -> str:
        """Format a number compactly; `—` when None, `0` for zero."""
        if value is None:
            return "—"
        value = float(value)
        if value == 0:
            return "0"
        return f"{value:,.{dp}f}"

    @staticmethod
    def _sign(value: float) -> str:
        return "+" if value >= 0 else ""

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ------------------------------------------------------------- lifecycle
    async def test(self) -> bool:
        """Verify the bot credentials against the API (retry with backoff)."""
        if not self._enabled or self._bot is None:
            log.info("[telegram] disabled")
            return False
        for attempt in range(3):
            try:
                if not await self._bot_ready():
                    return False
                me = await self._bot.get_me()
                log.info("[telegram] connected as @%s", me.username)
                return True
            except Exception as exc:  # noqa: BLE001
                log.warning("[telegram] connection failed (attempt %d/3): %s",
                            attempt + 1, exc)
                await asyncio.sleep(2.0 * (attempt + 1))
        return False

    async def send_startup(self, summary: str) -> None:
        """Startup card carrying the active config line."""
        await self._send(
            f"{ICONS['start']} **Bot Started**\n{SEP} `{self._now()}`\n{SEP} {summary}"
        )

    async def send_alert(self, title: str, detail: str = "") -> None:
        """Generic warning/health line."""
        body = f"{ICONS['alert']} **{title}**"
        if detail:
            body += f"\n{SEP} {detail}"
        await self._send(body)

    # ------------------------------------------------------------------- trade
    async def send_buy(
        self,
        mint: str,
        symbol: str,
        score: float,
        price_usd: Optional[float],
        liquidity_usd: float,
        volume_5m: float,
        buy_ratio: float,
        age_s: float,
        amount_usdc: float,
        balance_usdc: float,
    ) -> None:
        """A single buy card, values in USDC."""
        short = (mint or "")[:10]
        await self._send(
            f"{ICONS['buy']} **BUY** `{symbol}`\n"
            f"`{short}…`\n"
            f"{SEP} Score `{score:.1f}` {SEP} Price `${self._f(price_usd)}`\n"
            f"{SEP} Liquidity `${self._f(liquidity_usd)}` {SEP} Vol5m `${self._f(volume_5m)}`\n"
            f"{SEP} BuyRatio `{buy_ratio:.2f}` {SEP} Age `{age_s:.0f}s`\n"
            f"{SEP} Used `${self._f(amount_usdc)}` {SEP} Bal `${self._f(balance_usdc)}` USDC"
        )

    async def send_sell(
        self,
        mint: str,
        symbol: str,
        reason: str,
        pnl_usd: float,
        roi_pct: float,
        entry_usd: float,
        exit_usd: float,
        hold_s: float,
        balance_usdc: float,
        peak_roi_pct: Optional[float] = None,
    ) -> None:
        """A sell/exit card with PnL expressed in USDC."""
        icon = ICONS.get(reason, "🔻")
        card = ICONS["sell_win"] if pnl_usd >= 0 else ICONS["sell_loss"]
        s = self._sign(pnl_usd)
        peak = f" {SEP} Peak `{peak_roi_pct:.0f}%`" if peak_roi_pct is not None else ""
        await self._send(
            f"{card} **SELL {reason.upper()}** {icon}\n"
            f"`{(mint or '')[:10]}…`\n"
            f"{SEP} PnL `{s}${self._f(pnl_usd)}` {SEP} ROI `{s}{roi_pct:.1f}%`\n"
            f"{SEP} In `${self._f(entry_usd)}` {SEP} Out `${self._f(exit_usd)}`\n{peak}"
            f"{SEP} Held `{hold_s:.0f}s` {SEP} Bal `${self._f(balance_usdc)}` USDC"
        )

    async def send_signal(self, symbol: str, score: float, reason: str = "") -> None:
        """Compact signal card (used in dry-run to show what would trade)."""
        text = f"{ICONS['signal']} **SIGNAL** `{symbol}` score `{score:.1f}`"
        if reason:
            text += f" {SEP} {reason}"
        await self._send(text)

    async def send_status(
        self,
        runtime_s: float,
        trades: int,
        win_rate: float,
        pnl_usdc: float,
        balance_usdc: float,
        exit_counts: dict,
        skips: str = "",
        quotes: str = "",
    ) -> None:
        """Periodic summary card."""
        minutes = runtime_s / 60
        s = self._sign(pnl_usdc)
        exits = "\n".join(f"  • `{k}: {v}`" for k, v in sorted(exit_counts.items()))
        if not exits:
            exits = "  • `none yet`"
        await self._send(
            f"{ICONS['status']} **Summary**\n"
            f"{SEP} Runtime `{minutes:.0f}m` {SEP} Trades `{trades}`\n"
            f"{SEP} WinRate `{win_rate:.1f}%` {SEP} PnL `{s}${self._f(pnl_usdc)}`\n"
            f"{SEP} Balance `${self._f(balance_usdc)}` USDC\n"
            f"{SEP} Quote-gate `{skips}`\n"
            f"`{quotes}`\n"
            f"{exits}"
        )

    async def send_stopped(
        self,
        runtime_s: float,
        trades: int,
        win_rate: float,
        pnl_usdc: float,
        balance_usdc: float,
        exit_counts: dict,
        skips: str = "",
        quotes: str = "",
    ) -> None:
        """Shutdown card; same stats as send_status."""
        minutes = runtime_s / 60
        s = self._sign(pnl_usdc)
        await self._send(
            f"{ICONS['stop']} **Bot Stopped**\n"
            f"{SEP} Runtime `{minutes:.0f}m` {SEP} Trades `{trades}`\n"
            f"{SEP} WinRate `{win_rate:.1f}%` {SEP} PnL `{s}${self._f(pnl_usdc)}`\n"
            f"{SEP} Balance `${self._f(balance_usdc)}` USDC\n"
            f"{SEP} Quote-gate `{skips}`\n"
            f"`{quotes}`"
        )

    # ------------------------------------------------------------- commands
    def _authorized(self, update: Update) -> bool:
        chat = update.effective_chat
        return chat is not None and str(chat.id) == self._chat_id

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._authorized(update):
            log.warning("Ignoring /start from unauthorized chat")
            return
        if self._gate is not None:
            await self._gate.start()
        summary = self._stats.markdown() if self._stats is not None else ""
        await self._send(f"✅ **Bot started** — trading resumed.\n{summary}")

    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._authorized(update):
            log.warning("Ignoring /stop from unauthorized chat")
            return
        await self._send(
            "🛑 **Graceful shutdown** — closing the gate, finishing the "
            "current trade, then exiting…"
        )
        if self._gate is not None:
            await self._gate.stop()
            self._gate.request_shutdown()
        if self._on_stop is not None:
            await self._on_stop()

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._authorized(update):
            log.warning("Ignoring /status from unauthorized chat")
            return
        summary = self._stats.markdown() if self._stats is not None else "no stats yet"
        await self._send(summary)

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._authorized(update):
            return
        await self._send(
            "🤖 **Commands**\n"
            "`/start` — resume trading\n"
            "`/stop` — graceful shutdown\n"
            "`/status` — balance · winrate · PnL · position"
        )

    # -------------------------------------------------------------- polling
    async def start_polling(self) -> None:
        """Build the PTB Application and start getUpdates long-polling."""
        if not self._enabled or self._bot is None:
            log.info("Telegram disabled — command bot inactive")
            return
        app = Application.builder().bot(self._bot).build()
        app.add_handler(CommandHandler("start", self._cmd_start))
        app.add_handler(CommandHandler("stop", self._cmd_stop))
        app.add_handler(CommandHandler("status", self._cmd_status))
        app.add_handler(CommandHandler("help", self._cmd_help))
        await app.initialize()
        await app.start()
        await app.updater.start_polling(allowed_updates=["message"], drop_pending_updates=True)
        self._application = app
        log.info("Telegram command bot polling (chat %s)", self._chat_id)

    async def stop_polling(self) -> None:
        if self._application is not None:
            await self._application.updater.stop()
            await self._application.stop()
            await self._application.shutdown()
            self._application = None

    async def close(self) -> None:
        await self.stop_polling()
        if self._bot is not None and getattr(self._bot, "_initialized", False):
            try:
                await self._bot.shutdown()
            except Exception:  # noqa: BLE001
                pass


def build_telegram_bot(gate, stats, on_stop=None) -> TelegramNotifier:
    """Factory so main.py can share one instance with the trade loop."""
    from config import settings

    return TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
        gate=gate,
        stats=stats,
        on_stop=on_stop,
    )
