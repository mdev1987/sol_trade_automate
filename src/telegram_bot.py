"""Telegram command bot — /start, /stop, /status.

Long-polls the Bot API (getUpdates, timeout=25) and replies through
telegramify-markdown: `convert()` returns (text, entities) with UTF-16
offsets, so messages are sent with `entities` and **no parse_mode** — no
MarkdownV2 escaping headaches (docs: bot_plan/docs/telegram_bot_docs/).

Commands (only from the configured CHAT_ID):
  /start   — open the trade gate (resume trading)
  /stop    — graceful shutdown: gate closes, in-flight trade finishes, exit 0
  /status  — balance, winrate, PnL, active position (markdown + icons)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

import httpx
from telegramify_markdown import convert as md_convert

from config import settings

log = logging.getLogger("sniper_bot.telegram")

POLL_TIMEOUT_S = 25  # long-poll seconds
POLL_ERROR_BACKOFF_S = 5


class TelegramCommandBot:
    """Inbound command bot + outbound notifier in one (both use the Bot API)."""

    def __init__(self, gate, stats, bot_token: str = "", chat_id: str = "",
                 on_stop: Optional[Callable[[], Awaitable[None]]] = None):
        self.enabled = bool(bot_token and chat_id)
        self._token = bot_token
        self._chat_id = str(chat_id)
        self._gate = gate
        self._stats = stats
        self._on_stop = on_stop
        self._offset = 0
        self._client = httpx.AsyncClient(timeout=40)
        self._running = True

    # ------------------------------------------------------------------ send
    async def notify(self, text: str) -> None:
        """Send a markdown alert (trade results, etc.) to the configured chat."""
        if not self.enabled:
            return
        payload: dict[str, Any] = {"chat_id": self._chat_id}
        try:
            text_plain, entities = md_convert(text)
            payload["text"] = text_plain[:4000]
            if entities:
                payload["entities"] = [e.to_dict() for e in entities]
        except Exception:  # noqa: BLE001 — plain text fallback
            payload["text"] = text[:4000]
        try:
            await self._client.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage", json=payload
            )
        except Exception:  # noqa: BLE001 — never let alerts break trading
            log.exception("Telegram send failed")

    # ---------------------------------------------------------------- commands
    async def _handle(self, update: dict) -> None:
        msg = update.get("message") or {}
        chat_id = str((msg.get("chat") or {}).get("id", ""))
        if chat_id != self._chat_id:
            log.warning("Ignoring command from unauthorized chat %s", chat_id)
            return
        text = (msg.get("text") or "").strip().split()[0] if (msg.get("text") or "").strip() else ""
        if text == "/start":
            await self._gate.start()
            await self.notify(
                "✅ **Bot started** — trading resumed.\n" + self._stats.markdown()
            )
        elif text == "/stop":
            await self.notify(
                "🛑 **Graceful shutdown** — closing the gate, finishing the "
                "current trade, then exiting…"
            )
            await self._gate.stop()
            self._running = False
            if self._on_stop is not None:
                await self._on_stop()
        elif text in ("/status", "/stats"):
            await self.notify(self._stats.markdown())
        elif text in ("/help", "/h"):
            await self.notify(
                "🤖 **Commands**\n"
                "`/start` — resume trading\n"
                "`/stop` — graceful shutdown\n"
                "`/status` — balance · winrate · PnL · position"
            )
        else:
            await self.notify("❓ Unknown command. Try `/help`.")

    # ------------------------------------------------------------------- poll
    async def _poll_once(self) -> None:
        resp = await self._client.get(
            f"https://api.telegram.org/bot{self._token}/getUpdates",
            params={
                "timeout": POLL_TIMEOUT_S,
                "offset": self._offset,
                "allowed_updates": ["message"],
            },
        )
        resp.raise_for_status()
        for upd in resp.json().get("result", []):
            self._offset = upd["update_id"] + 1
            await self._handle(upd)

    async def run(self) -> None:
        """Long-poll forever (or until /stop)."""
        if not self.enabled:
            log.info("Telegram bot disabled (set BOT_TOKEN + CHAT_ID to enable)")
            return
        log.info("Telegram bot polling started (chat %s)", self._chat_id)
        while self._running:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("Telegram poll error: %s — retrying in %ds",
                            exc, POLL_ERROR_BACKOFF_S)
                await asyncio.sleep(POLL_ERROR_BACKOFF_S)
        log.info("Telegram bot polling stopped")

    # ------------------------------------------------------------------ close
    async def close(self) -> None:
        await self._client.aclose()


def build_telegram_bot(gate, stats, on_stop=None) -> TelegramCommandBot:
    """Factory so main.py can share one instance with the trade loop."""
    return TelegramCommandBot(
        gate, stats,
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
        on_stop=on_stop,
    )
