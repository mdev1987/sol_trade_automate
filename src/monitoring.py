"""
Monitoring & logging — bot.log, journal.json, trade_log.csv, Telegram (optional).
"""
from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

LOG_FILE = "bot.log"
JOURNAL_FILE = "journal.json"
TRADE_LOG_FILE = "trade_log.csv"

# --- logging setup ---------------------------------------------------------

def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("sniper_bot")
    if logger.handlers:  # already configured
        return logger
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    return logger


log = setup_logging()

# --- trade journal ----------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_journal(entry: dict[str, Any]) -> None:
    """Append a trade/scan entry to journal.json (JSONL)."""
    entry = {"ts": _now_iso(), **entry}
    with open(JOURNAL_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def append_trade_log(row: dict[str, Any]) -> None:
    """Append a completed trade to trade_log.csv."""
    fieldnames = [
        "ts", "mint", "symbol", "action", "side", "amount_usd", "entry_price",
        "exit_price", "pnl_usd", "exit_reason", "score", "signature", "dry_run",
    ]
    new_file = not os.path.exists(TRADE_LOG_FILE)
    with open(TRADE_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


# --- telegram (optional) -----------------------------------------------------
# Docs: bot_plan/docs/telegram_bot_docs/telegramify_markdown.md
#   convert(md) -> (text, entities) — entity offsets in UTF-16 units, exactly
#   what the Bot API wants, so no parse_mode / MarkdownV2 escaping is needed.

try:
    from telegramify_markdown import convert as _md_convert
except Exception:  # noqa: BLE001 — telegram is optional; degrade to plain text
    _md_convert = None


class TelegramNotifier:
    """Telegram notifier via Bot API. No-op unless BOT_TOKEN + CHAT_ID are set."""

    def __init__(self, bot_token: str = "", chat_id: str = ""):
        self.enabled = bool(bot_token and chat_id)
        self._token = bot_token
        self._chat_id = chat_id
        self._session = httpx.Client(timeout=10)

    async def notify(self, text: str) -> None:
        """Send a markdown-formatted alert. Plain text when converter missing."""
        if not self.enabled:
            return
        try:
            payload: dict[str, Any] = {"chat_id": self._chat_id}
            if _md_convert is not None:
                text_plain, entities = _md_convert(text)
                payload["text"] = text_plain[:4000]
                if entities:
                    # Telegram's entities are dict-serializable via .to_dict()
                    payload["entities"] = [e.to_dict() for e in entities]
            else:
                payload["text"] = text[:4000]
            self._session.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json=payload,
            )
        except Exception:  # noqa: BLE001 — never let notifications break trading
            log.exception("Telegram notify failed")

    async def close(self) -> None:
        self._session.close()
