"""
Monitoring & logging — bot.log, journal.json, trade_log.csv, Telegram (optional).
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

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
        "ts",
        "mint",
        "symbol",
        "action",
        "side",
        "amount_usd",
        "entry_price",
        "exit_price",
        "pnl_usd",
        "exit_reason",
        "score",
        "signature",
        "dry_run",
    ]
    new_file = not os.path.exists(TRADE_LOG_FILE)
    with open(TRADE_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if new_file:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})
