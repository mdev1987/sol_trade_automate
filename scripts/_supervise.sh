#!/bin/bash
# Inner auto-restart loop used by run_bot.sh (not meant to be called directly).
# Keeps the bot alive on hosts without systemd: restart 5s after any exit.
APP_DIR="$1"
cd "$APP_DIR" || exit 1
PY="$APP_DIR/.venv/bin/python"
LOG="$APP_DIR/bot_logs/supervisor.log"
mkdir -p "$(dirname "$LOG")"
while true; do
  echo "[$(date '+%F %T')] supervisor: starting bot" >> "$LOG"
  "$PY" "$APP_DIR/src/main.py" >> "$LOG" 2>&1
  rc=$?
  echo "[$(date '+%F %T')] supervisor: bot exited rc=$rc — restarting in 5s" >> "$LOG"
  sleep 5
done
