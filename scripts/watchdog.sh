#!/bin/bash
# Health watchdog for the nohup supervisor (scripts/run_bot.sh).
#
# Usage:  bash scripts/watchdog.sh [APP_DIR]          (defaults to repo root)
# Crontab:  */5 * * * * /opt/sol-bot/scripts/watchdog.sh /opt/sol-bot
#
# Watches two failure modes that _supervise.sh alone cannot survive:
#   1. the whole supervisor (nohup shell) died — host reboot / OOM of the
#      supervise loop / terminal kill. Restart it.
#   2. the supervisor is alive but the bot's log went silent for
#      WATCHDOG_STALE_MIN minutes — the bot is wedged (hang in I/O).
#      Restart it; the graceful-stop path sells any in-flight position first.
#
# On either action a Telegram alert is sent (BOT_TOKEN/CHAT_ID from .env)
# so an outage is never silent, even when the heartbeat card stops.

set -u
APP_DIR="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
PIDFILE="$APP_DIR/.sniper-bot-super.pid"
SUP_LOG="$APP_DIR/bot_logs/supervisor.log"
STALE_MIN="${WATCHDOG_STALE_MIN:-10}"

# Telegram alert (optional) — read creds from .env so the crontab line stays bare
BOT_TOKEN="${BOT_TOKEN:-$(grep -E '^BOT_TOKEN=' "$APP_DIR/.env" 2>/dev/null | cut -d= -f2-)}"
CHAT_ID="${CHAT_ID:-$(grep -E '^CHAT_ID=' "$APP_DIR/.env" 2>/dev/null | cut -d= -f2-)}"
alert() {
  [ -z "$BOT_TOKEN" ] || [ -z "$CHAT_ID" ] && return 0
  curl -s --max-time 10 -X POST "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" \
    -d "chat_id=$CHAT_ID" -d "text=$1" >/dev/null 2>&1
}

supervisor_alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

if supervisor_alive; then
  if [ -f "$SUP_LOG" ] && [ -n "$(find "$SUP_LOG" -mmin +"$STALE_MIN")" ]; then
    echo "[$(date '+%F %T')] watchdog: log silent > ${STALE_MIN}m — restarting"
    alert "⚠️ watchdog: bot log silent >${STALE_MIN}m — restarting"
    bash "$APP_DIR/scripts/run_bot.sh" restart
  fi
else
  echo "[$(date '+%F %T')] watchdog: supervisor not running — starting"
  alert "⚠️ watchdog: bot supervisor not running — restarting"
  bash "$APP_DIR/scripts/run_bot.sh" start
fi