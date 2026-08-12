#!/bin/bash
# Simple supervisor for hosts WITHOUT systemd.
# Usage: bash scripts/run_bot.sh {start|stop|status|restart}
set -u
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$APP_DIR/.sniper-bot-super.pid"

pid_alive() { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

start() {
  if pid_alive; then echo "already running (supervisor pid $(cat "$PIDFILE"))"; return 0; fi
  if [ ! -x "$APP_DIR/.venv/bin/python" ]; then
    echo "ERROR: $APP_DIR/.venv missing — build it first:"
    echo "  uv venv .venv --python 3.11 && uv pip install --python .venv/bin/python solders requests python-dotenv base58 websockets httpx telegramify-markdown python-telegram-bot"
    return 1
  fi
  nohup bash "$APP_DIR/scripts/_supervise.sh" "$APP_DIR" >/dev/null 2>&1 &
  echo $! > "$PIDFILE"
  sleep 1
  echo "started (supervisor pid $(cat "$PIDFILE")) — logs: bot_plan/bot_logs/{bot.log,supervisor.log}"
}

stop() {
  if [ ! -f "$PIDFILE" ]; then echo "not running"; return 0; fi
  # SIGTERM the bot first so it shuts down gracefully (writes final stats)
  pkill -TERM -f "$APP_DIR/src/main.py" 2>/dev/null
  sleep 3
  kill "$(cat "$PIDFILE")" 2>/dev/null
  rm -f "$PIDFILE"
  echo "stopped"
}

status() {
  if pid_alive; then
    echo "running (supervisor pid $(cat "$PIDFILE"))"
    pgrep -af "$APP_DIR/src/main.py" || echo "  (bot process not found — restarting soon)"
  else
    echo "not running"
  fi
}

case "${1:-}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; sleep 1; start ;;
  status)  status ;;
  *) echo "usage: $0 {start|stop|status|restart}"; exit 1 ;;
esac
