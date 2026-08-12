#!/bin/bash
# One-shot provisioning for a fresh host.
# Usage: bash scripts/deploy_host.sh [/path/to/app]
#   no arg  -> deploy in the repo you are standing in (git clone / tarball)
#   with arg -> deploy at that path (created if missing, e.g. /opt/sol-bot)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="${1:-$(cd "$SCRIPT_DIR/.." && pwd)}"
mkdir -p "$APP_DIR"
cd "$APP_DIR"
if [ ! -f src/main.py ]; then
  echo "ERROR: $APP_DIR is not the bot repo (src/main.py missing)."
  echo "Run from the cloned repo:  bash scripts/deploy_host.sh"
  echo "or pass the app dir:       bash scripts/deploy_host.sh /opt/sol-bot"
  exit 1
fi
echo "== deploying in $APP_DIR =="

echo "== [1/5] uv =="
if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
fi
uv --version

echo "== [2/5] slim venv (runtime deps only, ~47MB) =="
uv venv .venv --python 3.11 --clear
uv pip install --python .venv/bin/python \
  solders requests python-dotenv base58 websockets httpx telegramify-markdown python-telegram-bot

echo "== [3/5] import smoke test =="
PYTHONPATH=src .venv/bin/python - <<'PY'
import importlib, glob, os, sys
mods = [os.path.basename(p)[:-3] for p in glob.glob("src/*.py") if not p.endswith("__init__.py")]
bad = []
for m in mods:
    try:
        importlib.import_module(m)
    except Exception as e:
        bad.append(f"{m}: {e!r}")
if bad:
    print("IMPORT FAILURES:", bad); sys.exit(1)
print(f"OK — {len(mods)} modules import on slim venv")
PY

echo "== [4/5] .env =="
if [ ! -f .env ]; then
  echo "ERROR: .env missing — it is gitignored, so a clone has none."
  echo "  a) copy your dev .env:   scp .env user@HOST:/tmp/.env && cp /tmp/.env $APP_DIR/.env"
  echo "  b) or bootstrap one:     cp .env.example .env   (then fill in BOT_TOKEN/CHAT_ID/PRIVATE_KEY/...)"
  exit 1
fi
chmod 600 .env
if grep -q '^DRY_RUN=true' .env; then
  echo "DRY_RUN=true — paper trading (no real orders)"
else
  echo "!!! WARNING: DRY_RUN is NOT true — this host will place REAL orders"
fi

echo "== [5/5] process supervisor =="
if command -v systemctl >/dev/null 2>&1 && [ -d /etc/systemd/system ] && [ -w /etc/systemd/system ]; then
  echo "systemctl found — installing system service"
  cat > /etc/systemd/system/sol-bot.service <<UNIT
[Unit]
Description=Solana Pump.fun sniper bot (paper: DRY_RUN=true)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/.venv/bin/python src/main.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
# optional: match the local log timestamps
Environment=TZ=Asia/Tehran
StandardOutput=journal
StandardError=journal
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now sol-bot.service
sleep 6
systemctl status sol-bot.service --no-pager | grep -E "Active|Main PID|Memory"
echo
echo "--- log tail ---"
tail -n 4 "$APP_DIR/bot_logs/bot.log" 2>/dev/null || tail -n 4 "$APP_DIR/bot.log"
echo
echo "DONE. Commands:  systemctl status|restart|stop sol-bot   |   journalctl -u sol-bot -e"
else
  echo "no systemctl — using simple supervisor (nohup + auto-restart)"
  bash "$APP_DIR/scripts/run_bot.sh" start
  sleep 6
  bash "$APP_DIR/scripts/run_bot.sh" status
  echo
  echo "--- log tail ---"
  tail -n 4 "$APP_DIR/bot_logs/bot.log" 2>/dev/null || tail -n 4 "$APP_DIR/bot.log"
  echo
  echo "DONE. Commands:  bash scripts/run_bot.sh {start|stop|status|restart}"
  echo "NOTE: no auto-start on boot (no systemd). Add to crontab:"
  echo "  @reboot cd $APP_DIR && bash scripts/run_bot.sh start"
fi
