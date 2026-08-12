#!/bin/bash
# Build the minimal tarball to deploy the bot on a host.
# Usage: scripts/ship_for_host.sh [out.tgz]
set -euo pipefail
cd "$(dirname "$0")/.."
OUT="${1:-/tmp/sol-bot-ship.tgz}"
tar czf "$OUT" \
  --exclude=.git --exclude=.venv --exclude=bot_plan --exclude=bot.log \
  --exclude='__pycache__' --exclude='*.pyc' --exclude=.prime \
  --exclude=.sniper-bot.lock --exclude=.env \
  src tools tests scripts pyproject.toml README.md .env.example
echo "shipped: $OUT ($(du -h "$OUT" | cut -f1)) — copy it with:"
echo "  scp $OUT user@HOST:/tmp/"
echo "then on the host:"
echo "  sudo mkdir -p /opt/sol-bot && sudo tar xzf /tmp/$(basename "$OUT") -C /opt/sol-bot"
echo "  sudo cp /tmp/.env /opt/sol-bot/.env   # scp .env separately: scp .env user@HOST:/tmp/.env"
