# Solana Pump.fun Sniper Bot (Jupiter Compounding)

Automated trading bot that snipes new Pump.fun token launches in <1 second,
validates them (rug detection + filters + scoring), buys via Jupiter, monitors
price via DexScreener, and sells at 2x take-profit or 0.82x stop-loss, then
compounds 60% of winnings. Controlled from Telegram (`/start`, `/stop`,
`/status`).

Built from the course spec in `bot_plan/COMPLETE_SUMMARY.md`, the reference
Jupiter client in `bot_plan/sample_jupiter_code.txt`, and the API docs in
`bot_plan/docs/`.

## Layout

```
src/                  # application code (uv runtime, see pyproject.toml)
  main.py             # orchestration: scanner + bot + telegram, graceful shutdown
  token_scanner.py    # launch feed (PumpAPI primary → PumpDev fallback) → validate → score
  bot.py              # buy → monitor → sell → compound (quote gate + /order + /execute)
  telegram_bot.py     # command bot: /start /stop /status (telegramify-markdown)
  stats.py            # balance, winrate, PnL, active position (markdown)
  control.py          # trade gate (start/stop)
  jupiter_swap.py     # JupiterSwap: quote gate, sign, execute, slippage escalation
  dexscreener.py      # pair liquidity / volume / price (REST, 60 req/min)
  data_stream.py      # PumpAPI + PumpDev WebSocket feeds with failover
  rug_detection.py    # scam name, dev dump, honeypot, wash trading, mayhem
  scanner_filter.py   # course thresholds (age, liq, volume, txns, b/s, mcap)
  scoring_algorithm.py# 80-point scoring
  price_monitor.py    # 8s poll, TP 2x / SL 0.82x / dead pool <$25
  risk_management.py  # loss pause (2 losses → 5 min), play floor
  compounding.py      # 60/40 split
  monitoring.py       # logging, journal, trade log, notifier
tests/                # smoke + integration tests (no network)
```

## Setup (uv)

```bash
uv sync                                  # installs deps into .venv (see pyproject.toml)
cp .env.example .env                     # or use the values from bot_plan/sample_env.txt
```

Required to go live (`DRY_RUN=false`):

| Var | Purpose |
|---|---|
| `PRIVATE_KEY` | wallet keypair (base58) — **never your main wallet** |
| `JUPITER_API_KEY` | free at https://developers.jup.ag/portal (FREE tier = 1 RPS) |
| `BOT_TOKEN` / `CHAT_ID` | Telegram control + alerts (BotFather) |

Optional: `SOLANA_RPC_URL` (Helius/Alchemy, optional reads only), `PUMPDEV_API_KEY`.

## Run

```bash
uv run python src/main.py                # scanner + bot + telegram (recommended, 24/7)
uv run python src/token_scanner.py       # scanner only
uv run python src/bot.py                 # bot only
```

Always run with `DRY_RUN=true` first — the quote gate still validates real
routes against Jupiter but nothing is ever signed or executed. Review scored
tokens, then go live with $2 USDC.

## Telegram control

| Command | Action |
|---|---|
| `/start` | open the trade gate (resume trading) |
| `/stop` | graceful shutdown: gate closes, in-flight trade finishes, exit 0 |
| `/status` | balance, winrate, realized PnL, active position (markdown + icons) |

Only the configured `CHAT_ID` may send commands. `AUTO_START=true` (default)
begins trading on launch; set `AUTO_START=false` to require `/start`.

## Systemd (24/7 VPS)

```ini
# /etc/systemd/system/sniper-bot.service
[Unit]
Description=Solana Pump.fun Sniper Bot
After=network-online.target

[Service]
WorkingDirectory=/home/mdev/Programming/new_sol_automate_bot
ExecStart=/home/mdev/Programming/new_sol_automate_bot/.venv/bin/python src/main.py
Restart=on-failure        # restart on crash; /stop exits cleanly and stays down
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now sniper-bot
journalctl -u sniper-bot -f
```

## Logs

- `bot.log` — runtime log
- `journal.json` — trade journal (JSONL)
- `trade_log.csv` — trade history

## Tests

```bash
uv run python tests/_smoke_test.py        # pure-logic checks, no network
uv run python tests/_integration_test.py  # end-to-end trade cycle, mocked network
```
