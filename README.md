# Solana Pump.fun Sniper Bot (Jupiter Compounding)

Automated trading bot that snipes new Pump.fun token launches in <1 second,
validates them (rug detection + filters + scoring), buys via Jupiter, monitors
price via DexScreener, and sells at 2x take-profit or 0.82x stop-loss, then
compounds 60% of winnings. Controlled from Telegram (`/start`, `/stop`,
`/status`).

Built from the course spec in `bot_plan/COMPLETE_SUMMARY.md`, the reference
Jupiter client in `bot_plan/sample_jupiter_code.txt`, and the API docs in
`bot_plan/docs/`.

## Replay backtest (data-driven strategy validation)

`tools/build_replay_parquet.py` converts the hourly PumpAPI archives
(`bot_plan/downloads/YYYY/MM/DD/HH.jsonl.zst`, ~10GB/day) into per-hour
parquet (`bot_plan/parquet/YYYY-MM-DD/`, ~7GB/day, gitignored) with one
streaming pass per hour (2 workers, ~30-55s/hour, memory-safe on 16GB).

`tools/backtest.py` replays the live strategy over the parquet (same
pipeline modules, entry at latency + 2s with slippage tiers, TP/SL/dead
pool/no-trades/max-hold exits, 60/40 compounding, loss pause, daily kill
switch, dev-veto replay, pump.fun 1% per-swap fees). A/B knobs:
`--no-veto --min-liq --min-score --take-profit --stop-loss --max-hold-min
--daily-loss-limit --fee-bps`.

Validated on 2026-07-21 (16h) + 2026-08-09 (24h, out-of-sample):
- current live config loses money (-$25.62 / -$17.69 with fees)
- score>=60 + pool-liq floor $5k + dev veto ≈ breakeven-to-positive
  (-$1.58 / +$2.06); liquidity floor is the dominant lever.
- These findings are wired into the live bot (MIN_SCORE=60,
  MIN_LIQUIDITY_USD=5000, LIQ_CONFIRM_WINDOW_S=10).

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
| `STARTING_BALANCE` | `20` | paper wallet initial bankroll (`/status` balance start) |
| `PRIVATE_KEY` | wallet keypair (base58) — **never your main wallet** |
| `JUPITER_API_KEY` | free at https://developers.jup.ag/portal (FREE tier = 1 RPS) |
| `BOT_TOKEN` / `CHAT_ID` | Telegram control + alerts (BotFather) |

Hardening v2 (all optional, sane defaults):

| Variable | Default | Meaning |
|---|---|---|
| `DAILY_LOSS_LIMIT` | `10` | halt trading until UTC midnight when daily realized PnL ≤ −limit (`0` = off) |
| `STATUS_INTERVAL_MIN` | `15` | periodic `/status` heartbeat card (`0` = off) |
| `MAX_HOLD_MIN` | `30` | force-exit a position held this long (stuck-position watchdog) |
| `LIVE_FEED_EXIT` | `true` | PumpAPI buy/sell stream → sub-second TP/SL triggers (shares the scanner's single connection) |
| `MIN_SCORE` | `60` | minimum feed score for a launch to be queued (feed-data entry path; backtest-validated) |
| `STALE_EXIT_SEC` | `60` | dead-token exit: no live trades this long + no DexScreener pair → exit (frees the position slot) |
| `STALE_EXIT_GRACE_SEC` | `15` | grace period after entry before the dead-token exit can fire |
| `MAX_CANDIDATE_AGE_MIN` | `5` | drop queued candidates older than this at dequeue time |
| `MIN_LIQUIDITY_USD` | `5000` | entry floor: skip the buy unless the on-chain pool liquidity (2×quoteInPool×SOL) proves ≥ this. Backtest-validated (removes the dead/thin-pool bleed); `0` = off |
| `LIQ_CONFIRM_WINDOW_S` | `10` | how long the bot waits for a confirming buy to push the pool over the floor |
| `DEV_REP_ENABLED` | `true` | Helius dev-reputation veto (read-only, fail-open) |
| `DEV_REP_MAX_CREATES_24H` | `3` | veto devs with ≥ N pump.fun creates in 24h (serial launchers) |
| `DEV_REP_MIN_AGE_HOURS` | `0` | veto wallets younger than this; `0` = off (weakest signal) |
| `DEV_REP_CACHE_TTL_MIN` | `10` | per-wallet verdict cache |
| `DEV_REP_TIMEOUT_S` | `2.5` | lookup budget; runs parallel to the entry-price estimate |

Optional: `SOLANA_RPC_URL` (Helius/Alchemy, optional reads only), `PUMPDEV_API_KEY`,
`HELIUS_API_KEY` (defaults to the `api-key` embedded in `SOLANA_RPC_URL` when that
URL is Helius). The dev-reputation veto queries Helius enhanced transactions for
the launch's dev wallet and blocks serial launchers / prior-dump wallets *before*
the buy; any lookup error fails OPEN (trading never blocked by a flaky check).

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

Command bot (`python-telegram-bot` `Application`/`CommandHandler` — docs in
`bot_plan/docs/telegram_bot_docs/`) plus markdown trade cards
(`telegramify-markdown` entities, no `parse_mode`) ported from
`bot_plan/sample_telegram_code.txt`.

| Command | Action |
|---|---|
| `/start` | open the trade gate (resume trading) |
| `/stop` | graceful shutdown: gate closes, in-flight trade finishes, exit 0 |
| `/status` | balance, winrate, realized PnL, active position, quote-gate stats |
| `/help` | command list |

Cards posted automatically: 🚀 startup, 🟢 buy, 💰/🔻 sell (PnL, ROI, hold
time, balance), 🏁 stopped summary. Only the configured `CHAT_ID` may send
commands. `AUTO_START=true` (default) begins trading on launch; set
`AUTO_START=false` to require `/start`.

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
