# Solana Pump.fun Sniper Bot

Automated trading bot that detects token launches in <1s, validates them
(rug detection + filters + scoring), buys via Jupiter, monitors price, and
sells at TP/SL with 60/40 compounding. Controlled from Telegram (`/start`,
`/stop`, `/status`).

Two strategies, selected with `STRATEGY_MODE`:

| Mode | Scanner | Source | Scope |
|---|---|---|---|
| `launch` (default) | `token_scanner` | PumpAPI/PumpDev create feed | pump.fun bonding curve |
| `signal` | `signal_scanner` | debot smart-money feed | all DEXs (pump, meteora, raydium) |

## Strategy status

- **Launch sniper — validated.** Replay backtest over 4 full days (realtime
  SOL/USD + modeled price-impact slippage, 100bps fees, dev veto on): profitable
  every day, **+$16.38 total** (per-day: 07/20 +$4.42 · 07/21 +$1.46 · 07/22
  +$10.03 · 08/09 +$0.47; daily win-rate 24–44%). Wired into the live bot
  (`MIN_SCORE=60`, `MIN_LIQUIDITY_USD=5000`, `MAX_ENTRY_MULT=5`,
  `DAILY_LOSS_LIMIT=8`).
- **Signal strategy — promising, pre-deployment.** In-sample on one 28h window,
  band 1.0–1.87× + tight TP 1.5×/SL 0.70× = **+$3.51** (44.8% win-rate, 58
  trades) vs the default TP 2.0×/SL 0.82× which loses (−$4.93). The edge is
  thin and split-half unstable; it needs an out-of-sample day before going live.
  See `bot_plan/signal_replay_backtest.md`.

## Layout

```
src/                  # application code
  main.py             # orchestration: scanner + bot + telegram, graceful shutdown
  token_scanner.py    # launch feed → validate → score → queue
  signal_scanner.py   # debot signal feed → gate → score → queue (STRATEGY_MODE=signal)
  bot.py              # buy → monitor → sell → compound
  telegram_bot.py     # command bot: /start /stop /status (telegramify-markdown)
  stats.py            # balance, winrate, PnL, active position
  control.py          # trade gate (start/stop)
  jupiter_swap.py     # JupiterSwap: quote gate, sign, execute, slippage escalation
  dexscreener.py      # pair liquidity / volume / price (REST, 60 req/min)
  data_stream.py      # PumpAPI + PumpDev WebSocket feeds with failover
  rug_detection.py    # scam name, dev dump, honeypot, wash trading, mayhem
  scanner_filter.py   # thresholds (age, liq, volume, txns, b/s, mcap)
  scoring_algorithm.py# 80-point scoring
  price_monitor.py    # TP / SL / dead pool / no-trades / max-hold exits
  risk_management.py  # loss pause, play floor
  compounding.py      # 60/40 split
  monitoring.py       # logging, journal, trade log
tests/                # smoke + integration tests (no network)
scripts/              # ship_for_host.sh, deploy_host.sh, run_bot.sh, watchdog.sh, service
tools/                # backtest.py, backtest_signals.py, build_replay_parquet.py
```

## Setup

```bash
uv sync                                  # installs deps into .venv
cp .env.example .env
```

Key env vars (all documented with comments in `.env.example`):

| Var | Default | Purpose |
|---|---|---|
| `STRATEGY_MODE` | `launch` | `launch` = pump.fun sniper, `signal` = debot signal scanner |
| `STARTING_AMOUNT` | `2` | play size (USDC) |
| `TAKE_PROFIT` / `STOP_LOSS` | `2.0` / `0.82` | exit levels (signal mode: TP 1.5 / SL 0.70 recommended) |
| `MIN_SCORE` | `60` | minimum score to queue |
| `MIN_LIQUIDITY_USD` | `5000` | entry floor: skip buy unless on-chain pool liquidity ≥ this |
| `LIQ_CONFIRM_WINDOW_S` | `10` | wait for a confirming buy to push the pool over the floor |
| `MAX_ENTRY_MULT` | `5` | skip fills already > N× the launch price (anti-chase) |
| `DAILY_LOSS_LIMIT` | `8` | halt until UTC midnight when daily PnL ≤ −limit (`0` = off) |
| `LIVE_FEED_EXIT` | `true` | PumpAPI buy/sell stream → sub-second TP/SL triggers |
| `DEV_REP_ENABLED` | `true` | Helius dev-reputation veto (serial launchers, prior dumps; fail-open) |
| `PRIVATE_KEY` | — | wallet keypair (base58) — **never your main wallet** |
| `JUPITER_API_KEY` | — | free at https://developers.jup.ag/portal |
| `BOT_TOKEN` / `CHAT_ID` | — | Telegram control + alerts |

## Run

```bash
uv run python src/main.py                # scanner + bot + telegram (recommended, 24/7)
uv run python src/token_scanner.py       # launch scanner only
uv run python src/bot.py                 # bot only
```

On a host without systemd, use the built-in supervisor:

```bash
bash scripts/run_bot.sh start|stop|status|restart
```

Always run with `DRY_RUN=true` first — the quote gate still validates real
routes against Jupiter but nothing is signed or executed.

## Telegram control

| Command | Action |
|---|---|
| `/start` | open the trade gate (resume trading) |
| `/stop` | graceful shutdown: gate closes, in-flight trade finishes, exit 0 |
| `/status` | balance, winrate, realized PnL, active position, quote-gate stats |
| `/help` | command list |

Cards posted automatically: 🚀 startup, 🟢 buy, 💰/🔻 sell, 🏁 stopped summary.
Only the configured `CHAT_ID` may send commands.

## Backtesting

The backtests replay the exact live pipeline (same modules, entry latency +2s,
TP/SL/dead/no-trades/max-hold exits, 60/40 compounding, loss pause, daily kill
switch, dev-veto, 100bps fees) over PumpAPI historical archives:

```bash
# convert downloaded archives (last 24h) to parquet
uv run python tools/build_replay_parquet.py \
    --src bot_plan/downloads/2026/08/13 --out bot_plan/parquet/2026-08-13

# launch strategy
uv run python tools/backtest.py --data bot_plan/parquet/2026-08-13 \
    --sol-usd-file bot_plan/sol_usd_hourly.json --slippage-model impact

# signal strategy (needs bot_plan/signal_raw/channel_list.json from a live crawl)
uv run python tools/backtest_signals.py --data bot_plan/parquet/2026-08-13 \
    --signals bot_plan/signal_raw/channel_list.json \
    --band-min 1.0 --band-max 1.87 --take-profit 1.5 --stop-loss 0.70
```

The debot signal API is live-only (~24h retention): crawl signals now, then
download the matching replay archives (`replay.pumpapi.io/YYYY/MM/DD/HH.jsonl.zst`).
Replay archives are multi-GB — they stay out of git (`bot_plan/` is ignored).

## Logs

- `bot_logs/bot.log` — runtime log (absolute paths, CWD-independent)
- `bot_logs/journal.json` — trade journal (JSONL)
- `bot_logs/trade_log.csv` — trade history
- `bot_logs/supervisor.log` — only when run via `run_bot.sh`

Operational notes: SIGHUP handled like SIGINT/SIGTERM (graceful stop); the
shutdown tail is bounded (websocket close + 20s watchdog → force-exit); a
single-instance flock prevents two bots on the same host; every external call
(Telegram/Jupiter/DexScreener/Helius/SOL oracles) is time-boxed and fails open.

## Tests

```bash
uv run python tests/_smoke_test.py        # pure-logic checks, no network
uv run python tests/_integration_test.py  # end-to-end trade cycle, mocked network
```

## Deploy on a small host

Footprint: ~47MB venv + ~270KB code, ~50-60MB RSS, no inbound ports. Backtest
data stays on the dev machine — **do not run backtests on the host**.

```bash
# 1) local: build the ship tarball (excludes .git/.venv/bot_plan/.env)
scripts/ship_for_host.sh /tmp/sol-bot-ship.tgz
scp /tmp/sol-bot-ship.tgz user@HOST:/tmp/ && scp .env user@HOST:/tmp/.env

# 2) host:
sudo mkdir -p /opt/sol-bot && sudo tar xzf /tmp/sol-bot-ship.tgz -C /opt/sol-bot
sudo cp /tmp/.env /opt/sol-bot/.env && sudo chmod 600 /opt/sol-bot/.env
sudo bash /opt/sol-bot/scripts/deploy_host.sh /opt/sol-bot
```

Alternative: `git clone` into the repo dir and run `bash scripts/deploy_host.sh`
there (no /opt/sol-bot needed). Keep `DRY_RUN=true` until ready for real orders,
and run the bot on **one** machine (the single-instance lock is per-host).