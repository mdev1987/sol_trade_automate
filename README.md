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

Validated battery (4 full days, 100bps fees, dev veto on; full 24h 07/21):
- deployed config (score>=60 + $5k liq floor + daily kill switch): **+$6.42**
  → 07/20 −10.36 (halt) · 07/21 −10.16 (halt) · 07/22 +24.88 · 08/09 +2.06
- same config without the kill switch: −$3.47 — the daily loss limit is the
  difference-maker (adds +$9.89); old course config over the same days: −$116.54
- regime-dependent: daily win-rate 23–43%; 07/21 evening hours (16–23) are
  materially worse than the day (that's the -10.16 halt day)
- These findings are wired into the live bot (MIN_SCORE=60,
  MIN_LIQUIDITY_USD=5000, LIQ_CONFIRM_WINDOW_S=10, DAILY_LOSS_LIMIT=10).

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
scripts/              # ops: ship_for_host.sh, deploy_host.sh, run_bot.sh,
                      #      _supervise.sh (nohup supervisor), sol-bot.service
tools/                # replay backtest: build_replay_parquet.py, backtest.py
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
| `PUMPCOINS_SOL_PRICE_URL` / `COINGECKO_SOL_PRICE_URL` | pumpcoins.net / CoinGecko | extra SOL/USD oracles in the fail-open chain (DexScreener → Jupiter → pumpcoins → CoinGecko → last-known) |
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

On a host **without systemd** (small VPS/container), run it supervised with the
built-in nohup supervisor instead of a terminal:

```bash
bash scripts/run_bot.sh start            # nohup + auto-restart (5s) + pidfile
bash scripts/run_bot.sh status
bash scripts/run_bot.sh stop             # graceful; kill -9 fallback, no orphans
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

## Supervision (24/7)

Two supported ways to keep the bot alive; **`scripts/run_bot.sh` is the default**
(everywhere, including hosts without systemd):

```bash
bash scripts/run_bot.sh {start|stop|status|restart}
```

- `start` = `nohup` a supervisor loop: runs `src/main.py`, auto-restarts 5s
  after any exit (crash or OOM), pidfile `.sniper-bot-super.pid`;
  logs → `bot_plan/bot_logs/supervisor.log`
- `stop` = kill the supervisor first (no re-spawn race), graceful SIGTERM,
  ≤25s grace, then kill -9 stragglers — **never leaves an orphan bot**
- add `@reboot cd /opt/sol-bot && bash scripts/run_bot.sh start` to crontab
  for auto-start after host reboot

systemd is optional: `scripts/sol-bot.service` (user unit — `systemctl --user
install`) or `/etc/systemd/system/sol-bot.service` (root, `Restart=always`,
starts on boot, stderr/tracebacks in `journalctl -u sol-bot -e`).
`scripts/deploy_host.sh` picks one automatically (systemd+root → service,
otherwise → `run_bot.sh`).

## Logs

- `bot_plan/bot_logs/bot.log` — runtime log (absolute paths, CWD-independent)
- `bot_plan/bot_logs/journal.json` — trade journal (JSONL)
- `bot_plan/bot_logs/trade_log.csv` — trade history
- `bot_plan/bot_logs/supervisor.log` — only when run via `run_bot.sh`

Operational notes: SIGHUP is handled like SIGINT/SIGTERM (graceful stop — no
silent death when a terminal closes); the shutdown tail itself is bounded
(websocket close timeout 2s + 20s watchdog → force-exit, supervisor restarts);
single-instance flock prevents two bots on the same host; every external call
(Telegram/Jupiter/DexScreener/Helius/SOL oracles) is time-boxed and fails open.

## Tests

```bash
uv run python tests/_smoke_test.py        # pure-logic checks, no network
uv run python tests/_integration_test.py  # end-to-end trade cycle, mocked network
```

## Deploy on a small host (3GB disk / 4GB RAM / 2GHz)

Measured footprint: **~47MB venv + ~270KB code** — the bot is asyncio/I-O bound,
runs at ~50-60MB RSS, and needs **no inbound ports** (Telegram polling, all
feeds/APIs outbound). Backtest data (58GB of parquet) stays on the dev machine —
**do not run backtests on the host**.

```bash
# 1) local: build the ship tarball (excludes .git/.venv/bot_plan/.env)
scripts/ship_for_host.sh /tmp/sol-bot-ship.tgz
scp /tmp/sol-bot-ship.tgz user@HOST:/tmp/
scp .env user@HOST:/tmp/.env          # secrets travel separately

# 2) host:
sudo mkdir -p /opt/sol-bot && sudo tar xzf /tmp/sol-bot-ship.tgz -C /opt/sol-bot
sudo cp /tmp/.env /opt/sol-bot/.env && sudo chmod 600 /opt/sol-bot/.env

# 3) host: provision venv + supervisor
sudo bash /opt/sol-bot/scripts/deploy_host.sh /opt/sol-bot
#   - with systemd: installs sol-bot.service (auto-restart, starts on boot)
#   - without systemd (containers/small hosts): falls back to a simple
#     supervisor  →  bash scripts/run_bot.sh {start|stop|status|restart}
#     (nohup + auto-restart every 5s; add "@reboot ... run_bot.sh start"
#     to crontab if you want it back after host reboot)

# 4) check
systemctl status sol-bot && tail -f /opt/sol-bot/bot_plan/bot_logs/bot.log
```

Notes: keep `DRY_RUN=true` until you're ready for real orders; logs land in
`bot_plan/bot_logs/` (absolute paths) and stderr/tracebacks in
`journalctl -u sol-bot -e` (systemd) or `bot_plan/bot_logs/supervisor.log`
(`run_bot.sh`). The unit sets `TZ=Asia/Tehran` to match local timestamps —
edit `Environment=` if you prefer UTC. First deployment is paper-only; flip
`DRY_RUN=false` + `systemctl --user restart sol-bot` (or `run_bot.sh restart`)
only when you are ready for real orders — and then run the bot on **one**
machine (the single-instance lock is per-host, two hosts would double-buy).
