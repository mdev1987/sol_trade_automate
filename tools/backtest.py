"""Replay backtest of the sniper strategy against PumpAPI historical archives.

Streams the parquet one batch at a time (never loads more than a batch in
RAM — safe on 16GB machines) and replays the EXACT live pipeline:

  create -> TokenLaunch.from_event -> rug_check -> passes_feed_filters ->
            score_feed (>= MIN_SCORE) -> queue (FIFO, MAX_CANDIDATE_AGE_MIN)
  entry  -> after ENTRY_LATENCY_S, fill at the next buy event of that mint
            with slippage tiers (QuoteConfig.slippage_for)
  veto   -> replay of the dev-reputation rule: dev with >= 3 creates in the
            replay window is vetoed (mirrors the live Helius serial-launcher veto)
  monitor-> TP / SL / dead_pool / no_trades / max_hold on buy+sell events
  risk   -> 60/40 compounding (compounding.next_play_amount), loss pause
            (2 losses -> 5 min), play floor, daily loss kill switch

Replay-data caveats (documented in bot_plan/pumpapi_historical-replay.md):
  * real execution is slower than replay — ENTRY_LATENCY_S (default 2s)
    models our measured live latency; be conservative.
  * bundle atomicity (same-mint events within ~3ms) is not modeled in v1.
  * replay create events lack `initialBuy`; it is inferred as dev_sol/price
    (same semantics as the live filter: dev bought >0 at launch).

Usage:
  uv run python tools/backtest.py --data bot_plan/parquet/2026-07-21 \
      [--sol-usd 150] [--entry-latency-s 2.0] [--max-trades 0] \
      [--out /tmp/backtest_result.json]
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from collections import Counter, deque
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow.compute as pc

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from compounding import next_play_amount
from config import QuoteConfig, settings
from data_stream import TokenLaunch
from rug_detection import rug_check
from scanner_filter import passes_feed_filters
from scoring_algorithm import score_feed

# columns the pipeline needs (raw key names preserved)
COLS = ["timestamp", "action", "signature", "mint", "txSigner", "price",
        "tokenAmount", "quoteAmount", "marketCapQuote", "supply",
        "tokensInPool", "quoteInPool", "quoteMint", "name", "symbol",
        "isMayhemMode", "isCashbackEnabled", "burnedLiquidity",
        "freezeAuthority", "mintAuthority", "poolFeeRate"]

DEAD_POOL_LIQUIDITY_USD = 25.0


def make_launch(row: dict) -> TokenLaunch:
    launch = TokenLaunch.from_event(row, source="pumpapi")
    # replay creates lack initialBuy -> infer dev's launch buy (tokens)
    if launch.initial_buy_tokens <= 0 and launch.dev_sol and row.get("price"):
        launch.initial_buy_tokens = launch.dev_sol / row["price"]
        row["initialBuy"] = launch.initial_buy_tokens
    return launch


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="parquet folder (HH.parquet files)")
    ap.add_argument("--sol-usd", type=float, default=150.0, help="SOL price for USD conversion")
    ap.add_argument("--entry-latency-s", type=float, default=2.0)
    ap.add_argument("--max-trades", type=int, default=0, help="0 = unlimited")
    ap.add_argument("--out", default="/tmp/backtest_result.json")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--daily-loss-limit", type=float, default=None,
                    help="halt at this daily PnL (default: settings/10)")
    ap.add_argument("--no-veto", action="store_true", help="disable the dev-rep replay veto")
    ap.add_argument("--min-liq", type=float, default=0.0,
                    help="skip fills when the pool liquidity is below this USD")
    ap.add_argument("--max-hold-min", type=float, default=None)
    ap.add_argument("--min-score", type=float, default=None,
                    help="score gate (default: settings/45)")
    ap.add_argument("--fee-bps", type=float, default=100.0,
                    help="per-swap protocol fee in bps (pump.fun = 100 = 1 pct each side)")
    ap.add_argument("--min-mult", type=float, default=0.0,
                    help="skip fills unless price/create-price >= this (momentum gate)")
    ap.add_argument("--max-mult", type=float, default=0.0,
                    help="skip fills when price/create-price exceeds this (0 = off)")
    ap.add_argument("--take-profit", type=float, default=None)
    ap.add_argument("--stop-loss", type=float, default=None)
    args = ap.parse_args()
    if args.daily_loss_limit is not None:
        daily_limit = args.daily_loss_limit
    else:
        daily_limit = settings.daily_loss_limit

    sol_usd = args.sol_usd
    quote = QuoteConfig()

    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    if not files:
        raise SystemExit(f"no parquet in {args.data}")
    total_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    print(f"backtest: {len(files)} hours, {total_rows:,} rows, "
          f"sol_usd=${sol_usd:g}, latency={args.entry_latency_s}s", flush=True)

    # ---- replay state (bounded: queues + one position + per-mint price maps)
    pending = deque()  # (qualified_at_s, launch, score) FIFO — mirrors the bot queue
    armed = None  # (fill_from_s, launch, score) candidate being entered
    pos = None  # dict position
    dev_creates = Counter()
    last_price: dict[str, float] = {}
    last_liq: dict[str, float] = {}
    last_trade_ts: dict[str, float] = {}

    # ---- risk (mirrors RiskManager + compounding on replay time)
    play_amount = settings.starting_amount
    consec_losses = 0
    paused_until = 0.0
    daily_pnl = 0.0

    stats = Counter()  # launches/rug/filter/score/vetoed/qualified/entries/exits...
    exit_reasons = Counter()
    trades = []
    t0 = time.time()

    def slippage(liq: float) -> float:
        return quote.slippage_for(liq) / 10_000.0

    def open_position(launch, score, fill_price, liq, ts_s):
        nonlocal play_amount, pos, armed
        usd_fill = fill_price * sol_usd
        fee = 1.0 - args.fee_bps / 10_000.0
        tokens = (play_amount * fee) / usd_fill if usd_fill > 0 else 0.0
        pos = {
            "mint": launch.mint, "symbol": launch.symbol, "score": score,
            "entry_price_sol": fill_price, "entry_usd": usd_fill,
            "tokens": tokens, "amount": play_amount,
            "tp": fill_price * (args.take_profit or settings.take_profit),
            "sl": fill_price * (args.stop_loss or settings.stop_loss),
            "entered_at": ts_s, "last_price": fill_price,
            "entry_liq": liq, "last_liq": liq, "last_trade_ts": ts_s,
            # momentum at fill: fill price vs the create event price (SOL)
            "entry_mult": fill_price / max(launch.raw.get("price") or 0.0, 1e-30),
        }
        armed = None

    def close_position(reason, exit_price, liq, ts_s):
        nonlocal pos, play_amount, consec_losses, paused_until, daily_pnl
        p = pos
        slip = slippage(liq if liq and liq > 0 else (p["last_liq"] or 0.0))
        proceeds = p["tokens"] * exit_price * sol_usd * (1.0 - slip) * (1.0 - args.fee_bps / 10_000.0)
        won = proceeds > p["amount"]
        pnl = proceeds - p["amount"]
        daily_pnl += pnl
        trades.append({
            "ts": round(ts_s, 1), "symbol": p["symbol"], "mint": p["mint"],
            "score": round(p["score"], 1), "entry_usd": round(p["entry_usd"], 8),
            "entry_liq": round(p.get("entry_liq", 0.0), 0),
            "entry_mult": round(p.get("entry_mult", 0.0), 2),
            "exit_reason": reason, "exit_usd": round(exit_price * sol_usd, 8),
            "pnl": round(pnl, 4), "proceeds": round(proceeds, 4),
            "amount": round(p["amount"], 2), "held_s": round(ts_s - p["entered_at"], 1),
        })
        play_amount = next_play_amount(p["amount"], won, reason)
        if won:
            consec_losses = 0
        else:
            consec_losses += 1
            if consec_losses >= settings.loss_pause_trigger:
                paused_until = ts_s + settings.loss_pause_minutes * 60
                consec_losses = 0
        exit_reasons[reason] += 1
        stats["wins" if won else "losses"] += 1
        pos = None

    def try_arm(ts_s):
        """Take the oldest fresh candidate; drop stale ones (queue aging)."""
        nonlocal armed
        while pending:
            q_at, launch, score = pending.popleft()
            age = ts_s - q_at
            if age > settings.max_candidate_age_min * 60:
                stats["aged_out"] += 1
                continue
            armed = (max(ts_s, q_at + args.entry_latency_s), launch, score)
            return
        armed = None

    # ---------------- stream the replay ----------------
    batch_i = 0
    halted = False
    for fpath in files:  # files are hour-partitioned + sorted by timestamp
        pf = pq.ParquetFile(fpath)
        for rb in pf.iter_batches(batch_size=500_000, columns=COLS):
            batch_i += 1
            d = rb.to_pydict()
            n = len(d["timestamp"])
            for i in range(n):
                action = d["action"][i]
                mint = d["mint"][i]
                ts_ms = d["timestamp"][i]
                if ts_ms is None:
                    continue
                ts_s = ts_ms / 1000.0

                if action == "create":
                    if mint is None:
                        continue
                    stats["launches"] += 1
                    row = {c: d[c][i] for c in COLS}
                    dev = row.get("txSigner")
                    if dev:
                        dev_creates[dev] += 1
                    try:
                        launch = make_launch(row)
                    except Exception:
                        stats["parse_error"] += 1
                        continue
                    if not rug_check(launch, None, row).passed:
                        stats["rug"] += 1
                        continue
                    passed, _ = passes_feed_filters(launch)
                    if not passed:
                        stats["filter"] += 1
                        continue
                    score = score_feed(launch)
                    if score < (args.min_score if args.min_score is not None else settings.min_score):
                        stats["score_skip"] += 1
                        continue
                    if not args.no_veto and dev and dev_creates[dev] >= 3:
                        stats["dev_veto"] += 1  # replay of the Helius serial-launcher veto
                        continue
                    stats["qualified"] += 1
                    pending.append((ts_s, launch, score))
                    continue

                if action not in ("buy", "sell") or mint is None:
                    continue
                price = d["price"][i]
                liq = (d["quoteInPool"][i] or 0.0) * 2.0 * sol_usd
                if price:
                    last_price[mint] = price
                if liq > 0:
                    last_liq[mint] = liq
                last_trade_ts[mint] = ts_s

                # --- entry: armed candidate gets filled by its next buy ---
                if pos is None and armed is not None:
                    if mint == armed[1].mint and ts_s >= armed[0]:
                        fill_liq = last_liq.get(mint, 0.0)
                        if fill_liq < args.min_liq:
                            stats["thin_pool"] += 1  # quote gate would reject
                            try_arm(ts_s)
                            continue
                        mult = price / max(armed[1].raw.get("price") or 0.0, 1e-30)
                        if args.min_mult and mult < args.min_mult:
                            stats["low_mult"] += 1  # weak momentum at fill
                            try_arm(ts_s)
                            continue
                        if args.max_mult and mult > args.max_mult:
                            stats["high_mult"] += 1  # too late / topped out
                            try_arm(ts_s)
                            continue
                        open_position(armed[1], armed[2], price, fill_liq, ts_s)
                        stats["entries"] += 1
                        if args.verbose:
                            print(f"  ENTER {pos['symbol']} @ ${price*sol_usd:.8f} "
                                  f"(score {pos['score']:.0f}, liq ${fill_liq:.0f})", flush=True)
                    elif ts_s > armed[0] + 10.0:
                        # armed but no fill within 10s — dead token, move on
                        stats["no_fill"] += 1
                        try_arm(ts_s)

                # --- monitor the open position on this mint's events ---
                if pos is not None and mint == pos["mint"]:
                    if price:
                        pos["last_price"] = price
                    if liq > 0:
                        pos["last_liq"] = liq
                    pos["last_trade_ts"] = ts_s
                    p = price or pos["last_price"]
                    lq = pos["last_liq"]
                    if p >= pos["tp"]:
                        close_position("take_profit", p, lq, ts_s)
                    elif p <= pos["sl"]:
                        close_position("stop_loss", p, lq, ts_s)
                    elif lq < DEAD_POOL_LIQUIDITY_USD:
                        close_position("dead_pool", p, lq, ts_s)

                # --- time-based exits + entry gating (checked every event) ---
                if pos is not None:
                    p = pos
                    age = ts_s - p["entered_at"]
                    quiet = ts_s - p["last_trade_ts"]
                    if age > (args.max_hold_min or settings.max_hold_min) * 60:
                        close_position("max_hold", p["last_price"], p["last_liq"], ts_s)
                    elif quiet >= settings.stale_exit_sec and age >= settings.stale_exit_grace_sec:
                        close_position("no_trades", p["last_price"], p["last_liq"], ts_s)
                if pos is None and armed is None:
                    if daily_limit > 0 and daily_pnl <= -abs(daily_limit):
                        stats["daily_halt"] += 1
                        halted = True
                        break
                    if ts_s < paused_until:
                        continue  # loss pause — skip entries
                    try_arm(ts_s)
                if args.max_trades and stats["entries"] >= args.max_trades:
                    halted = True
                    break
                if halted:
                    break
            if halted:
                break
            if args.verbose and batch_i % 10 == 0:
                print(f"  ... {batch_i} batches, {stats['entries']} entries, "
                      f"{stats['wins']}W/{stats['losses']}L ({(time.time()-t0):.0f}s)", flush=True)
        if halted:
            break

    # ---- report ----
    wins = stats["wins"]; losses = stats["losses"]; entries = stats["entries"]
    pnl = sum(t["pnl"] for t in trades)
    res = {
        "data": args.data, "hours": len(files), "rows": total_rows,
        "sol_usd": sol_usd, "entry_latency_s": args.entry_latency_s,
        "funnel": {k: stats[k] for k in
                   ("launches", "rug", "filter", "score_skip", "dev_veto", "qualified",
                    "entries", "no_fill", "thin_pool", "low_mult", "high_mult",
                    "aged_out", "parse_error", "daily_halt")},
        "exit_reasons": dict(exit_reasons),
        "trades": len(trades), "wins": wins, "losses": losses,
        "winrate": round(wins / entries * 100, 1) if entries else 0.0,
        "pnl_usd": round(pnl, 2),
        "avg_pnl": round(pnl / entries, 4) if entries else 0.0,
        "final_play_amount": round(play_amount, 2),
        "final_balance": round(settings.starting_balance + pnl, 2),
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2)
    with open(str(Path(args.out).with_suffix(".csv")), "w", encoding="utf-8") as f:
        f.write("ts,symbol,score,entry_usd,entry_liq,entry_mult,exit_reason,exit_usd,pnl,amount,held_s\n")
        for t in trades:
            f.write(f"{t['ts']},{t['symbol']},{t['score']},{t['entry_usd']},{t['entry_liq']},{t['entry_mult']},{t['exit_reason']},"
                    f"{t['exit_usd']},{t['pnl']},{t['amount']},{t['held_s']}\n")
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
