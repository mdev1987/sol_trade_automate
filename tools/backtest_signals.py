"""Replay backtest of the debot smart-money SIGNAL strategy against PumpAPI
historical archives — apples-to-apples with tools/backtest.py (launch sniper).

Differs from the launch backtest in ONE way: entry triggers are debot signal
events (bot_plan/signal_raw/channel_list.json) instead of pump.fun create
events. Every downstream gate is shared:
  entry  -> arm at signal create_time + ENTRY_LATENCY_S, fill at the mint's
            next buy event, fill-time liquidity floor (quote gate). The
            entry-mult gate FAILS OPEN for signals (debot prices are
            USD-normalized, not comparable to the on-chain launch price —
            the pump band below is the real anti-chase rule).
  monitor-> TP / SL / dead_pool / no_trades / max_hold on buy+sell events
  risk   -> 60/40 compounding, loss pause, play floor, daily loss kill switch

Pattern gate A/B (the researched signal pattern):
  --mode raw    accept every signal event (no pattern filter)
  --mode band   pump band only: max_price_gain in [min_gain, max_gain]
  --mode full   live scanner gate: pump band + liquidity band + holders +
                top10 + mcap + vol24 + wallets + tiers + score >= min_score
                (default, mirrors src/signal_scanner.gate_signal/score_signal)

Replay-data caveats (same as launch backtest):
  * ENTRY_LATENCY_S (2s) models measured live latency; the live bot also polls
    debot every 20s, so real reaction is up to ~20s later than create_time —
    entry fills here are optimistic by that amount.
  * signal create_time is when debot fired the alert; the token's first replay
    buy may be a few seconds after (pump_swap/meteora events included).

Usage:
  uv run python tools/backtest_signals.py \
      --data bot_plan/parquet/2026-08-12 \
      --signals bot_plan/signal_raw/channel_list.json \
      --mode full [--out bot_plan/signal_backtest_full.json]
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from compounding import next_play_amount
from config import QuoteConfig, settings
from signal_scanner import SignalGate, gate_signal, score_signal

COLS = ["timestamp", "action", "mint", "poolId", "price", "quoteInPool"]

DEAD_POOL_LIQUIDITY_USD = 25.0


def _f(v) -> float | None:
    """Coerce a value to float, returning None for junk/empty."""
    try:
        if v in (None, "", "null"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v) -> int:
    """Coerce a value to int, returning 0 for junk/empty."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def load_signal_candidates(signals_path: str, mode: str, min_signals: int = 0,
                           band_min: float | None = None,
                           band_max: float | None = None) -> list[dict]:
    """Load signal events, dedup (same token within 60s), apply the mode gate.

    Returns candidates sorted by create_time: {create_time, mint, symbol,
    score, gain, gate_reason}.
    """
    raw = json.loads(Path(signals_path).read_text())
    results = raw["results"]
    meta = raw["meta"]
    gate = SignalGate()

    candidates: list[dict] = []
    seen_recent: dict[str, float] = {}  # mint -> last create_time accepted
    skipped: Counter = Counter()
    for ev in results:
        mint = ev.get("token") or ""
        ct = _f(ev.get("create_time")) or 0.0
        if not mint or not ct:
            skipped["no_token"] += 1
            continue
        if mint in seen_recent and ct - seen_recent[mint] < 60.0:
            skipped["dup_event"] += 1
            continue

        token_meta = (meta.get("tokens") or {}).get(mint) or {}
        sig_meta = (meta.get("signals") or {}).get(mint) or {}
        metrics = (meta.get("metrics") or {}).get(mint) or {}
        gain = _f(sig_meta.get("max_price_gain")) or 0.0

        if mode == "raw":
            passed, reason = True, ""
        elif mode == "band":
            lo = gate.min_gain if band_min is None else band_min
            hi = gate.max_gain if band_max is None else band_max
            if gain <= 0:
                passed, reason = False, "no_gain"
            elif gain < lo:
                passed, reason = False, f"gain:{gain:.2f}<{lo}"
            elif gain > hi:
                passed, reason = False, f"gain:{gain:.2f}>{hi}"
            else:
                passed, reason = True, ""
        else:  # full — live scanner gate, freshness disabled (replayed at signal time)
            passed, reason = gate_signal(
                ev, token_meta, sig_meta, metrics, gate, now=ct
            )
        if not passed:
            skipped[reason.split(":")[0]] += 1
            continue

        if min_signals > 0 and (_i(sig_meta.get("signal_count")) or 0) < min_signals:
            skipped[f"signals<{min_signals}"] += 1
            continue

        score = score_signal(ev, token_meta, sig_meta, metrics, gate)
        if mode == "full" and score < settings.min_score:
            skipped[f"score<{settings.min_score}"] += 1
            continue

        candidates.append({
            "create_time": ct,
            "mint": mint,
            "pool": metrics.get("pair") or "",
            "symbol": (token_meta.get("symbol") or mint[:6]),
            "score": score,
            "gain": round(gain, 3),
            "reason": reason,
        })
        seen_recent[mint] = ct

    candidates.sort(key=lambda c: c["create_time"])
    return candidates, skipped


def main() -> None:
    """Replay the signal strategy over parquet + signal events, write results."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="parquet folder (HH.parquet files)")
    ap.add_argument("--signals", required=True,
                    help="bot_plan/signal_raw/channel_list.json")
    ap.add_argument("--mode", choices=["raw", "band", "full"], default="full")
    ap.add_argument("--band-min", type=float, default=None, help="override band low edge")
    ap.add_argument("--band-max", type=float, default=None, help="override band high edge")
    ap.add_argument("--min-signals", type=int, default=0,
                    help="only candidates with signal_count >= this (0 = off)")
    ap.add_argument("--sol-usd", type=float, default=150.0,
                    help="static SOL/USD fallback (ignored if --sol-usd-file given)")
    ap.add_argument("--sol-usd-file", default=None,
                    help="JSON {hour_epoch_sec: usd} realtime SOL/USD series (overrides --sol-usd)")
    ap.add_argument("--entry-latency-s", type=float, default=2.0)
    ap.add_argument("--max-trades", type=int, default=0, help="0 = unlimited")
    ap.add_argument("--out", default="/tmp/signal_backtest_result.json")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--min-liq", type=float, default=None,
                    help="fill liquidity floor in USD (default: settings.min_liquidity_usd)")
    ap.add_argument("--max-hold-min", type=float, default=None)
    ap.add_argument("--fee-bps", type=float, default=100.0)
    ap.add_argument("--take-profit", type=float, default=None)
    ap.add_argument("--stop-loss", type=float, default=None)
    ap.add_argument("--slippage-model", choices=["tier", "impact"], default="tier",
                    help="tier=QuoteConfig.slippage_for tolerance as cost (conservative, "
                         "default); impact=2*trade_size/liquidity realized price impact")
    ap.add_argument("--daily-loss-limit", type=float, default=None)
    args = ap.parse_args()

    sol_usd = args.sol_usd
    sol_series: dict[int, float] = {}
    if args.sol_usd_file:
        sol_series = {int(k): float(v) for k, v in json.load(
            open(args.sol_usd_file)).items()}
        if sol_series:
            sol_usd = sol_series[min(sol_series)]

    def sol_usd_at(ts_s: float) -> float:
        """Realtime SOL/USD: nearest hourly sample (live bot polls with 60s TTL)."""
        if not sol_series:
            return sol_usd
        h = int(ts_s // 3600) * 3600
        nxt = h + 3600
        if h in sol_series and nxt in sol_series:
            frac = (ts_s - h) / 3600.0
            return sol_series[h] * (1 - frac) + sol_series[nxt] * frac
        return sol_series.get(h, sol_series.get(nxt, sol_usd))
    min_liq = settings.min_liquidity_usd if args.min_liq is None else args.min_liq
    quote = QuoteConfig()

    candidates, skipped = load_signal_candidates(args.signals, args.mode, args.min_signals,
                                             args.band_min, args.band_max)
    print(f"signals: {len(candidates)} accepted "
          f"(mode={args.mode}, skipped={dict(skipped)})", flush=True)
    if not candidates:
        raise SystemExit("no accepted candidates")

    files = sorted(glob.glob(str(Path(args.data) / "*.parquet")))
    if not files:
        raise SystemExit(f"no parquet in {args.data}")
    total_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    print(f"backtest: {len(files)} hours, {total_rows:,} rows, "
          f"sol_usd=${sol_usd:g}, latency={args.entry_latency_s}s, "
          f"floor=${min_liq:g}", flush=True)

    # ---- state (mirrors tools/backtest.py + signal queue)
    cand_i = 0
    pending = deque()  # candidates armed at or before current replay time
    armed = None
    pos = None
    last_price: dict[str, float] = {}
    last_liq: dict[str, float] = {}
    last_trade_ts: dict[str, float] = {}

    play_amount = settings.starting_amount
    consec_losses = 0
    paused_until = 0.0
    daily_pnl = 0.0
    daily_key = None  # UTC day id (local midnight for the loss limit)
    daily_halted_until = 0.0

    stats = Counter({"candidates": len(candidates)})
    exit_reasons = Counter()
    trades = []
    t0 = time.time()

    def slippage(liq: float) -> float:
        """Exit slippage as a fraction: modeled impact or configured tier."""
        if args.slippage_model == "impact":
            # realized price impact of the *exit* trade: size ~= position value,
            # impact ~ 2*size/liquidity (AMM constant-product), capped at 10%
            if pos is not None and liq and liq > 0:
                val = pos["tokens"] * pos["last_price"] * sol_usd_at(pos["entered_at"])
                return min(0.10, 2.0 * val / liq)
            return 0.0
        return quote.slippage_for(liq) / 10_000.0

    def open_position(cand, fill_price, liq, ts_s):
        """Open a position: debit the play amount, remember entry + stops."""
        nonlocal play_amount, pos, armed
        usd_fill = fill_price * sol_usd_at(ts_s)
        fee = 1.0 - args.fee_bps / 10_000.0
        tokens = (play_amount * fee) / usd_fill if usd_fill > 0 else 0.0
        pos = {
            "mint": cand["mint"], "symbol": cand["symbol"], "score": cand["score"],
            "pool": cand["pool"],
            "entry_price_sol": fill_price, "entry_usd": usd_fill,
            "tokens": tokens, "amount": play_amount,
            "tp": fill_price * (args.take_profit or settings.take_profit),
            "sl": fill_price * (args.stop_loss or settings.stop_loss),
            "entered_at": ts_s, "last_price": fill_price,
            "entry_liq": liq, "last_liq": liq, "last_trade_ts": ts_s,
            "gain": cand["gain"],
        }
        armed = None

    def close_position(reason, exit_price, liq, ts_s):
        """Close the position: credit proceeds, apply fees/slippage, update stats."""
        nonlocal pos, play_amount, consec_losses, paused_until, daily_pnl
        p = pos
        slip = slippage(liq if liq and liq > 0 else (p["last_liq"] or 0.0))
        proceeds = p["tokens"] * exit_price * sol_usd_at(ts_s) * (1.0 - slip) * (1.0 - args.fee_bps / 10_000.0)
        won = proceeds > p["amount"]
        pnl = proceeds - p["amount"]
        daily_pnl += pnl
        trades.append({
            "ts": round(ts_s, 1), "symbol": p["symbol"], "mint": p["mint"],
            "score": round(p["score"], 1), "gain": p["gain"],
            "entry_usd": round(p["entry_usd"], 8),
            "entry_liq": round(p.get("entry_liq", 0.0), 0),
            "exit_reason": reason, "exit_usd": round(exit_price * sol_usd_at(ts_s), 8),
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
            cand = pending.popleft()
            age = ts_s - cand["create_time"]
            if age > settings.max_candidate_age_min * 60:
                stats["aged_out"] += 1
                continue
            armed = (max(ts_s, cand["create_time"] + args.entry_latency_s), cand)
            return
        armed = None

    # ---------------- stream the replay ----------------
    halted = False
    for fpath in files:
        pf = pq.ParquetFile(fpath)
        for rb in pf.iter_batches(batch_size=500_000, columns=COLS):
            d = rb.to_pydict()
            n = len(d["timestamp"])
            for i in range(n):
                action = d["action"][i]
                mint = d["mint"][i]
                ts_ms = d["timestamp"][i]
                if ts_ms is None:
                    continue
                ts_s = ts_ms / 1000.0

                # daily loss limit resets at UTC midnight (matches live RiskManager)
                day = int(ts_s) // 86400
                if daily_key is None:
                    daily_key = day
                elif day != daily_key:
                    daily_key = day
                    daily_pnl = 0.0
                    consec_losses = 0
                    paused_until = 0.0
                    daily_halted_until = 0.0

                # --- arm signal candidates whose create_time has arrived ---
                while cand_i < len(candidates) and candidates[cand_i]["create_time"] <= ts_s:
                    pending.append(candidates[cand_i])
                    cand_i += 1

                if action not in ("buy", "sell") or mint is None:
                    continue
                # A mint can trade on multiple pools (bonding curve vs the
                # post-graduation AMM) with INCOMPATIBLE price scales. Only the
                # pool debot reported for this signal (metrics.pair == poolId)
                # is a valid venue; events on other pools are ignored so the
                # position's TP/SL and liquidity are never poisoned by them.
                active_pool = pos["pool"] if pos is not None else (
                    armed[1]["pool"] if armed is not None else None
                )
                if active_pool:
                    pool_id = d["poolId"][i]
                    if pool_id != active_pool:
                        continue
                price = d["price"][i]
                liq = (d["quoteInPool"][i] or 0.0) * 2.0 * sol_usd_at(ts_s)
                if price:
                    last_price[mint] = price
                if liq > 0:
                    last_liq[mint] = liq
                last_trade_ts[mint] = ts_s

                # --- entry: armed candidate filled by its next buy ---
                if pos is None and armed is not None:
                    if mint == armed[1]["mint"] and ts_s >= armed[0]:
                        fill_liq = last_liq.get(mint, 0.0)
                        if fill_liq < min_liq:
                            stats["thin_pool"] += 1
                            try_arm(ts_s)
                            continue
                        open_position(armed[1], price, fill_liq, ts_s)
                        stats["entries"] += 1
                        if args.verbose:
                            print(f"  ENTER {pos['symbol']} @ ${price*sol_usd_at(ts_s):.8f} "
                                  f"(score {pos['score']:.0f}, gain {pos['gain']:.2f}, "
                                  f"liq ${fill_liq:.0f})", flush=True)
                    elif ts_s > armed[0] + 10.0:
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

                # --- time-based exits + entry gating ---
                if pos is not None:
                    p = pos
                    age = ts_s - p["entered_at"]
                    quiet = ts_s - p["last_trade_ts"]
                    if age > (args.max_hold_min or settings.max_hold_min) * 60:
                        close_position("max_hold", p["last_price"], p["last_liq"], ts_s)
                    elif quiet >= settings.stale_exit_sec and age >= settings.stale_exit_grace_sec:
                        close_position("no_trades", p["last_price"], p["last_liq"], ts_s)
                if pos is None and armed is None:
                    daily_limit = (args.daily_loss_limit if args.daily_loss_limit is not None
                                   else settings.daily_loss_limit)
                    if daily_limit > 0 and daily_pnl <= -abs(daily_limit):
                        # daily kill: stop ENTERING until next UTC midnight
                        stats["daily_halt"] += 1
                        daily_halted_until = (daily_key + 1) * 86400.0
                        daily_pnl = 0.0  # reset so the halt persists via the timer
                        consec_losses = 0
                    if ts_s < daily_halted_until:
                        continue
                    if ts_s < paused_until:
                        continue
                    try_arm(ts_s)
                if args.max_trades and stats["entries"] >= args.max_trades:
                    halted = True
                    break
                if halted:
                    break
            if halted:
                break
            if args.verbose and ts_s % 3600 < 1:
                print(f"  ... {len(files)}h done: {stats['entries']} entries, "
                      f"{stats['wins']}W/{stats['losses']}L ({(time.time()-t0):.0f}s)", flush=True)
        if halted:
            break

    stats["cand_after_window"] = max(0, len(candidates) - cand_i)

    # ---- report ----
    wins = stats["wins"]; losses = stats["losses"]; entries = stats["entries"]
    pnl = sum(t["pnl"] for t in trades)
    res = {
        "data": args.data, "signals": args.signals, "mode": args.mode,
        "hours": len(files), "rows": total_rows, "sol_usd": sol_usd,
        "entry_latency_s": args.entry_latency_s,
        "funnel": {k: stats[k] for k in
                   ("candidates", "entries", "no_fill", "thin_pool", "aged_out",
                    "daily_halt", "cand_after_window")},
        "skipped": dict(skipped),
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
        f.write("ts,symbol,score,gain,entry_usd,entry_liq,exit_reason,exit_usd,pnl,amount,held_s\n")
        for t in trades:
            f.write(f"{t['ts']},{t['symbol']},{t['score']},{t['gain']},{t['entry_usd']},"
                    f"{t['entry_liq']},{t['exit_reason']},{t['exit_usd']},{t['pnl']},"
                    f"{t['amount']},{t['held_s']}\n")
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()