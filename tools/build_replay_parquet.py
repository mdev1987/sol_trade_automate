"""Memory-safe replay archive -> parquet builder.

Streams each hourly .jsonl.zst and writes one parquet per hour to OUT_DIR
(one file per hour, sorted by timestamp). Never holds more than one hour in
memory; row groups are flushed every BATCH rows.

Usage:
    uv run python tools/build_replay_parquet.py \
        --src bot_plan/downloads/2026/07/21 \
        --out bot_plan/parquet/2026-07-21 \
        [--workers 2] [--batch 500000]

The columns mirror the raw PumpAPI event keys so a create/buy/sell row can be
reconstructed into the dict our pipeline expects (data_stream.TokenLaunch,
rug_detection, scoring_algorithm, scanner_filter).
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import zstandard

log = logging.getLogger("build_replay")

SCHEMA = pa.schema(
    [
        ("timestamp", pa.int64()),  # ms since epoch (raw PumpAPI field)
        ("action", pa.string()),
        ("signature", pa.string()),
        ("mint", pa.string()),
        ("txSigner", pa.string()),
        ("poolId", pa.string()),
        ("pool", pa.string()),
        ("price", pa.float64()),
        ("tokenAmount", pa.float64()),
        ("quoteAmount", pa.float64()),
        ("marketCapQuote", pa.float64()),
        ("supply", pa.float64()),
        ("tokensInPool", pa.float64()),
        ("quoteInPool", pa.float64()),
        ("quoteMint", pa.string()),
        ("name", pa.string()),
        ("symbol", pa.string()),
        ("isMayhemMode", pa.bool_()),
        ("isCashbackEnabled", pa.bool_()),
        ("burnedLiquidity", pa.float64()),
        ("freezeAuthority", pa.string()),
        ("mintAuthority", pa.string()),
        ("poolFeeRate", pa.float64()),
        ("localTimestamp", pa.int64()),  # replay-server receipt (ms), nullable
    ]
)

FIELDS = [f.name for f in SCHEMA]


def _num(v):
    """float() that tolerates junk ('0%', '', None) — rug_check's own float()
    calls strip '%' too, so numeric coercion preserves its semantics."""
    if v is None:
        return None
    try:
        return float(str(v).replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def _txt(v):
    """str() that returns None for None (schema-safe)."""
    return str(v) if v is not None else None


def _row(ev: dict) -> list:
    """Project one raw event onto the schema columns (None-safe)."""
    return [
        int(ev["timestamp"]) if ev.get("timestamp") is not None else None,
        ev.get("action"),
        ev.get("signature"),
        ev.get("mint"),
        ev.get("txSigner"),
        _txt(ev.get("poolId")),
        _txt(ev.get("pool")),
        _num(ev.get("price")),
        _num(ev.get("tokenAmount")),
        _num(ev.get("quoteAmount")),
        _num(ev.get("marketCapQuote")),
        _num(ev.get("supply")),
        _num(ev.get("tokensInPool")),
        _num(ev.get("quoteInPool")),
        _txt(ev.get("quoteMint")),
        _txt(ev.get("name")),
        _txt(ev.get("symbol")),
        bool(ev.get("isMayhemMode")),
        bool(ev.get("isCashbackEnabled")),
        _num(ev.get("burnedLiquidity")),
        _txt(ev.get("freezeAuthority")),
        _txt(ev.get("mintAuthority")),
        _num(ev.get("poolFeeRate")),
        int(ev["localTimestamp"]) if ev.get("localTimestamp") is not None else None,
    ]


def build_hour(src_path: str, out_path: str, batch: int = 500_000) -> dict:
    """Stream one .jsonl.zst into a sorted parquet file. Returns stats.

    Memory-safe: at most `batch` rows are held as Python lists at a time;
    batches are written as row groups to a single ParquetWriter kept open
    for the whole hour. The hour file is then sorted by timestamp in a
    separate pass (~1.7M rows as arrow — a few hundred MB, safe).
    """
    t0 = time.time()
    cols = {f: [] for f in FIELDS}
    rows = 0
    creates = 0
    writer = pq.ParquetWriter(out_path, SCHEMA)
    dctx = zstandard.ZstdDecompressor()
    try:
        with open(src_path, "rb") as fh, dctx.stream_reader(fh, read_across_frames=True) as r:
            carry = b""
            while True:
                chunk = r.read(8_000_000)
                if not chunk:
                    break
                data = carry + chunk
                lines = data.split(b"\n")
                carry = lines.pop()  # last fragment may be partial
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        ev = orjson.loads(line)
                    except Exception:  # noqa: BLE001 — one bad line must not kill the hour
                        continue
                    if not ev.get("action"):
                        continue
                    for f, v in zip(FIELDS, _row(ev)):
                        cols[f].append(v)
                    rows += 1
                    if ev["action"] == "create":
                        creates += 1
                    if rows % batch == 0:
                        _write_batch(writer, cols)
        if carry.strip():
            try:
                ev = orjson.loads(carry)
                if ev.get("action"):
                    for f, v in zip(FIELDS, _row(ev)):
                        cols[f].append(v)
                    rows += 1
                    if ev["action"] == "create":
                        creates += 1
            except Exception:  # noqa: BLE001
                pass
        if rows % batch != 0:
            _write_batch(writer, cols)
    finally:
        writer.close()

    # sort the hour file by timestamp (raw order is only approximately chronological)
    t1 = time.time()
    table = pq.read_table(out_path)
    idx = pc.sort_indices(table["timestamp"])
    pq.write_table(table.take(idx), out_path)
    del table, idx
    return {"src": os.path.basename(src_path), "rows": rows, "creates": creates,
            "seconds": round(time.time() - t0, 1), "sort_s": round(time.time() - t1, 1)}


def _write_batch(writer: pq.ParquetWriter, cols: dict) -> None:
    """Flush the accumulated row columns as one parquet row group."""
    table = pa.Table.from_pydict(cols, schema=SCHEMA)
    writer.write_table(table)
    for f in FIELDS:
        cols[f].clear()


def main() -> None:
    """Build one parquet per hourly archive in --src into --out."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="folder with HH.jsonl.zst files")
    ap.add_argument("--out", required=True, help="output folder (one parquet per hour)")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--batch", type=int, default=500_000)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    hours = sorted(f for f in os.listdir(args.src) if f.endswith(".jsonl.zst"))
    if not hours:
        raise SystemExit(f"no .jsonl.zst in {args.src}")
    jobs = [(os.path.join(args.src, h), os.path.join(args.out, h.replace(".jsonl.zst", ".parquet")))
            for h in hours]
    print(f"{len(hours)} hours -> {args.out} (workers={args.workers}, batch={args.batch})", flush=True)
    total_rows = total_creates = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(build_hour, src, out, args.batch): src for src, out in jobs}
        for fut in as_completed(futs):
            st = fut.result()
            total_rows += st["rows"]; total_creates += st["creates"]
            print(f"  {st['src']}: {st['rows']:,} rows ({st['creates']:,} creates) "
                  f"in {st['seconds']}s (sort {st['sort_s']}s)", flush=True)
    print(f"DONE: {total_rows:,} rows, {total_creates:,} creates", flush=True)


if __name__ == "__main__":
    main()
