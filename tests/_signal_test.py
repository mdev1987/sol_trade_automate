"""Signal-scanner tests — pure logic, no network (synthetic debot payloads).

Covers: pump-band gate (the researched winner feature), hard hygiene gates,
scoring monotonicity, Candidate building (TokenLaunch shape for the pipeline),
and freshness/backlog handling.
"""

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
import sys

from config import settings
from signal_scanner import SignalGate, build_candidate, gate_signal, score_signal

# --- synthetic debot payloads (shape matches bot_plan/signal_raw) ---
TOKEN = "TokenMintAddrPump"
PAIR = "PoolAddrXYZ"


def make_event(create_time, wallets=3, **tts_over):
    """A synthetic debot signal event (wallet_stats + token_trading_stat)."""
    tts = {
        "fdv": 50000, "holders": 200, "liquidity": 30000, "mkt_cap": 45000,
        "price": 1e-5, "percent1h": 40, "percent5m": 5,
        "volume_1h": 20000, "volume_5minutes": 4000, "volume_24h": 60000,
    }
    tts.update(tts_over)
    return {
        "id": f"ev-{create_time}", "chain": "solana", "channel_id": 1,
        "group_name": "SmartMoney#5min#5num", "token": TOKEN,
        "create_time": create_time, "avg_wallet_volume": 150.0,
        "token_trading_stat": tts,
        "wallet_stats": [{"wallet": f"w{i}", "volume": "50"} for i in range(wallets)],
    }


def make_meta(gain=1.2, tier="", signal_count=2, liquidity=None, holders=None,
              top10=None, mcap=None, vol24=None):
    """Synthetic debot meta block (tokens/signals/metrics) for one token."""
    return {
        "tokens": {TOKEN: {
            "chain": "solana", "address": TOKEN, "creator_address": "DevWallet",
            "decimals": 6, "name": "Signal Token", "symbol": "SIG",
            "total_supply": 1e9, "creation_timestamp": 1786500000,
            "launchpad": "pump",
        }},
        "signals": {TOKEN: {
            "signal_count": signal_count, "first_time": 1786500001,
            "first_price": 8e-6, "max_price": 8e-6 * max(gain, 0.1),
            "max_price_gain": gain, "token_level": tier,
        }},
        "metrics": {TOKEN: {
            "dex_name": "pump_swap", "holder_count": holders if holders is not None else 200,
            "liquidity": liquidity if liquidity is not None else 30000.0,
            "market_cap": mcap if mcap is not None else 45000.0,
            "pair": PAIR, "price": 1e-5, "token_reserve": 5e8,
            "top10_position": top10 if top10 is not None else 0.25,
            "volume_24h": vol24 if vol24 is not None else 60000.0,
        }},
    }


NOW = 1786579999.0
G = SignalGate()

# --- 1) healthy signal passes and builds a Candidate ---
ev = make_event(NOW - 10)
meta = make_meta(gain=1.2)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert passed, reason
cand = build_candidate(ev, meta, G, now=NOW)
assert cand is not None
assert cand.launch.source == "debot_signal"
assert cand.launch.mint == TOKEN and cand.launch.symbol == "SIG"
assert cand.launch.creator == "DevWallet"
assert cand.launch.created_at == NOW - 10  # signal time, not token creation
assert cand.launch.raw["max_price_gain"] == 1.2
assert cand.launch.raw["n_wallets"] == 3
assert cand.launch.raw["pair"] == PAIR
assert cand.launch.raw["decimals"] == 6
assert cand.score >= settings.min_score, cand.score
print(f"[OK] healthy signal passes gate + builds Candidate (score {cand.score})")

# --- 2) pump-band gate: the researched winner feature ---
# flat (gain < min) -> reject
ev = make_event(NOW - 10)
meta = make_meta(gain=0.5)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "gain" in reason, reason
# already-2x (gain > max) -> reject (anti-chase)
meta = make_meta(gain=3.0)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "gain" in reason, reason
# inside the band -> pass
meta = make_meta(gain=1.0)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert passed, reason
print("[OK] pump-band gate (flat/already-2x rejected, mid-band passes)")

# --- 3) hygiene gates ---
# low liquidity
meta = make_meta(liquidity=1000.0)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "liq" in reason, reason
# too much liquidity (already-established token = edge gone)
meta = make_meta(liquidity=2_000_000.0)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "liq" in reason, reason
# too few holders
meta = make_meta(holders=5)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "holders" in reason, reason
# whale concentration (top10 too high)
meta = make_meta(top10=0.80)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "top10" in reason, reason
# market cap too big
meta = make_meta(mcap=5_000_000.0)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "mcap" in reason, reason
# dead token (no 24h volume)
meta = make_meta(vol24=0.0)
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "vol24" in reason, reason
# no smart wallets in the event
ev_nw = make_event(NOW - 10, wallets=0)
passed, reason = gate_signal(ev_nw, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "wallets" in reason, reason
print("[OK] hygiene gates (liq band, holders, top10, mcap, vol24, wallets)")

# --- 4) freshness: stale signal is rejected ---
ev_old = make_event(NOW - 10_000)
meta = make_meta(gain=1.2)
passed, reason = gate_signal(ev_old, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert not passed and "stale" in reason, reason
print("[OK] stale-signal rejection (SIGNAL_MAX_AGE_SEC)")

# --- 5) tier rejection (configurable; default rejects nothing) ---
meta = make_meta(gain=1.2, tier="silver")
passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                             meta["metrics"][TOKEN], G, now=NOW)
assert passed, "default config should not reject tiers"
old_tiers = settings.signal_reject_tiers
settings.signal_reject_tiers = ("silver", "gold")
try:
    g2 = SignalGate()
    passed, reason = gate_signal(ev, meta["tokens"][TOKEN], meta["signals"][TOKEN],
                                 meta["metrics"][TOKEN], g2, now=NOW)
    assert not passed and "tier" in reason, reason
finally:
    settings.signal_reject_tiers = old_tiers
print("[OK] tier rejection configurable (silver/gold veto when enabled)")

# --- 6) scoring: mid-band > outside-band, more wallets > fewer ---
meta_a = make_meta(gain=1.2)
meta_b = make_meta(gain=0.3)
s_in = score_signal(ev, meta_a["tokens"][TOKEN], meta_a["signals"][TOKEN],
                    meta_a["metrics"][TOKEN], G)
s_out = score_signal(ev, meta_b["tokens"][TOKEN], meta_b["signals"][TOKEN],
                     meta_b["metrics"][TOKEN], G)
assert s_in > s_out, (s_in, s_out)
ev_many = make_event(NOW - 10, wallets=5)
s_few = score_signal(make_event(NOW - 10, wallets=1),
                     meta_a["tokens"][TOKEN], meta_a["signals"][TOKEN],
                     meta_a["metrics"][TOKEN], G)
s_many = score_signal(ev_many, meta_a["tokens"][TOKEN], meta_a["signals"][TOKEN],
                      meta_a["metrics"][TOKEN], G)
assert s_many > s_few, (s_many, s_few)
assert 0 <= s_in <= 100
print(f"[OK] scoring monotonicity (mid-band {s_in} > flat {s_out}; wallets {s_many} > {s_few})")

# --- 7) dedup: the scanner's seen-set skips a re-poll of the same event ---
from signal_scanner import SignalScanner


async def _dedup_case():
    """The scanner's seen-set must skip a re-poll of the same event id."""
    sc = SignalScanner(gate=G)
    try:
        e1 = "ev-111"
        assert sc._is_new(e1) is True
        assert sc._is_new(e1) is False
        return True
    finally:
        await sc.close()

import asyncio

assert asyncio.run(_dedup_case())
print("[OK] event dedup (seen-set skips repeats)")

print("\nALL SIGNAL-SCANNER TESTS PASSED")