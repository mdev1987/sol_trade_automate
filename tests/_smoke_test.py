
"""Smoke test for the scaffold — exercises pure logic without network calls."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
import asyncio, json, sys

# 1) config defaults
from config import settings
assert settings.starting_amount == 2.0
assert settings.stop_loss == 0.82
assert settings.take_profit == 2.0
assert settings.slippage_bps == 150
assert settings.play_floor == 1.0
assert settings.dry_run is True
print("[OK] config defaults")

# 2) data_stream TokenLaunch parsing (PumpDev create event shape)
from data_stream import TokenLaunch
ev = {
    "signature": "abc123", "mint": "MintAddr", "traderPublicKey": "Creator",
    "txType": "create", "name": "Test Coin", "symbol": "TST", "uri": "https://x",
    "initialBuy": 100000000, "initialQuoteAmount": 1.5,
    "quoteMint": "So11111111111111111111111111111111111111112",
    "solAmount": 1.5, "marketCapSol": 32.7, "isMayhemMode": False,
    "isCashbackEnabled": False,
}
launch = TokenLaunch.from_event(ev)
assert launch.dev_sol == 1.5 and launch.symbol == "TST"
print("[OK] TokenLaunch.from_event")

# 3) rug detection
from rug_detection import rug_check
scam_launch = TokenLaunch.from_event({**ev, "name": "SAFEMOON 100x"})
assert not rug_check(scam_launch, None).passed
dev_dump = TokenLaunch.from_event({**ev, "initialQuoteAmount": 50.0})
assert not rug_check(dev_dump, None).passed
assert rug_check(launch, None).passed
print("[OK] rug detection (scam name, dev dump, clean pass)")

# 4) dexscreener Pair parsing + filters + scoring
from dexscreener import Pair
pair_json = {
    "pairAddress": "PairAddr", "dexId": "pump", "url": "https://dexscreener.com/x",
    "baseToken": {"address": "MintAddr", "name": "Test Coin", "symbol": "TST"},
    "quoteToken": {"address": "So11111111111111111111111111111111111111112", "symbol": "SOL"},
    "priceNative": "0.000001", "priceUsd": "0.0001",
    "txns": {"m5": {"buys": 20, "sells": 8}},
    "volume": {"m5": 1500, "h24": 5000},
    "priceChange": {"m5": 50, "h24": 200},
    "liquidity": {"usd": 20000, "base": 100000000, "quote": 2.0},
    "fdv": 800000, "marketCap": 100000,
    "pairCreatedAt": 1700000000000,
    "boosts": {"active": 0},
}
import time
pair = Pair.from_json(pair_json)
assert pair.txns_m5 == 28 and pair.buy_sell_ratio == 2.5
print("[OK] Pair.from_json + derived metrics")

from scanner_filter import passes_filters, Thresholds
# pair is "old" (created 1970) — patch created_at to now for the filter test
pair.pair_created_at = int(time.time() * 1000) - 60_000  # 1 min old
passed, fails = passes_filters(launch, pair)
assert passed, fails
print("[OK] filters pass for healthy pair")

from scoring_algorithm import score_token
s = score_token(launch, pair)
assert 0 <= s <= 80, s  # listed weights sum to 80
print(f"[OK] scoring -> {s}")

# 5) compounding + risk
from compounding import split_proceeds, next_play_amount
reinvest, saved = split_proceeds(4.0)
assert reinvest == 2.4 and saved == 1.6
assert next_play_amount(2.0, won=True, exit_reason="take_profit") == 2.4
assert next_play_amount(2.0, won=False, exit_reason="loss") == 1.64  # 2.0*0.82
assert next_play_amount(1.0, won=False, exit_reason="loss") == 2.0   # play floor reset
print("[OK] compounding + play floor")

from risk_management import RiskManager
risk = RiskManager()
risk.play_amount = 2.0
risk.record_result(won=False)  # loss 1
risk.record_result(won=False)  # loss 2 -> triggers pause
assert risk.paused, "expected loss-pause after 2 losses"
risk.paused_until = 0  # clear pause
risk.record_result(won=True)
assert risk.consecutive_losses == 0
print("[OK] risk manager (loss pause + reset)")

# 6) wallet keypair round-trip (generate a fresh keypair)
import base58
from solders.keypair import Keypair as K
from wallet import get_keypair
kp = get_keypair(base58.b58encode(K().to_bytes()).decode())
assert len(str(kp.pubkey())) in (43, 44)  # base58 of 32 bytes can be 43-44 chars
print("[OK] wallet keypair (pubkey %s)" % str(kp.pubkey())[:8])

print("\\nALL SMOKE TESTS PASSED")
