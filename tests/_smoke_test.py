"""Smoke test for the scaffold — exercises pure logic without network calls."""

import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
import sys

# 1) config defaults
from config import settings

assert settings.starting_amount == 2.0
assert settings.starting_balance == 20.0  # paper wallet bankroll
assert settings.min_score == 65.0          # feed-score gate (backtest-validated, Aug-13 replay)
assert settings.min_liquidity_usd == 4000.0  # entry liquidity floor (validated, Aug-13 replay)
assert settings.entry_latency_s == 2.0        # give the pool time to grow before confirming
assert settings.liq_confirm_window_s == 10.0  # ...then up to this long to cross the floor
assert settings.stale_exit_sec == 60.0
assert settings.max_candidate_age_min == 5.0
assert settings.stop_loss == 0.82
assert settings.take_profit == 2.0
assert settings.trail_exit_pct == 0.15   # trailing stop (0 = fixed TP/SL only)
assert settings.dry_stop_fill == 0.25    # realistic dry-run stop-loss fill
assert settings.max_entry_peak_pct == 0.0  # anti-peak entry gate (off by default)
assert settings.slippage_bps == 150
assert settings.play_floor == 1.0
assert settings.dry_run is True
assert settings.dev_rep_min_interval_s == 1.0
assert settings.dev_rep_retry_after_cap_s == 30.0
assert settings.dev_rep_consec_429_limit == 3
assert settings.dev_rep_cooldown_s == 30.0
assert settings.helius_base_url == "https://mainnet.helius-rpc.com"
assert len(settings.helius_api_keys) >= 1, "at least one Helius key configured"
assert settings.helius_api_key == settings.helius_api_keys[0]
print("[OK] config defaults")

# 2) data_stream TokenLaunch parsing (PumpDev create event shape)
from data_stream import TokenLaunch

ev = {
    "signature": "abc123",
    "mint": "MintAddr",
    "traderPublicKey": "Creator",
    "txType": "create",
    "name": "Test Coin",
    "symbol": "TST",
    "uri": "https://x",
    "initialBuy": 100000000,
    "initialQuoteAmount": 1.5,
    "quoteMint": "So11111111111111111111111111111111111111112",
    "solAmount": 1.5,
    "marketCapSol": 32.7,
    "isMayhemMode": False,
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
    "pairAddress": "PairAddr",
    "dexId": "pump",
    "url": "https://dexscreener.com/x",
    "baseToken": {"address": "MintAddr", "name": "Test Coin", "symbol": "TST"},
    "quoteToken": {"address": "So11111111111111111111111111111111111111112", "symbol": "SOL"},
    "priceNative": "0.000001",
    "priceUsd": "0.0001",
    "txns": {"m5": {"buys": 20, "sells": 8}},
    "volume": {"m5": 1500, "h24": 5000},
    "priceChange": {"m5": 50, "h24": 200},
    "liquidity": {"usd": 20000, "base": 100000000, "quote": 2.0},
    "fdv": 800000,
    "marketCap": 100000,
    "pairCreatedAt": 1700000000000,
    "boosts": {"active": 0},
}

pair = Pair.from_json(pair_json)
assert pair.txns_m5 == 28 and pair.buy_sell_ratio == 2.5
print("[OK] Pair.from_json + derived metrics")

from scanner_filter import passes_feed_filters, passes_filters

# pair is "old" (created 1970) — patch created_at to now for the filter test
pair.pair_created_at = int(time.time() * 1000) - 60_000  # 1 min old
passed, fails = passes_filters(launch, pair)
assert passed, fails
print("[OK] filters pass for healthy pair")

# feed-only entry filters (hardening v2)
passed, fails = passes_feed_filters(launch)  # dev 1.5 SOL, initial buy, mcap 32.7
assert passed, fails
big_dev = TokenLaunch.from_event({**ev, "initialQuoteAmount": 8.0})  # dev > 3 SOL
assert not passes_feed_filters(big_dev)[0]
no_buy = TokenLaunch.from_event({**ev, "initialBuy": 0})
assert not passes_feed_filters(no_buy)[0]
print("[OK] feed filters (dev cap, initial buy)")

from scoring_algorithm import score_feed, score_token

s = score_token(launch, pair)
assert 0 <= s <= 80, s  # listed weights sum to 80
sf = score_feed(launch)
assert 0 <= sf <= 80, sf
print(f"[OK] scoring -> {s} (pair) / {sf} (feed)")

# 5) compounding + risk
from compounding import next_play_amount, split_proceeds

reinvest, saved = split_proceeds(4.0)
assert reinvest == 2.4 and saved == 1.6
assert next_play_amount(2.0, won=True, exit_reason="take_profit") == 2.4
assert next_play_amount(2.0, won=False, exit_reason="loss") == 1.64  # 2.0*0.82
assert next_play_amount(1.0, won=False, exit_reason="loss") == 2.0  # play floor reset
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
print(f"[OK] wallet keypair (pubkey {str(kp.pubkey())[:8]})")

# 7) single-instance lock (hardening v2)
from singleton import SingleInstanceLock

l1 = SingleInstanceLock(path="/tmp/smoke_sniper.lock")
l2 = SingleInstanceLock(path="/tmp/smoke_sniper.lock")
assert l1.acquire(), "first acquire should succeed"
assert not l2.acquire(), "second acquire should fail (already locked)"
l1.release()
assert l2.acquire(), "acquire after release should succeed"
l2.release()
print("[OK] single-instance lock (exclusive + release)")

# 8) daily loss kill switch (hardening v2)
from config import settings
from stats import TradeStats

settings.daily_loss_limit = 10.0
st = TradeStats(dry_run=True)
st.daily_pnl_usd = -5.0
assert not st.daily_loss_limit_hit()
st.daily_pnl_usd = -10.5
assert st.daily_loss_limit_hit()
assert st.next_day_reset_seconds() > 0
print("[OK] daily loss limit (10.0) + UTC reset")

# 9) dead-token exit (no_trades) — real PriceMonitor with fakes
import asyncio

import price_monitor as pm_mod


class FakeLiveFeedDead:
    """Live feed that never sees trades for the mint (dead-token case)."""
    def __init__(self, last_age, feed_age=None):
        """Set the last-trade age and feed age for the stale check."""
        self._last = last_age
        self._fa = feed_age
    async def price_usd(self, mint, max_age_s=10.0):
        """No price available."""
        return
    def last_trade_age(self, mint):
        """The configured last-trade age."""
        return self._last
    def feed_age(self):
        """The configured stream age."""
        return self._fa

class FakeDSEmpty:
    """DexScreener stub returning no pairs."""
    async def token_pairs(self, mint):
        """No indexed pairs."""
        return []
    def pick_pair(self, pairs):
        """First pair or None."""
        return pairs[0] if pairs else None

class FakeJupNoPrice:
    """Jupiter stub that never has a price."""
    async def price_usd(self, mint):
        """No price available."""
        return

async def _stale_case(last_age, feed_age, grace=0.0):
    """A monitor over a dead feed must emit a no_trades exit signal."""
    old_grace = settings.stale_exit_grace_sec
    settings.stale_exit_grace_sec = grace
    try:
        mon = pm_mod.PriceMonitor(FakeDSEmpty(), FakeJupNoPrice(),
                                  entry_price_usd=1e-6, mint="DeadMint",
                                  live_feed=FakeLiveFeedDead(last_age, feed_age))
        sig = await mon.check()
    finally:
        settings.stale_exit_grace_sec = old_grace
    return sig

sig = asyncio.run(_stale_case(last_age=120.0, feed_age=300.0, grace=0.0))
assert sig.exit and sig.reason == "no_trades", sig
sig2 = asyncio.run(_stale_case(last_age=5.0, feed_age=300.0, grace=0.0))
assert not sig2.exit, "recent trade -> keep holding"
# never seen a trade, but feed just reconnected (feed_age < grace+stale) -> hold
sig3 = asyncio.run(_stale_case(last_age=None, feed_age=10.0, grace=0.0))
assert not sig3.exit, "feed reconnected -> no false exit"
print("[OK] dead-token exit (no_trades: stale->exit, recent->hold, reconnect->hold)")

# 9b) trailing stop — a position that peaks and rolls over exits ~-15% BEFORE
# the fixed SL, and locks gains when it pumps first. Direct _evaluate tests.
class FakeLivePeak:
    """Live feed with a controllable price for the trailing-stop case."""
    def __init__(self, price):
        """Set the current price."""
        self.price = price
    async def price_usd(self, mint, max_age_s=10.0):
        """Return the configured price."""
        return self.price
    def last_trade_age(self, mint):
        """Recently traded -> not stale."""
        return 5.0
    def feed_age(self):
        """An old stream age."""
        return 300.0

def _mon():
    """A monitor with the current default TRAIL_EXIT_PCT (0.15)."""
    return pm_mod.PriceMonitor(FakeDSEmpty(), FakeJupNoPrice(),
                               entry_price_usd=1e-6, mint="TrailMint",
                               live_feed=FakeLivePeak(1e-6))

mon = _mon()
old_trail = settings.trail_exit_pct
old_sl = settings.stop_loss
settings.trail_exit_pct = 0.15
settings.stop_loss = 0.82
try:
    # peak rises to +50% -> trailing_peak tracks it; price still near peak
    mon.trailing_peak = 1.5e-6
    mon.live_feed.price = 1.45e-6
    sig = asyncio.run(mon.check())
    assert not sig.exit, "above trail trigger -> hold"
    # falls 20% below the peak but still above the 0.82 SL -> trail_stop
    sig = asyncio.run(_mon().check())
    # simulate the rollover: peak 1.5e-6, price 1.2e-6 (20% off peak)
    mon2 = _mon(); mon2.trailing_peak = 1.5e-6
    sig = pm_mod.PriceMonitor._evaluate(mon2, 1.2e-6, None)
    assert sig.exit and sig.reason == "trail_stop", sig
    # flat entry that dumps: peak == entry -> trail exits at -15% (0.85x),
    # BEFORE the 0.82 SL would trigger — the sell-on-first-red rule
    mon3 = _mon()  # trailing_peak == entry_price_usd
    sig = pm_mod.PriceMonitor._evaluate(mon3, 0.84e-6, None)
    assert sig.exit and sig.reason == "trail_stop", sig
    # deep gap straight through both levels -> trail_stop still fires first
    # (trail threshold 0.85*peak sits above the 0.82 SL whenever peak >= entry)
    mon4 = _mon(); mon4.trailing_peak = 1.5e-6
    sig = pm_mod.PriceMonitor._evaluate(mon4, 0.7e-6, None)
    assert sig.exit and sig.reason == "trail_stop", sig
    # TP still wins even after a big peak (take_profit checked first)
    mon5 = _mon(); mon5.trailing_peak = 1.5e-6
    sig = pm_mod.PriceMonitor._evaluate(mon5, 2.1e-6, None)
    assert sig.exit and sig.reason == "take_profit", sig
    # disabled (TRAIL_EXIT_PCT=0) -> legacy fixed TP/SL behavior
    settings.trail_exit_pct = 0.0
    mon6 = _mon(); mon6.trailing_peak = 1.5e-6
    sig = pm_mod.PriceMonitor._evaluate(mon6, 1.2e-6, None)
    assert not sig.exit, "trail disabled -> hold until SL/TP"
finally:
    settings.trail_exit_pct = old_trail
    settings.stop_loss = old_sl
print("[OK] trailing stop (trail_stop ~-15% before SL, gains locked, TP first)")

# 10) monitor network resilience — a failing DexScreener/Jupiter must NOT raise
class FakeDSExplodes:
    """DexScreener stub that always raises (resilience test)."""
    async def token_pairs(self, mint):
        """Always raise a connection error."""
        raise ConnectionError("DNS blip")

class FakeJupExplodes:
    """Jupiter stub that always raises (resilience test)."""
    async def price_usd(self, mint):
        """Always raise a connection error."""
        raise ConnectionError("DNS blip")

class FakeLiveQuiet:
    """Live feed that is quiet but recently traded (not stale)."""
    async def price_usd(self, mint, max_age_s=10.0):
        """No fresh price."""
        return
    def last_trade_age(self, mint):
        """Recently traded -> not stale."""
        return 5.0  # recently traded -> not stale
    def feed_age(self):
        """An old stream age."""
        return 300.0

async def _resilience_case():
    """A monitor over exploding sources must hold, not raise."""
    mon = pm_mod.PriceMonitor(FakeDSExplodes(), FakeJupExplodes(),
                              entry_price_usd=1e-6, mint="BlipMint",
                              live_feed=FakeLiveQuiet())
    sig = await mon.check()  # must not raise
    return sig

sig = asyncio.run(_resilience_case())
assert not sig.exit, "network failure -> keep holding, no crash"
print("[OK] monitor network resilience (DNS failure -> hold, no crash)")

# 11) dev-reputation veto (Helius) — serial launcher blocked, clean passes,
#     network failure fails OPEN, per-wallet cache
import httpx as _httpx

from dev_rep import DevReputationClient


def _mk_launch(creator):
    """A minimal TokenLaunch fixture keyed by the given creator wallet."""
    from data_stream import TokenLaunch
    return TokenLaunch(mint="DevMint", name="T", symbol="T", uri="", creator=creator,
                       signature="", initial_buy_tokens=0.0, dev_sol=None,
                       market_cap_sol=None, quote_mint="", is_mayhem_mode=False,
                       is_cashback_enabled=False, source="pumpapi")

def _tx(ttype, ts, source="PUMP_FUN", mints=(), transfers=()):
    """A synthetic Helius getSignaturesForAddress-style transaction dict."""
    tx = {"type": ttype, "timestamp": ts, "source": source,
          "accountData": [{"tokenBalanceChanges": [{"mint": m} for m in mints]}],
          "tokenTransfers": [{"mint": m, "fromUserAccount": f, "toUserAccount": t}
                             for m, f, t in transfers]}
    return tx

NOW = time.time()
SERIAL_TXS = [_tx("CREATE", NOW - i * 600) for i in range(4)]  # 4 creates in 4h
CLEAN_TXS = [_tx("CREATE", NOW - 7200, mints=("CleanMint",))]
DUMP_TXS = [
    _tx("CREATE", NOW - 5000, mints=("DumpMint",)),
    _tx("SWAP", NOW - 1000, transfers=[("DumpMint", "DumpDev", "Somebody")]),
]

def _client_with(txs):
    """A DevReputationClient whose HTTP layer returns the given txs."""
    def handler(request):
        """Mock handler returning the fixed transaction list."""
        return _httpx.Response(200, json=txs)
    dr = DevReputationClient(api_key="test", timeout_s=2.0,
                             transport=_httpx.MockTransport(handler))
    return dr

async def _veto_case(txs, creator):
    """Veto verdict for a creator whose Helius history is `txs`."""
    dr = _client_with(txs)
    try:
        return await dr.veto(_mk_launch(creator))
    finally:
        await dr.close()

# serial launcher -> blocked
blk, reason = asyncio.run(_veto_case(SERIAL_TXS, "SerialDev"))
assert blk and "serial launcher" in reason, (blk, reason)
print(f"[OK] dev-rep: serial launcher vetoed ({reason})")
# clean wallet -> pass
blk, reason = asyncio.run(_veto_case(CLEAN_TXS, "CleanDev"))
assert not blk, reason
print("[OK] dev-rep: clean wallet passes")
# dump evidence -> blocked
blk, reason = asyncio.run(_veto_case(DUMP_TXS, "DumpDev"))
assert blk and "dumped" in reason, (blk, reason)
print("[OK] dev-rep: prior-dump vetoed")
# fail-open: transport raises -> pass, no exception
def boom(request):
    """A mock handler that always raises (fail-open test)."""
    raise _httpx.ConnectError("DNS blip")
dr = DevReputationClient(api_key="test", timeout_s=1.0,
                         transport=_httpx.MockTransport(boom))
blk, reason = asyncio.run(dr.veto(_mk_launch("FlakyDev")))
assert not blk and not reason
asyncio.run(dr.close())
print("[OK] dev-rep: network failure fails OPEN (never blocks trading)")
# cache: second veto for same wallet does not refetch
calls = {"n": 0}
def counting(request):
    """A mock handler that counts invocations (cache test)."""
    calls["n"] += 1
    return _httpx.Response(200, json=CLEAN_TXS)
dr = DevReputationClient(api_key="test", timeout_s=2.0,
                         transport=_httpx.MockTransport(counting))
l1 = asyncio.run(dr.veto(_mk_launch("CachedDev")))
l2 = asyncio.run(dr.veto(_mk_launch("CachedDev")))
assert calls["n"] == 1, calls
asyncio.run(dr.close())
print("[OK] dev-rep: per-wallet cache (1 fetch for 2 veto calls)")

# 11b) dev-rep rate limiting: Retry-After honored, then success
def rate_limited(request):
    """429 once with Retry-After, then return the clean history."""
    rate_limited.n += 1
    if rate_limited.n == 1:
        return _httpx.Response(429, json={"error": "Too Many Requests"},
                               headers={"Retry-After": "1"})
    return _httpx.Response(200, json=CLEAN_TXS)
rate_limited.n = 0
dr = DevReputationClient(api_key="test", timeout_s=5.0,
                         min_interval_s=0.0, retry_after_cap_s=2.0,
                         transport=_httpx.MockTransport(rate_limited))
t0 = time.monotonic()
blk, reason = asyncio.run(dr.veto(_mk_launch("RetryDev")))
dt = time.monotonic() - t0
assert not blk and not reason
assert rate_limited.n == 2, rate_limited.n   # 429 then retry -> 200
assert dt >= 1.0, f"Retry-After sleep too short: {dt:.2f}s"
asyncio.run(dr.close())
print("[OK] dev-rep: 429 honors Retry-After then succeeds")

# 11c) dev-rep cooldown: consecutive 429s pause lookups (fail-open, no block)
calls2 = {"n": 0}
def always429(request):
    """Always 429 — trips the cooldown, then lookups fail-open."""
    calls2["n"] += 1
    return _httpx.Response(429, json={"error": "Too Many Requests"},
                           headers={"Retry-After": "1"})
dr = DevReputationClient(api_key="test", timeout_s=5.0,
                         min_interval_s=0.0, retry_after_cap_s=2.0,
                         consec_429_limit=2, cooldown_s=60.0,
                         transport=_httpx.MockTransport(always429))
# first wallet: exhausts the 2 retries -> cooldown trips, fail-open pass
blk, reason = asyncio.run(dr.veto(_mk_launch("HotWalletA")))
assert not blk and not reason
assert calls2["n"] == 2, calls2          # exactly 2 HTTP attempts before cooldown
# second wallet during cooldown: no HTTP call, still fails open
n_before = calls2["n"]
blk, reason = asyncio.run(dr.veto(_mk_launch("HotWalletB")))
assert not blk and not reason
assert calls2["n"] == n_before, calls2   # zero calls during cooldown
asyncio.run(dr.close())
print("[OK] dev-rep: repeated 429 -> fail-open cooldown (no HTTP, no block)")

# 11d) dev-rep min-interval throttle: lookups spaced apart
def counting2(request):
    """Always return clean history and count calls."""
    counting2.n += 1
    return _httpx.Response(200, json=CLEAN_TXS)
counting2.n = 0
dr = DevReputationClient(api_key="test", timeout_s=2.0, min_interval_s=0.2,
                         transport=_httpx.MockTransport(counting2))
t0 = time.monotonic()
l1 = asyncio.run(dr.veto(_mk_launch("ThrottleDev")))
t1 = time.monotonic()
l2 = asyncio.run(dr.veto(_mk_launch("ThrottleDev2")))
t2 = time.monotonic()
assert counting2.n == 2, counting2.n
assert t2 - t1 >= 0.2, f"lookups too close: {(t2 - t1):.3f}s"
asyncio.run(dr.close())
print("[OK] dev-rep: min-interval throttle spaces lookups")

# 11e) dev-rep key failover: first key exhausted (429), second key succeeds
key_hits = {"a": 0, "b": 0}
def failover(request):
    """Key A always 429s; key B returns the clean history."""
    req_key = request.url.params.get("api-key")
    if req_key == "key-a":
        key_hits["a"] += 1
        return _httpx.Response(429, json={"error": "Too Many Requests"})
    key_hits["b"] += 1
    return _httpx.Response(200, json=CLEAN_TXS)
dr = DevReputationClient(api_keys=["key-a", "key-b"], timeout_s=5.0,
                         min_interval_s=0.0, consec_429_limit=2,
                         transport=_httpx.MockTransport(failover))
blk, reason = asyncio.run(dr.veto(_mk_launch("FailoverDev")))
assert not blk and not reason
assert key_hits["a"] == 1, key_hits          # rotated away after the first 429
assert key_hits["b"] == 1, key_hits          # succeeded on the fallback key
asyncio.run(dr.close())
print("[OK] dev-rep: exhausted key A rotates to key B (failover works)")

# 11f) dev-rep all-keys exhausted: cooldown trips (fail-open, no block)
calls3 = {"n": 0}
def always429_any(request):
    """Every key 429s — all-key exhaustion trips the cooldown."""
    calls3["n"] += 1
    return _httpx.Response(429, json={"error": "Too Many Requests"},
                           headers={"Retry-After": "1"})
dr = DevReputationClient(api_keys=["key-a", "key-b"], timeout_s=5.0,
                         min_interval_s=0.0, retry_after_cap_s=2.0,
                         consec_429_limit=2, cooldown_s=60.0,
                         transport=_httpx.MockTransport(always429_any))
blk, reason = asyncio.run(dr.veto(_mk_launch("AllHotA")))
assert not blk and not reason
assert calls3["n"] == 4, calls3  # both keys 429'd, backoff cycle, both again
n3 = calls3["n"]
blk, reason = asyncio.run(dr.veto(_mk_launch("AllHotB")))
assert not blk and not reason
assert calls3["n"] == n3, calls3  # zero calls during cooldown
asyncio.run(dr.close())
print("[OK] dev-rep: all keys 429 -> cooldown (no HTTP during pause, fail-open)")

# 12) PumpEventHub dispatch: pumpapi curve + pumpdev curve both feed the
#     trades queue; pumpdev AMM events (pool address present) are dropped;
#     on-chain liquidity is normalized from both event shapes.
from data_stream import PumpEventHub


async def _hub_case():
    """Dispatch routing: curve events -> trades, AMM dropped, creates separate."""
    hub = PumpEventHub()
    # pumpapi curve trade
    hub._dispatch({"action": "buy", "mint": "A", "price": 0.001,
                   "pool": "pump", "quoteInPool": 3.0})
    # pumpdev curve trade (bonding-curve events omit `pool`)
    hub._dispatch({"txType": "sell", "mint": "B", "price": 0.002,
                   "vSolInBondingCurve": 40.0})
    # pumpdev curve, raw SOL reserves (UI = raw / 1e9)
    hub._dispatch({"txType": "buy", "mint": "C", "price": 0.003,
                   "quoteMint": "So11111111111111111111111111111111111111112",
                   "vQuoteInBondingCurve": 50_000_000_000})
    # pumpdev AMM trade (pool present) -> must NOT reach the trades queue
    hub._dispatch({"txType": "buy", "mint": "D", "price": 0.004,
                   "pool": "PoolAddr", "poolEffectiveQuoteReserves": 123})
    # create -> creates queue, not trades
    hub._dispatch({"action": "create", "mint": "E"})
    got = []
    for _ in range(3):
        got.append(await hub.trades().__anext__())
    return got, hub._creates.qsize()

got, creates_q = asyncio.run(_hub_case())
assert got == [("A", 0.001, 3.0), ("B", 0.002, 40.0), ("C", 0.003, 50.0)], got
assert creates_q == 1, "create must route to the creates queue"
print("[OK] hub dispatch: pumpapi+pumpdev curve feed trades, AMM dropped, liq normalized")

print("\nALL SMOKE TESTS PASSED")
