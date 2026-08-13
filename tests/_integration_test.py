"""End-to-end wiring test: scanner produces a candidate -> bot trade cycle (mocked network).

IMPORTANT: `price_monitor.PriceMonitor` is patched BEFORE `bot` is imported, so
`bot.execute_trade` sees the fake monitor (it imports the name at module load).
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

import asyncio

from config import settings

settings.dry_run = True  # force dry-run regardless of .env

# --- fake PriceMonitor exit (avoid 8s polling) — MUST precede `from bot import ...` ---
import price_monitor
from data_stream import TokenLaunch
from dexscreener import Pair
from jupiter_swap import SwapResult
from risk_management import RiskManager
from stats import TradeStats
from token_scanner import Candidate


class FakeMonitor:
    """Stands in for PriceMonitor: returns a canned exit signal immediately."""
    exit_signal = "take_profit"
    take_profit_price = 0.0002
    stop_loss_price = 0.000082
    live_feed = None  # hardening v2: bot reads monitor.live_feed for logging

    def __init__(self, *a, **k):
        """No-op constructor (drop-in PriceMonitor replacement)."""

    async def run_until_exit(self):
        """Immediately return the canned exit signal."""
        return price_monitor.ExitSignal(True, self.exit_signal, 0.0002, 20_000.0)


price_monitor.PriceMonitor = FakeMonitor

from bot import execute_trade
from telegram_bot import TelegramNotifier


def make_launch():
    """A TokenLaunch fixture that passes feed filters and scoring."""
    return TokenLaunch(
        mint="TestMintPump", name="Test Coin", symbol="TST", uri="https://x",
        creator="Creator", signature="sig1", initial_buy_tokens=100_000_000,
        dev_sol=0.5, market_cap_sol=32.7, quote_mint="So11111111111111111111111111111111111111112",
        is_mayhem_mode=False, is_cashback_enabled=False,
        source="pumpdev",
    )


def make_pair():
    """A DexScreener Pair fixture with healthy liquidity/volume."""
    import time
    return Pair(
        pair_address="PairAddr", dex_id="pump", base_mint="TestMintPump",
        base_symbol="TST", quote_symbol="SOL", price_usd=0.0001, price_native=1e-6,
        liquidity_usd=20_000.0, volume_m5=1_500.0, txns_m5_buys=20, txns_m5_sells=8,
        market_cap=100_000.0, fdv=100_000.0, pair_created_at=int(time.time()*1000)-60_000,
    )


# --- fake Jupiter (new JupiterSwap API: buy/sell/price_usd -> SwapResult) ---
class FakeJupiter:
    """Stands in for JupiterSwap with scripted buy/sell/price outcomes."""
    def __init__(self, buy_ok=True, sell_output=4_000_000):
        """Configure buy success and the raw USDC sell proceeds."""
        self.buy_ok = buy_ok
        self.sell_output = sell_output  # raw USDC returned on sell (1e6 = $1)

    async def buy(self, mint, amount_usd, liquidity_usd=0.0):
        """Scripted buy: success or a canned failure SwapResult."""
        print(f"  [fake buy] {mint} ${amount_usd} liq=${liquidity_usd}")
        if not self.buy_ok:
            return SwapResult(False, "", int(amount_usd*1e6), 0, "fake buy failure")
        return SwapResult(True, "fake-buy-sig", int(amount_usd*1e6), 100_000_000, "")

    async def sell(self, mint, token_amount_raw):
        """Scripted sell returning the configured USDC proceeds."""
        print(f"  [fake sell] {mint} {token_amount_raw} raw -> ${self.sell_output/1e6:.2f}")
        return SwapResult(True, "fake-sell-sig", token_amount_raw, self.sell_output, "")

    async def price_usd(self, mint):
        """A fixed USD price."""
        return 0.0001

    async def close(self):
        """No-op for the fake."""


class FakeDexScreener:
    """Stands in for DexScreenerClient returning the fixture pair."""
    async def token_pairs(self, mint):
        """Return the single fixture pair."""
        return [make_pair()]

    def pick_pair(self, pairs):
        """Return the first pair or None."""
        return pairs[0] if pairs else None

    async def close(self):
        """No-op for the fake."""


async def main():
    """Run the full trade-cycle wiring scenarios end to end."""
    launch, pair = make_launch(), make_pair()
    cand = Candidate(launch=launch, pair=pair, score=62.5, scanned_at="now")

    # --- happy path: take_profit exit -> win -> 60/40 compounding ---
    risk = RiskManager()
    risk.play_amount = 2.0
    stats = TradeStats(dry_run=True)
    won, reason = await execute_trade(cand, risk, FakeJupiter(), FakeDexScreener(),
                                      TelegramNotifier(), stats)
    print(f"  result: won={won} reason={reason} next_play=${risk.play_amount}")
    assert won and reason == "take_profit"
    assert risk.play_amount == 2.4, risk.play_amount  # 60% of $4 proceeds
    # stats: proceeds = 4_000_000 raw USDC / 1e6 = $4.0 ; pnl = 4.0 - 2.0 = +2.0
    assert stats.trades == 1 and stats.wins == 1 and stats.winrate == 1.0
    assert stats.balance_usd == 22.0, stats.balance_usd  # 20.0 bankroll - 2.0 + 4.0
    assert not stats.in_trade
    print("[OK] win path -> compounding + stats")
    print("--- status markdown preview ---")
    print(stats.markdown())

    # --- loss path: stop_loss exit -> loss -> 0.82x reduction ---
    FakeMonitor.exit_signal = "stop_loss"
    risk2 = RiskManager()
    risk2.play_amount = 2.0
    won2, reason2 = await execute_trade(cand, risk2, FakeJupiter(sell_output=1_600_000),
                                        FakeDexScreener(), TelegramNotifier(),
                                        TradeStats(dry_run=True))
    assert not won2 and reason2 == "stop_loss"
    assert risk2.play_amount == 1.64, risk2.play_amount  # 2.0 * 0.82
    print("[OK] loss path -> position shrink")

    # --- buy failure path: trade aborts cleanly, no crash ---
    won3, reason3 = await execute_trade(cand, RiskManager(), FakeJupiter(buy_ok=False),
                                        FakeDexScreener(), TelegramNotifier(),
                                        TradeStats(dry_run=True))
    assert not won3 and reason3 == "buy_failed", (won3, reason3)
    print("[OK] buy-failure path aborts cleanly")

    # --- regression: dry-run paper path (no real fill) — simulated proceeds ---
    # FakeJupiter.buy returns the paper-quote sentinel => simulated token amount
    import bot as bot_mod  # for PAPER_QUOTE_SENTINEL constant
    class PaperJupiter(FakeJupiter):
        """Simulates dry-run paper quoting (verified quote, no transaction)."""
        async def buy(self, mint, amount_usd, liquidity_usd=0.0):
            """Return the paper-quote sentinel instead of a real fill."""
            print(f"  [paper buy] {mint} ${amount_usd} liq=${liquidity_usd}")
            return SwapResult(False, "", 0, 0, bot_mod.PAPER_QUOTE_SENTINEL)
    FakeMonitor.exit_signal = "stop_loss"
    risk5 = RiskManager(); risk5.play_amount = 2.0
    s5 = TradeStats(dry_run=True)
    won5, reason5 = await execute_trade(cand, risk5, PaperJupiter(), FakeDexScreener(),
                                        TelegramNotifier(), s5)
    assert not won5 and reason5 == "stop_loss", (won5, reason5)
    assert s5.trades == 1 and s5.losses == 1, "simulated loss must be recorded"
    assert s5.balance_usd == 20.0 - 2.0 + 1.64, s5.balance_usd  # proceeds = 2.0*0.82
    assert risk5.play_amount == 1.64, risk5.play_amount
    print("[OK] regression: dry-run paper sell proceeds (amount*0.82)")

    # --- regression: pair with liquidity_usd=None must not crash the quote ---
    class NoLiqDex(FakeDexScreener):
        """Returns a pair with unknown liquidity (must not crash)."""
        async def token_pairs(self, mint):
            """Pair with liquidity_usd=None to exercise the fallback."""
            p = make_pair()
            p.liquidity_usd = None
            return [p]
    FakeMonitor.exit_signal = "take_profit"
    won6, reason6 = await execute_trade(cand, RiskManager(), FakeJupiter(), NoLiqDex(),
                                        TelegramNotifier(), TradeStats(dry_run=True))
    assert won6 and reason6 == "take_profit", (won6, reason6)
    print("[OK] regression: liquidity_usd=None falls back to 0 slippage tier")

    # --- daily loss kill switch (hardening v2) ---
    s4 = TradeStats(dry_run=True)
    limit = settings.daily_loss_limit
    assert limit > 0, "DAILY_LOSS_LIMIT must be > 0 for the kill-switch tests"
    s4.daily_pnl_usd = -limit + 1.0
    assert not s4.daily_loss_limit_hit(), "should NOT halt above the limit"
    s4.daily_pnl_usd = -limit
    assert s4.daily_loss_limit_hit(), "should halt at exactly the limit"
    s5 = TradeStats(dry_run=True)
    s5.record_exit(False, -2.0, 0.0, "stop_loss", 0.00008)
    assert s5.daily_pnl_usd == -2.0 and s5.day_key, "daily pnl tracking"
    assert s5.next_day_reset_seconds() > 0, "UTC midnight reset"
    print("[OK] daily-loss kill switch + daily pnl tracking")

    # --- dev-reputation veto aborts BEFORE the buy (no position, stats) ---
    class BlockingDevRep:
        """A dev-reputation client that always vetoes."""
        async def veto(self, launch):
            """Return a canned veto verdict."""
            return True, "serial launcher: 5 pump.fun creates in 24h"

    st_veto = TradeStats(dry_run=True)
    bal_before = st_veto.balance_usd
    won_v, reason_v = await execute_trade(
        cand, RiskManager(), FakeJupiter(), NoLiqDex(),
        TelegramNotifier(), st_veto, dev_rep=BlockingDevRep())
    assert not won_v and reason_v.startswith("dev_veto:"), reason_v
    assert st_veto.dev_vetoes == 1, st_veto.dev_vetoes
    assert st_veto.balance_usd == bal_before, "no buy -> bankroll untouched"
    assert not st_veto.in_trade, "no position opened"
    print("[OK] dev-rep veto aborts before buy (bankroll untouched, stats recorded)")

    # --- liquidity floor: thin pool aborts before the buy; rich pool proceeds ---
    class FakeLiveThin:
        """Live feed reporting pool liquidity below the entry floor."""
        async def sol_usd(self):
            """A fixed SOL/USD rate."""
            return 150.0
        def pool_liquidity_usd(self, mint, sol_usd=150.0, max_age_s=60.0):
            """A thin $800 pool (below the $5k floor)."""
            return 800.0  # below the $5k floor

    class FakeLiveRich:
        """Live feed reporting pool liquidity above the entry floor."""
        async def sol_usd(self):
            """A fixed SOL/USD rate."""
            return 150.0
        def pool_liquidity_usd(self, mint, sol_usd=150.0, max_age_s=60.0):
            """A rich $12k pool (above the floor)."""
            return 12_000.0  # above the floor

    old_win = settings.liq_confirm_window_s
    settings.liq_confirm_window_s = 0.3
    try:
        st_t = TradeStats(dry_run=True)
        won_t, reason_t = await execute_trade(
            cand, RiskManager(), FakeJupiter(), NoLiqDex(),
            TelegramNotifier(), st_t, live_feed=FakeLiveThin())
        assert not won_t and reason_t == "thin_liquidity", (won_t, reason_t)
        assert st_t.thin_pools == 1, st_t.thin_pools
        assert st_t.balance_usd == 20.0 and not st_t.in_trade, "no buy -> bankroll untouched"
        FakeMonitor.exit_signal = "take_profit"
        won_r, reason_r = await execute_trade(
            cand, RiskManager(), FakeJupiter(), NoLiqDex(),
            TelegramNotifier(), TradeStats(dry_run=True), live_feed=FakeLiveRich())
        assert won_r and reason_r == "take_profit", (won_r, reason_r)
    finally:
        settings.liq_confirm_window_s = old_win
    print("[OK] liquidity floor: thin pool aborts (bankroll untouched), rich pool proceeds")

    print("\nINTEGRATION TEST PASSED")


asyncio.run(main())
