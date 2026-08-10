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
    exit_signal = "take_profit"
    take_profit_price = 0.0002
    stop_loss_price = 0.000082
    live_feed = None  # hardening v2: bot reads monitor.live_feed for logging

    def __init__(self, *a, **k):
        pass

    async def run_until_exit(self):
        return price_monitor.ExitSignal(True, self.exit_signal, 0.0002, 20_000.0)


price_monitor.PriceMonitor = FakeMonitor

from bot import execute_trade
from telegram_bot import TelegramNotifier


def make_launch():
    return TokenLaunch(
        mint="TestMintPump",
        name="Test Coin",
        symbol="TST",
        uri="https://x",
        creator="Creator",
        signature="sig1",
        initial_buy_tokens=100_000_000,
        dev_sol=0.5,
        market_cap_sol=32.7,
        quote_mint="So11111111111111111111111111111111111111112",
        is_mayhem_mode=False,
        is_cashback_enabled=False,
        source="pumpdev",
    )


def make_pair():
    import time

    return Pair(
        pair_address="PairAddr",
        dex_id="pump",
        base_mint="TestMintPump",
        base_symbol="TST",
        quote_symbol="SOL",
        price_usd=0.0001,
        price_native=1e-6,
        liquidity_usd=20_000.0,
        volume_m5=1_500.0,
        txns_m5_buys=20,
        txns_m5_sells=8,
        market_cap=100_000.0,
        fdv=100_000.0,
        pair_created_at=int(time.time() * 1000) - 60_000,
    )


# --- fake Jupiter (new JupiterSwap API: buy/sell/price_usd -> SwapResult) ---
class FakeJupiter:
    def __init__(self, buy_ok=True, sell_output=4_000_000):
        self.buy_ok = buy_ok
        self.sell_output = sell_output  # raw USDC returned on sell (1e6 = $1)

    async def buy(self, mint, amount_usd, liquidity_usd=0.0):
        print(f"  [fake buy] {mint} ${amount_usd} liq=${liquidity_usd}")
        if not self.buy_ok:
            return SwapResult(False, "", int(amount_usd * 1e6), 0, "fake buy failure")
        return SwapResult(True, "fake-buy-sig", int(amount_usd * 1e6), 100_000_000, "")

    async def sell(self, mint, token_amount_raw):
        print(f"  [fake sell] {mint} {token_amount_raw} raw -> ${self.sell_output / 1e6:.2f}")
        return SwapResult(True, "fake-sell-sig", token_amount_raw, self.sell_output, "")

    async def price_usd(self, mint):
        return 0.0001

    async def close(self):
        pass


class FakeDexScreener:
    async def token_pairs(self, mint):
        return [make_pair()]

    def pick_pair(self, pairs):
        return pairs[0] if pairs else None

    async def close(self):
        pass


async def main():
    launch, pair = make_launch(), make_pair()
    cand = Candidate(launch=launch, pair=pair, score=62.5, scanned_at="now")

    # --- happy path: take_profit exit -> win -> 60/40 compounding ---
    risk = RiskManager()
    risk.play_amount = 2.0
    stats = TradeStats(dry_run=True)
    won, reason = await execute_trade(
        cand, risk, FakeJupiter(), FakeDexScreener(), TelegramNotifier(), stats
    )
    print(f"  result: won={won} reason={reason} next_play=${risk.play_amount}")
    assert won and reason == "take_profit"
    assert risk.play_amount == 2.4, risk.play_amount  # 60% of $4 proceeds
    # stats: proceeds = 4_000_000 raw USDC / 1e6 = $4.0 ; pnl = 4.0 - 2.0 = +2.0
    assert stats.trades == 1 and stats.wins == 1 and stats.winrate == 1.0
    assert stats.balance_usd == 4.0, stats.balance_usd  # 2.0 - 2.0 + 4.0
    assert not stats.in_trade
    print("[OK] win path -> compounding + stats")
    print("--- status markdown preview ---")
    print(stats.markdown())

    # --- loss path: stop_loss exit -> loss -> 0.82x reduction ---
    FakeMonitor.exit_signal = "stop_loss"
    risk2 = RiskManager()
    risk2.play_amount = 2.0
    won2, reason2 = await execute_trade(
        cand,
        risk2,
        FakeJupiter(sell_output=1_600_000),
        FakeDexScreener(),
        TelegramNotifier(),
        TradeStats(dry_run=True),
    )
    assert not won2 and reason2 == "stop_loss"
    assert risk2.play_amount == 1.64, risk2.play_amount  # 2.0 * 0.82
    print("[OK] loss path -> position shrink")

    # --- buy failure path: trade aborts cleanly, no crash ---
    won3, reason3 = await execute_trade(
        cand,
        RiskManager(),
        FakeJupiter(buy_ok=False),
        FakeDexScreener(),
        TelegramNotifier(),
        TradeStats(dry_run=True),
    )
    assert not won3 and reason3 == "buy_failed", (won3, reason3)
    print("[OK] buy-failure path aborts cleanly")

    # --- daily loss kill switch (hardening v2) ---
    s4 = TradeStats(dry_run=True)
    s4.daily_pnl_usd = -9.5
    assert not s4.daily_loss_limit_hit(), "should NOT halt above the limit"
    s4.daily_pnl_usd = -10.0
    assert s4.daily_loss_limit_hit(), "should halt at exactly -10"
    s5 = TradeStats(dry_run=True)
    s5.record_exit(False, -2.0, 0.0, "stop_loss", 0.00008)
    assert s5.daily_pnl_usd == -2.0 and s5.day_key, "daily pnl tracking"
    assert s5.next_day_reset_seconds() > 0, "UTC midnight reset"
    print("[OK] daily-loss kill switch + daily pnl tracking")

    print("\nINTEGRATION TEST PASSED")


asyncio.run(main())
