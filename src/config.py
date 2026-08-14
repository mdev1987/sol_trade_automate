"""
Configuration loader — the bot's control panel (.env).
Env names follow bot_plan/sample_env.txt.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv
from solders.keypair import Keypair

load_dotenv()

# Well-known Solana addresses
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC (6 decimals)
SOL_MINT = "So11111111111111111111111111111111111111112"  # WSOL/native SOL (9 decimals)
USDC_DECIMALS = 6


def _get_bool(name: str, default: bool) -> bool:
    """Read a boolean env var; true for 1/true/yes/on, else the default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _get_int(name: str, default: int) -> int:
    """Read an integer env var, falling back to default on junk."""
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    """Read a float env var, falling back to default on junk."""
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class QuoteConfig:
    """Quote-gate settings (see jupiter_swap.QuoteResult skip taxonomy)."""

    rate_per_sec: float = 1.0  # Jupiter FREE tier = 1 RPS
    max_price_impact_pct: float = 10.0  # reject quotes above this impact
    cache_ttl_sec: float = 30.0  # collapse launch bursts
    retries: int = 3  # retries for retryable quote failures
    retry_delay_sec: float = 0.5
    slippage_tiers: tuple = (
        # (min_liquidity_usd, slippage_bps) — first match wins, desc order
        (50_000, 150),
        (10_000, 300),
        (2_000, 500),
        (0, 1_000),
    )

    def slippage_for(self, liquidity_usd: float) -> int:
        """Dynamic slippage: thinner pools get wider slippage tolerance."""
        for min_liq, bps in self.slippage_tiers:
            if liquidity_usd >= min_liq:
                return bps
        return self.slippage_tiers[-1][1]


def _parse_slippage_tiers(raw: str | None) -> tuple:
    """Parse '50000:150,10000:300,2000:500,0:1000' into desc-order tiers."""
    if not raw:
        return QuoteConfig.slippage_tiers
    tiers = []
    for part in raw.split(","):
        min_liq, bps = part.split(":")
        tiers.append((float(min_liq), int(bps)))
    return tuple(sorted(tiers, key=lambda t: t[0], reverse=True))


def _load_keypair() -> Keypair | None:
    """Load the wallet keypair from PRIVATE_KEY (None in dry-run without one)."""
    pk = os.getenv("PRIVATE_KEY", "")
    if not pk:
        return None
    try:
        return Keypair.from_base58_string(pk)
    except Exception:  # noqa: BLE001
        return None


def _parse_helius_key(url: str) -> str:
    """Extract api-key from a Helius RPC URL (https://.../?api-key=KEY) if present."""
    try:
        from urllib.parse import parse_qs, urlparse

        q = parse_qs(urlparse(url).query)
        return (q.get("api-key") or [""])[0]
    except Exception:  # noqa: BLE001
        return ""


@dataclass
class Settings:
    """All runtime configuration, read from env vars at import time."""

    # --- trade parameters ---
    starting_amount: float = field(default_factory=lambda: _get_float("STARTING_AMOUNT", 2.0))
    # paper wallet initial bankroll (what /status balance starts at); position
    # size stays STARTING_AMOUNT regardless
    starting_balance: float = field(default_factory=lambda: _get_float("STARTING_BALANCE", 20.0))
    stop_loss: float = field(default_factory=lambda: _get_float("STOP_LOSS", 0.82))
    take_profit: float = field(default_factory=lambda: _get_float("TAKE_PROFIT", 2.0))
    slippage_bps: int = field(default_factory=lambda: _get_int("SLIPPAGE", 150))
    poll_interval: int = field(default_factory=lambda: _get_int("POLL_INTERVAL", 8))
    max_scan_window_min: int = field(default_factory=lambda: _get_int("MAX_SCAN_WINDOW", 15))

    # --- risk management ---
    loss_pause_trigger: int = field(default_factory=lambda: _get_int("LOSS_PAUSE_TRIGGER", 2))
    loss_pause_minutes: int = field(default_factory=lambda: _get_int("LOSS_PAUSE_MINUTES", 5))
    play_floor: float = field(default_factory=lambda: _get_float("PLAY_FLOOR", 1.0))
    reinvest_ratio: float = field(default_factory=lambda: _get_float("REINVEST_RATIO", 0.60))
    # daily loss kill switch: halt trading until next UTC day when daily
    # realized PnL <= -DAILY_LOSS_LIMIT (0 = disabled). Replay-validated:
    # 8 halts the bleed on blowing days (-$8.18 vs -$10.36 at 10) without
    # costing good days any upside (+17.94/+17.99/+0.86 unchanged).
    daily_loss_limit: float = field(default_factory=lambda: _get_float("DAILY_LOSS_LIMIT", 8.0))

    # --- exits / monitoring ---
    # max hold time before a position is force-exited (stuck-position watchdog)
    max_hold_min: float = field(default_factory=lambda: _get_float("MAX_HOLD_MIN", 30.0))
    # use the PumpAPI buy/sell stream for sub-second TP/SL triggers
    live_feed_exit: bool = field(default_factory=lambda: _get_bool("LIVE_FEED_EXIT", True))
    # trailing stop: exit when price falls TRAIL_EXIT_PCT below the running
    # peak since entry. Live analysis (DexPaprika 1m OHLCV, 5 real fills)
    # showed entries land at/near the launch-pump ATH and then gap 75-85% in
    # the same minute — the fixed 0.82 SL books ~-70% while a 15% trailing
    # stop converts that to ~-15% (and locks gains when a position does pump).
    # 0 = disable (legacy fixed TP/SL only).
    trail_exit_pct: float = field(default_factory=lambda: _get_float("TRAIL_EXIT_PCT", 0.15))
    # dry-run stop-loss fill realism: a real stop on these tokens fills near
    # the dump bottom (~0.2-0.3x entry), not at the 0.82 trigger. Dry-run PnL
    # books proceeds * DRY_STOP_FILL on a stop loss so projections aren't
    # optimistic (live mode always uses the real fill price).
    dry_stop_fill: float = field(default_factory=lambda: _get_float("DRY_STOP_FILL", 0.25))
    # minimum feed score for a launch to be queued (feed-data entry path).
    # Replay backtest (07/21 + 08/09) shows 45 lets in the anti-predictive
    # high-scorer cohort; 60 is the validated gate.
    min_score: float = field(default_factory=lambda: _get_float("MIN_SCORE", 60.0))
    # entry liquidity floor: skip the buy unless the on-chain pool liquidity
    # (2 x quoteInPool x SOL) is >= MIN_LIQUIDITY_USD. Backtest-validated
    # (kills the dead/thin-pool bleed; 0 = disabled). The bot waits up to
    # LIQ_CONFIRM_WINDOW_S for a confirming buy to push the pool over the floor.
    #
    # LIQ_CONFIRM_WINDOW_S aligns with the replay backtest's entry_latency_s
    # (2.0s): live fills at ~2s instead of waiting a full 10s per candidate.
    # The old 10s window was the serial bottleneck (10s per candidate -> the
    # 46% queue backlog + ATH-chasing fills); 2s gives 5x candidate throughput
    # and skips pools that haven't proven $MIN_LIQUIDITY_USD by then, exactly
    # like the backtest's first-fill-event check.
    min_liquidity_usd: float = field(default_factory=lambda: _get_float("MIN_LIQUIDITY_USD", 5000.0))
    # how long after the launch we keep polling on-chain liquidity to prove the
    # pool >= min_liquidity_usd, PLUS the latency before we start counting.
    # Anchored to the launch's created_at (not trade start) so it matches the
    # replay backtest: it arms at create + entry_latency_s and fills at the
    # next buy event, up to +no_fill_timeout_s — i.e. pool-check window is
    # [create+latency, create+latency+window]. A short window polled from trade
    # start (create+~0.2s) samples the pool too early and rejects pools that
    # cross the floor a couple of seconds later (observed: 0 fills in 8h live
    # vs 193 in the backtest on the same config).
    entry_latency_s: float = field(default_factory=lambda: _get_float("ENTRY_LATENCY_S", 2.0))
    liq_confirm_window_s: float = field(
        default_factory=lambda: _get_float("LIQ_CONFIRM_WINDOW_S", 10.0)
    )
    # skip the buy when the token has already traded at more than
    # MAX_ENTRY_MULT x the launch (create) price. On pump.fun a fill far
    # above the launch price is chasing an initial burst that usually dumps —
    # the replay backtest (mm5, 4-day battery) raised worst-day and total PnL
    # (+$28.61 vs +$6.42 with the pre-gate config; 0 = gate off).
    max_entry_mult: float = field(default_factory=lambda: _get_float("MAX_ENTRY_MULT", 5.0))
    # skip the buy when the current price is within MAX_ENTRY_PEAK_PCT of the
    # token's post-launch peak so far (i.e. at/near the top of the launch
    # burst). Live analysis found real fills landed at 57-98% of the ATH and
    # then dumped — an entry inside the top MAX_ENTRY_PEAK_PCT of the observed
    # peak is chasing that burst. 0 = gate off.
    max_entry_peak_pct: float = field(default_factory=lambda: _get_float("MAX_ENTRY_PEAK_PCT", 0.0))

    # dead-token exit: exit when the live feed saw no trade for this mint for
    # STALE_EXIT_SEC AND no DexScreener pair ever appeared (frees the single
    # position slot — most pump launches die within a minute)
    stale_exit_sec: float = field(default_factory=lambda: _get_float("STALE_EXIT_SEC", 60.0))
    stale_exit_grace_sec: float = field(
        default_factory=lambda: _get_float("STALE_EXIT_GRACE_SEC", 15.0)
    )
    # drop queued candidates older than this at dequeue time (queue backlog)
    max_candidate_age_min: float = field(
        default_factory=lambda: _get_float("MAX_CANDIDATE_AGE_MIN", 5.0)
    )
    # periodic /status heartbeat card interval (minutes; 0 = disabled)
    status_interval_min: int = field(default_factory=lambda: _get_int("STATUS_INTERVAL_MIN", 15))

    # --- mode ---
    dry_run: bool = field(default_factory=lambda: _get_bool("DRY_RUN", True))
    auto_start: bool = field(default_factory=lambda: _get_bool("AUTO_START", True))

    # --- wallet ---
    private_key: str = field(default_factory=lambda: os.getenv("PRIVATE_KEY", ""))
    keypair: Keypair | None = field(default_factory=_load_keypair)

    # --- jupiter (execution + price fallback) ---
    jupiter_api_key: str = field(default_factory=lambda: os.getenv("JUPITER_API_KEY", ""))
    jupiter_api: str = field(
        default_factory=lambda: (
            os.getenv("JUPITER_API_URI") or os.getenv("JUPITER_API", "https://api.jup.ag")
        )
    )

    # --- pump.fun feeds (primary pumpapi, fallback pumpdev) ---
    pumpapi_ws_url: str = field(
        default_factory=lambda: os.getenv("PUMPAPI_WSS", "wss://stream.pumpapi.io/")
    )
    pumpdev_ws_url: str = field(
        default_factory=lambda: os.getenv("PUMPDEV_WSS", "wss://pumpdev.io/ws")
    )
    pump_api_key: str = field(default_factory=lambda: os.getenv("PUMPDEV_API_KEY", ""))

    # --- dexscreener (pair liquidity / volume / price, ~60 req/min) ---
    dexscreener_base: str = field(
        default_factory=lambda: os.getenv("DEXSCREENER_API", "https://api.dexscreener.com")
    )

    # --- quote gate (jupiter) ---
    quote: QuoteConfig = field(
        default_factory=lambda: QuoteConfig(
            rate_per_sec=_get_float("QUOTE_RATE_PER_SEC", 1.0),
            max_price_impact_pct=_get_float("MAX_PRICE_IMPACT_PCT", 10.0),
            cache_ttl_sec=_get_float("QUOTE_CACHE_TTL_SEC", 30.0),
            retries=_get_int("QUOTE_RETRIES", 3),
            retry_delay_sec=_get_float("QUOTE_RETRY_DELAY_SEC", 0.5),
            slippage_tiers=_parse_slippage_tiers(os.getenv("SLIPPAGE_TIERS")),
        )
    )

    # --- helius dev-reputation veto (read-only signal, fail-open) ---
    # Helius enhanced-transactions API key; falls back to the api-key embedded
    # in SOLANA_RPC_URL so no extra secret is needed if that URL is Helius.
    helius_api_key: str = field(
        default_factory=lambda: os.getenv("HELIUS_API_KEY")
        or _parse_helius_key(os.getenv("SOLANA_RPC_URL", ""))
    )
    dev_rep_enabled: bool = field(default_factory=lambda: _get_bool("DEV_REP_ENABLED", True))
    # veto a dev wallet that created >= N pump.fun tokens in the last 24h
    dev_rep_max_creates_24h: int = field(default_factory=lambda: _get_int("DEV_REP_MAX_CREATES_24H", 3))
    # veto wallets younger than this (hours); 0 = disabled (default: off —
    # brand-new wallets are common on pump.fun, this is the weakest signal)
    dev_rep_min_age_hours: float = field(default_factory=lambda: _get_float("DEV_REP_MIN_AGE_HOURS", 0.0))
    # per-wallet verdict cache + HTTP timeout (the lookup runs concurrently
    # with the entry-price estimate, so latency impact is ~0 in the common case)
    dev_rep_cache_ttl_min: float = field(default_factory=lambda: _get_float("DEV_REP_CACHE_TTL_MIN", 10.0))
    dev_rep_timeout_s: float = field(default_factory=lambda: _get_float("DEV_REP_TIMEOUT_S", 2.5))
    # --- dev-rep rate limiting (Helius throttles aggressively) ---
    # global min seconds between Helius lookups (serializes calls; keeps the
    # burst of qualified candidates well inside free-tier limits)
    dev_rep_min_interval_s: float = field(
        default_factory=lambda: _get_float("DEV_REP_MIN_INTERVAL_S", 1.0)
    )
    # on a 429, wait Retry-After (if given) but never longer than this cap
    dev_rep_retry_after_cap_s: float = field(
        default_factory=lambda: _get_float("DEV_REP_RETRY_AFTER_CAP_S", 30.0)
    )
    # consecutive 429s after which lookups pause for a cooldown (fail-open:
    # wallets checked during cooldown pass; trading is never blocked)
    dev_rep_consec_429_limit: int = field(
        default_factory=lambda: _get_int("DEV_REP_CONSEC_429_LIMIT", 3)
    )
    dev_rep_cooldown_s: float = field(
        default_factory=lambda: _get_float("DEV_REP_COOLDOWN_S", 30.0)
    )

    # --- telegram alerts / reporting ---
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID", "")
    )

    # --- strategy mode: which candidate source drives trading ---
    # launch = pump.fun launch sniper (create events on the bonding curve,
    #          rug+scoring+dev-veto funnel) — the default, replay-validated.
    # signal = debot smart-money signal scanner (all DEXs: pump_swap, pump,
    #          meteora, raydium, ...) — experimental, needs replay validation.
    strategy_mode: str = field(
        default_factory=lambda: os.getenv("STRATEGY_MODE", "launch").strip().lower()
    )

    # --- debot smart-money signal scanner (used when strategy_mode == "signal") ---
    # Polls debot /channel/list for signal events and feeds the SAME queue the
    # launch scanner uses. Pattern + defaults calibrated on 469 crawled events
    # (bot_plan/signal_pattern_report.md): the only robust winner feature is a
    # *moderate pre-signal pump* (max_price_gain 0.72-1.87x -> ~30% win vs
    # 8-10% for flat/already-2x), so the gain band is the anti-chase gate.
    signal_poll_sec: float = field(default_factory=lambda: _get_float("SIGNAL_POLL_SEC", 20.0))
    signal_max_age_sec: float = field(
        default_factory=lambda: _get_float("SIGNAL_MAX_AGE_SEC", 300.0)
    )
    signal_min_gain: float = field(default_factory=lambda: _get_float("SIGNAL_MIN_GAIN", 0.72))
    signal_max_gain: float = field(default_factory=lambda: _get_float("SIGNAL_MAX_GAIN", 1.87))
    signal_min_wallets: int = field(default_factory=lambda: _get_int("SIGNAL_MIN_WALLETS", 1))
    signal_min_liquidity_usd: float = field(
        default_factory=lambda: _get_float("SIGNAL_MIN_LIQUIDITY_USD", 5000.0)
    )
    signal_max_liquidity_usd: float = field(
        default_factory=lambda: _get_float("SIGNAL_MAX_LIQUIDITY_USD", 500_000.0)
    )
    signal_min_holders: int = field(default_factory=lambda: _get_int("SIGNAL_MIN_HOLDERS", 30))
    signal_max_top10: float = field(default_factory=lambda: _get_float("SIGNAL_MAX_TOP10", 0.35))
    signal_max_mcap_usd: float = field(
        default_factory=lambda: _get_float("SIGNAL_MAX_MCAP_USD", 500_000.0)
    )
    signal_min_vol24_usd: float = field(
        default_factory=lambda: _get_float("SIGNAL_MIN_VOL24_USD", 1000.0)
    )
    signal_min_signals: int = field(default_factory=lambda: _get_int("SIGNAL_MIN_SIGNALS", 1))
    # comma-separated token_level tiers to reject (research: silver/gold showed
    # 0% win rate but tiny n=7/3 — default rejects nothing)
    signal_reject_tiers: tuple = field(
        default_factory=lambda: tuple(
            t.strip().lower()
            for t in os.getenv("SIGNAL_REJECT_TIERS", "").split(",")
            if t.strip()
        )
    )

    # --- jupiter price fallback ---
    jupiter_price_base: str = field(
        default_factory=lambda: (
            (os.getenv("JUPITER_API_URI") or os.getenv("JUPITER_API", "https://api.jup.ag")).rstrip(
                "/"
            )
            + "/price/v3"
        )
    )
    # secondary SOL/USD oracles: tried in order after the primary when the
    # higher-priority endpoint is unreachable (transient DNS/network must
    # never cost us the price — every source is fail-open)
    pumpcoins_sol_price_url: str = field(
        default_factory=lambda: os.getenv(
            "PUMPCOINS_SOL_PRICE_URL", "https://pumpcoins.net/api/sol-price"
        )
    )
    coingecko_sol_price_url: str = field(
        default_factory=lambda: os.getenv(
            "COINGECKO_SOL_PRICE_URL",
            "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
        )
    )

    def validate(self) -> None:
        """Fail fast on missing secrets (only when going live)."""
        if self.dry_run:
            return
        if self.strategy_mode not in ("launch", "signal"):
            raise ValueError(
                f"STRATEGY_MODE must be 'launch' or 'signal', got {self.strategy_mode!r}"
            )
        missing = []
        if not self.private_key or self.keypair is None:
            missing.append("PRIVATE_KEY")
        if not self.jupiter_api_key:
            missing.append("JUPITER_API_KEY")
        if missing:
            raise RuntimeError(
                f"Missing required .env variables for live trading: {', '.join(missing)}"
            )


settings = Settings()
