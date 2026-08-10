"""
Configuration loader — the bot's control panel (.env).
Env names follow bot_plan/sample_env.txt.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv
from solders.keypair import Keypair

load_dotenv()

# Well-known Solana addresses
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC (6 decimals)
SOL_MINT = "So11111111111111111111111111111111111111112"  # WSOL/native SOL (9 decimals)
USDC_DECIMALS = 6


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
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


def _load_keypair() -> Optional[Keypair]:
    """Load the wallet keypair from PRIVATE_KEY (None in dry-run without one)."""
    pk = os.getenv("PRIVATE_KEY", "")
    if not pk:
        return None
    try:
        return Keypair.from_base58_string(pk)
    except Exception:  # noqa: BLE001
        return None


@dataclass
class Settings:
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
    # realized PnL <= -DAILY_LOSS_LIMIT (0 = disabled)
    daily_loss_limit: float = field(default_factory=lambda: _get_float("DAILY_LOSS_LIMIT", 10.0))

    # --- exits / monitoring ---
    # max hold time before a position is force-exited (stuck-position watchdog)
    max_hold_min: float = field(default_factory=lambda: _get_float("MAX_HOLD_MIN", 30.0))
    # use the PumpAPI buy/sell stream for sub-second TP/SL triggers
    live_feed_exit: bool = field(default_factory=lambda: _get_bool("LIVE_FEED_EXIT", True))
    # minimum feed score for a launch to be queued (feed-data entry path)
    min_score: float = field(default_factory=lambda: _get_float("MIN_SCORE", 40.0))
    # periodic /status heartbeat card interval (minutes; 0 = disabled)
    status_interval_min: int = field(default_factory=lambda: _get_int("STATUS_INTERVAL_MIN", 15))

    # --- mode ---
    dry_run: bool = field(default_factory=lambda: _get_bool("DRY_RUN", True))
    auto_start: bool = field(default_factory=lambda: _get_bool("AUTO_START", True))

    # --- wallet ---
    private_key: str = field(default_factory=lambda: os.getenv("PRIVATE_KEY", ""))
    keypair: Optional[Keypair] = field(default_factory=_load_keypair)

    # --- solana rpc (optional reads; not required for /order + /execute) ---
    rpc_url: str = field(
        default_factory=lambda: os.getenv("SOLANA_RPC_URL") or os.getenv("RPC_URL", "")
    )

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

    # --- telegram alerts / reporting ---
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID", "")
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

    def validate(self) -> None:
        """Fail fast on missing secrets (only when going live)."""
        if self.dry_run:
            return
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
