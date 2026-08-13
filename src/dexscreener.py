"""
DexScreener REST client — price / liquidity / volume / txns.

Rate limit: 60 requests/minute → throttle to ~1.1s between calls.
Docs: bot_plan/docs/dex_screener_reference.md
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import httpx

from config import settings

log = logging.getLogger("sniper_bot.dexscreener")

# Which DEXes we consider for a launch pair (course: pump.fun focus)
PREFERRED_DEXES = ("pump", "pump-amm", "pumpswap", "raydium")


@dataclass
class Pair:
    """A DexScreener pair, with the fields the bot's filters need."""

    pair_address: str
    dex_id: str
    base_mint: str
    base_symbol: str
    quote_symbol: str
    price_usd: Optional[float]
    price_native: Optional[float]
    liquidity_usd: Optional[float]
    volume_m5: float
    txns_m5_buys: int
    txns_m5_sells: int
    market_cap: Optional[float]
    fdv: Optional[float]
    pair_created_at: Optional[int]  # unix ms
    url: str = ""

    # --- derived metrics used by filters/scoring ---
    @property
    def txns_m5(self) -> int:
        """Total buys + sells in the last 5 minutes."""
        return self.txns_m5_buys + self.txns_m5_sells

    @property
    def buy_sell_ratio(self) -> float:
        """Buy:sell ratio over m5 txns (inf when only buys, 0 when none)."""
        if self.txns_m5_sells <= 0:
            return float("inf") if self.txns_m5_buys > 0 else 0.0
        return self.txns_m5_buys / self.txns_m5_sells

    @classmethod
    def from_json(cls, p: dict) -> "Pair":
        """Build a Pair from a DexScreener pair JSON object."""
        base = p.get("baseToken") or {}
        quote = p.get("quoteToken") or {}
        txns = (p.get("txns") or {}).get("m5") or {}
        volume = p.get("volume") or {}
        liq = p.get("liquidity") or {}
        return cls(
            pair_address=p.get("pairAddress", ""),
            dex_id=p.get("dexId", ""),
            base_mint=base.get("address", ""),
            base_symbol=base.get("symbol", ""),
            quote_symbol=(quote or {}).get("symbol", ""),
            price_usd=_to_float(p.get("priceUsd")),
            price_native=_to_float(p.get("priceNative")),
            liquidity_usd=_to_float(liq.get("usd")),
            volume_m5=_to_float(volume.get("m5")) or 0.0,
            txns_m5_buys=int(txns.get("buys") or 0),
            txns_m5_sells=int(txns.get("sells") or 0),
            market_cap=_to_float(p.get("marketCap")),
            fdv=_to_float(p.get("fdv")),
            pair_created_at=p.get("pairCreatedAt"),
            url=p.get("url", ""),
        )


def _to_float(v) -> Optional[float]:
    """Coerce a value to float, returning None for junk/empty."""
    if v in (None, "", "null"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class DexScreenerClient:
    """Async REST client with a simple rate-limit throttle (60/min)."""

    def __init__(self, base_url: str = ""):
        """Create the client (base_url defaults to settings.dexscreener_base)."""
        self.base_url = base_url or settings.dexscreener_base
        self._client = httpx.AsyncClient(timeout=10)
        self._min_interval = 1.1  # seconds between requests
        self._last_call = 0.0

    async def _get(self, path: str) -> dict | list:
        """GET a path with a rate-limit throttle; raises on HTTP errors."""
        # throttle
        now = asyncio.get_event_loop().time()
        wait = self._min_interval - (now - self._last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_call = asyncio.get_event_loop().time()
        resp = await self._client.get(f"{self.base_url}{path}")
        resp.raise_for_status()
        return resp.json()

    async def token_pairs(self, mint: str) -> list[Pair]:
        """All pairs for a token mint: GET /token-pairs/v1/solana/{mint}"""
        try:
            data = await self._get(f"/token-pairs/v1/solana/{mint}")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return []  # not indexed yet — the scanner retries
            raise
        pairs = [Pair.from_json(p) for p in data] if isinstance(data, list) else []
        return pairs

    def pick_pair(self, pairs: list[Pair]) -> Optional[Pair]:
        """Pick the pair we actually trade — prefers pump/pump-amm, then raydium."""
        if not pairs:
            return None
        for dex in PREFERRED_DEXES:
            for p in pairs:
                if p.dex_id == dex:
                    return p
        # fallback: any pair with liquidity
        for p in sorted(pairs, key=lambda x: x.liquidity_usd or 0, reverse=True):
            return p
        return pairs[0]

    async def close(self) -> None:
        """Close the underlying httpx client."""
        await self._client.aclose()
