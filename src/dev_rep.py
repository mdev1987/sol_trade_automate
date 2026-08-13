"""
Dev-reputation veto — Helius enhanced-transactions (read-only, fail-open).

Before buying a launch we ask Helius for the dev wallet's transaction history
and veto the trade on hard evidence:

  * serial launcher : >= DEV_REP_MAX_CREATES_24H pump.fun CREATE txs in 24h
  * dump evidence   : a SWAP/TRANSFER where the wallet sells a token it created
  * newborn wallet  : wallet age < DEV_REP_MIN_AGE_HOURS (optional, default off)

Design rules:

  * READ-ONLY — safe in dry-run (never signs, never executes).
  * FAIL-OPEN  — any network error / timeout / malformed response returns
    (blocked=False); a flaky reputation lookup must never block trading.
  * TIME-BOXED — one GET per wallet, HTTP timeout DEV_REP_TIMEOUT_S; the
    caller runs it concurrently with the entry price estimate so it adds
    ~0 latency when the price lookup is slower (typical case).
  * CACHED     — verdicts cached per wallet for DEV_REP_CACHE_TTL_MIN.
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from config import settings

log = logging.getLogger("sniper_bot.devrep")

HELIUS_TX_URL = "{base}/v0/addresses/{wallet}/transactions/"


def _created_mints(tx: dict) -> set:
    """Mints created by a pump.fun CREATE tx (from accountData token balances)."""
    mints = set()
    for acc in tx.get("accountData") or []:
        for tbc in acc.get("tokenBalanceChanges") or []:
            m = tbc.get("mint")
            if m:
                mints.add(m)
    return mints


class DevReputationClient:
    """Helius dev-wallet reputation lookup with per-wallet cache + fail-open."""

    def __init__(
        self,
        api_key: str | None = None,
        base: str | None = None,
        timeout_s: float | None = None,
        cache_ttl_s: float | None = None,
        max_creates_24h: int | None = None,
        min_age_hours: float | None = None,
        transport=None,  # httpx transport (tests inject MockTransport)
    ) -> None:
        """Create the client with per-wallet cache and fail-open defaults."""
        self._key = api_key if api_key is not None else settings.helius_api_key
        self._base = (base or "https://mainnet.helius-rpc.com").rstrip("/")
        self._timeout = timeout_s if timeout_s is not None else settings.dev_rep_timeout_s
        self._ttl = cache_ttl_s if cache_ttl_s is not None else settings.dev_rep_cache_ttl_min * 60
        self._max_creates = (
            max_creates_24h if max_creates_24h is not None else settings.dev_rep_max_creates_24h
        )
        self._min_age = (
            min_age_hours if min_age_hours is not None else settings.dev_rep_min_age_hours
        )
        self._cache: dict[str, tuple[float, tuple[bool, str]]] = {}
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            headers={"User-Agent": "sniper-bot/1.0"},
            transport=transport,
        )
        self.lookups = 0  # Helius API calls made
        self.vetoes = 0  # wallets blocked by hard evidence
        self.last_error = ""

    # ------------------------------------------------------------------ public
    async def veto(self, launch) -> tuple[bool, str]:
        """(blocked, reason) for a TokenLaunch. Fail-open; cached per wallet."""
        wallet = (getattr(launch, "creator", "") or "").strip()
        if not wallet:
            return False, ""
        now = time.monotonic()
        hit = self._cache.get(wallet)
        if hit and now - hit[0] < self._ttl:
            return hit[1]
        verdict = await self._check_wallet(wallet)
        self._cache[wallet] = (now, verdict)
        return verdict

    async def close(self) -> None:
        """Close the underlying httpx client."""
        await self._client.aclose()

    def summary(self) -> str:
        """One-line status for status cards / logs."""
        parts = [f"dev-rep: {self.lookups} lookups / {self.vetoes} vetoes"]
        if self.last_error:
            parts.append(f"last-err: {self.last_error[:40]}")
        return " | ".join(parts)

    # ----------------------------------------------------------------- internal
    async def _check_wallet(self, wallet: str) -> tuple[bool, str]:
        """Look up one wallet and return (blocked, reason), failing open."""
        try:
            txs = await self._fetch(wallet)
        except Exception as exc:  # noqa: BLE001 — fail-open
            self.last_error = str(exc)[:120]
            log.warning("Dev-rep lookup failed for %s: %s", wallet[:8], exc)
            return False, ""

        now = time.time()
        creates_24h = 0
        created_mints: set = set()
        first_ts: float | None = None

        for tx in txs or []:
            ts = tx.get("timestamp") or 0
            if ts and (first_ts is None or ts < first_ts):
                first_ts = float(ts)
            ttype = (tx.get("type") or "").upper()
            source = (tx.get("source") or "").upper()
            if ttype == "CREATE" and source == "PUMP_FUN":
                if ts >= now - 86_400:
                    creates_24h += 1
                created_mints |= _created_mints(tx)

        if creates_24h >= self._max_creates:
            return True, f"serial launcher: {creates_24h} pump.fun creates in 24h"

        # dump evidence: the wallet SOLD a token it previously created
        for tx in txs or []:
            ttype = (tx.get("type") or "").upper()
            if ttype not in ("SWAP", "TRANSFER"):
                continue
            for tt in tx.get("tokenTransfers") or []:
                if tt.get("mint") in created_mints and tt.get("fromUserAccount") == wallet:
                    return True, "dumped a previously created token"

        if self._min_age > 0 and first_ts and (now - first_ts) < self._min_age * 3600:
            age_h = (now - first_ts) / 3600
            return True, f"newborn wallet ({age_h:.1f}h old)"

        return False, ""

    async def _fetch(self, wallet: str) -> list:
        """One bounded fetch with one fast retry — DNS blips must not cost the
        signal, but the whole lookup stays within the timeout budget."""
        url = HELIUS_TX_URL.format(base=self._base, wallet=wallet)
        params = {"api-key": self._key, "limit": 100}
        deadline = time.monotonic() + self._timeout
        for attempt in (1, 2):
            remaining = deadline - time.monotonic()
            if remaining <= 0.3:
                break
            try:
                resp = await self._client.get(url, params=params, timeout=remaining)
                resp.raise_for_status()
                data = resp.json()
                self.lookups += 1
                return data if isinstance(data, list) else []
            except Exception as exc:
                self.last_error = repr(exc)[:120]
                if attempt == 1:
                    log.warning("Dev-rep fetch failed for %s (retrying): %s", wallet[:8], exc)
                    await asyncio.sleep(0.4)
                else:
                    raise
        raise RuntimeError("dev-rep lookup unavailable")
