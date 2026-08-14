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
  * RATE-LIMITED — Helius throttles aggressively (429). Lookups are
    serialized (DEV_REP_MIN_INTERVAL_S between calls), a 429 honors
    Retry-After (capped), and a run of consecutive 429s trips a short
    fail-open cooldown so a rate-limited key never storms the API or
    blocks trading.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import httpx

from config import settings

log = logging.getLogger("sniper_bot.devrep")

HELIUS_TX_URL = "{base}/v0/addresses/{wallet}/transactions/"


def _retry_after_s(resp: httpx.Response, cap_s: float) -> float:
    """Seconds to wait before retrying a 429, from Retry-After or a default.

    Retry-After may be delta-seconds or an HTTP-date. Returns the (capped)
    delta; falls back to a conservative 2s when absent/invalid so a walled
    endpoint isn't hammered.
    """
    raw = resp.headers.get("Retry-After")
    if raw:
        try:
            delta = float(raw)
            return min(max(delta, 0.0), cap_s)
        except ValueError:
            try:
                when = parsedate_to_datetime(raw).astimezone(timezone.utc)
                delta = (when - datetime.now(timezone.utc)).total_seconds()
                return min(max(delta, 0.0), cap_s)
            except (ValueError, TypeError):
                pass
    return min(2.0, cap_s)


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
        min_interval_s: float | None = None,
        retry_after_cap_s: float | None = None,
        consec_429_limit: int | None = None,
        cooldown_s: float | None = None,
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
        self._min_interval = (
            min_interval_s if min_interval_s is not None else settings.dev_rep_min_interval_s
        )
        self._retry_cap = (
            retry_after_cap_s if retry_after_cap_s is not None else settings.dev_rep_retry_after_cap_s
        )
        self._consec_limit = (
            consec_429_limit if consec_429_limit is not None else settings.dev_rep_consec_429_limit
        )
        self._cooldown = (
            cooldown_s if cooldown_s is not None else settings.dev_rep_cooldown_s
        )
        # rate-limit state: serializes lookups, remembers when we may call again,
        # and trips a fail-open cooldown after repeated 429s
        self._gate = asyncio.Semaphore(1)
        self._next_call_mono = 0.0
        self._consec_429 = 0
        self._cooldown_until = 0.0
        self._degraded = False
        self._cache: dict[str, tuple[float, tuple[bool, str]]] = {}
        # failed lookups cached briefly so an outage doesn't re-hammer wallets
        self._fail_cache: dict[str, float] = {}
        self._fail_ttl = 10.0
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
        # rate-limit cooldown: a 429 run pauses lookups, fail-open (no block)
        if self._cooldown_until > now:
            return False, ""
        # recently-failed wallet: skip re-hammering during an outage (fail-open)
        fail_at = self._fail_cache.get(wallet)
        if fail_at and now - fail_at < self._fail_ttl:
            return False, ""
        verdict = await self._check_wallet(wallet)
        if verdict is None:
            self._fail_cache[wallet] = now
            return False, ""
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
    async def _check_wallet(self, wallet: str) -> tuple[bool, str] | None:
        """Look up one wallet and return (blocked, reason), failing open.

        Returns None when the lookup itself failed (network/429) — the caller
        caches the miss briefly and treats it as pass (fail-open).
        """
        try:
            txs = await self._fetch(wallet)
        except Exception as exc:  # noqa: BLE001 — fail-open
            self.last_error = str(exc)[:120]
            log.warning("Dev-rep lookup failed for %s: %s", wallet[:8], exc)
            return None

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
        """One bounded fetch with a 429-aware retry — DNS blips must not cost
        the signal, but the whole lookup stays within the timeout budget.

        Global rate limiting: lookups are serialized and spaced by
        DEV_REP_MIN_INTERVAL_S. A 429 waits Retry-After (capped); repeated
        consecutive 429s trip a fail-open cooldown (see veto()).
        """
        async with self._gate:
            # space requests: never fire before the last call + min interval
            wait = self._next_call_mono - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            url = HELIUS_TX_URL.format(base=self._base, wallet=wallet)
            params = {"api-key": self._key, "limit": 100}
            deadline = time.monotonic() + self._timeout
            for attempt in (1, 2):
                remaining = deadline - time.monotonic()
                if remaining <= 0.3:
                    break
                try:
                    resp = await self._client.get(url, params=params, timeout=remaining)
                    self._next_call_mono = time.monotonic() + self._min_interval
                    if resp.status_code == 429:
                        self._consec_429 += 1
                        self._degraded = True
                        wait = _retry_after_s(resp, self._retry_cap)
                        self.last_error = "429 Too Many Requests (Retry-After handled)"
                        if attempt == 1 and self._consec_429 < self._consec_limit:
                            log.warning(
                                "Dev-rep rate-limited for %s — backing off %.0fs (consec 429=%d)",
                                wallet[:8], wait, self._consec_429,
                            )
                            await asyncio.sleep(min(wait, remaining))
                            continue
                        if self._consec_429 >= self._consec_limit:
                            self._cooldown_until = time.monotonic() + self._cooldown
                            log.warning(
                                "Dev-rep throttled %d x — pausing lookups %.0fs (fail-open)",
                                self._consec_429, self._cooldown,
                            )
                        raise RuntimeError("dev-rep lookup unavailable (429)")
                    self._consec_429 = 0
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
