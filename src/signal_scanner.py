"""
signal_scanner.py — second candidate source: debot smart-money signal events.

The launch scanner reacts to pump.fun *create* events; this scanner reacts to
debot's /channel/list smart-money signal feed instead — tokens that an
algorithm already flagged as being accumulated by "smart money" wallets.

Pattern gate (from bot_plan/signal_pattern_report.md, research artifact):
  - the ONLY robust feature was a *moderate pre-signal pump*: tokens whose
    max_price_gain (highest price / first-signal price) sat in the 0.72-1.87x
    band won ~30% of the time vs 8-10% for flat (Q1/Q2) or already-2x (Q4)
    tokens. Everything else (signal_count, n_wallets, token_tier) was weak or
    overfit noise, so it is configurable but defaults to loose.
  - secondary hard gates reuse the bot's own hygiene: liquidity band,
    top10 concentration (rug/whale proxy), holder count, market cap.

Signals enter the SAME queue as launch candidates, so every downstream gate
(buy, quote, TP/SL, daily loss cap, telegram) applies unchanged. The entry-mult
gate is left to fail open: debot prices are USD-normalized, not comparable to
the on-chain launch price, and the gain band above is the real anti-chase rule.

Docs: bot_plan/useful_api.txt (endpoints), bot_plan/signal_pattern_report.md
(pattern), bot_plan/signal_raw/ (crawled payloads used to calibrate defaults).
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field

import httpx

from config import SOL_MINT, settings
from control import TradeGate
from data_stream import TokenLaunch
from monitoring import append_journal
from token_scanner import Candidate

log = logging.getLogger("sniper_bot.signal")

DEBOT_LIST = "https://debot.ai/api/community/signal/channel/list"
REQUEST_TIMEOUT_S = 20.0
PAGE_SIZE = 24


@dataclass
class SignalGate:
    """The researched signal pattern, as configurable gates."""

    # pre-signal pump band (max_price_gain = max_price / first_price):
    # the ONLY feature that robustly separated winners (Q3 0.72-1.87x ~30%)
    # from losers (Q1/Q2 ~8-10%). Reject both dead-flat and already-2x tokens.
    min_gain: float = field(default_factory=lambda: settings.signal_min_gain)
    max_gain: float = field(default_factory=lambda: settings.signal_max_gain)

    min_wallets: int = field(default_factory=lambda: settings.signal_min_wallets)
    min_liquidity_usd: float = field(default_factory=lambda: settings.signal_min_liquidity_usd)
    max_liquidity_usd: float = field(default_factory=lambda: settings.signal_max_liquidity_usd)
    min_holders: int = field(default_factory=lambda: settings.signal_min_holders)
    max_top10: float = field(default_factory=lambda: settings.signal_max_top10)
    max_mcap_usd: float = field(default_factory=lambda: settings.signal_max_mcap_usd)
    min_vol24_usd: float = field(default_factory=lambda: settings.signal_min_vol24_usd)
    min_signals: int = field(default_factory=lambda: settings.signal_min_signals)
    reject_tiers: tuple = field(default_factory=lambda: tuple(settings.signal_reject_tiers))
    max_age_sec: float = field(default_factory=lambda: settings.signal_max_age_sec)


def _to_float(v) -> float | None:
    """Coerce a value to float, returning None for junk/empty."""
    try:
        if v in (None, "", "null"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v) -> int:
    """Coerce a value to int, returning 0 for junk/empty."""
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def score_signal(
    ev: dict,
    token_meta: dict,
    sig_meta: dict,
    metrics: dict,
    gate: SignalGate,
) -> float:
    """0-100: how well this signal fits the researched winner pattern.

    Moderate pre-signal pump is the dominant term (the only robust feature);
    concentration, holders, wallets and volume add smaller bonus points.
    """
    gain = _to_float(sig_meta.get("max_price_gain")) or 0.0
    holders = _to_int(metrics.get("holder_count"))
    top10 = _to_float(metrics.get("top10_position")) or 0.0
    wallets = len(ev.get("wallet_stats") or [])
    vol24 = _to_float(metrics.get("volume_24h")) or 0.0
    signals = _to_int(sig_meta.get("signal_count"))
    tier = (sig_meta.get("token_level") or "").strip()

    s = 0.0

    # pump band (40 pts): peak inside [min_gain, max_gain]; taper outside
    if gate.min_gain <= gain <= gate.max_gain:
        s += 40.0
    elif gain > 0:
        # near the band (within ~35%) still earns partial credit
        lo, hi = gate.min_gain * 0.65, gate.max_gain * 1.35
        if lo <= gain <= hi:
            s += 20.0

    # concentration (20): the lower the top-10 share, the safer the pump
    if top10 > 0:
        s += min(20.0, 20.0 * max(0.0, (gate.max_top10 - top10)) / gate.max_top10)

    # holders (15): more = more organic demand
    if holders > 0:
        s += min(15.0, 15.0 * holders / 200.0)

    # smart wallets (15): more wallets in the signal event = stronger accumulation
    s += min(15.0, 15.0 * wallets / 5.0)

    # volume (10): the signal should be on a token that is actually trading
    if vol24 > 0:
        s += min(10.0, 10.0 * vol24 / 50_000.0)

    # small bonus for repeated signals / known tier (research says weak — keep tiny)
    if signals >= gate.min_signals and signals >= 2:
        s += 5.0
    if tier:
        s += 2.0

    return round(min(s, 100.0), 1)


def gate_signal(
    ev: dict,
    token_meta: dict,
    sig_meta: dict,
    metrics: dict,
    gate: SignalGate,
    now: float,
) -> tuple[bool, str]:
    """(passed, reason) — hard reject reasons first, then the pump band."""
    # freshness: a signal older than the window is stale by the time we react
    create_time = _to_float(ev.get("create_time"))
    if create_time and now - create_time > gate.max_age_sec:
        return False, f"stale:{now - create_time:.0f}s"

    gain = _to_float(sig_meta.get("max_price_gain")) or 0.0
    if gain <= 0:
        return False, "no_gain"
    if gain < gate.min_gain:
        return False, f"gain:{gain:.2f}<{gate.min_gain}"
    if gain > gate.max_gain:
        return False, f"gain:{gain:.2f}>{gate.max_gain}"

    wallets = len(ev.get("wallet_stats") or [])
    if wallets < gate.min_wallets:
        return False, f"wallets:{wallets}<{gate.min_wallets}"

    liquidity = _to_float(metrics.get("liquidity")) or 0.0
    if liquidity < gate.min_liquidity_usd:
        return False, f"liq:{liquidity:.0f}<{gate.min_liquidity_usd:.0f}"
    if liquidity > gate.max_liquidity_usd:
        return False, f"liq:{liquidity:.0f}>{gate.max_liquidity_usd:.0f}"

    holders = _to_int(metrics.get("holder_count"))
    if holders < gate.min_holders:
        return False, f"holders:{holders}<{gate.min_holders}"

    top10 = _to_float(metrics.get("top10_position")) or 0.0
    if top10 > gate.max_top10:
        return False, f"top10:{top10:.2f}>{gate.max_top10}"

    mcap = _to_float(metrics.get("market_cap")) or 0.0
    if mcap > gate.max_mcap_usd:
        return False, f"mcap:{mcap:.0f}>{gate.max_mcap_usd:.0f}"

    vol24 = _to_float(metrics.get("volume_24h")) or 0.0
    if vol24 < gate.min_vol24_usd:
        return False, f"vol24:{vol24:.0f}<{gate.min_vol24_usd:.0f}"

    signals = _to_int(sig_meta.get("signal_count"))
    if signals < gate.min_signals:
        return False, f"signals:{signals}<{gate.min_signals}"

    tier = (sig_meta.get("token_level") or "").strip().lower()
    if tier in gate.reject_tiers:
        return False, f"tier:{tier}"

    return True, ""


def build_candidate(
    ev: dict,
    meta: dict,
    gate: SignalGate,
    now: float,
) -> Candidate | None:
    """Validate one signal event and build a Candidate, or None."""
    token = ev.get("token") or ""
    if not token:
        return None
    token_meta = (meta.get("tokens") or {}).get(token) or {}
    sig_meta = (meta.get("signals") or {}).get(token) or {}
    metrics = (meta.get("metrics") or {}).get(token) or {}
    if not token_meta or not sig_meta or not metrics:
        return None

    passed, reason = gate_signal(ev, token_meta, sig_meta, metrics, gate, now)
    score = score_signal(ev, token_meta, sig_meta, metrics, gate)
    if not passed:
        log.info(
            "SIGNAL-SKIP %s (%s) — %s (gain=%.2f liq=$%.0f h=%d w=%d)",
            (token_meta.get("symbol") or "?")[:12],
            token[:8],
            reason,
            _to_float(sig_meta.get("max_price_gain")) or 0.0,
            _to_float(metrics.get("liquidity")) or 0.0,
            _to_int(metrics.get("holder_count")),
            len(ev.get("wallet_stats") or []),
        )
        return None
    if score < settings.min_score:
        log.info(
            "SIGNAL-SKIP %s — score %.1f < min %.1f",
            (token_meta.get("symbol") or "?")[:12], score, settings.min_score,
        )
        return None

    # A signal candidate IS a real token that already launched: create a
    # launch-shaped object so the rest of the pipeline is oblivious. created_at
    # = the SIGNAL time (that is when the edge is fresh; queue-aging applies).
    # raw carries no "price" on purpose: debot prices are USD-normalized, and
    # the pump band (max_price_gain) above is the anti-chase gate — the bot's
    # entry-mult gate fails open for signals.
    decimals = _to_int(token_meta.get("decimals")) or 6
    supply = _to_float(token_meta.get("total_supply")) or 0.0
    raw = {
        "decimals": decimals,
        "supply": supply,
        "signal_count": _to_int(sig_meta.get("signal_count")),
        "max_price_gain": _to_float(sig_meta.get("max_price_gain")) or 0.0,
        "first_price": _to_float(sig_meta.get("first_price")) or 0.0,
        "n_wallets": len(ev.get("wallet_stats") or []),
        "top10": _to_float(metrics.get("top10_position")) or 0.0,
        "holders": _to_int(metrics.get("holder_count")),
        "liq_usd": _to_float(metrics.get("liquidity")) or 0.0,
        "mcap_usd": _to_float(metrics.get("market_cap")) or 0.0,
        "pair": metrics.get("pair") or "",
        "dex": metrics.get("dex_name") or "",
        "tier": sig_meta.get("token_level") or "",
    }
    launch = TokenLaunch(
        mint=token,
        name=(token_meta.get("name") or ""),
        symbol=(token_meta.get("symbol") or "") or token[:6],
        uri=(token_meta.get("logo") or ""),
        creator=(token_meta.get("creator_address") or ""),
        signature="",  # signals aren't tied to one create tx
        initial_buy_tokens=0.0,  # unknown for post-launch signals
        dev_sol=None,  # unknown — dev-rep veto fails open (no wallet)
        market_cap_sol=None,  # unknown; mcap_usd in raw instead
        quote_mint=SOL_MINT,
        is_mayhem_mode=False,
        is_cashback_enabled=False,
        source="debot_signal",
        created_at=_to_float(ev.get("create_time")) or 0.0,
        raw=raw,
    )
    candidate = Candidate(launch=launch, pair=None, score=score,
                          scanned_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    append_journal({
        "type": "signal",
        "mint": token,
        "symbol": launch.symbol,
        "name": launch.name[:30],
        "score": score,
        "gain": raw["max_price_gain"],
        "wallets": raw["n_wallets"],
        "liq_usd": raw["liq_usd"],
        "mcap_usd": raw["mcap_usd"],
        "top10": raw["top10"],
        "holders": raw["holders"],
        "tier": raw["tier"],
        "pair": raw["pair"],
    })
    return candidate


class SignalScanner:
    """Polls debot /channel/list (newest first) and emits Candidates."""

    def __init__(self, gate: SignalGate | None = None):
        """Create the scanner with a gate, dedup deque and httpx client."""
        self.gate = gate or SignalGate()
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT_S))
        # bounded dedup: event ids we've already processed
        self._seen: deque = deque(maxlen=500)
        self._seen_set: set = set()
        # only react to events newer than this (skip the historical backlog)
        self._min_create_time: float | None = None

    async def fetch_page(self) -> dict:
        """One page of newest signal events."""
        params = {
            "request_id": str(uuid.uuid4()),
            "chain": "solana",
            "page_size": PAGE_SIZE,
        }
        r = await self._client.get(DEBOT_LIST, params=params)
        r.raise_for_status()
        return r.json()

    def _is_new(self, event_id: str) -> bool:
        """True when this event id hasn't been processed recently."""
        if event_id in self._seen_set:
            return False
        if len(self._seen) >= self._seen.maxlen:
            old = self._seen.popleft()
            self._seen_set.discard(old)
        self._seen.append(event_id)
        self._seen_set.add(event_id)
        return True

    async def poll_once(self, now: float) -> int:
        """Fetch + process one page. Returns the number of candidates queued."""
        data = await self.fetch_page()
        page = data.get("data") or {}
        meta = page.get("meta") or {}
        results = page.get("results") or []
        queued = 0
        for ev in results:
            event_id = ev.get("id")
            if not event_id or not self._is_new(event_id):
                continue
            create_time = _to_float(ev.get("create_time")) or 0.0
            if self._min_create_time is not None and create_time < self._min_create_time:
                continue
            if ev.get("chain") and ev.get("chain") != "solana":
                continue
            try:
                candidate = build_candidate(ev, meta, self.gate, now)
                if candidate is not None:
                    queued += 1
            except Exception:
                log.exception("Error building signal candidate for %s", ev.get("token"))
        if self._min_create_time is None and results:
            # after the first page, only accept events fresher than the newest
            # create_time we just saw — the backlog is handled only on boot by
            # max_age_sec inside gate_signal.
            newest = max((_to_float(e.get("create_time")) or 0.0) for e in results)
            self._min_create_time = newest
        return queued

    async def close(self) -> None:
        """Close the underlying httpx client."""
        await self._client.aclose()


async def signal_scan_loop(
    queue: asyncio.Queue[Candidate],
    gate: TradeGate | None = None,
) -> None:
    """Poll debot signals forever; pause when the trade gate is closed."""
    scanner = SignalScanner()
    try:
        while True:
            if gate is not None:
                await gate.wait()
                if gate.shutdown:
                    log.info("Gate shutdown — signal scanner exiting")
                    break
            now = time.time()
            try:
                queued = await scanner.poll_once(now)
                if queued:
                    log.info("Signal scan: %d new candidates queued", queued)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Signal poll failed")
            await asyncio.sleep(settings.signal_poll_sec)
    finally:
        await scanner.close()
        log.info("Signal scanner stopped")


if __name__ == "__main__":
    from monitoring import setup_logging

    setup_logging()
    settings.validate()
    q: asyncio.Queue[Candidate] = asyncio.Queue()
    asyncio.run(signal_scan_loop(q))
