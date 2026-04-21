"""
paper_sim.py — Paper trade simulation for bonding mode.

Runs the full scanner → scorer pipeline in read-only mode. Logs every
hypothetical order to a JSONL file for win-rate analysis after markets resolve.
No orders are placed and no CLOB credentials are required.

Run standalone:
    cd bot/
    python -m bonding.paper_sim

Or with a custom output path:
    PAPER_LOG=/tmp/paper.jsonl python -m bonding.paper_sim
"""
import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from bonding.market_scanner import scan_weather_markets
from bonding.opportunity_scorer import score_all, ScoredOpportunity, TIER_CHEAP, TIER_CORE
from bonding.weather_client import get_consensus_forecasts
import config as _config
from config import LOG_LEVEL

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bond.paper")

PAPER_LOG = Path(os.getenv("PAPER_LOG", "/app/logs/paper_trades.jsonl"))


def _append_record(record: dict) -> bool:
    """Append a record to the JSONL log. Returns True on success, False on failure."""
    try:
        PAPER_LOG.parent.mkdir(parents=True, exist_ok=True)
        with PAPER_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
        return True
    except Exception as exc:
        log.error(f"paper_sim: failed to append record to JSONL ({exc}); record={record.get('event')} market={record.get('market_id', '')[:8]}")
        return False


def _load_seen_market_ids() -> tuple[set[str], set[str]]:
    """
    Return (blocked_ids, sold_ids) based on WOULD_BUY records in the JSONL.

    blocked_ids: market_ids that must NOT be re-entered — outcome=None (still live)
                 or outcome='YES'/'NO' (market resolved, scanner may still see it).
    sold_ids:    market_ids the bot exited early (outcome='SOLD') — eligible for re-entry.

    The JSONL is the authoritative source of truth for sell events, because
    _patch_would_buy updates it synchronously on every WOULD_SELL.
    """
    if not PAPER_LOG.exists():
        log.info("_load_seen_market_ids: JSONL not found, starting fresh")
        return set(), set()
    # Track the latest WOULD_BUY outcome per market_id (later lines overwrite earlier).
    latest_outcome: dict[str, str | None] = {}
    try:
        for line in PAPER_LOG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if rec.get("event") == "WOULD_BUY":
                mid = rec.get("market_id")
                if mid:
                    latest_outcome[mid] = rec.get("outcome")  # None = open
    except Exception as exc:
        log.error(f"_load_seen_market_ids: failed to read JSONL ({exc}), starting fresh")
        return set(), set()
    blocked = {mid for mid, outcome in latest_outcome.items() if outcome != "SOLD"}
    sold = {mid for mid, outcome in latest_outcome.items() if outcome == "SOLD"}
    log.info(
        f"_load_seen_market_ids: {len(latest_outcome)} WOULD_BUY records → "
        f"{len(blocked)} blocked (open/resolved), {len(sold)} SOLD (eligible for re-entry)"
    )
    return blocked, sold


@dataclass
class PaperPosition:
    market_id: str
    token_id: str
    question: str
    city: str
    date: str
    resolution_time: str   # ISO8601
    tier: str
    shares: int
    side: str              # "YES" or "NO"
    entry_price: float
    entry_ts: str          # ISO8601
    status: str = "OPEN"  # OPEN | SOLD
    exit_price: Optional[float] = None
    exit_ts: Optional[str] = None
    pnl: Optional[float] = None
    prob: float = 0.0                    # P(this token resolves $1) at entry time


class PaperExitManager:
    """
    Tracks open paper positions and logs WOULD_SELL events when exit criteria
    are met. Receives price updates via on_price_tick(), called by BondPriceFeed
    on every WS price event — no polling needed.

    Positions are persisted to a JSON ledger alongside the paper trades JSONL so
    they survive restarts.
    """

    def __init__(
        self,
        paper_log: Path,
        seen_ids: set[str] | None = None,
        sold_market_ids: set[str] | None = None,
    ) -> None:
        self._paper_log = paper_log
        self._ledger = paper_log.parent / "paper_positions.json"
        self._positions: dict[str, PaperPosition] = {}  # token_id → position (all statuses)
        self._seen_ids = seen_ids  # shared reference; cleared on sell to allow re-entry
        self._sold_market_ids = sold_market_ids or set()
        self._stop_loss_strikes: dict[str, int] = {}  # token_id → consecutive hit count
        self._load()

    def _load(self) -> None:
        if not self._ledger.exists():
            return
        try:
            data = json.loads(self._ledger.read_text(encoding="utf-8"))
            for d in data.get("positions", []):
                pos = PaperPosition(**d)
                self._positions[pos.token_id] = pos

            # Reconcile: the JSONL is authoritative for sell events because _patch_would_buy
            # updates it synchronously on every WOULD_SELL. If the JSONL says a market was
            # SOLD but the ledger still shows OPEN (e.g. _save() failed mid-flight), trust
            # the JSONL and mark the ledger position as SOLD so re-entry remains allowed.
            reconciled = 0
            for pos in self._positions.values():
                if pos.status == "OPEN" and pos.market_id in self._sold_market_ids:
                    pos.status = "SOLD"
                    reconciled += 1
            if reconciled:
                log.info(
                    f"paper_exit: reconciled {reconciled} positions — "
                    f"JSONL says SOLD but ledger was stale; re-saving ledger"
                )
                self._save()

            open_count = sum(1 for p in self._positions.values() if p.status == "OPEN")

            # Belt-and-suspenders: sync any open positions the JSONL doesn't know about
            # (e.g. JSONL write failed but ledger write succeeded) into seen_ids so they
            # stay blocked. Markets the JSONL already marks as SOLD are excluded — we
            # never re-block them, preserving their eligible-for-re-entry status.
            if self._seen_ids is not None:
                open_market_ids = {p.market_id for p in self._positions.values() if p.status == "OPEN"}
                # Only add markets the JSONL doesn't already track (truly missing entries).
                # Excludes both blocked (already in seen_ids) and SOLD (in sold_market_ids).
                truly_missing = open_market_ids - self._seen_ids - self._sold_market_ids
                if truly_missing:
                    self._seen_ids.update(truly_missing)
                    log.info(f"paper_exit: synced {len(truly_missing)} open market IDs from ledger into seen_ids (JSONL gaps)")
            log.info(f"paper_exit: loaded {open_count} open / {len(self._positions)} total positions")
        except Exception as exc:
            log.error(f"paper_exit: failed to load ledger: {exc}")

    def _save(self) -> None:
        try:
            self._ledger.parent.mkdir(parents=True, exist_ok=True)
            tmp = Path(str(self._ledger) + ".tmp")
            payload = json.dumps(
                {"positions": [asdict(p) for p in self._positions.values()]}, indent=2
            )
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(self._ledger)
        except Exception as exc:
            log.error(f"paper_exit: failed to save ledger: {exc}")

    def has_open_position(self, token_id: str) -> bool:
        """Return True if there is an OPEN paper position for this token."""
        pos = self._positions.get(token_id)
        return pos is not None and pos.status == "OPEN"

    def add_position(self, opp: ScoredOpportunity) -> None:
        """Record a new open paper position after a WOULD_BUY is logged."""
        existing = self._positions.get(opp.token_id)
        if existing is not None and existing.status == "OPEN":
            return  # already tracking an open position for this token
        pos = PaperPosition(
            market_id=opp.market.market_id,
            token_id=opp.token_id,
            question=opp.market.question,
            city=opp.market.city,
            date=opp.market.target_date.isoformat(),
            resolution_time=opp.market.resolution_time.isoformat(),
            tier=opp.tier,
            shares=opp.shares_immediate,  # only what FOK would fill
            side=opp.outcome,
            entry_price=opp.side_ask,
            entry_ts=datetime.now(timezone.utc).isoformat(),
            prob=round(opp.prob, 4),
        )
        self._positions[pos.token_id] = pos
        self._save()
        log.info(
            f"paper_exit: tracking {pos.city} {pos.side} tier={pos.tier} "
            f"entry={pos.entry_price:.4f} shares={pos.shares}"
        )

    @property
    def seen_ids(self) -> set[str] | None:
        """Shared reference to the seen market IDs set (mutated in-place by callers)."""
        return self._seen_ids

    async def run(self) -> None:
        """Persistent polling task — checks exits every 60s via HTTP price fetches.

        Runs independently of the WS price feed so exit criteria are evaluated
        even when the main scan loop is stopped.
        """
        CLOB_API = "https://clob.polymarket.com"
        log.info("PaperExitManager polling started")
        while True:
            try:
                await self._poll_exits(CLOB_API)
            except asyncio.CancelledError:
                log.info("PaperExitManager polling stopped")
                return
            except Exception as exc:
                log.error(f"paper_exit: polling error: {exc}", exc_info=True)
            await asyncio.sleep(60)

    async def _poll_exits(self, clob_api: str) -> None:
        """Fetch current bid prices for all open positions and apply exit rules.

        Uses bids[0]["price"] (best bid = highest price a buyer will pay) as the
        reference price, which is what you'd actually receive selling immediately.
        This prevents false exits from thin-liquidity asks: on an unresolved market
        where cheap asks were exhausted, asks[0] can show 0.99 while bids remain
        low, correctly blocking a premature profit exit.
        """
        import aiohttp
        open_positions = [p for p in self._positions.values() if p.status == "OPEN"]
        if not open_positions:
            return
        timeout = aiohttp.ClientTimeout(total=5)
        for pos in open_positions:
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(
                        f"{clob_api}/book", params={"token_id": pos.token_id}
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        # Prefer best bid; fall back to best ask only when no bids exist
                        # (e.g. fully-resolved market where all buyers have been filled).
                        if bids:
                            price = float(bids[0]["price"])
                        elif asks:
                            price = float(asks[0]["price"])
                        else:
                            continue
                        # Stop-loss check uses full bid book for depth validation.
                        hours = self._hours_left(pos)
                        if self._check_stop_loss(pos, bids, hours):
                            continue
                        await self.on_price_tick(pos.token_id, price)
            except Exception as exc:
                log.debug(f"paper_exit: price poll failed for {pos.token_id[:12]}: {exc}")

    async def on_price_tick(self, token_id: str, price: float) -> None:
        """Called by BondPriceFeed on every WS price event for a tracked token."""
        pos = self._positions.get(token_id)
        if pos is None or pos.status != "OPEN":
            return
        hours = self._hours_left(pos)
        if self._should_exit(pos, price, hours):
            self._record_sell(pos, price)

    def _has_resolved(self, pos: PaperPosition) -> bool:
        """Return True if the market's resolution day has fully elapsed (end-of-day UTC).

        resolution_time stores Gamma's end_date_iso — midnight *start* of the resolution
        day (e.g. "2026-04-16T00:00:00+00:00"), NOT end-of-day.  Checking
        ``now >= resolution_time`` fires all day on the resolution date, long before the
        temperature data is published.  We must use the same end-of-day arithmetic as
        _hours_left: the market is settled only after midnight UTC of the *next* day.
        """
        return self._hours_left(pos) <= 0.0

    def _hours_left(self, pos: PaperPosition) -> float:
        # resolution_time stores Gamma's end_date_iso — midnight start-of-day UTC, not
        # actual resolution. Extract the date portion and compute end-of-day instead.
        try:
            date_str = pos.resolution_time[:10]  # "2026-04-08"
            end_of_day = datetime.fromisoformat(
                date_str + "T00:00:00+00:00"
            ) + timedelta(days=1)
            return max(0.0, (end_of_day - datetime.now(timezone.utc)).total_seconds() / 3600)
        except Exception:
            return 999.0

    def _should_exit(self, pos: PaperPosition, price: float, hours: float) -> bool:
        if price <= 0.0:
            return False
        # Rule 1: too close to resolution — gas not worth it (same discipline as live)
        if hours < _config.BOND_GAS_FLOOR_HOURS:
            return False
        # Universal near-certainty guard (all tiers): if the price is at or above
        # the exit threshold, only exit once the market has actually resolved.
        # This blocks Rules 2, 3, and 4 from triggering on inflated bid prices
        # caused by liquidity exhaustion — the root cause of all fake profit exits.
        if price >= _config.BOND_EARLY_EXIT_PRICE:
            return self._has_resolved(pos)
        # Rule 3: 10× on any tier (price < BOND_EARLY_EXIT_PRICE here)
        if price >= pos.entry_price * 10:
            return True
        # Rule 4: CHEAP multiplier + absolute gain threshold
        if pos.tier == TIER_CHEAP:
            gain = (price - pos.entry_price) * pos.shares
            if (
                price >= pos.entry_price * _config.BOND_CHEAP_EXIT_MULTIPLIER
                and gain >= _config.BOND_CHEAP_MIN_ABS_GAIN
            ):
                return True
        return False

    def refresh_seen_ids(self) -> int:
        """
        Rebuild seen_ids to only contain market_ids of currently OPEN positions.

        Stale entries — resolved markets, already-sold positions, or markets
        that have simply left Polymarket — are cleared so the next scan cycle
        can re-evaluate them. OPEN positions remain blocked (no double-entry).

        Returns the number of market_ids removed.
        """
        if self._seen_ids is None:
            return 0
        open_market_ids = {p.market_id for p in self._positions.values() if p.status == "OPEN"}
        before = len(self._seen_ids)
        self._seen_ids.clear()
        self._seen_ids.update(open_market_ids)
        removed = before - len(self._seen_ids)
        if removed:
            log.info(
                f"paper_exit: seen_ids refreshed — {removed} stale entries removed, "
                f"{len(open_market_ids)} open position(s) retained"
            )
        return removed

    def _check_stop_loss(self, pos: PaperPosition, bids: list, hours: float) -> bool:
        """
        Check stop-loss using bid-side order book depth. Returns True if triggered.

        Guards against false triggers from thin/manipulative bids:
        - Requires fillable depth at or above stop_price >= BOND_STOP_LOSS_MIN_FILL_FRACTION of shares
        - Requires condition true in BOND_STOP_LOSS_CONFIRM_POLLS consecutive polls
        - Only fires when >BOND_STOP_LOSS_HOURS remain (position can still be saved)
        """
        if _config.BOND_STOP_LOSS_RATIO <= 0 or hours <= _config.BOND_STOP_LOSS_HOURS:
            self._stop_loss_strikes.pop(pos.token_id, None)
            return False
        stop_price = pos.entry_price * _config.BOND_STOP_LOSS_RATIO
        if not bids:
            self._stop_loss_strikes.pop(pos.token_id, None)
            return False
        best_bid = float(bids[0]["price"])
        fillable = sum(
            float(b.get("size", 0))
            for b in bids
            if float(b["price"]) >= stop_price
        )
        min_fill = pos.shares * _config.BOND_STOP_LOSS_MIN_FILL_FRACTION
        if best_bid > 0 and best_bid <= stop_price and fillable >= min_fill:
            strikes = self._stop_loss_strikes.get(pos.token_id, 0) + 1
            self._stop_loss_strikes[pos.token_id] = strikes
            if strikes >= _config.BOND_STOP_LOSS_CONFIRM_POLLS:
                self._stop_loss_strikes.pop(pos.token_id, None)
                self._record_sell(pos, best_bid, reason="STOP_LOSS")
                return True
            log.debug(
                f"paper_exit: stop-loss strike [{strikes}/{_config.BOND_STOP_LOSS_CONFIRM_POLLS}] "
                f"{pos.city} bid={best_bid:.4f} stop={stop_price:.4f} "
                f"fillable={fillable:.0f}/{pos.shares}"
            )
        else:
            self._stop_loss_strikes.pop(pos.token_id, None)
        return False

    def _record_sell(self, pos: PaperPosition, exit_price: float, reason: str = "PROFIT_EXIT") -> None:
        pnl = (exit_price - pos.entry_price) * pos.shares
        pos.status = "SOLD"
        pos.exit_price = round(exit_price, 4)
        pos.exit_ts = datetime.now(timezone.utc).isoformat()
        pos.pnl = round(pnl, 4)
        # Patch the original WOULD_BUY entry so the log is self-contained.
        self._patch_would_buy(pos)
        record = {
            "ts":          pos.exit_ts,
            "event":       "WOULD_SELL",
            "market_id":   pos.market_id,
            "question":    pos.question,
            "city":        pos.city,
            "date":        pos.date,
            "tier":        pos.tier,
            "shares":      pos.shares,
            "side":        pos.side,
            "entry_price": pos.entry_price,
            "exit_price":  pos.exit_price,
            "pnl":         pos.pnl,
            "reason":      reason,
        }
        _append_record(record)
        self._save()
        # Allow re-entry into this market if conditions become profitable again.
        if self._seen_ids is not None:
            self._seen_ids.discard(pos.market_id)
        log.info(
            f"WOULD_SELL [{reason}] city={pos.city} side={pos.side} tier={pos.tier} "
            f"entry={pos.entry_price:.4f} exit={exit_price:.4f} pnl={pnl:+.2f}"
        )

    def _patch_would_buy(self, pos: PaperPosition) -> None:
        """
        Rewrite the JSONL so the original WOULD_BUY entry for this market shows
        outcome='SOLD', exit_price, and pnl — making the log self-contained.
        """
        if not self._paper_log.exists():
            return
        try:
            lines = self._paper_log.read_text(encoding="utf-8").splitlines()
            patched = []
            for line in lines:
                if not line.strip():
                    patched.append(line)
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    patched.append(line)
                    continue
                if (
                    rec.get("event") == "WOULD_BUY"
                    and rec.get("market_id") == pos.market_id
                    and rec.get("outcome") is None
                ):
                    rec["outcome"]    = "SOLD"
                    rec["exit_price"] = pos.exit_price
                    rec["pnl"]        = pos.pnl
                    line = json.dumps(rec)
                patched.append(line)
            tmp = Path(str(self._paper_log) + ".tmp")
            tmp.write_text("\n".join(patched) + "\n", encoding="utf-8")
            tmp.replace(self._paper_log)
        except Exception as exc:
            log.error(f"paper_exit: failed to patch WOULD_BUY record: {exc}")


def _end_of_day_utc(city: str, target_date) -> str:
    """Return ISO8601 UTC for end of market day in the city's local timezone."""
    tz_name = _config.BOND_CITY_TIMEZONES.get(city)
    if tz_name:
        try:
            city_tz = ZoneInfo(tz_name)
            eod = datetime(target_date.year, target_date.month, target_date.day, 18, 0, 0, tzinfo=city_tz)
            return eod.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    # fallback: 18:00 UTC same day
    return datetime(target_date.year, target_date.month, target_date.day, 18, 0, 0, tzinfo=timezone.utc).isoformat()


def log_opportunity(opp, seen_ids: set[str]) -> bool:
    """
    Log a single scored opportunity to the JSONL file if not already seen.

    Uses depth-aware share counts: only logs shares_immediate (what a FOK order
    would actually fill given current ask book depth).

    When shares_immediate == 0 the opportunity is logged as a DEPTH_MISS event
    (not added to seen_ids so the market stays eligible for re-check each cycle)
    and False is returned — no position is tracked.

    When shares_immediate > 0 a WOULD_BUY is logged, the market_id is added to
    seen_ids, and True is returned so the caller tracks a paper position.

    Updates seen_ids in-place. Returns True if a WOULD_BUY was logged.
    Used by both the REST fallback pass and the WS price-feed callback.
    """
    if opp.market.market_id in seen_ids:
        return False

    source = "ws" if opp.market.ask_book else "rest"

    # No profitable depth available — log for stats but don't block re-entry.
    if opp.shares_immediate == 0:
        record = {
            "ts":          datetime.now(timezone.utc).isoformat(),
            "event":       "DEPTH_MISS",
            "market_id":   opp.market.market_id,
            "city":        opp.market.city,
            "date":        opp.market.target_date.isoformat(),
            "tier":        opp.tier,
            "shares_wanted": opp.shares,
            "side":        opp.outcome,
            "ask":         opp.side_ask,
            "source":      source,
        }
        _append_record(record)
        log.debug(
            f"DEPTH_MISS city={opp.market.city} date={opp.market.target_date} "
            f"side={opp.outcome} tier={opp.tier} ask={opp.side_ask:.4f}"
        )
        return False

    seen_ids.add(opp.market.market_id)
    fillable_capital = round(opp.shares_immediate * opp.side_ask, 4)
    record = {
        "ts":              datetime.now(timezone.utc).isoformat(),
        "event":           "WOULD_BUY",
        "market_id":       opp.market.market_id,
        "question":        opp.market.question,
        "city":            opp.market.city,
        "date":            opp.market.target_date.isoformat(),
        "resolution_time": _end_of_day_utc(opp.market.city, opp.market.target_date),
        "tier":            opp.tier,
        "shares":          opp.shares_immediate,  # depth-aware: only what FOK would fill
        "shares_wanted":   opp.shares,            # total target before depth cap
        "shares_limit":    opp.shares_limit,       # remainder that would queue as GTC
        "side":            opp.outcome,            # "YES" or "NO" — which token was bet
        "ask":             opp.side_ask,
        "prob":            round(opp.prob, 4),
        "ev":              round(opp.ev, 4),
        "edge":            round(opp.edge, 4),
        "capital":         fillable_capital,       # cost basis for shares_immediate only
        "outcome":         None,  # filled post-resolution by analysis script
        "pnl":             None,
        "source":          source,
    }
    _append_record(record)
    depth_note = f"depth={opp.shares_immediate}/{opp.shares}" if opp.shares_limit > 0 else f"shares={opp.shares_immediate}"
    log.info(
        f"WOULD_BUY [{source.upper()}] city={opp.market.city} "
        f"date={opp.market.target_date} side={opp.outcome} tier={opp.tier} {depth_note} "
        f"ask={opp.side_ask:.4f} ev={opp.ev:.4f} edge={opp.edge:.4f}"
    )
    return True


async def run_cycle() -> int:
    """
    Run one REST scan cycle. Returns number of new opportunities logged.
    Used by the standalone runner; the main loop uses BondPriceFeed directly.
    """
    markets = await scan_weather_markets()
    if not markets:
        log.info("paper_sim: no markets found this cycle")
        return 0

    city_date_pairs = list({(m.city, m.target_date) for m in markets})
    forecasts = await get_consensus_forecasts(city_date_pairs)

    opps = score_all(markets, forecasts)[:_config.BOND_MAX_MARKETS_PER_RUN]

    seen_ids, _ = _load_seen_market_ids()
    logged = sum(1 for opp in opps if log_opportunity(opp, seen_ids))
    skipped = len(opps) - logged
    if skipped:
        log.info(f"paper_sim: skipped {skipped} already-logged market(s)")
    if not logged:
        log.info("paper_sim: no new opportunities this cycle")
        return 0

    log.info(f"paper_sim: cycle complete — {logged} new opportunities logged to {PAPER_LOG}")
    return logged


async def run() -> None:
    log.info(f"Paper simulation started — logging to {PAPER_LOG}")
    log.info(f"Poll interval: {_config.BOND_POLL_INTERVAL_SECS}s | Max per cycle: {_config.BOND_MAX_MARKETS_PER_RUN}")
    cycle = 0
    while True:
        cycle += 1
        log.info(f"── Cycle {cycle} ──────────────────────────────────────────")
        try:
            await run_cycle()
        except Exception as exc:
            log.error(f"paper_sim: cycle {cycle} failed: {exc}", exc_info=True)
        await asyncio.sleep(_config.BOND_POLL_INTERVAL_SECS)


def analyse(log_path: str = str(PAPER_LOG)) -> None:
    """
    Print a summary of paper trade results grouped by tier.
    Run after markets have resolved and you have manually updated
    the 'outcome' / 'pnl' fields, or use as a starting point.

        python -c "from bonding.paper_sim import analyse; analyse()"
    """
    from collections import defaultdict

    all_records = [json.loads(l) for l in Path(log_path).read_text().splitlines() if l.strip()]

    buys: dict[str, list] = defaultdict(list)       # tier → WOULD_BUY records
    misses: dict[str, list] = defaultdict(list)      # tier → DEPTH_MISS records
    for r in all_records:
        tier = r.get("tier", "UNKNOWN")
        if r.get("event") == "WOULD_BUY":
            buys[tier].append(r)
        elif r.get("event") == "DEPTH_MISS":
            misses[tier].append(r)

    total_buys = sum(len(v) for v in buys.values())
    print(f"\nPaper simulation summary — {total_buys} simulated bets")
    print(f"Log file: {log_path}\n")

    # ── YES / NO bets by tier ─────────────────────────────────────
    for tier in ("CHEAP", "CORE"):
        recs = buys.get(tier, [])
        if not recs:
            continue
        total_cap = sum(r["capital"] for r in recs)
        avg_ev    = sum(r["ev"] for r in recs) / len(recs)
        resolved  = [r for r in recs if r.get("outcome") is not None]
        wins      = [r for r in resolved if r.get("outcome") == "YES"]
        win_rate  = len(wins) / len(resolved) if resolved else None
        print(
            f"  {tier:10s}  n={len(recs):4d}  capital=${total_cap:7.2f}  "
            f"avg_ev={avg_ev:.4f}  "
            + (f"win_rate={win_rate:.1%} ({len(wins)}/{len(resolved)})" if win_rate is not None else "no outcomes yet")
        )

    # ── Order book depth stats ────────────────────────────────────
    print(f"\n  {'─' * 70}")
    print(f"  Order book depth at profitable prices\n")
    print(
        f"  {'Tier':<12}  {'Fillable':>8}  {'No depth':>8}  "
        f"{'Fill rate':>9}  {'Avg depth':>12}"
    )
    print(f"  {'─' * 12}  {'─' * 8}  {'─' * 8}  {'─' * 9}  {'─' * 12}")
    for tier in ("CHEAP", "CORE"):
        buy_recs  = buys.get(tier, [])
        miss_recs = misses.get(tier, [])

        # Deduplicate by market_id: count unique markets that were fillable vs
        # markets that only ever appeared as DEPTH_MISS.
        fillable_ids  = {r["market_id"] for r in buy_recs}
        miss_only_ids = {r["market_id"] for r in miss_recs} - fillable_ids
        n_fillable    = len(fillable_ids)
        n_miss        = len(miss_only_ids)
        n_total       = n_fillable + n_miss

        if n_total == 0:
            continue

        fill_rate = n_fillable / n_total
        if buy_recs:
            avg_shares    = sum(r["shares"] for r in buy_recs) / len(buy_recs)
            avg_wanted    = sum(r.get("shares_wanted", r["shares"]) for r in buy_recs) / len(buy_recs)
            depth_str     = f"{avg_shares:.1f} / {avg_wanted:.0f}"
        else:
            depth_str = "—"

        print(
            f"  {tier:<12}  {n_fillable:>8d}  {n_miss:>8d}  "
            f"{fill_rate:>8.1%}  {depth_str:>12}"
        )
    print()


if __name__ == "__main__":
    asyncio.run(run())
