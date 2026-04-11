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


def _append_record(record: dict) -> None:
    PAPER_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PAPER_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def _load_seen_market_ids() -> set[str]:
    """
    Return set of market_ids that are blocked from re-entry on startup.

    Blocked: outcome=None (position still live), outcome='YES'/'NO' (market resolved —
    Polymarket may not have fully closed it yet, so the scanner still returns it).
    Eligible for re-entry: outcome='SOLD' only (early exit while market is still active).
    """
    if not PAPER_LOG.exists():
        return set()
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
    except Exception:
        pass
    # Block open positions (None) and resolved markets (YES/NO) — only SOLD allows re-entry.
    return {mid for mid, outcome in latest_outcome.items() if outcome != "SOLD"}


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


class PaperExitManager:
    """
    Tracks open paper positions and logs WOULD_SELL events when exit criteria
    are met. Receives price updates via on_price_tick(), called by BondPriceFeed
    on every WS price event — no polling needed.

    Positions are persisted to a JSON ledger alongside the paper trades JSONL so
    they survive restarts.
    """

    def __init__(self, paper_log: Path, seen_ids: set[str] | None = None) -> None:
        self._paper_log = paper_log
        self._ledger = paper_log.parent / "paper_positions.json"
        self._positions: dict[str, PaperPosition] = {}  # token_id → position (all statuses)
        self._seen_ids = seen_ids  # shared reference; cleared on sell to allow re-entry
        self._load()

    def _load(self) -> None:
        if not self._ledger.exists():
            return
        try:
            data = json.loads(self._ledger.read_text(encoding="utf-8"))
            for d in data.get("positions", []):
                pos = PaperPosition(**d)
                self._positions[pos.token_id] = pos
            open_count = sum(1 for p in self._positions.values() if p.status == "OPEN")
            log.info(f"paper_exit: loaded {open_count} open / {len(self._positions)} total positions")
        except Exception as exc:
            log.error(f"paper_exit: failed to load ledger: {exc}")

    def _save(self) -> None:
        self._ledger.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(self._ledger) + ".tmp")
        payload = json.dumps(
            {"positions": [asdict(p) for p in self._positions.values()]}, indent=2
        )
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(self._ledger)

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
        )
        self._positions[pos.token_id] = pos
        self._save()
        log.info(
            f"paper_exit: tracking {pos.city} {pos.side} tier={pos.tier} "
            f"entry={pos.entry_price:.4f} shares={pos.shares}"
        )

    async def on_price_tick(self, token_id: str, price: float) -> None:
        """Called by BondPriceFeed on every WS price event for a tracked token."""
        pos = self._positions.get(token_id)
        if pos is None or pos.status != "OPEN":
            return
        hours = self._hours_left(pos)
        if self._should_exit(pos, price, hours):
            self._record_sell(pos, price)

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
        # Rule 2: CORE near-certainty exit
        if pos.tier == TIER_CORE and price >= _config.BOND_EARLY_EXIT_PRICE:
            return True
        # Rule 3: 10× on any tier
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

    def _record_sell(self, pos: PaperPosition, exit_price: float) -> None:
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
        }
        _append_record(record)
        self._save()
        # Allow re-entry into this market if conditions become profitable again.
        if self._seen_ids is not None:
            self._seen_ids.discard(pos.market_id)
        log.info(
            f"WOULD_SELL city={pos.city} side={pos.side} tier={pos.tier} "
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

    seen_ids = _load_seen_market_ids()
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
