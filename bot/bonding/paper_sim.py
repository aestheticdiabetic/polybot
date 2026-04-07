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
from datetime import datetime, timezone
from pathlib import Path

from bonding.market_scanner import scan_weather_markets
from bonding.opportunity_scorer import score_all
from bonding.weather_client import get_all_forecasts
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
    """Return set of market_ids already recorded in the paper log."""
    if not PAPER_LOG.exists():
        return set()
    seen: set[str] = set()
    try:
        for line in PAPER_LOG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    rec = json.loads(line)
                    mid = rec.get("market_id")
                    if mid:
                        seen.add(mid)
                except Exception:
                    pass
    except Exception:
        pass
    return seen


async def run_cycle() -> int:
    """Run one scan cycle. Returns number of new opportunities logged."""
    markets = await scan_weather_markets()
    if not markets:
        log.info("paper_sim: no markets found this cycle")
        return 0

    city_date_pairs = list({(m.city, m.target_date) for m in markets})
    forecasts = await get_all_forecasts(city_date_pairs)

    opps = score_all(markets, forecasts)[:_config.BOND_MAX_MARKETS_PER_RUN]

    # De-duplicate: skip markets already paper-traded this run
    seen_ids = _load_seen_market_ids()
    new_opps = [o for o in opps if o.market.market_id not in seen_ids]
    skipped = len(opps) - len(new_opps)
    if skipped:
        log.info(f"paper_sim: skipped {skipped} already-logged market(s)")
    if not new_opps:
        log.info("paper_sim: no new opportunities this cycle")
        return 0

    ts = datetime.now(timezone.utc).isoformat()
    for opp in new_opps:
        record = {
            "ts":              ts,
            "event":           "WOULD_BUY",
            "market_id":       opp.market.market_id,
            "question":        opp.market.question,
            "city":            opp.market.city,
            "date":            opp.market.target_date.isoformat(),
            "resolution_time": opp.market.resolution_time.isoformat(),
            "tier":            opp.tier,
            "shares":          opp.shares,
            "ask":             opp.market.best_ask,
            "prob":            round(opp.prob, 4),
            "ev":              round(opp.ev, 4),
            "edge":            round(opp.edge, 4),
            "capital":         round(opp.capital, 4),
            "outcome":         None,  # filled in post-resolution by analysis script
            "pnl":             None,
        }
        _append_record(record)
        log.info(
            f"WOULD_BUY city={opp.market.city} date={opp.market.target_date} "
            f"tier={opp.tier} shares={opp.shares} ask={opp.market.best_ask:.4f} "
            f"ev={opp.ev:.4f} edge={opp.edge:.4f}"
        )

    log.info(f"paper_sim: cycle complete — {len(new_opps)} new opportunities logged to {PAPER_LOG}")
    return len(new_opps)


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
    records = [json.loads(l) for l in Path(log_path).read_text().splitlines() if l.strip()]
    by_tier: dict[str, list] = defaultdict(list)
    for r in records:
        by_tier[r["tier"]].append(r)

    print(f"\nPaper simulation summary — {len(records)} total bets")
    print(f"Log file: {log_path}\n")
    for tier in ("CORE", "SECONDARY", "WING"):
        recs = by_tier.get(tier, [])
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


if __name__ == "__main__":
    asyncio.run(run())
