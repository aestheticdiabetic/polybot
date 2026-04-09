"""
sure_thing_scorer.py — CERTAIN tier: high-confidence, high-ask bets.

Produces ScoredOpportunity(tier="CERTAIN") when all three weather sources
tightly agree that a YES outcome is very likely AND the market still offers
≥5% edge. Conservative sizing (20 shares) during the validation phase.

Called from the REST scan loop alongside score_all(). Uses the same
SourceConsensus dict produced by get_consensus_forecasts().
"""
import logging
import statistics
from typing import Optional

import config as _config
from bonding.market_scanner import MarketCandidate
from bonding.opportunity_scorer import ScoredOpportunity, passes_time_gate
from bonding.weather_client import SourceConsensus, prob_in_range, fahrenheit_to_celsius

log = logging.getLogger("bond.certain")

TIER_CERTAIN = "CERTAIN"

# Module-level alias so tests can patch it cleanly
_passes_time_gate = passes_time_gate


def _has_open_position(market_id: str) -> bool:
    """
    Returns True if an OPEN position already exists for this market.
    Reads the bonding positions ledger to prevent duplicate entries.
    """
    import json
    from pathlib import Path
    ledger_path = Path(_config.BOND_LEDGER_FILE)
    if not ledger_path.exists():
        return False
    try:
        positions = json.loads(ledger_path.read_text())
        return any(
            p.get("market_id") == market_id and p.get("status") == "OPEN"
            for p in positions
        )
    except Exception:
        return False


def _convert_temps(market: MarketCandidate) -> tuple[Optional[float], Optional[float]]:
    temp_min = market.temp_min
    temp_max = market.temp_max
    if market.unit == "F":
        if temp_min is not None:
            temp_min = fahrenheit_to_celsius(temp_min)
        if temp_max is not None:
            temp_max = fahrenheit_to_celsius(temp_max)
    return temp_min, temp_max


def _score_one(
    market: MarketCandidate,
    consensus: SourceConsensus,
) -> Optional[ScoredOpportunity]:
    """
    Apply all CERTAIN gates to one market/consensus pair.
    Returns ScoredOpportunity or None if any gate fails.
    """
    ask = market.best_ask

    # Gate 1: ask range
    if not (_config.CERTAIN_ASK_MIN <= ask <= _config.CERTAIN_ASK_MAX):
        return None

    # Gate 2: all three sources must be present
    if consensus.available_sources() < _config.CERTAIN_MIN_SOURCES:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"only {consensus.available_sources()} sources (need {_config.CERTAIN_MIN_SOURCES})"
        )
        return None

    # Gate 3: no open position for this market
    if _has_open_position(market.market_id):
        return None

    # Gate 4: time gate (same logic as standard scorer)
    if not _passes_time_gate(market, consensus.gfs.forecast_peak_hour):
        return None

    temp_min, temp_max = _convert_temps(market)
    if temp_min is None or temp_max is None:
        return None

    # Gate 5: each source must individually reach min probability
    gfs_prob   = prob_in_range(consensus.gfs, temp_min, temp_max)
    ecmwf_prob = prob_in_range(consensus.ecmwf, temp_min, temp_max)
    tio_prob   = prob_in_range(consensus.tomorrowio, temp_min, temp_max)

    for source_name, source_prob in [("gfs", gfs_prob), ("ecmwf", ecmwf_prob), ("tio", tio_prob)]:
        if source_prob < _config.CERTAIN_MIN_SOURCE_PROB:
            log.debug(
                f"certain: {market.city} {market.target_date} — "
                f"{source_name} prob {source_prob:.3f} < {_config.CERTAIN_MIN_SOURCE_PROB}"
            )
            return None

    # Gate 6: inter-source point-forecast delta
    point_forecasts = consensus.point_forecasts()
    temp_delta = max(point_forecasts) - min(point_forecasts)
    if temp_delta > _config.CERTAIN_MAX_TEMP_DELTA_C:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"temp delta {temp_delta:.2f}°C > {_config.CERTAIN_MAX_TEMP_DELTA_C}"
        )
        return None

    # Gate 7: combined ensemble spread
    all_members = consensus.all_ensemble_members()
    if len(all_members) >= 2:
        spread = statistics.stdev(all_members)
        if spread > _config.CERTAIN_MAX_SPREAD_C:
            log.debug(
                f"certain: {market.city} {market.target_date} — "
                f"spread {spread:.2f}°C > {_config.CERTAIN_MAX_SPREAD_C}"
            )
            return None

    # Gate 8: consensus probability floor
    consensus_prob = consensus.consensus_prob(temp_min, temp_max)
    if consensus_prob < _config.CERTAIN_MIN_CONSENSUS_PROB:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"consensus_prob {consensus_prob:.3f} < {_config.CERTAIN_MIN_CONSENSUS_PROB}"
        )
        return None

    # Gate 9: minimum edge
    edge = consensus_prob - ask
    if edge < _config.CERTAIN_MIN_EDGE:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"edge {edge:.3f} < {_config.CERTAIN_MIN_EDGE}"
        )
        return None

    log.info(
        f"certain: OPPORTUNITY {market.city} {market.target_date} "
        f"ask={ask:.3f} consensus_prob={consensus_prob:.3f} edge={edge:.3f} "
        f"gfs={gfs_prob:.3f} ecmwf={ecmwf_prob:.3f} tio={tio_prob:.3f} "
        f"delta={temp_delta:.2f}°C"
    )

    return ScoredOpportunity(
        market=market,
        forecast=consensus.gfs,
        prob=consensus_prob,
        ev=edge,
        edge=edge,
        tier=TIER_CERTAIN,
        shares=_config.CERTAIN_SHARES,
        capital=_config.CERTAIN_SHARES * ask,
        shares_immediate=_config.CERTAIN_SHARES,
        shares_limit=0,
        limit_price=ask,
        outcome="YES",
        token_id=market.token_id,
        side_ask=ask,
    )


def score_certain(
    markets: list[MarketCandidate],
    forecasts: dict[tuple, SourceConsensus],
) -> list[ScoredOpportunity]:
    """
    Score all markets for CERTAIN tier opportunities.
    Returns list sorted by edge descending, with per-cluster capital cap applied.
    """
    results: list[ScoredOpportunity] = []

    for market in markets:
        consensus = forecasts.get((market.city, market.target_date))
        if consensus is None:
            continue
        opp = _score_one(market, consensus)
        if opp is not None:
            results.append(opp)

    if not results:
        return []

    results.sort(key=lambda o: o.edge, reverse=True)
    results = _apply_cluster_cap(results)

    log.info(f"certain: {len(markets)} markets → {len(results)} CERTAIN opportunities")
    return results


def _apply_cluster_cap(opps: list[ScoredOpportunity]) -> list[ScoredOpportunity]:
    """
    Apply CERTAIN_MAX_CAPITAL_PER_CLUSTER per (city, target_date) cluster.
    Greedy inclusion by edge descending (list is already sorted).
    """
    cluster_spend: dict[tuple, float] = {}
    accepted: list[ScoredOpportunity] = []

    for opp in opps:
        key = (opp.market.city, opp.market.target_date)
        spent = cluster_spend.get(key, 0.0)
        if spent + opp.capital <= _config.CERTAIN_MAX_CAPITAL_PER_CLUSTER:
            cluster_spend[key] = spent + opp.capital
            accepted.append(opp)

    return accepted
