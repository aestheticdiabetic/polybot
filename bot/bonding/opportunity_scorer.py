"""
opportunity_scorer.py — Join forecast data with market data.

Computes expected value per share for each market candidate, assigns a
position tier (CORE / SECONDARY / WING), and enforces per-cluster capital caps.
"""
import logging
from dataclasses import dataclass
from typing import Optional

import config as _config
from bonding.market_scanner import MarketCandidate
from bonding.weather_client import ForecastResult, prob_in_range, fahrenheit_to_celsius

log = logging.getLogger("bond.scorer")

TIER_CORE      = "CORE"
TIER_SECONDARY = "SECONDARY"
TIER_WING      = "WING"

# Ask price ranges that define each tier
_CORE_ASK_MAX = 0.08
_CORE_ASK_MIN = 0.02
_SECONDARY_ASK_MAX = 0.019
_SECONDARY_ASK_MIN = 0.009
_WING_ASK_MAX = 0.008
_WING_ASK_MIN = 0.001


@dataclass
class ScoredOpportunity:
    market: MarketCandidate
    forecast: ForecastResult
    prob: float          # true probability from forecast (0–1)
    ev: float            # expected value per share = prob*1.0 - best_ask
    edge: float          # prob - best_ask (true vs implied probability gap)
    tier: str            # CORE | SECONDARY | WING
    shares: int          # total target shares
    capital: float       # total cost basis = shares * best_ask
    shares_immediate: int  # shares to buy now via FOK (available at profitable prices)
    shares_limit: int      # shares to queue as GTC limit order
    limit_price: float     # price for the GTC limit order (= best_ask at scan time)


def score_all(
    markets: list[MarketCandidate],
    forecasts: dict[tuple, ForecastResult],
) -> list[ScoredOpportunity]:
    """
    Score all market candidates against forecast data.
    Returns list of ScoredOpportunity sorted by EV descending,
    with per-cluster capital caps applied.
    """
    scored: list[ScoredOpportunity] = []

    for market in markets:
        forecast = forecasts.get((market.city, market.target_date))
        if forecast is None:
            log.debug(f"scorer: no forecast for {market.city} {market.target_date}, skipping")
            continue

        opp = score_market(market, forecast)
        if opp is not None:
            scored.append(opp)

    if not scored:
        return []

    # Sort by EV descending
    scored.sort(key=lambda o: o.ev, reverse=True)

    # Apply per-cluster capital caps
    capped = _apply_cluster_caps(scored)

    log.info(
        f"scorer: {len(markets)} markets → {len(scored)} scored → "
        f"{len(capped)} after cluster caps"
    )
    return capped


def score_market(
    market: MarketCandidate,
    forecast: ForecastResult,
) -> Optional[ScoredOpportunity]:
    """
    Score a single market. Returns None if it doesn't meet any tier criteria.
    """
    ask = market.best_ask
    if ask <= 0.0 or ask >= 1.0:
        return None

    # Convert temperature bounds to Celsius if market is in Fahrenheit
    temp_min = market.temp_min
    temp_max = market.temp_max
    if market.unit == "F":
        if temp_min is not None:
            temp_min = fahrenheit_to_celsius(temp_min)
        if temp_max is not None:
            temp_max = fahrenheit_to_celsius(temp_max)

    # Can't compute probability without a bucket
    if temp_min is None or temp_max is None:
        return None

    prob = prob_in_range(forecast, temp_min, temp_max)
    ev   = (prob * 1.0) - ask
    edge = prob - ask

    tier = assign_tier(ask, ev, prob)
    if tier is None:
        return None

    # All tiers must meet minimum edge floor
    if edge < _config.BOND_EDGE_FLOOR:
        log.debug(
            f"scorer: {market.city} {market.target_date} ask={ask:.4f} "
            f"edge={edge:.4f} < BOND_EDGE_FLOOR — skip"
        )
        return None

    shares = _shares_for_tier(tier)

    # Compute fill split: how many shares are immediately available at profitable
    # prices vs how many need a resting GTC limit order.
    # max_profitable_price = highest price where we still satisfy the edge floor.
    max_profitable_price = prob - _config.BOND_EDGE_FLOOR
    shares_immediate, shares_limit = _compute_fill_split(
        market.ask_book, shares, max_profitable_price
    )

    # Need at least something to do
    if shares_immediate == 0 and shares_limit == 0:
        return None

    capital = round(shares * ask, 4)

    return ScoredOpportunity(
        market=market,
        forecast=forecast,
        prob=prob,
        ev=ev,
        edge=edge,
        tier=tier,
        shares=shares,
        capital=capital,
        shares_immediate=shares_immediate,
        shares_limit=shares_limit,
        limit_price=ask,
    )


def assign_tier(ask: float, ev: float, prob: float) -> Optional[str]:
    """
    Assign a position tier based on ask price, EV, and probability.
    Returns None if no tier criteria are met.
    """
    if _CORE_ASK_MIN <= ask <= _CORE_ASK_MAX:
        if ev > _config.BOND_MIN_EV_CORE and prob > _config.BOND_CONFIDENCE_FLOOR:
            return TIER_CORE

    if _SECONDARY_ASK_MIN <= ask <= _SECONDARY_ASK_MAX:
        if ev > _config.BOND_MIN_EV_SECONDARY:
            return TIER_SECONDARY

    if _WING_ASK_MIN <= ask <= _WING_ASK_MAX:
        if ev > 0:  # positive EV is sufficient for wing bets
            return TIER_WING

    return None


def _compute_fill_split(
    ask_book: list,
    shares_wanted: int,
    max_profitable_price: float,
) -> tuple[int, int]:
    """
    Given the ask book and target share count, compute how many shares can be
    filled immediately (at or below max_profitable_price) vs queued as a limit.

    Returns (shares_immediate, shares_limit).
    When ask_book is empty (Gamma-embedded price, depth unknown), all shares are
    attempted immediately via FOK — old behaviour.
    """
    if not ask_book:
        return shares_wanted, 0
    available = sum(size for price, size in ask_book if price <= max_profitable_price)
    shares_immediate = min(shares_wanted, int(available))
    shares_limit = shares_wanted - shares_immediate
    return shares_immediate, shares_limit


def _shares_for_tier(tier: str) -> int:
    if tier == TIER_CORE:
        return _config.BOND_SHARES_CORE
    if tier == TIER_SECONDARY:
        return _config.BOND_SHARES_SECONDARY
    return _config.BOND_SHARES_WING


def _apply_cluster_caps(
    opps: list[ScoredOpportunity],
) -> list[ScoredOpportunity]:
    """
    Group opportunities by (city, date). Within each cluster, greedily include
    opportunities in EV order until BOND_MAX_CAPITAL_PER_CLUSTER is reached.
    Opportunities already sorted by EV descending.
    """
    cluster_spend: dict[tuple, float] = {}
    result: list[ScoredOpportunity] = []

    for opp in opps:
        key = (opp.market.city, opp.market.target_date)
        spent = cluster_spend.get(key, 0.0)
        if spent + opp.capital <= _config.BOND_MAX_CAPITAL_PER_CLUSTER:
            cluster_spend[key] = spent + opp.capital
            result.append(opp)
        else:
            log.debug(
                f"scorer: cluster cap reached for {opp.market.city} "
                f"{opp.market.target_date} — skipping {opp.tier}"
            )

    return result
