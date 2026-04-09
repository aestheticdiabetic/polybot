"""
opportunity_scorer.py — Join forecast data with market data.

Computes expected value per share for each market candidate, assigns a
position tier (CORE / SECONDARY / WING), and enforces per-cluster capital caps.
"""
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Optional

import config as _config
from bonding import peak_hour_stats as _peak_stats
from bonding.market_scanner import MarketCandidate
from bonding.weather_client import ForecastResult, SourceConsensus, prob_in_range, fahrenheit_to_celsius
from bonding.weather_client import _peak_hour_stats as _loaded_stats

log = logging.getLogger("bond.scorer")

# REST-scan suppression cache: (city, target_date) → suppressed_until (Unix timestamp).
# Set when score_market skips a market due to the time gate, so the REST scan avoids
# re-evaluating the same market every 60 s until local midnight passes.
_scan_suppressions: dict[tuple, float] = {}

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
    prob: float          # P(this token resolves $1): P(YES) for YES side, P(NO) for NO side
    ev: float            # expected value per share = prob - side_ask
    edge: float          # prob - side_ask (true vs implied probability gap)
    tier: str            # CORE | SECONDARY | WING
    shares: int          # total target shares
    capital: float       # total cost basis = shares * side_ask
    shares_immediate: int  # shares to buy now via FOK (available at profitable prices)
    shares_limit: int      # shares to queue as GTC limit order
    limit_price: float     # price for the GTC limit order (= side_ask at scan time)
    outcome: str           # "YES" or "NO" — which token to trade
    token_id: str          # token ID for CLOB orders (YES or NO token)
    side_ask: float        # best ask for the chosen side


def passes_time_gate(market: "MarketCandidate", forecast_peak_hour: Optional[int]) -> bool:
    """
    Returns True if the market is still within its valid betting window.
    Shared by score_market() and sure_thing_scorer.score_certain().

    Side-effect: populates _scan_suppressions on gate-fail so subsequent REST scan
    iterations skip re-evaluating the same market until local midnight.
    """
    if time.time() < _scan_suppressions.get((market.city, market.target_date), 0.0):
        return False

    tz_name = _config.BOND_CITY_TIMEZONES.get(market.city)
    if not tz_name:
        return False
    try:
        city_tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return False

    now_utc   = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(city_tz)

    if market.target_date == now_local.date():
        current_local_hour = now_local.hour
        current_month      = now_local.month
        gate_hour = _peak_stats.get_gate_hour(
            market.city, forecast_peak_hour, current_month, _loaded_stats
        )
        if current_local_hour >= gate_hour:
            next_day = market.target_date + timedelta(days=1)
            end_of_day_utc = datetime(
                next_day.year, next_day.month, next_day.day, 0, 0, 0,
                tzinfo=city_tz,
            ).astimezone(timezone.utc)
            suppress_secs = max(
                (end_of_day_utc - now_utc).total_seconds(), 0
            ) + 300
            _scan_suppressions[(market.city, market.target_date)] = (
                time.time() + suppress_secs
            )
            return False
    else:
        next_day = market.target_date + timedelta(days=1)
        end_of_day_utc = datetime(
            next_day.year, next_day.month, next_day.day, 0, 0, 0,
            tzinfo=city_tz,
        ).astimezone(timezone.utc)
        if (end_of_day_utc - now_utc).total_seconds() <= 0:
            _scan_suppressions[(market.city, market.target_date)] = (
                time.time() + 24 * 3600
            )
            return False

    return True


def score_all(
    markets: list[MarketCandidate],
    forecasts: dict[tuple, SourceConsensus],
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
    forecast: SourceConsensus,
) -> Optional[ScoredOpportunity]:
    """
    Score YES and NO sides of a market. Returns the higher-EV side, or None if
    neither meets tier criteria. Never returns both sides (prevents same-market hedging).
    """
    if not _config.BOND_CITY_TIMEZONES.get(market.city):
        log.warning(
            f"scorer: {market.city} {market.target_date} — no timezone configured, skipping "
            f"(add to BOND_CITY_TIMEZONES in config.py)"
        )
        return None

    if not passes_time_gate(market, forecast.gfs.forecast_peak_hour):
        return None

    temp_min, temp_max = _convert_temps(market)
    if temp_min is None or temp_max is None:
        return None

    prob_yes = forecast.consensus_prob(temp_min, temp_max)
    prob_no  = 1.0 - prob_yes

    yes_opp = _score_side(
        market=market, forecast=forecast,
        prob=prob_yes, ask=market.best_ask, ask_book=market.ask_book,
        token_id=market.token_id, outcome="YES",
    )
    no_opp: Optional[ScoredOpportunity] = None
    if market.no_token_id and market.no_best_ask is not None:
        no_opp = _score_side(
            market=market, forecast=forecast,
            prob=prob_no, ask=market.no_best_ask, ask_book=market.no_ask_book,
            token_id=market.no_token_id, outcome="NO",
        )

    if yes_opp is None and no_opp is None:
        return None
    if yes_opp is None:
        return no_opp
    if no_opp is None:
        return yes_opp
    return yes_opp if yes_opp.ev >= no_opp.ev else no_opp


def _convert_temps(market: MarketCandidate) -> tuple[Optional[float], Optional[float]]:
    """Convert market temperature bounds to Celsius."""
    temp_min = market.temp_min
    temp_max = market.temp_max
    if market.unit == "F":
        if temp_min is not None:
            temp_min = fahrenheit_to_celsius(temp_min)
        if temp_max is not None:
            temp_max = fahrenheit_to_celsius(temp_max)
    return temp_min, temp_max


def _score_side(
    market: MarketCandidate,
    forecast: SourceConsensus,
    prob: float,
    ask: float,
    ask_book: list,
    token_id: str,
    outcome: str,
) -> Optional[ScoredOpportunity]:
    """
    Score a single side (YES or NO) of a market.
    prob = P(this token resolves $1).
    Returns None if it doesn't meet any tier criteria.
    """
    if ask <= 0.0 or ask >= 1.0:
        return None

    # Market-implied confidence cap: when our model disagrees with the market by
    # more than BOND_MARKET_DISAGREEMENT_RATIO-fold, cap prob down to ask × ratio.
    # Rationale: a market pricing a side at 0.001 is expressing ~100% certainty in
    # the other side. Our ensemble can't reliably beat that signal — it more likely
    # means the market has real-time information we don't (e.g. observed temperature).
    market_cap = ask * _config.BOND_MARKET_DISAGREEMENT_RATIO
    if prob > market_cap:
        log.debug(
            f"scorer: {market.city} {market.target_date} {outcome} "
            f"prob capped {prob:.3f}→{market_cap:.3f} "
            f"(ratio={prob/ask:.1f}x > {_config.BOND_MARKET_DISAGREEMENT_RATIO}x)"
        )
        prob = market_cap

    ev   = prob - ask
    edge = ev

    tier = assign_tier(ask, ev, prob)
    if tier is None:
        return None

    if edge < _config.BOND_EDGE_FLOOR:
        log.debug(
            f"scorer: {market.city} {market.target_date} {outcome} ask={ask:.4f} "
            f"edge={edge:.4f} < BOND_EDGE_FLOOR — skip"
        )
        return None

    shares = _shares_for_tier(tier)
    max_profitable_price = prob - _config.BOND_EDGE_FLOOR
    shares_immediate, shares_limit = _compute_fill_split(ask_book, shares, max_profitable_price)

    if shares_immediate == 0 and shares_limit == 0:
        return None

    capital = round(shares * ask, 4)

    return ScoredOpportunity(
        market=market,
        forecast=forecast.gfs,      # store GFS ForecastResult for downstream compatibility
        prob=prob,
        ev=ev,
        edge=edge,
        tier=tier,
        shares=shares,
        capital=capital,
        shares_immediate=shares_immediate,
        shares_limit=shares_limit,
        limit_price=ask,
        outcome=outcome,
        token_id=token_id,
        side_ask=ask,
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
