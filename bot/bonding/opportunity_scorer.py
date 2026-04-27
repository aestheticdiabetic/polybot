"""
opportunity_scorer.py — Join forecast data with market data.

Computes expected value per share for each market candidate, assigns a
position tier (CHEAP / CORE), and enforces per-cluster capital caps.
"""
import logging
import math
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

TIER_CHEAP = "CHEAP"
TIER_CORE  = "CORE"

# Entry-time bucket labels (must match dashboard UI and BOND_DISABLED_ENTRY_BUCKETS values)
_ENTRY_BUCKETS = [
    ("0-12h",  0,   12),
    ("10-20h", 12,  20),
    ("20-30h", 20,  30),
    ("30-48h", 30,  48),
    ("48h+",   48,  float("inf")),
]


def _entry_bucket_label(hours_to_resolution: float) -> Optional[str]:
    """Return the entry-time bucket label for a given hours-to-resolution value."""
    for label, lo, hi in _ENTRY_BUCKETS:
        if lo <= hours_to_resolution < hi:
            return label
    return None


# Ask price ranges that define each tier
_CHEAP_ASK_MIN = 0.02   # 2¢ — minimum for $1 order with reasonable share count
_CHEAP_ASK_MAX = 0.08   # 8¢
_CORE_ASK_MIN  = 0.20   # 20¢ — 0.08-0.20 showed 0% win rate in data analysis
_CORE_ASK_MAX  = 0.30   # 30¢


@dataclass
class ScoredOpportunity:
    market: MarketCandidate
    forecast: ForecastResult
    prob: float          # P(this token resolves $1): P(YES) for YES side, P(NO) for NO side
    ev: float            # expected value per share = prob - side_ask
    edge: float          # prob - side_ask (true vs implied probability gap)
    tier: str            # CHEAP | CORE
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
    iterations skip re-evaluating the same market until local midnight passes.
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

    # ── Entry-time bucket toggle ──────────────────────────────────────────────
    disabled_buckets = _config.BOND_DISABLED_ENTRY_BUCKETS
    if disabled_buckets:
        now_utc = datetime.now(timezone.utc)
        hours_to_res = (market.resolution_time - now_utc).total_seconds() / 3600
        bucket = _entry_bucket_label(max(hours_to_res, 0))
        if bucket and bucket in disabled_buckets:
            log.debug(
                f"scorer: {market.city} {market.target_date} — entry bucket "
                f"'{bucket}' disabled, skipping"
            )
            return None

    temp_min, temp_max = _convert_temps(market)
    if temp_min is None or temp_max is None:
        return None

    prob_yes = forecast.consensus_prob(temp_min, temp_max)
    prob_no  = 1.0 - prob_yes

    disabled_sides = _config.BOND_DISABLED_SIDES
    yes_opp: Optional[ScoredOpportunity] = None
    if "YES" not in disabled_sides:
        yes_opp = _score_side(
            market=market, forecast=forecast,
            prob=prob_yes, ask=market.best_ask, ask_book=market.ask_book,
            token_id=market.token_id, outcome="YES",
        )
    no_opp: Optional[ScoredOpportunity] = None
    if market.no_token_id and market.no_best_ask is not None and "NO" not in disabled_sides:
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


def _passes_source_quality(
    forecast: SourceConsensus,
    tier: str,
    city: str,
    target_date,
) -> bool:
    """
    Reject bets where forecast sources disagree too much or too few sources are present.
    CHEAP requires ≥2 met sources and ≤4°C spread; CORE requires ≥2 and ≤3°C spread.
    """
    if tier == TIER_CHEAP:
        min_sources = _config.BOND_CHEAP_MIN_SOURCES
        max_spread  = _config.BOND_CHEAP_MAX_SOURCE_SPREAD_C
    elif tier == TIER_CORE:
        min_sources = _config.BOND_CORE_MIN_SOURCES
        max_spread  = _config.BOND_CORE_MAX_SOURCE_SPREAD_C
    else:
        return True  # CERTAIN tier has its own gates

    if forecast.available_sources() < min_sources:
        log.debug(
            f"scorer: {city} {target_date} {tier} only "
            f"{forecast.available_sources()} source(s) < {min_sources} required — skip"
        )
        return False

    points = forecast.point_forecasts()
    if len(points) >= 2:
        spread = max(points) - min(points)
        if spread > max_spread:
            log.debug(
                f"scorer: {city} {target_date} {tier} source spread "
                f"{spread:.1f}°C > {max_spread}°C max — skip"
            )
            return False

    return True


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

    tier = assign_tier(ask, edge)
    if tier is None:
        return None

    if not _passes_source_quality(forecast, tier, market.city, market.target_date):
        return None

    if tier in _config.BOND_DISABLED_TIERS:
        log.debug(
            f"scorer: {market.city} {market.target_date} {outcome} "
            f"tier '{tier}' disabled — skip"
        )
        return None

    # Targeted side restrictions (data-driven).
    # CHEAP NO bets: 6.1% WR historically. CORE NO <15¢: 0% WR.
    # CORE YES bets: 11% WR, -$38.61 total — model miscalibrated at 20-30¢ range.
    if outcome == "NO":
        if tier == TIER_CHEAP and not _config.BOND_CHEAP_NO_ENABLED:
            log.debug(
                f"scorer: {market.city} {market.target_date} NO ask={ask:.3f} "
                f"CHEAP NO bets disabled — skip"
            )
            return None
        if tier == TIER_CORE and ask < _config.BOND_CORE_NO_MIN_ASK:
            log.debug(
                f"scorer: {market.city} {market.target_date} NO ask={ask:.3f} "
                f"< BOND_CORE_NO_MIN_ASK={_config.BOND_CORE_NO_MIN_ASK:.2f} — skip"
            )
            return None
    if outcome == "YES" and tier == TIER_CORE and not _config.BOND_CORE_YES_ENABLED:
        log.debug(
            f"scorer: {market.city} {market.target_date} YES ask={ask:.3f} "
            f"CORE YES bets disabled — skip"
        )
        return None

    min_edge = (
        _config.BOND_MIN_EDGE_CHEAP if tier == TIER_CHEAP else _config.BOND_MIN_EDGE_CORE
    )
    if edge < min_edge:
        log.debug(
            f"scorer: {market.city} {market.target_date} {outcome} ask={ask:.4f} "
            f"edge={edge:.4f} < {min_edge} ({tier} min) — skip"
        )
        return None

    shares = _shares_for_tier(tier, ask)
    # Only fill at prices that maintain the tier's minimum edge requirement.
    max_profitable_price = prob - min_edge
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


def assign_tier(ask: float, edge: float) -> Optional[str]:
    """
    Assign a position tier based on ask price and edge.
    Returns None if no tier criteria are met.

    CHEAP: 2-8¢  — long-shot bets, edge >= BOND_MIN_EDGE_CHEAP
    CORE:  8-30¢ — underdog bets, edge >= BOND_MIN_EDGE_CORE
    """
    if _CHEAP_ASK_MIN <= ask < _CHEAP_ASK_MAX:
        return TIER_CHEAP

    if _CORE_ASK_MIN <= ask <= _CORE_ASK_MAX:
        return TIER_CORE

    return None


def _shares_for_tier(tier: str, ask: float) -> int:
    """
    Compute share count for a given tier and ask price.

    CHEAP: adaptive — ceil(1.00 / ask), capped at BOND_SHARES_CHEAP_MAX.
           Ensures every order costs >= $1 at the CLOB minimum.
    CORE:  max(BOND_SHARES_CORE, ceil(1.00 / ask)) — uses target count but
           floors up to hit $1 minimum at the low end of the price range.
    """
    if tier == TIER_CHEAP:
        return min(math.ceil(1.00 / ask), _config.BOND_SHARES_CHEAP_MAX)
    # CORE
    return max(_config.BOND_SHARES_CORE, math.ceil(1.00 / ask))


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
