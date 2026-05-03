"""
Tests for the cross-bucket cluster filter in opportunity_scorer.py.

The filter applies a tighter disagreement ratio for NO bets when the
city/date cluster has a dominant YES bucket (≥ BOND_CROSS_BUCKET_YES_THRESHOLD).
"""
import sys
import os
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bot"))

import config as _config
from bonding.opportunity_scorer import score_all, _score_side, ScoredOpportunity
from bonding.market_scanner import MarketCandidate
from bonding.weather_client import ForecastResult, SourceConsensus


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_forecast(prob_yes: float, city: str = "London") -> SourceConsensus:
    """Return a minimal SourceConsensus that yields the given P(YES)."""
    members = [1.0] * int(prob_yes * 100) + [0.0] * (100 - int(prob_yes * 100))
    fr = ForecastResult(
        city=city,
        target_date=date(2026, 5, 10),
        daily_max_c=20.0,
        ensemble_members=[20.0] * 30,
        forecast_peak_hour=14,
    )
    sc = MagicMock(spec=SourceConsensus)
    sc.gfs = fr
    sc.consensus_prob.return_value = prob_yes
    sc.available_sources.return_value = 2
    sc.point_forecasts.return_value = [20.0, 20.5]
    return sc


def _make_market(
    city: str = "London",
    yes_ask: float = 0.75,
    no_ask: float = 0.27,
    market_id: str = "m1",
) -> MarketCandidate:
    return MarketCandidate(
        market_id=market_id,
        token_id=f"yes_{market_id}",
        question=f"Will the highest temperature in {city} be 20°C on May 10?",
        city=city,
        target_date=date(2026, 5, 10),
        temp_min=19.5,
        temp_max=20.5,
        unit="C",
        best_ask=yes_ask,
        resolution_time=datetime(2026, 5, 10, 18, 0, tzinfo=timezone.utc),
        ask_book=[(yes_ask, 100)],
        no_token_id=f"no_{market_id}",
        no_best_ask=no_ask,
        no_ask_book=[(no_ask, 100)],
    )


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_cross_bucket_veto_fires_for_concentrated_cluster(monkeypatch):
    """
    When cluster has YES ≥ threshold and cluster_size ≥ min_cluster_size,
    a NO bet whose prob exceeds the tight cap should be vetoed.
    """
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_YES_THRESHOLD", 0.60)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_DISAGREE_RATIO", 1.8)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_MIN_CLUSTER_SIZE", 2)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CORE_YES_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", False)

    forecast = _make_forecast(0.36)  # P(YES)=0.36, so P(NO)=0.64

    # ask_NO = 0.27; tight_cap = 0.27 * 1.8 = 0.486; P(NO)=0.64 > 0.486 → veto
    result = _score_side(
        market=_make_market(no_ask=0.27),
        forecast=forecast,
        prob=0.64,       # P(NO)
        ask=0.27,
        ask_book=[(0.27, 100)],
        token_id="no_m1",
        outcome="NO",
        cluster_dominant_yes_ask=0.73,   # dominant YES bucket in cluster
        cluster_size=3,
    )
    assert result is None, "Cross-bucket filter should have vetoed this NO bet"


def test_cross_bucket_passes_when_prob_within_tight_cap(monkeypatch):
    """
    If the model prob is low enough (within the tight cap) the bet should pass.
    """
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_YES_THRESHOLD", 0.60)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_DISAGREE_RATIO", 1.8)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_MIN_CLUSTER_SIZE", 2)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CORE_YES_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_CORE_NO_MIN_ASK", 0.15)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CORE", 0.15)

    # ask_NO=0.22; tight_cap=0.22*1.8=0.396; prob=0.38 < 0.396 → passes filter
    result = _score_side(
        market=_make_market(no_ask=0.22),
        forecast=_make_forecast(0.62),
        prob=0.38,
        ask=0.22,
        ask_book=[(0.22, 100)],
        token_id="no_m1",
        outcome="NO",
        cluster_dominant_yes_ask=0.73,
        cluster_size=3,
    )
    # May be None for other reasons (e.g. min_edge), but NOT because of cross-bucket
    # The important thing is the function didn't veto due to cross-bucket alone.
    # ev = 0.38 - 0.22 = 0.16 ≥ BOND_MIN_EDGE_CORE(0.15) so it should qualify.
    assert result is not None, "Should not be vetoed when prob is within tight cap"


def test_cross_bucket_disabled_passes_all(monkeypatch):
    """When filter is disabled, NO bets should not be filtered by cluster signal."""
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CORE_NO_MIN_ASK", 0.15)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CORE", 0.15)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", False)

    # Normally this would be vetoed with cross-bucket enabled.
    result = _score_side(
        market=_make_market(no_ask=0.22),
        forecast=_make_forecast(0.62),
        prob=min(0.64, 0.22 * 2.5),  # prob after market cap = 0.55
        ask=0.22,
        ask_book=[(0.22, 100)],
        token_id="no_m1",
        outcome="NO",
        cluster_dominant_yes_ask=0.90,
        cluster_size=5,
    )
    # With BOND_MIN_EDGE_CORE=0.15: ev = 0.55-0.22=0.33 ≥ 0.15, should qualify.
    assert result is not None


def test_cross_bucket_not_applied_to_small_cluster(monkeypatch):
    """Filter only fires when cluster_size ≥ BOND_CROSS_BUCKET_MIN_CLUSTER_SIZE."""
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_YES_THRESHOLD", 0.60)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_DISAGREE_RATIO", 1.8)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_MIN_CLUSTER_SIZE", 3)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CORE_NO_MIN_ASK", 0.15)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CORE", 0.15)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", False)

    # cluster_size=2 < min_cluster_size=3 → filter not applied
    result = _score_side(
        market=_make_market(no_ask=0.22),
        forecast=_make_forecast(0.62),
        prob=min(0.64, 0.22 * 2.5),  # prob after market cap = 0.55
        ask=0.22,
        ask_book=[(0.22, 100)],
        token_id="no_m1",
        outcome="NO",
        cluster_dominant_yes_ask=0.90,
        cluster_size=2,   # below min
    )
    assert result is not None


def test_cross_bucket_yes_bets_never_filtered(monkeypatch):
    """Cross-bucket filter only applies to NO bets, never YES."""
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_YES_THRESHOLD", 0.10)  # very low threshold
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_DISAGREE_RATIO", 0.5)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_MIN_CLUSTER_SIZE", 1)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CORE_YES_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CHEAP", 0.07)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", True)

    # CHEAP YES bet: ask=0.05, prob=0.14 → ev=0.09 ≥ 0.07 min_edge → should qualify
    result = _score_side(
        market=_make_market(yes_ask=0.05),
        forecast=_make_forecast(0.14),
        prob=0.14,
        ask=0.05,
        ask_book=[(0.05, 20)],
        token_id="yes_m1",
        outcome="YES",
        cluster_dominant_yes_ask=0.90,
        cluster_size=5,
    )
    assert result is not None, "YES bets should never be filtered by cross-bucket"
