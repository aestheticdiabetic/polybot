"""
Tests for the price momentum filter in opportunity_scorer.py.
"""
import sys
import os
import time
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bot"))

import config as _config
from bonding.opportunity_scorer import (
    record_price_tick,
    get_momentum,
    _score_side,
    _price_history,
)
from bonding.market_scanner import MarketCandidate
from bonding.weather_client import ForecastResult, SourceConsensus


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_forecast(prob_yes: float = 0.12) -> SourceConsensus:
    fr = ForecastResult(
        city="London",
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


def _make_market(yes_ask: float = 0.05) -> MarketCandidate:
    return MarketCandidate(
        market_id="m_mom",
        token_id="yes_mom",
        question="Will the highest temperature in London be 20°C on May 10?",
        city="London",
        target_date=date(2026, 5, 10),
        temp_min=19.5,
        temp_max=20.5,
        unit="C",
        best_ask=yes_ask,
        resolution_time=datetime(2026, 5, 10, 18, 0, tzinfo=timezone.utc),
        ask_book=[(yes_ask, 50)],
        no_token_id=None,
        no_best_ask=None,
    )


def _clear_history(token_id: str) -> None:
    _price_history.pop(token_id, None)


# ── record_price_tick / get_momentum ─────────────────────────────────────────

def test_get_momentum_no_history():
    _clear_history("tok_new")
    assert get_momentum("tok_new") is None


def test_get_momentum_single_tick():
    _clear_history("tok_one")
    record_price_tick("tok_one", 0.10)
    assert get_momentum("tok_one") is None  # need ≥2 ticks in window


def test_get_momentum_rising():
    _clear_history("tok_rise")
    now = time.time()
    # Simulate ticks 30 minutes apart
    _price_history["tok_rise"] = __import__("collections").deque(maxlen=120)
    _price_history["tok_rise"].append((now - 1800, 0.10))
    _price_history["tok_rise"].append((now - 60,   0.15))
    m = get_momentum("tok_rise", lookback_secs=3600)
    assert m is not None
    assert abs(m - 0.05) < 1e-9, f"Expected +0.05, got {m}"


def test_get_momentum_falling():
    _clear_history("tok_fall")
    now = time.time()
    _price_history["tok_fall"] = __import__("collections").deque(maxlen=120)
    _price_history["tok_fall"].append((now - 1800, 0.20))
    _price_history["tok_fall"].append((now - 60,   0.10))
    m = get_momentum("tok_fall", lookback_secs=3600)
    assert m is not None
    assert abs(m - (-0.10)) < 1e-9, f"Expected -0.10, got {m}"


def test_get_momentum_ignores_old_ticks():
    """Ticks outside the lookback window should not contribute to momentum."""
    _clear_history("tok_old")
    now = time.time()
    _price_history["tok_old"] = __import__("collections").deque(maxlen=120)
    _price_history["tok_old"].append((now - 7200, 0.05))  # 2h ago — outside 1h window
    _price_history["tok_old"].append((now - 60,   0.15))
    m = get_momentum("tok_old", lookback_secs=3600)
    # Only one tick in window → None
    assert m is None


# ── Momentum filter in _score_side ───────────────────────────────────────────

def test_momentum_filter_veto_on_adverse_move(monkeypatch):
    """Strongly falling price (adverse momentum) should veto the entry."""
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_VETO_THRESHOLD", 0.03)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_LOOKBACK_SECS", 3600)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CHEAP", 0.07)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", False)

    token = "yes_veto"
    _clear_history(token)
    now = time.time()
    _price_history[token] = __import__("collections").deque(maxlen=120)
    _price_history[token].append((now - 1800, 0.10))
    _price_history[token].append((now - 60,   0.05))   # -0.05 adverse move

    result = _score_side(
        market=_make_market(yes_ask=0.05),
        forecast=_make_forecast(0.14),
        prob=0.14,
        ask=0.05,
        ask_book=[(0.05, 50)],
        token_id=token,
        outcome="YES",
    )
    assert result is None, "Should be vetoed due to adverse momentum"
    _clear_history(token)


def test_momentum_filter_allows_favourable_move(monkeypatch):
    """Rising price (favourable momentum) should not veto the entry."""
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_VETO_THRESHOLD", 0.03)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_LOOKBACK_SECS", 3600)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CHEAP", 0.07)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", False)

    token = "yes_ok"
    _clear_history(token)
    now = time.time()
    _price_history[token] = __import__("collections").deque(maxlen=120)
    _price_history[token].append((now - 1800, 0.04))
    _price_history[token].append((now - 60,   0.05))   # +0.01 favourable

    result = _score_side(
        market=_make_market(yes_ask=0.05),
        forecast=_make_forecast(0.14),
        prob=0.14,
        ask=0.05,
        ask_book=[(0.05, 50)],
        token_id=token,
        outcome="YES",
    )
    assert result is not None, "Should not be vetoed for favourable momentum"
    _clear_history(token)


def test_momentum_filter_disabled_ignores_adverse_move(monkeypatch):
    """When filter is disabled, adverse momentum should not block entries."""
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CHEAP", 0.07)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", False)

    token = "yes_disabled"
    _clear_history(token)
    now = time.time()
    _price_history[token] = __import__("collections").deque(maxlen=120)
    _price_history[token].append((now - 1800, 0.15))
    _price_history[token].append((now - 60,   0.05))   # huge adverse move

    result = _score_side(
        market=_make_market(yes_ask=0.05),
        forecast=_make_forecast(0.14),
        prob=0.14,
        ask=0.05,
        ask_book=[(0.05, 50)],
        token_id=token,
        outcome="YES",
    )
    assert result is not None, "Filter disabled — should not veto"
    _clear_history(token)


def test_momentum_no_history_does_not_block(monkeypatch):
    """With no price history, momentum is None and entry should proceed normally."""
    monkeypatch.setattr(_config, "BOND_MOMENTUM_FILTER_ENABLED", True)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_VETO_THRESHOLD", 0.03)
    monkeypatch.setattr(_config, "BOND_MOMENTUM_LOOKBACK_SECS", 3600)
    monkeypatch.setattr(_config, "BOND_MARKET_DISAGREEMENT_RATIO", 2.5)
    monkeypatch.setattr(_config, "BOND_CROSS_BUCKET_ENABLED", False)
    monkeypatch.setattr(_config, "BOND_MIN_EDGE_CHEAP", 0.07)
    monkeypatch.setattr(_config, "BOND_CHEAP_NO_ENABLED", False)

    token = "yes_nohist"
    _clear_history(token)

    result = _score_side(
        market=_make_market(yes_ask=0.05),
        forecast=_make_forecast(0.14),
        prob=0.14,
        ask=0.05,
        ask_book=[(0.05, 50)],
        token_id=token,
        outcome="YES",
    )
    assert result is not None, "No history → no veto"
