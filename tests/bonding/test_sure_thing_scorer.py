"""Tests for CERTAIN tier scoring — one test per hard gate."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../bot"))

from datetime import date
from unittest.mock import patch, MagicMock
import pytest

from bonding.weather_client import ForecastResult, SourceConsensus
from bonding.market_scanner import MarketCandidate


def _make_fr(daily_max: float, n_members: int = 100, spread: float = 0.5) -> ForecastResult:
    import random
    rng = random.Random(42)
    members = [rng.gauss(daily_max, spread) for _ in range(n_members)]
    return ForecastResult("London", date(2026, 4, 15), daily_max, members)


def _make_consensus(gfs_max=20.0, ecmwf_max=20.0, tio_max=20.0, spread=0.5) -> SourceConsensus:
    return SourceConsensus(
        city="London",
        target_date=date(2026, 4, 15),
        gfs=_make_fr(gfs_max, spread=spread),
        ecmwf=_make_fr(ecmwf_max, spread=spread),
        tomorrowio=_make_fr(tio_max, spread=spread),
    )


def _make_market(ask: float = 0.85, temp_min: float = 18.0, temp_max: float = 22.0) -> MarketCandidate:
    return MarketCandidate(
        market_id="test-market",
        token_id="test-token-yes",
        question="Will London daily high be 18–22°C on 2026-04-15?",
        city="London",
        target_date=date(2026, 4, 15),
        temp_min=temp_min,
        temp_max=temp_max,
        unit="C",
        best_ask=ask,
        resolution_time=MagicMock(),
        ask_book=[(ask, 200)],
        no_token_id="test-token-no",
        no_best_ask=1.0 - ask,
        no_ask_book=[(1.0 - ask, 200)],
    )


def _run_score_certain(markets, forecasts):
    from bonding.sure_thing_scorer import score_certain
    # Patch time gate to always pass
    with patch("bonding.sure_thing_scorer._passes_time_gate", return_value=True):
        # Patch ledger check to always allow (no open positions)
        with patch("bonding.sure_thing_scorer._has_open_position", return_value=False):
            return score_certain(markets, forecasts)


def test_returns_certain_opportunity_when_all_gates_pass():
    market = _make_market(ask=0.85)
    consensus = _make_consensus(gfs_max=20.0, ecmwf_max=20.5, tio_max=19.5, spread=0.5)
    forecasts = {("London", date(2026, 4, 15)): consensus}

    result = _run_score_certain([market], forecasts)

    assert len(result) == 1
    assert result[0].tier == "CERTAIN"
    assert result[0].outcome == "YES"
    assert result[0].shares == 20


def test_blocked_when_ask_below_min():
    market = _make_market(ask=0.70)   # below CERTAIN_ASK_MIN = 0.75
    consensus = _make_consensus()
    forecasts = {("London", date(2026, 4, 15)): consensus}
    assert _run_score_certain([market], forecasts) == []


def test_blocked_when_ask_above_max():
    market = _make_market(ask=0.97)   # above CERTAIN_ASK_MAX = 0.95
    consensus = _make_consensus()
    forecasts = {("London", date(2026, 4, 15)): consensus}
    assert _run_score_certain([market], forecasts) == []


def test_blocked_when_fewer_than_three_sources():
    market = _make_market(ask=0.85)
    consensus = SourceConsensus(
        city="London", target_date=date(2026, 4, 15),
        gfs=_make_fr(20.0), ecmwf=None, tomorrowio=None,  # only 1 source
    )
    forecasts = {("London", date(2026, 4, 15)): consensus}
    assert _run_score_certain([market], forecasts) == []


def test_blocked_when_source_prob_below_min():
    # temp_min=18, temp_max=22. Make GFS members all at 10°C → P(YES) ≈ 0
    market = _make_market(ask=0.85, temp_min=18.0, temp_max=22.0)
    gfs = ForecastResult("London", date(2026, 4, 15), 10.0, [10.0] * 100)
    ecmwf = _make_fr(20.0, spread=0.3)
    tio = _make_fr(20.0, spread=0.3)
    consensus = SourceConsensus("London", date(2026, 4, 15), gfs, ecmwf, tio)
    forecasts = {("London", date(2026, 4, 15)): consensus}
    assert _run_score_certain([market], forecasts) == []


def test_blocked_when_temp_delta_too_large():
    market = _make_market(ask=0.85)
    # GFS says 20°C, ECMWF says 15°C — delta = 5°C > CERTAIN_MAX_TEMP_DELTA_C = 2.0
    consensus = _make_consensus(gfs_max=20.0, ecmwf_max=15.0, tio_max=20.0, spread=0.3)
    forecasts = {("London", date(2026, 4, 15)): consensus}
    assert _run_score_certain([market], forecasts) == []


def test_blocked_when_spread_too_large():
    market = _make_market(ask=0.85)
    # Large spread → std dev > CERTAIN_MAX_SPREAD_C = 1.5
    consensus = _make_consensus(spread=3.0)
    forecasts = {("London", date(2026, 4, 15)): consensus}
    assert _run_score_certain([market], forecasts) == []


def test_blocked_when_edge_too_small():
    # consensus_prob ≈ 0.80, ask = 0.79, edge ≈ 0.01 < 0.05 → blocked
    market = _make_market(ask=0.79, temp_min=18.0, temp_max=22.0)
    gfs = ForecastResult("London", date(2026, 4, 15), 20.0, [20.0] * 80 + [10.0] * 20)
    ecmwf = ForecastResult("London", date(2026, 4, 15), 20.0, [20.0] * 80 + [10.0] * 20)
    tio   = ForecastResult("London", date(2026, 4, 15), 20.0, [20.0] * 80 + [10.0] * 20)
    consensus = SourceConsensus("London", date(2026, 4, 15), gfs, ecmwf, tio)
    forecasts = {("London", date(2026, 4, 15)): consensus}
    assert _run_score_certain([market], forecasts) == []
