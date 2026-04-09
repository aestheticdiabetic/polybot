"""Tests for SourceConsensus dataclass."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../bot"))

from datetime import date
import pytest
from bonding.weather_client import ForecastResult, SourceConsensus


def _make_fr(daily_max: float, members: list[float]) -> ForecastResult:
    return ForecastResult(
        city="TestCity",
        target_date=date(2026, 4, 10),
        daily_max_c=daily_max,
        ensemble_members=members,
    )


def test_consensus_prob_single_source_only():
    gfs = _make_fr(15.0, [13.0, 14.0, 15.0, 16.0, 17.0])
    c = SourceConsensus("TestCity", date(2026, 4, 10), gfs=gfs, ecmwf=None, tomorrowio=None)
    # members in [14, 16]: 14.0, 15.0, 16.0 → 3 of 5 = 0.6
    assert c.consensus_prob(14.0, 16.0) == pytest.approx(0.6)


def test_consensus_prob_averages_three_sources():
    gfs   = _make_fr(15.0, [13.0, 14.0, 15.0, 16.0, 17.0])  # 3/5 = 0.60
    ecmwf = _make_fr(15.0, [15.0, 15.0, 15.0, 15.0, 15.0])  # 5/5 = 1.00
    tio   = _make_fr(13.0, [13.0, 13.0, 13.0, 13.0, 13.0])  # 0/5 = 0.00
    c = SourceConsensus("TestCity", date(2026, 4, 10), gfs=gfs, ecmwf=ecmwf, tomorrowio=tio)
    # Average: (0.60 + 1.00 + 0.00) / 3 ≈ 0.5333
    assert c.consensus_prob(14.0, 16.0) == pytest.approx((0.6 + 1.0 + 0.0) / 3)


def test_consensus_prob_two_sources():
    gfs   = _make_fr(15.0, [15.0, 15.0])   # 2/2 = 1.0
    ecmwf = _make_fr(13.0, [13.0, 13.0])   # 0/2 = 0.0
    c = SourceConsensus("TestCity", date(2026, 4, 10), gfs=gfs, ecmwf=ecmwf, tomorrowio=None)
    assert c.consensus_prob(14.0, 16.0) == pytest.approx(0.5)


def test_available_sources_counts_non_none():
    gfs = _make_fr(15.0, [15.0])
    assert SourceConsensus("C", date(2026, 4, 10), gfs, None, None).available_sources() == 1
    assert SourceConsensus("C", date(2026, 4, 10), gfs, gfs, None).available_sources() == 2
    assert SourceConsensus("C", date(2026, 4, 10), gfs, gfs, gfs).available_sources() == 3


def test_point_forecasts_returns_daily_max_from_each_source():
    gfs  = _make_fr(15.0, [15.0])
    ecmwf = _make_fr(16.0, [16.0])
    tio  = _make_fr(14.0, [14.0])
    c = SourceConsensus("C", date(2026, 4, 10), gfs=gfs, ecmwf=ecmwf, tomorrowio=tio)
    assert c.point_forecasts() == [15.0, 16.0, 14.0]


def test_all_ensemble_members_concatenates_all_sources():
    gfs   = _make_fr(15.0, [14.0, 15.0])
    ecmwf = _make_fr(15.0, [15.0, 16.0])
    tio   = _make_fr(15.0, [13.0])
    c = SourceConsensus("C", date(2026, 4, 10), gfs=gfs, ecmwf=ecmwf, tomorrowio=tio)
    assert c.all_ensemble_members() == [14.0, 15.0, 15.0, 16.0, 13.0]


# ── ECMWF fetch tests ─────────────────────────────────────────────────────────

import asyncio
from unittest.mock import AsyncMock, patch


def _make_ecmwf_response(target_date: date, control_temp: float, n_members: int = 3) -> dict:
    """Minimal Open-Meteo ensemble response for ECMWF model."""
    date_str = target_date.isoformat()
    members = {f"temperature_2m_max_member{i:02d}": [control_temp + i * 0.5] for i in range(n_members)}
    return {
        "daily": {
            "time": [date_str],
            "temperature_2m_max": [control_temp],
            **members,
        }
    }


def test_get_ecmwf_forecast_returns_forecast_result():
    from bonding.weather_client import get_ecmwf_forecast
    target = date(2026, 4, 15)
    raw = _make_ecmwf_response(target, 18.0, n_members=3)

    async def run():
        with patch("bonding.weather_client._fetch_ensemble_range", new=AsyncMock(return_value=raw)):
            with patch("bonding.weather_client._resolve_city", return_value=("London", 51.5, -0.1)):
                return await get_ecmwf_forecast("London", target)

    result = asyncio.run(run())
    assert result is not None
    assert result.city == "London"
    assert result.target_date == target
    assert result.daily_max_c == pytest.approx(18.0)
    assert len(result.ensemble_members) >= 1


def test_get_ecmwf_forecast_returns_none_on_unknown_city():
    from bonding.weather_client import get_ecmwf_forecast, UnknownCityError

    async def run():
        with patch("bonding.weather_client._resolve_city", side_effect=UnknownCityError("nope")):
            return await get_ecmwf_forecast("Atlantis", date(2026, 4, 15))

    assert asyncio.run(run()) is None


def test_get_ecmwf_forecast_returns_none_on_api_error():
    from bonding.weather_client import get_ecmwf_forecast

    async def run():
        with patch("bonding.weather_client._resolve_city", return_value=("London", 51.5, -0.1)):
            with patch("bonding.weather_client._fetch_ensemble_range", side_effect=RuntimeError("boom")):
                return await get_ecmwf_forecast("London", date(2026, 4, 15))

    assert asyncio.run(run()) is None
