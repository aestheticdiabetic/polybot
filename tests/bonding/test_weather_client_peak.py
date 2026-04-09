"""
Tests for the forecast_peak_hour extraction and dynamic gate logic
added to weather_client._parse_nearterm_forecast in Task 3.
"""
import json
import os
import tempfile
from datetime import date, timedelta

import pytest

import bonding.weather_client as wc
from bonding.weather_client import _parse_nearterm_forecast, init_peak_stats


def _make_hourly_raw(target_date: date, peak_hour: int, base_temp: float = 20.0) -> dict:
    """Build a minimal Open-Meteo hourly response for a single day."""
    date_str = target_date.isoformat()
    times = [f"{date_str}T{h:02d}:00" for h in range(24)]
    temps = [base_temp + (5.0 if h == peak_hour else 0.0) for h in range(24)]
    return {
        "hourly": {"time": times, "temperature_2m": temps},
        "utc_offset_seconds": 0,
        "current": {"temperature_2m": base_temp},
    }


def test_forecast_peak_hour_extracted_correctly():
    """forecast_peak_hour in ForecastResult is between 0-23 for a today-dated call."""
    today = date.today()
    raw = _make_hourly_raw(today, peak_hour=15)
    result = _parse_nearterm_forecast("Seattle", today, raw)
    # forecast_peak_hour must be set and within valid range for a same-day parse
    assert result.forecast_peak_hour is not None
    assert 0 <= result.forecast_peak_hour <= 23


def test_forecast_peak_hour_matches_max_temp_hour():
    """forecast_peak_hour should correspond to the hour with the highest temperature."""
    today = date.today()
    peak_hour = 13
    raw = _make_hourly_raw(today, peak_hour=peak_hour)
    result = _parse_nearterm_forecast("Seattle", today, raw)
    # The peak hour in the result must be the hour that has the highest temp (13)
    assert result.forecast_peak_hour == peak_hour


def test_forecast_peak_hour_is_none_for_future_date():
    """For a next-day forecast, forecast_peak_hour should be None."""
    tomorrow = date.today() + timedelta(days=1)
    raw = _make_hourly_raw(tomorrow, peak_hour=14)
    result = _parse_nearterm_forecast("Seattle", tomorrow, raw)
    assert result.forecast_peak_hour is None


def test_forecast_result_has_expected_fields():
    """ForecastResult returned by _parse_nearterm_forecast has all required fields."""
    today = date.today()
    raw = _make_hourly_raw(today, peak_hour=14, base_temp=22.0)
    result = _parse_nearterm_forecast("Seattle", today, raw)
    assert result.city == "Seattle"
    assert result.target_date == today
    assert isinstance(result.daily_max_c, float)
    assert isinstance(result.ensemble_members, list)
    assert len(result.ensemble_members) == wc.NEARTERM_MEMBERS


def test_running_max_floors_ensemble_members():
    """For same-day, all synthetic members should be >= the observed running max."""
    today = date.today()
    # High base_temp means running_max will be substantial
    raw = _make_hourly_raw(today, peak_hour=23, base_temp=30.0)
    result = _parse_nearterm_forecast("Seattle", today, raw)
    # Each member must be at or above the running max (30.0°C for observed hours)
    assert all(m >= 30.0 for m in result.ensemble_members)


def test_next_day_sigma_yields_spread():
    """Next-day ensemble should have spread > 0 (sigma=1.5°C, 100 members)."""
    tomorrow = date.today() + timedelta(days=1)
    raw = _make_hourly_raw(tomorrow, peak_hour=15, base_temp=20.0)
    result = _parse_nearterm_forecast("Seattle", tomorrow, raw)
    spread = max(result.ensemble_members) - min(result.ensemble_members)
    assert spread > 0.0


def test_init_peak_stats_loads_into_module_global():
    """init_peak_stats() populates _peak_hour_stats from the given path."""
    stats = {
        "Seattle": {
            "monthly": {
                "7": {
                    "hour_counts": [0] * 24,
                    "sample_count": 62,
                    "p75_peak_hour": 16,
                }
            },
            "last_seeded": "2024-01-01",
            "last_observed": None,
        }
    }
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as f:
        json.dump(stats, f)
        path = f.name
    try:
        init_peak_stats(path)
        assert "Seattle" in wc._peak_hour_stats
        assert wc._peak_hour_stats["Seattle"]["monthly"]["7"]["p75_peak_hour"] == 16
    finally:
        os.unlink(path)
        # Reset global to empty for other tests
        wc._peak_hour_stats = {}


def test_init_peak_stats_missing_file_returns_empty():
    """init_peak_stats() with a non-existent path leaves _peak_hour_stats empty."""
    wc._peak_hour_stats = {}
    init_peak_stats("/nonexistent/path/stats.json")
    assert wc._peak_hour_stats == {}
