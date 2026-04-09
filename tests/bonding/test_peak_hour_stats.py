import json
import os
import tempfile
import pytest
from bonding.peak_hour_stats import (
    compute_p75,
    load_stats,
    get_gate_hour,
    record_observation,
    save_stats,
)


def test_compute_p75_uniform():
    """P75 of a uniform distribution across hours 12-17 should be 16."""
    counts = [0] * 24
    for h in range(12, 18):
        counts[h] = 10  # 60 total samples, 6 hours
    assert compute_p75(counts) == 16


def test_compute_p75_concentrated():
    """P75 when all observations cluster at hour 15."""
    counts = [0] * 24
    counts[15] = 100
    assert compute_p75(counts) == 15


def test_compute_p75_empty_returns_fallback():
    """Empty counts fall back to hour 14 (existing conservative default)."""
    assert compute_p75([0] * 24) == 14


def test_get_gate_hour_uses_max_of_forecast_and_p75():
    """Gate = max(forecast_peak, p75_for_city_month) + 1."""
    stats = {
        "Seattle": {
            "monthly": {
                "7": {"hour_counts": [0]*24, "sample_count": 62, "p75_peak_hour": 16}
            },
            "last_seeded": None,
            "last_observed": None,
        }
    }
    # forecast peak earlier than P75 → use P75
    assert get_gate_hour("Seattle", forecast_peak_hour=14, month=7, stats=stats) == 17
    # forecast peak later than P75 → use forecast
    assert get_gate_hour("Seattle", forecast_peak_hour=18, month=7, stats=stats) == 19


def test_get_gate_hour_fallback_when_no_city():
    """Falls back to 15 when city has no data."""
    assert get_gate_hour("Unknown City", forecast_peak_hour=None, month=4, stats={}) == 15


def test_get_gate_hour_fallback_when_no_month_bucket():
    """Falls back to 15 when month bucket is missing."""
    stats = {"Seattle": {"monthly": {}, "last_seeded": None, "last_observed": None}}
    assert get_gate_hour("Seattle", forecast_peak_hour=None, month=4, stats=stats) == 15


def test_get_gate_hour_with_none_forecast_uses_p75_only():
    """When forecast_peak_hour is None, gate = p75 + 1."""
    stats = {
        "Seattle": {
            "monthly": {
                "4": {"hour_counts": [0]*24, "sample_count": 61, "p75_peak_hour": 15}
            },
            "last_seeded": None,
            "last_observed": None,
        }
    }
    assert get_gate_hour("Seattle", forecast_peak_hour=None, month=4, stats=stats) == 16


def test_record_observation_updates_counts_and_p75():
    """record_observation increments hour_counts and recomputes p75_peak_hour."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({}, f)
        path = f.name
    try:
        stats = {}
        for _ in range(80):
            record_observation("Seattle", month=7, peak_hour=15, stats=stats, path=path)
        bucket = stats["Seattle"]["monthly"]["7"]
        assert bucket["hour_counts"][15] == 80
        assert bucket["sample_count"] == 80
        assert bucket["p75_peak_hour"] == 15
    finally:
        os.unlink(path)


def test_load_stats_returns_empty_dict_for_missing_file():
    assert load_stats("/nonexistent/path.json") == {}
