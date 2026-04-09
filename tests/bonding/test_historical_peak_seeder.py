import json
import os
import tempfile
from unittest.mock import AsyncMock, patch
import pytest
from bonding.historical_peak_seeder import (
    extract_daily_peak_hours,
    needs_seeding,
    SEED_MIN_SAMPLES,
)


def test_extract_daily_peak_hours_returns_hour_per_day():
    """Given hourly data for 2 days, returns a list of (date_str, peak_hour) tuples."""
    times = []
    temps = []
    # Day 1: peak at hour 15
    for h in range(24):
        times.append(f"2024-01-01T{h:02d}:00")
        temps.append(10.0 + (1.0 if h == 15 else 0.0))
    # Day 2: peak at hour 13
    for h in range(24):
        times.append(f"2024-01-02T{h:02d}:00")
        temps.append(10.0 + (1.0 if h == 13 else 0.0))

    raw = {"hourly": {"time": times, "temperature_2m": temps}}
    result = extract_daily_peak_hours(raw)
    assert result == [("2024-01-01", 15), ("2024-01-02", 13)]


def test_extract_daily_peak_hours_skips_none_temps():
    """Hours with None temperature are ignored."""
    times = [f"2024-03-01T{h:02d}:00" for h in range(24)]
    temps = [None] * 24
    temps[14] = 20.0
    raw = {"hourly": {"time": times, "temperature_2m": temps}}
    result = extract_daily_peak_hours(raw)
    assert result == [("2024-03-01", 14)]


def test_needs_seeding_true_when_city_missing():
    assert needs_seeding("Seattle", stats={}) is True


def test_needs_seeding_true_when_samples_below_threshold():
    stats = {
        "Seattle": {
            "monthly": {
                str(m): {"hour_counts": [0]*24, "sample_count": 5, "p75_peak_hour": 14}
                for m in range(1, 13)
            }
        }
    }
    assert needs_seeding("Seattle", stats=stats) is True


def test_needs_seeding_false_when_all_months_have_enough_samples():
    stats = {
        "Seattle": {
            "monthly": {
                str(m): {"hour_counts": [0]*24, "sample_count": SEED_MIN_SAMPLES, "p75_peak_hour": 14}
                for m in range(1, 13)
            }
        }
    }
    assert needs_seeding("Seattle", stats=stats) is False
