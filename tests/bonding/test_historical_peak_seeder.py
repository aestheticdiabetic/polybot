import json
import os
import tempfile
from unittest.mock import AsyncMock, patch, MagicMock
import pytest
import pytest_asyncio
import aiohttp
from bonding.historical_peak_seeder import (
    extract_daily_peak_hours,
    needs_seeding,
    seed_missing_cities,
    SEED_MIN_SAMPLES,
)

pytest_plugins = ("pytest_asyncio",)


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


@pytest.mark.asyncio
async def test_seed_missing_cities_populates_buckets():
    """seed_missing_cities fetches data and populates monthly buckets."""
    cities = {"Seattle": (47.6062, -122.3321)}
    stats = {}

    # Synthetic 2-day archive response: Jan 1 peaks at 15, Jan 2 peaks at 13
    mock_raw = {
        "hourly": {
            "time": [f"2024-01-01T{h:02d}:00" for h in range(24)] +
                    [f"2024-01-02T{h:02d}:00" for h in range(24)],
            "temperature_2m": [10.0 + (5.0 if h == 15 else 0.0) for h in range(24)] +
                              [10.0 + (5.0 if h == 13 else 0.0) for h in range(24)],
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({}, f)
        path = f.name
    try:
        with patch(
            "bonding.historical_peak_seeder.fetch_historical_hourly",
            new=AsyncMock(return_value=mock_raw)
        ):
            await seed_missing_cities(cities, stats, path=path)

        assert "Seattle" in stats
        jan_bucket = stats["Seattle"]["monthly"]["1"]
        assert jan_bucket["hour_counts"][15] == 1
        assert jan_bucket["hour_counts"][13] == 1
        assert jan_bucket["sample_count"] == 2
    finally:
        os.unlink(path)


@pytest.mark.asyncio
async def test_seed_missing_cities_skips_already_seeded():
    """Cities with enough samples for all months are not fetched."""
    cities = {"Seattle": (47.6062, -122.3321)}
    stats = {
        "Seattle": {
            "monthly": {
                str(m): {"hour_counts": [0]*24, "sample_count": SEED_MIN_SAMPLES, "p75_peak_hour": 14}
                for m in range(1, 13)
            },
            "last_seeded": "2024-01-01",
            "last_observed": None,
        }
    }

    with patch(
        "bonding.historical_peak_seeder.fetch_historical_hourly",
        new=AsyncMock()
    ) as mock_fetch:
        await seed_missing_cities(cities, stats)
        mock_fetch.assert_not_called()


@pytest.mark.asyncio
async def test_seed_missing_cities_continues_on_api_failure():
    """API failure for one city is logged and processing continues."""
    cities = {"Seattle": (47.6062, -122.3321), "Miami": (25.7617, -80.1918)}
    stats = {}

    mock_raw = {
        "hourly": {
            "time": [f"2024-07-01T{h:02d}:00" for h in range(24)],
            "temperature_2m": [30.0 + (1.0 if h == 14 else 0.0) for h in range(24)],
        }
    }

    call_count = 0

    async def mock_fetch(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise aiohttp.ClientError("connection refused")
        return mock_raw

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({}, f)
        path = f.name
    try:
        with patch("bonding.historical_peak_seeder.fetch_historical_hourly", new=mock_fetch):
            await seed_missing_cities(cities, stats, path=path)

        # One city should have failed, one should have succeeded
        seeded = [c for c in cities if c in stats and stats[c].get("last_seeded")]
        assert len(seeded) == 1
    finally:
        os.unlink(path)
