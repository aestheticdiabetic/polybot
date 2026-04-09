# Dynamic Peak-Hour Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hardcoded 10-hour/2pm entry gate in BOND mode with a dynamic gate anchored to each city's historically-observed and forecast peak temperature hour, bucketed by calendar month.

**Architecture:** Two new modules (`peak_hour_stats.py`, `historical_peak_seeder.py`) handle data storage and seeding. `ForecastResult` gains a `forecast_peak_hour` field populated by the near-term parser. The scorer, price feed, and weather client all call `get_gate_hour()` from `peak_hour_stats.py` instead of comparing against the hardcoded constants.

**Tech Stack:** Python 3.11+, aiohttp (already a dependency), json (stdlib), pytest for tests.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `bot/bonding/peak_hour_stats.py` | Load/query/update peak hour distributions |
| Create | `bot/bonding/historical_peak_seeder.py` | One-time bootstrap from Open-Meteo archive API |
| Create | `bot/bonding/data/peak_hour_stats.json` | Runtime data file (written by seeder + recorder) |
| Create | `tests/bonding/test_peak_hour_stats.py` | Unit tests for stats module |
| Create | `tests/bonding/test_historical_peak_seeder.py` | Unit tests for seeder |
| Modify | `bot/bonding/weather_client.py` | Add `forecast_peak_hour` to `ForecastResult`; dynamic decay anchor; record observations |
| Modify | `bot/bonding/opportunity_scorer.py` | Replace `hours_to_day_end < BOND_MIN_ENTRY_HOURS` with `get_gate_hour()` |
| Modify | `bot/bonding/price_feed.py` | Same gate replacement in WS path |
| Modify | `bot/main.py` | Call seeder at startup before bonding loop |
| Modify | `bot/config.py` | Mark `BOND_MIN_ENTRY_HOURS` as fallback-only |

---

## Task 1: Create `peak_hour_stats.py` — core data module

**Files:**
- Create: `bot/bonding/peak_hour_stats.py`
- Test: `tests/bonding/test_peak_hour_stats.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/bonding/test_peak_hour_stats.py
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
```

- [ ] **Step 2: Run tests to confirm they all fail**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_peak_hour_stats.py -v 2>&1 | head -40
```
Expected: ImportError or ModuleNotFoundError — `peak_hour_stats` doesn't exist yet.

- [ ] **Step 3: Implement `bot/bonding/peak_hour_stats.py`**

```python
"""
peak_hour_stats.py — Per-city, per-month peak temperature hour distributions.

Provides:
  load_stats()       — load JSON file into memory dict
  save_stats()       — persist to JSON file
  get_gate_hour()    — dynamic entry gate for the scorer / price feed / weather client
  record_observation() — append a confirmed daily peak hour observation
  compute_p75()      — derive 75th-percentile peak hour from a count distribution
"""
import json
import logging
import os
from datetime import date
from typing import Optional

log = logging.getLogger("bond.peak_stats")

_STATS_PATH = os.environ.get("PEAK_HOUR_STATS_PATH", "/app/data/peak_hour_stats.json")
_FALLBACK_GATE_HOUR = 15  # 14 + 1: safe default when no data exists
_SEED_MIN_SAMPLES = 100   # minimum sample_count before a city-month is considered seeded


def compute_p75(hour_counts: list[int]) -> int:
    """
    Return the 75th-percentile peak hour from a 24-element count distribution.
    Falls back to 14 (existing conservative default) if the distribution is empty.
    """
    total = sum(hour_counts)
    if total == 0:
        return 14
    target = total * 0.75
    cumulative = 0
    for hour, count in enumerate(hour_counts):
        cumulative += count
        if cumulative >= target:
            return hour
    return 23  # should not be reached


def load_stats(path: str = _STATS_PATH) -> dict:
    """
    Load peak hour stats from JSON. Returns empty dict if file is missing or corrupt.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning(f"peak_stats: failed to load {path}: {exc}")
        return {}


def save_stats(stats: dict, path: str = _STATS_PATH) -> None:
    """Persist stats dict to JSON, creating parent directories if needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(stats, f, indent=2)
    os.replace(tmp, path)


def get_gate_hour(
    city: str,
    forecast_peak_hour: Optional[int],
    month: int,
    stats: dict,
) -> int:
    """
    Return the dynamic entry gate hour for a city in a given month.

    gate = max(forecast_peak_hour, p75_historical_for_city_month) + 1

    Falls back to _FALLBACK_GATE_HOUR (15) if no stats exist for the city/month.
    forecast_peak_hour may be None for ensemble forecasts (2+ day markets);
    in that case only the historical P75 is used.
    """
    city_data = stats.get(city)
    if not city_data:
        return _FALLBACK_GATE_HOUR

    month_key = str(month)
    bucket = city_data.get("monthly", {}).get(month_key)
    if not bucket:
        return _FALLBACK_GATE_HOUR

    p75 = bucket.get("p75_peak_hour", 14)
    if forecast_peak_hour is not None:
        return max(forecast_peak_hour, p75) + 1
    return p75 + 1


def record_observation(
    city: str,
    month: int,
    peak_hour: int,
    stats: dict,
    path: str = _STATS_PATH,
) -> None:
    """
    Record a confirmed daily peak hour observation for a city-month bucket.
    Recomputes p75_peak_hour and persists to disk.
    """
    if city not in stats:
        stats[city] = {"monthly": {}, "last_seeded": None, "last_observed": None}

    monthly = stats[city].setdefault("monthly", {})
    month_key = str(month)
    if month_key not in monthly:
        monthly[month_key] = {
            "hour_counts": [0] * 24,
            "sample_count": 0,
            "p75_peak_hour": 14,
        }

    bucket = monthly[month_key]
    if 0 <= peak_hour <= 23:
        bucket["hour_counts"][peak_hour] += 1
        bucket["sample_count"] += 1
        bucket["p75_peak_hour"] = compute_p75(bucket["hour_counts"])

    stats[city]["last_observed"] = date.today().isoformat()
    save_stats(stats, path)
    log.debug(
        f"peak_stats: recorded {city} month={month} peak_hour={peak_hour} "
        f"→ p75={bucket['p75_peak_hour']}"
    )
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_peak_hour_stats.py -v
```
Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd x:/CODING/polybot && git add bot/bonding/peak_hour_stats.py tests/bonding/test_peak_hour_stats.py && git commit -m "feat: add peak_hour_stats module with P75 gate logic and monthly buckets"
```

---

## Task 2: Create `historical_peak_seeder.py` — bootstrap from Open-Meteo archive

**Files:**
- Create: `bot/bonding/historical_peak_seeder.py`
- Test: `tests/bonding/test_historical_peak_seeder.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/bonding/test_historical_peak_seeder.py
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
    """Given hourly data for 3 days, returns a list of (date_str, peak_hour) tuples."""
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_historical_peak_seeder.py -v 2>&1 | head -30
```
Expected: ImportError — module doesn't exist yet.

- [ ] **Step 3: Implement `bot/bonding/historical_peak_seeder.py`**

```python
"""
historical_peak_seeder.py — Bootstrap per-city peak-hour distributions from the
Open-Meteo archive API.

For each city in BOND_CITIES that hasn't been seeded yet, fetches 2 years of
hourly temperature data (1 API call per city) and populates the monthly
hour_counts buckets in peak_hour_stats.json.

Seeding is lazy: only cities below SEED_MIN_SAMPLES per month are seeded.
Once a city has enough data it is never re-seeded.
"""
import asyncio
import logging
import time
from datetime import date, timedelta
from typing import Optional

import aiohttp

import config as _config
from bonding.peak_hour_stats import (
    compute_p75,
    load_stats,
    save_stats,
    needs_seeding,
    _STATS_PATH,
)

log = logging.getLogger("bond.seeder")

HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"
SEED_MIN_SAMPLES   = 100   # minimum samples per city-month before skipping
_SEED_YEARS        = 2     # how many years of history to fetch
_REQUEST_INTERVAL  = 3.0   # seconds between API calls (matches weather_client rate)


def needs_seeding(city: str, stats: dict) -> bool:
    """
    Return True if any calendar month for this city has fewer than
    SEED_MIN_SAMPLES observations (or the city is absent entirely).
    """
    city_data = stats.get(city)
    if not city_data:
        return True
    monthly = city_data.get("monthly", {})
    for m in range(1, 13):
        bucket = monthly.get(str(m), {})
        if bucket.get("sample_count", 0) < SEED_MIN_SAMPLES:
            return True
    return False


def extract_daily_peak_hours(raw: dict) -> list[tuple[str, int]]:
    """
    Given Open-Meteo archive API response, return list of (date_str, peak_hour)
    where peak_hour is the local hour index (0–23) with the highest temperature.
    """
    hourly = raw.get("hourly", {})
    times: list[str] = hourly.get("time", [])
    temps: list      = hourly.get("temperature_2m", [])

    days: dict[str, list[tuple[int, float]]] = {}
    for ts, t in zip(times, temps):
        if t is None:
            continue
        date_str = ts[:10]
        hour     = int(ts[11:13])
        days.setdefault(date_str, []).append((hour, float(t)))

    result = []
    for date_str in sorted(days):
        hour_temps = days[date_str]
        if hour_temps:
            peak_hour = max(hour_temps, key=lambda x: x[1])[0]
            result.append((date_str, peak_hour))
    return result


async def fetch_historical_hourly(
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
) -> dict:
    """
    Fetch hourly temperature_2m for a lat/lon over a date range from
    the Open-Meteo archive API. Returns raw API response dict.
    timezone=auto ensures returned timestamps are local to the city.
    """
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "hourly":     "temperature_2m",
        "timezone":   "auto",
        "start_date": start_date.isoformat(),
        "end_date":   end_date.isoformat(),
    }
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(HISTORICAL_API_URL, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()


async def seed_missing_cities(
    cities: dict[str, tuple[float, float]],
    stats: dict,
    path: str = _STATS_PATH,
) -> None:
    """
    For each city in `cities` that needs seeding, fetch 2 years of hourly
    archive data and populate the monthly peak-hour distributions.

    Cities are processed sequentially with _REQUEST_INTERVAL seconds between
    calls to stay well within Open-Meteo rate limits.
    """
    end_date   = date.today() - timedelta(days=1)  # archive is not real-time
    start_date = end_date - timedelta(days=365 * _SEED_YEARS)

    to_seed = [city for city in cities if needs_seeding(city, stats)]
    if not to_seed:
        log.info("peak_seeder: all cities already seeded — nothing to do")
        return

    log.info(f"peak_seeder: seeding {len(to_seed)} cities from {start_date} to {end_date}")

    last_request = 0.0
    for city in to_seed:
        lat, lon = cities[city]
        gap = time.time() - last_request
        if gap < _REQUEST_INTERVAL:
            await asyncio.sleep(_REQUEST_INTERVAL - gap)

        try:
            raw = await fetch_historical_hourly(lat, lon, start_date, end_date)
        except Exception as exc:
            log.warning(f"peak_seeder: failed to fetch {city}: {exc}")
            last_request = time.time()
            continue

        last_request = time.time()
        daily_peaks = extract_daily_peak_hours(raw)

        if city not in stats:
            stats[city] = {"monthly": {}, "last_seeded": None, "last_observed": None}

        monthly = stats[city].setdefault("monthly", {})
        for date_str, peak_hour in daily_peaks:
            month_key = date_str[5:7].lstrip("0") or "1"  # "01" → "1"
            if month_key not in monthly:
                monthly[month_key] = {
                    "hour_counts": [0] * 24,
                    "sample_count": 0,
                    "p75_peak_hour": 14,
                }
            bucket = monthly[month_key]
            if 0 <= peak_hour <= 23:
                bucket["hour_counts"][peak_hour] += 1
                bucket["sample_count"] += 1

        # Recompute P75 for all months after bulk insert
        for bucket in monthly.values():
            bucket["p75_peak_hour"] = compute_p75(bucket["hour_counts"])

        from datetime import date as _date
        stats[city]["last_seeded"] = _date.today().isoformat()
        save_stats(stats, path)
        log.info(
            f"peak_seeder: {city} seeded — "
            f"{len(daily_peaks)} days across "
            f"{len(monthly)} months"
        )
```

Note: `needs_seeding` is defined in this module (not `peak_hour_stats`) because it uses `SEED_MIN_SAMPLES` which is seeder-specific. The import in the test file matches.

- [ ] **Step 4: Fix the import in `peak_hour_stats.py`**

The test imports `needs_seeding` from `historical_peak_seeder`. Remove the reference to it in `peak_hour_stats.py` (it was never added there — just confirm the module doesn't accidentally define it).

- [ ] **Step 5: Run tests to confirm they pass**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_historical_peak_seeder.py -v
```
Expected: all 5 tests PASS.

- [ ] **Step 6: Commit**

```bash
cd x:/CODING/polybot && git add bot/bonding/historical_peak_seeder.py tests/bonding/test_historical_peak_seeder.py && git commit -m "feat: add historical peak hour seeder from Open-Meteo archive API"
```

---

## Task 3: Add `forecast_peak_hour` to `ForecastResult` and update `_parse_nearterm_forecast`

**Files:**
- Modify: `bot/bonding/weather_client.py`

This task adds the `forecast_peak_hour: Optional[int]` field to `ForecastResult` (line 83), extracts it in `_parse_nearterm_forecast`, updates the post-peak decay to use the dynamic gate from `peak_hour_stats`, and records observations once the gate hour has passed.

- [ ] **Step 1: Add `forecast_peak_hour` to `ForecastResult` dataclass**

In `bot/bonding/weather_client.py`, find the `ForecastResult` dataclass at line 83 and add the new field:

```python
@dataclass
class ForecastResult:
    city: str
    target_date: date
    daily_max_c: float            # ensemble control or hourly forecast daily high (°C)
    ensemble_members: list[float] # daily max from each member (real or synthetic) (°C)
    forecast_peak_hour: Optional[int] = None  # local hour (0-23) of forecast daily max; None for ensemble
```

- [ ] **Step 2: Add imports for `peak_hour_stats` and `_observation_recorded` cache**

At the top of `bot/bonding/weather_client.py`, add after the existing imports:

```python
from bonding import peak_hour_stats as _peak_stats
```

And add a module-level cache to prevent duplicate observations:

```python
# (city, date) pairs for which a peak-hour observation has already been recorded today.
# Cleared of stale entries on each access in _parse_nearterm_forecast.
_observation_recorded: set[tuple[str, date]] = set()

# In-memory peak hour stats — loaded once at startup via init_peak_stats()
_peak_hour_stats: dict = {}
```

- [ ] **Step 3: Add `init_peak_stats()` public function**

Add this function to `weather_client.py` after the `_get_rate_lock` function:

```python
def init_peak_stats(path: Optional[str] = None) -> None:
    """Load peak hour stats into module-level cache. Call once at bot startup."""
    global _peak_hour_stats
    if path:
        _peak_hour_stats = _peak_stats.load_stats(path)
    else:
        _peak_hour_stats = _peak_stats.load_stats()
    log.info(f"weather: loaded peak hour stats for {len(_peak_hour_stats)} cities")
```

- [ ] **Step 4: Update `_parse_nearterm_forecast` to extract `forecast_peak_hour`, use dynamic decay anchor, and record observations**

Replace the entire same-day block (lines 541–577) with the updated version. The full replacement for the function's same-day processing section is:

```python
    if target_date == today:
        utc_offset_secs: int = raw.get("utc_offset_seconds", 0)
        local_ts = datetime.now(timezone.utc).timestamp() + utc_offset_secs
        current_hour_local = int((local_ts % 86400) // 3600)
        current_month = int(datetime.now(timezone.utc).strftime("%m"))

        observed = [
            float(t) for ts, t in zip(times, temps)
            if ts.startswith(date_str) and t is not None
            and int(ts[11:13]) <= current_hour_local
        ]
        running_max = max(observed) if observed else None
        sigma = NEARTERM_SIGMA_SAME_DAY

        # Incorporate real-time current temperature (Open-Meteo ~15-min refresh).
        raw_ct = (raw.get("current") or {}).get("temperature_2m")
        if raw_ct is not None:
            try:
                ct = float(raw_ct)
                running_max = max(running_max, ct) if running_max is not None else ct
            except (TypeError, ValueError):
                pass

        # Dynamic post-peak decay anchor: use the city's gate hour from peak_hour_stats.
        # Find forecast peak hour (hour with highest forecast temp for today).
        hour_temps = [
            (int(ts[11:13]), float(t))
            for ts, t in zip(times, temps)
            if ts.startswith(date_str) and t is not None
        ]
        forecast_peak_hour: Optional[int] = (
            max(hour_temps, key=lambda x: x[1])[0] if hour_temps else None
        )
        post_peak_hour = _peak_stats.get_gate_hour(
            city, forecast_peak_hour, current_month, _peak_hour_stats
        ) - 1  # gate_hour is "skip from here"; decay starts 1h before gate

        # Post-peak decay: linearly weight forecast_max → running_max over 2 hours
        # starting at post_peak_hour. At full decay sigma=0 and forecast_max=running_max.
        if running_max is not None and current_hour_local >= post_peak_hour:
            hours_past_peak = current_hour_local - post_peak_hour
            decay_weight = min(hours_past_peak / _POST_PEAK_DECAY_HOURS, 1.0)
            forecast_max = (1.0 - decay_weight) * forecast_max + decay_weight * running_max
            forecast_max = max(running_max, forecast_max)
            sigma *= (1.0 - decay_weight)
            log.debug(
                f"weather nearterm: {city} {date_str} post-peak decay "
                f"hour={current_hour_local} post_peak_hour={post_peak_hour} "
                f"weight={decay_weight:.2f} "
                f"→ forecast_max={forecast_max:.1f}°C sigma={sigma:.2f}"
            )

        # Record observation once per city-day after the gate hour has fully passed.
        gate_hour = post_peak_hour + 1
        if running_max is not None and current_hour_local >= gate_hour:
            # Clean stale entries from previous days
            global _observation_recorded
            today_date = date.today()
            _observation_recorded = {
                (c, d) for c, d in _observation_recorded if d >= today_date
            }
            obs_key = (city, target_date)
            if obs_key not in _observation_recorded:
                # Find the hour the running max was first reached
                peak_obs_hour: Optional[int] = None
                peak_obs_temp = float("-inf")
                for ts, t in zip(times, temps):
                    if ts.startswith(date_str) and t is not None and int(ts[11:13]) <= current_hour_local:
                        if float(t) > peak_obs_temp:
                            peak_obs_temp = float(t)
                            peak_obs_hour = int(ts[11:13])
                if peak_obs_hour is not None:
                    _peak_stats.record_observation(
                        city, current_month, peak_obs_hour, _peak_hour_stats
                    )
                    _observation_recorded.add(obs_key)
    else:
        sigma = NEARTERM_SIGMA_NEXT_DAY
        forecast_peak_hour = None
```

Also update the `return` statement at the end of the function to include `forecast_peak_hour`:

```python
    return ForecastResult(
        city=city,
        target_date=target_date,
        daily_max_c=control,
        ensemble_members=members,
        forecast_peak_hour=forecast_peak_hour if target_date == today else None,
    )
```

- [ ] **Step 5: Verify the module imports cleanly**

```bash
cd x:/CODING/polybot/bot && python -c "from bonding.weather_client import ForecastResult; print(ForecastResult.__dataclass_fields__.keys())"
```
Expected output includes `forecast_peak_hour`.

- [ ] **Step 6: Commit**

```bash
cd x:/CODING/polybot && git add bot/bonding/weather_client.py && git commit -m "feat: add forecast_peak_hour to ForecastResult and dynamic post-peak decay anchor"
```

---

## Task 4: Replace hard gate in `opportunity_scorer.py`

**Files:**
- Modify: `bot/bonding/opportunity_scorer.py` (lines 101–151)

- [ ] **Step 1: Write a failing integration smoke test**

```python
# Add to tests/bonding/test_peak_hour_stats.py

def test_gate_hour_greater_than_14_for_seattle_july():
    """
    With Seattle P75=16 for July and forecast_peak=15, gate should be 17,
    meaning markets are not suppressed until after 5pm local.
    This is the core regression: the old hardcoded gate would block at 14:00.
    """
    stats = {
        "Seattle": {
            "monthly": {
                "7": {"hour_counts": [0]*24, "sample_count": 62, "p75_peak_hour": 16}
            },
            "last_seeded": "2024-01-15",
            "last_observed": "2026-04-08",
        }
    }
    gate = get_gate_hour("Seattle", forecast_peak_hour=15, month=7, stats=stats)
    assert gate == 17
    assert gate > 14  # must be later than the old hardcoded cutoff
```

- [ ] **Step 2: Run to confirm it passes (it uses already-implemented logic)**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_peak_hour_stats.py::test_gate_hour_greater_than_14_for_seattle_july -v
```
Expected: PASS (the stats module already handles this correctly).

- [ ] **Step 3: Update `opportunity_scorer.py` — replace the time gate block**

Add import at top of `opportunity_scorer.py` (after existing imports):

```python
from bonding import peak_hour_stats as _peak_stats
from bonding.weather_client import _peak_hour_stats as _loaded_stats
```

Replace the entire gate block (lines 108–151) in `score_market()`:

```python
    # Dynamic peak-hour gate: skip markets where the city's peak hour has passed.
    # gate_hour = max(forecast_peak_hour_today, p75_historical_for_city_month) + 1
    # This replaces the old hardcoded BOND_MIN_ENTRY_HOURS=10 (≈14:00 local) gate.
    tz_name = _config.BOND_CITY_TIMEZONES.get(market.city)
    if not tz_name:
        log.warning(
            f"scorer: {market.city} {market.target_date} — no timezone configured, skipping "
            f"(add to BOND_CITY_TIMEZONES in config.py)"
        )
        return None
    try:
        city_tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        log.warning(
            f"scorer: {market.city} {market.target_date} — invalid timezone '{tz_name}', skipping"
        )
        return None

    now_local = datetime.now(timezone.utc).astimezone(city_tz)

    # Only apply the gate for today's markets. Future-date markets are never
    # close enough to their local day-end for the gate to be relevant.
    target_is_today = market.target_date == now_local.date()
    if target_is_today:
        current_local_hour = now_local.hour
        current_month      = now_local.month
        forecast_peak_hour = forecast.forecast_peak_hour  # None for ensemble forecasts
        gate_hour = _peak_stats.get_gate_hour(
            market.city, forecast_peak_hour, current_month, _loaded_stats
        )
        if current_local_hour >= gate_hour:
            # Suppress until local midnight so we don't re-evaluate every 60s
            next_day = market.target_date + timedelta(days=1)
            end_of_day_utc = datetime(
                next_day.year, next_day.month, next_day.day, 0, 0, 0,
                tzinfo=city_tz,
            ).astimezone(timezone.utc)
            suppress_secs = max(
                (end_of_day_utc - datetime.now(timezone.utc)).total_seconds(), 0
            ) + 300  # 5 min buffer past midnight
            _scan_suppressions[(market.city, market.target_date)] = (
                time.time() + suppress_secs
            )
            log.info(
                f"scorer: {market.city} {market.target_date} — "
                f"past gate hour {gate_hour} (current={current_local_hour}, "
                f"forecast_peak={forecast_peak_hour}, month={current_month}): "
                f"suppressed for {suppress_secs/3600:.1f}h"
            )
            return None
    else:
        # Check that the target day hasn't already ended (handles edge cases
        # where a future market flips to "today" during a scan cycle).
        next_day = market.target_date + timedelta(days=1)
        end_of_day_utc = datetime(
            next_day.year, next_day.month, next_day.day, 0, 0, 0,
            tzinfo=city_tz,
        ).astimezone(timezone.utc)
        hours_to_day_end = (end_of_day_utc - datetime.now(timezone.utc)).total_seconds() / 3600
        if hours_to_day_end <= 0:
            suppress_secs = 24 * 3600
            _scan_suppressions[(market.city, market.target_date)] = (
                time.time() + suppress_secs
            )
            log.info(
                f"scorer: {market.city} {market.target_date} — "
                f"local day already ended: suppressed for 24h"
            )
            return None
```

- [ ] **Step 4: Verify module imports cleanly**

```bash
cd x:/CODING/polybot/bot && python -c "from bonding.opportunity_scorer import score_market; print('OK')"
```
Expected: `OK`

- [ ] **Step 5: Commit**

```bash
cd x:/CODING/polybot && git add bot/bonding/opportunity_scorer.py && git commit -m "feat: replace hardcoded 10h entry gate with dynamic peak-hour gate in scorer"
```

---

## Task 5: Replace hard gate in `price_feed.py`

**Files:**
- Modify: `bot/bonding/price_feed.py` (lines 240–277)

- [ ] **Step 1: Add imports to `price_feed.py`**

At the top of `price_feed.py`, add after existing imports:

```python
from bonding import peak_hour_stats as _peak_stats
from bonding.weather_client import _peak_hour_stats as _loaded_stats
```

- [ ] **Step 2: Replace the gate block (lines 240–277)**

Replace from the comment `# If the target day is nearly over...` through `return` at line 277 with:

```python
        # Dynamic peak-hour gate — mirrors scorer logic exactly.
        import config as _config
        tz_name = _config.BOND_CITY_TIMEZONES.get(market.city)
        if not tz_name:
            log.warning(
                f"feed: {market.city} {market.target_date} — no timezone configured, skipping"
            )
            self._cooldowns[asset_id] = time.time() - COOLDOWN_SECS + 300
            return
        try:
            city_tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            log.warning(
                f"feed: {market.city} {market.target_date} — invalid timezone '{tz_name}', skipping"
            )
            self._cooldowns[asset_id] = time.time() - COOLDOWN_SECS + 300
            return

        now_local = datetime.now(timezone.utc).astimezone(city_tz)
        if market.target_date == now_local.date():
            current_local_hour = now_local.hour
            current_month      = now_local.month
            forecast_peak_hour = forecast.forecast_peak_hour
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
                    (end_of_day_utc - datetime.now(timezone.utc)).total_seconds() + 300, 300
                )
                self._cooldowns[asset_id] = time.time() - COOLDOWN_SECS + suppress_secs
                log.info(
                    f"feed: {market.city} {market.target_date} — "
                    f"past gate hour {gate_hour} (current={current_local_hour}): "
                    f"suppressed {suppress_secs/3600:.1f}h"
                )
                return
```

- [ ] **Step 3: Verify module imports cleanly**

```bash
cd x:/CODING/polybot/bot && python -c "from bonding.price_feed import BondPriceFeed; print('OK')"
```
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
cd x:/CODING/polybot && git add bot/bonding/price_feed.py && git commit -m "feat: replace hardcoded gate in price_feed WS path with dynamic peak-hour gate"
```

---

## Task 6: Wire seeder into `main.py` and update `bot/config.py`

**Files:**
- Modify: `bot/main.py`
- Modify: `bot/config.py`

- [ ] **Step 1: Update `bot/config.py` — mark `BOND_MIN_ENTRY_HOURS` as fallback-only**

Find line 165 in `bot/config.py` and update the comment:

```python
BOND_MIN_ENTRY_HOURS      = 10    # FALLBACK ONLY — superseded by dynamic peak-hour gate
                                  # (peak_hour_stats.py). Retained as safety net when
                                  # peak_hour_stats.json is absent.
```

- [ ] **Step 2: Add seeder call to `run_bonding_loop` in `main.py`**

In `run_bonding_loop`, after the imports block (around line 63), add the seeder call before the initial scan:

```python
    # Seed peak-hour stats for any cities missing data.
    # Runs once at startup: ~65 API calls at 3s intervals (~3 min total).
    from bonding.historical_peak_seeder import seed_missing_cities
    from bonding.weather_client import init_peak_stats
    from bonding.peak_hour_stats import _STATS_PATH

    init_peak_stats()  # load existing stats into memory
    from bonding.weather_client import _peak_hour_stats as _loaded_stats_ref
    # Import config for city list
    from config import BOND_CITIES
    await seed_missing_cities(BOND_CITIES, _loaded_stats_ref)
    init_peak_stats()  # reload after seeding to pick up new data
    log.info("BOND mode: peak hour stats seeded and loaded")
```

Place this block **before** the line `log.info("BOND mode: running initial scan...")` at line 94.

- [ ] **Step 3: Verify bot starts without errors (dry-run import check)**

```bash
cd x:/CODING/polybot/bot && python -c "
import asyncio
import sys
sys.path.insert(0, '.')
from bonding.peak_hour_stats import load_stats, get_gate_hour
from bonding.historical_peak_seeder import needs_seeding
from bonding.weather_client import init_peak_stats, ForecastResult
print('All imports OK')
print('ForecastResult fields:', list(ForecastResult.__dataclass_fields__.keys()))
"
```
Expected:
```
All imports OK
ForecastResult fields: ['city', 'target_date', 'daily_max_c', 'ensemble_members', 'forecast_peak_hour']
```

- [ ] **Step 4: Commit**

```bash
cd x:/CODING/polybot && git add bot/main.py bot/config.py && git commit -m "feat: wire peak hour seeder into BOND mode startup"
```

---

## Task 7: Create the data directory placeholder and verify end-to-end

**Files:**
- Create: `bot/bonding/data/.gitkeep`

- [ ] **Step 1: Create the data directory placeholder**

```bash
touch x:/CODING/polybot/bot/bonding/data/.gitkeep
```

- [ ] **Step 2: Run all tests**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/ -v
```
Expected: all tests PASS.

- [ ] **Step 3: Spot-check seeder output for Seattle**

Run a quick seeder smoke test in a Python shell:

```bash
cd x:/CODING/polybot/bot && python -c "
import asyncio, json
from bonding.peak_hour_stats import load_stats
from bonding.historical_peak_seeder import seed_missing_cities, needs_seeding
from config import BOND_CITIES

stats = load_stats()
print('Cities needing seed:', [c for c in ['Seattle', 'Phoenix', 'Miami'] if needs_seeding(c, stats)])

async def run():
    test_cities = {k: v for k, v in BOND_CITIES.items() if k in ['Seattle', 'Phoenix', 'Miami']}
    await seed_missing_cities(test_cities, stats)
    for city in test_cities:
        monthly = stats.get(city, {}).get('monthly', {})
        for m in ['1', '4', '7', '10']:
            bucket = monthly.get(m, {})
            print(f'{city} month={m}: p75={bucket.get(\"p75_peak_hour\", \"n/a\")} samples={bucket.get(\"sample_count\", 0)}')

asyncio.run(run())
"
```
Expected: Seattle July p75 ~15–16, Seattle January p75 ~12–13, Phoenix July p75 ~13–14.

- [ ] **Step 4: Verify gate is now later than 14 for Seattle in summer**

```bash
cd x:/CODING/polybot/bot && python -c "
from bonding.peak_hour_stats import load_stats, get_gate_hour
stats = load_stats()
gate = get_gate_hour('Seattle', forecast_peak_hour=15, month=7, stats=stats)
print(f'Seattle July gate (forecast_peak=15): {gate}')
assert gate > 14, f'Gate {gate} should be > 14 for Seattle July'
print('PASS: gate is later than the old 14:00 hardcoded cutoff')
"
```

- [ ] **Step 5: Final commit**

```bash
cd x:/CODING/polybot && git add bot/bonding/data/.gitkeep tests/ && git commit -m "feat: add data directory placeholder and verify dynamic peak-hour gate end-to-end"
```
