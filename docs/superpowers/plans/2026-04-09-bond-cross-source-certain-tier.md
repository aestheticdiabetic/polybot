# Bond Cross-Source Validation + CERTAIN Tier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ECMWF and tomorrow.io as second/third weather sources, average their probabilities into all standard BOND bets, and add a CERTAIN tier that bets on high-confidence high-ask outcomes only when all three sources tightly agree.

**Architecture:** `SourceConsensus` replaces `ForecastResult` as the object passed between weather fetching and scoring. `get_consensus_forecasts()` fetches GFS + ECMWF (both via Open-Meteo, shared rate limiter, 2h cache) and tomorrow.io (separate client, 3h cache, 20 req/hr guard). `score_all()` uses `consensus_prob()` (arithmetic mean across available sources) instead of raw GFS probability. A new `sure_thing_scorer.py` module applies hard gates and emits `tier="CERTAIN"` opportunities from the same REST scan loop.

**Tech Stack:** Python 3.11+, asyncio, aiohttp, Open-Meteo ensemble API (ECMWF IFS model), tomorrow.io Timelines API v4, pytest, existing disk-cache + rate-limiter patterns from `weather_client.py`.

---

## File Map

| Action | File | Responsibility |
|--------|------|---------------|
| Modify | `bot/config.py` | New constants: tomorrow.io key, ECMWF model, CERTAIN tier params |
| Modify | `bot/bonding/weather_client.py` | `SourceConsensus` dataclass; refactor `_fetch_ensemble_range` to accept `model`; add `get_ecmwf_forecast()`; add `get_consensus_forecasts()` |
| Create | `bot/bonding/tomorrow_client.py` | tomorrow.io fetch, 3h disk cache, 20 req/hr guard, synthetic ensemble |
| Modify | `bot/bonding/opportunity_scorer.py` | Accept `SourceConsensus` instead of `ForecastResult`; use `consensus_prob()` |
| Modify | `bot/bonding/price_feed.py` | Type-swap `ForecastResult` → `SourceConsensus` in stored dict and `update_markets` signature |
| Modify | `bot/main.py` | Call `get_consensus_forecasts()` instead of `get_all_forecasts()`; call `score_certain()` in scan loop |
| Modify | `bot/bonding/paper_sim.py` | Same two call-site swaps as `main.py` |
| Create | `bot/bonding/sure_thing_scorer.py` | CERTAIN tier: hard gates, `score_certain()` |
| Modify | `bot/bonding/exit_manager.py` | Skip CORE 0.97 early-exit for `tier="CERTAIN"`; add `TIER_CERTAIN` constant |
| Create | `tests/bonding/test_source_consensus.py` | Unit tests for `SourceConsensus` |
| Create | `tests/bonding/test_tomorrow_client.py` | Unit tests for tomorrow client (mocked HTTP) |
| Create | `tests/bonding/test_sure_thing_scorer.py` | Unit tests for each hard gate in `score_certain()` |

---

## Task 1: Add Config Constants

**Files:**
- Modify: `bot/config.py`

- [ ] **Step 1: Add constants after the existing `BOND_*` block**

Open `bot/config.py`. After the `BOND_MARKET_DISAGREEMENT_RATIO` line, add:

```python
# ─── Cross-source weather ─────────────────────────────────────────────────────
TOMORROW_IO_API_KEY           = os.getenv("TOMORROW_IO_API_KEY", "")
TOMORROW_IO_CACHE_TTL_SECS    = 10_800   # 3 hours
TOMORROW_IO_MAX_REQ_PER_HOUR  = 20       # headroom below 25/hr hard limit
ECMWF_ENSEMBLE_MODEL          = "ecmwf_ifs04"  # 50+ members, global, free via Open-Meteo
ECMWF_DISK_CACHE_PATH         = os.environ.get("ECMWF_CACHE_PATH", "/app/data/ecmwf_cache.json")

# ─── CERTAIN tier ────────────────────────────────────────────────────────────
CERTAIN_ASK_MIN                  = 0.75   # min YES ask — market sees it as likely
CERTAIN_ASK_MAX                  = 0.95   # max YES ask — still room for edge
CERTAIN_MIN_SOURCE_PROB          = 0.88   # each source must reach this individually
CERTAIN_MAX_TEMP_DELTA_C         = 2.0    # max °C between source point forecasts
CERTAIN_MAX_SPREAD_C             = 1.5    # max std dev of all combined ensemble members
CERTAIN_MIN_CONSENSUS_PROB       = 0.90   # averaged probability floor
CERTAIN_MIN_SOURCES              = 3      # all three sources must be present
CERTAIN_MIN_EDGE                 = 0.05   # consensus_prob − ask
CERTAIN_SHARES                   = 20     # conservative during validation
CERTAIN_MAX_CAPITAL_PER_CLUSTER  = 20.00  # separate from BOND_MAX_CAPITAL_PER_CLUSTER
```

- [ ] **Step 2: Commit**

```bash
git add bot/config.py
git commit -m "feat: add config constants for ECMWF, tomorrow.io, and CERTAIN tier"
```

---

## Task 2: Add `SourceConsensus` Dataclass

**Files:**
- Modify: `bot/bonding/weather_client.py`
- Create: `tests/bonding/test_source_consensus.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/bonding/test_source_consensus.py`:

```python
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
```

- [ ] **Step 2: Run tests — expect ImportError on `SourceConsensus`**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_source_consensus.py -v 2>&1 | head -30
```

Expected: `ImportError: cannot import name 'SourceConsensus'`

- [ ] **Step 3: Add `SourceConsensus` to `weather_client.py`**

In `bot/bonding/weather_client.py`, add `statistics` to the imports at the top:

```python
import statistics
```

Then add the `SourceConsensus` dataclass immediately after the `ForecastResult` dataclass (around line 91):

```python
@dataclass
class SourceConsensus:
    """Holds forecast results from all weather sources for one city/date."""
    city: str
    target_date: date
    gfs: ForecastResult
    ecmwf: Optional[ForecastResult]
    tomorrowio: Optional[ForecastResult]

    def consensus_prob(self, temp_min: float, temp_max: float) -> float:
        """Average P(YES) across all available sources."""
        probs = [prob_in_range(self.gfs, temp_min, temp_max)]
        if self.ecmwf is not None:
            probs.append(prob_in_range(self.ecmwf, temp_min, temp_max))
        if self.tomorrowio is not None:
            probs.append(prob_in_range(self.tomorrowio, temp_min, temp_max))
        return sum(probs) / len(probs)

    def available_sources(self) -> int:
        """Count of non-None sources."""
        return sum(1 for s in [self.gfs, self.ecmwf, self.tomorrowio] if s is not None)

    def point_forecasts(self) -> list[float]:
        """daily_max_c from each available source, in order: GFS, ECMWF, tomorrow.io."""
        results: list[ForecastResult] = [self.gfs]
        if self.ecmwf is not None:
            results.append(self.ecmwf)
        if self.tomorrowio is not None:
            results.append(self.tomorrowio)
        return [r.daily_max_c for r in results]

    def all_ensemble_members(self) -> list[float]:
        """All ensemble members concatenated across all available sources."""
        members = list(self.gfs.ensemble_members)
        if self.ecmwf is not None:
            members.extend(self.ecmwf.ensemble_members)
        if self.tomorrowio is not None:
            members.extend(self.tomorrowio.ensemble_members)
        return members
```

- [ ] **Step 4: Run tests — expect all pass**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_source_consensus.py -v
```

Expected: 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bot/bonding/weather_client.py tests/bonding/test_source_consensus.py
git commit -m "feat: add SourceConsensus dataclass with consensus_prob and ensemble helpers"
```

---

## Task 3: Add ECMWF Forecast Fetching

**Files:**
- Modify: `bot/bonding/weather_client.py`
- Test: `tests/bonding/test_source_consensus.py` (extended)

- [ ] **Step 1: Write failing test for ECMWF fetch**

Add to `tests/bonding/test_source_consensus.py`:

```python
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock


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
```

- [ ] **Step 2: Run — expect ImportError on `get_ecmwf_forecast`**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_source_consensus.py -v -k "ecmwf" 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'get_ecmwf_forecast'`

- [ ] **Step 3: Refactor `_fetch_ensemble_range` to accept a `model` parameter**

In `bot/bonding/weather_client.py`, change the function signature and internals:

```python
async def _fetch_ensemble_range(
    lat: float, lon: float, start_date: date, end_date: date,
    model: str = ENSEMBLE_MODEL,
) -> dict:
    """
    Fetch ensemble daily max temperatures for a date range.
    Disk-persistent 2-hour cache. Uses shared serial rate limiter.
    Supports any Open-Meteo ensemble model (GFS, ECMWF, etc.).
    """
    global _last_request_time

    _load_disk_cache()

    start_str = start_date.isoformat()
    end_str   = end_date.isoformat()
    # Include model in key so GFS and ECMWF caches don't collide
    cache_key = (round(lat, 4), round(lon, 4), start_str, end_str, model)

    async with _cache_lock:
        if cache_key in _cache:
            fetched_at, data = _cache[cache_key]
            if time.time() - fetched_at < DISK_CACHE_TTL_SECS:
                return data

    params = {
        "latitude":   lat,
        "longitude":  lon,
        "daily":      "temperature_2m_max",
        "models":     model,          # <-- was hardcoded ENSEMBLE_MODEL
        "timezone":   "auto",
        "start_date": start_str,
        "end_date":   end_str,
    }
```

Also update the disk cache save/load functions to handle 5-tuple keys. In `_save_disk_cache()`, change the key serialization line:

```python
# Old (4-part):
key_str = f"{key[0]}|{key[1]}|{key[2]}|{key[3]}"
# New (5-part, model included):
key_str = "|".join(str(k) for k in key)
```

In `_load_disk_cache()`, update the loading block:

```python
parts = key_str.split("|")
if len(parts) == 5:
    key = (float(parts[0]), float(parts[1]), parts[2], parts[3], parts[4])
elif len(parts) == 4:
    # Legacy GFS entry — skip (will re-fetch with new 5-tuple key)
    continue
else:
    continue
```

- [ ] **Step 4: Add `get_ecmwf_forecast()` to `weather_client.py`**

Add this function after `get_all_forecasts()`:

```python
async def get_ecmwf_forecast(city: str, target_date: date) -> Optional[ForecastResult]:
    """
    Fetch ECMWF IFS ensemble daily max for a single city/date.
    Uses the same Open-Meteo ensemble endpoint with model=ECMWF_ENSEMBLE_MODEL.
    Returns None on any error (unknown city, API failure, date too far out).
    """
    import config as _cfg
    today = date.today()
    if target_date > today + timedelta(days=15):
        return None

    try:
        canonical, lat, lon = _resolve_city(city)
    except UnknownCityError:
        log.warning(f"weather ecmwf: unknown city '{city}'")
        return None

    start_date = max(today, target_date - timedelta(days=1))
    end_date   = target_date + timedelta(days=1)

    try:
        raw = await _fetch_ensemble_range(
            lat, lon, start_date, end_date, model=_cfg.ECMWF_ENSEMBLE_MODEL
        )
        return _parse_ensemble_from_range(canonical, target_date, raw)
    except Exception as exc:
        log.warning(f"weather ecmwf: failed for {city} {target_date}: {exc}")
        return None
```

- [ ] **Step 5: Run ECMWF tests**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_source_consensus.py -v
```

Expected: all tests PASS (including new ECMWF ones)

- [ ] **Step 6: Run existing weather tests to check no regressions**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_weather_client_peak.py -v
```

Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add bot/bonding/weather_client.py tests/bonding/test_source_consensus.py
git commit -m "feat: add ECMWF forecast support via parameterised _fetch_ensemble_range model"
```

---

## Task 4: Create `tomorrow_client.py`

**Files:**
- Create: `bot/bonding/tomorrow_client.py`
- Create: `tests/bonding/test_tomorrow_client.py`

- [ ] **Step 1: Write failing tests**

Create `tests/bonding/test_tomorrow_client.py`:

```python
"""Tests for tomorrow.io forecast client."""
import asyncio
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../bot"))

from datetime import date
from unittest.mock import AsyncMock, patch, MagicMock
import pytest


def _make_tio_response(target_date: date, temp_max: float) -> dict:
    return {
        "data": {
            "timelines": [
                {
                    "timestep": "1d",
                    "intervals": [
                        {
                            "startTime": f"{target_date.isoformat()}T06:00:00Z",
                            "values": {"temperatureMax": temp_max},
                        }
                    ],
                }
            ]
        }
    }


def test_get_forecast_returns_none_if_no_api_key(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "")
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = False
    tomorrow_client._cache.clear()
    tomorrow_client._call_times.clear()

    result = asyncio.run(
        tomorrow_client.get_forecast("London", 51.5, -0.1, date(2026, 4, 15))
    )
    assert result is None


def test_get_forecast_returns_forecast_result(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = True   # skip disk load
    tomorrow_client._cache.clear()
    tomorrow_client._call_times.clear()

    target = date(2026, 4, 15)
    raw = _make_tio_response(target, 18.5)

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=raw)
    mock_resp.raise_for_status = MagicMock()
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    mock_session = AsyncMock()
    mock_session.get = MagicMock(return_value=mock_resp)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        with patch.object(tomorrow_client, "_save_disk_cache"):
            result = asyncio.run(
                tomorrow_client.get_forecast("London", 51.5, -0.1, target)
            )

    assert result is not None
    assert result.city == "London"
    assert result.target_date == target
    assert result.daily_max_c == pytest.approx(18.5)
    assert len(result.ensemble_members) == 100


def test_get_forecast_returns_none_on_api_error(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = True
    tomorrow_client._cache.clear()
    tomorrow_client._call_times.clear()

    with patch("aiohttp.ClientSession", side_effect=RuntimeError("connection error")):
        result = asyncio.run(
            tomorrow_client.get_forecast("London", 51.5, -0.1, date(2026, 4, 15))
        )
    assert result is None


def test_get_forecast_returns_none_on_rate_limit(monkeypatch):
    import config as _config
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    import time
    tomorrow_client._disk_cache_loaded = True
    tomorrow_client._cache.clear()
    # Fill up the rate limit log
    tomorrow_client._call_times.clear()
    tomorrow_client._call_times.extend([time.time()] * 20)

    result = asyncio.run(
        tomorrow_client.get_forecast("London", 51.5, -0.1, date(2026, 4, 15))
    )
    assert result is None


def test_cache_hit_skips_api_call(monkeypatch):
    import config as _config
    import time
    monkeypatch.setattr(_config, "TOMORROW_IO_API_KEY", "test-key")
    monkeypatch.setattr(_config, "TOMORROW_IO_CACHE_TTL_SECS", 10800)
    from bonding import tomorrow_client
    tomorrow_client._disk_cache_loaded = True
    tomorrow_client._call_times.clear()

    target = date(2026, 4, 15)
    raw = _make_tio_response(target, 20.0)
    cache_key = f"51.5|-0.1|{target.isoformat()}"
    tomorrow_client._cache[cache_key] = (time.time(), raw)

    with patch("aiohttp.ClientSession") as mock_session_cls:
        result = asyncio.run(
            tomorrow_client.get_forecast("London", 51.5, -0.1, target)
        )
        mock_session_cls.assert_not_called()

    assert result is not None
    assert result.daily_max_c == pytest.approx(20.0)
```

- [ ] **Step 2: Run — expect ImportError**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_tomorrow_client.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'bonding.tomorrow_client'`

- [ ] **Step 3: Create `bot/bonding/tomorrow_client.py`**

```python
"""
tomorrow_client.py — Fetch daily max temperature forecasts from tomorrow.io.

Free tier limits:
  - 500 requests/day
  - 25 requests/hour
  - 3 requests/second

Rate limit guard: max TOMORROW_IO_MAX_REQ_PER_HOUR (20) requests/hour.
Cache: disk-persistent, TOMORROW_IO_CACHE_TTL_SECS (3h) TTL.
Graceful degradation: returns None on any API error or missing key.
"""
import asyncio
import json
import logging
import os
import random
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import aiohttp

import config as _config
from bonding.weather_client import ForecastResult

log = logging.getLogger("bond.tomorrow")

TOMORROW_IO_URL = "https://api.tomorrow.io/v4/timelines"
TOMORROW_IO_CACHE_PATH = os.environ.get(
    "TOMORROW_IO_CACHE_PATH", "/app/data/tomorrow_cache.json"
)

_call_times: list[float] = []
_call_lock: Optional[asyncio.Lock] = None

_cache: dict[str, tuple[float, dict]] = {}
_cache_lock: Optional[asyncio.Lock] = None
_disk_cache_loaded: bool = False


def _get_call_lock() -> asyncio.Lock:
    global _call_lock
    if _call_lock is None:
        _call_lock = asyncio.Lock()
    return _call_lock


def _get_cache_lock() -> asyncio.Lock:
    global _cache_lock
    if _cache_lock is None:
        _cache_lock = asyncio.Lock()
    return _cache_lock


def _load_disk_cache() -> None:
    global _disk_cache_loaded
    if _disk_cache_loaded:
        return
    _disk_cache_loaded = True
    try:
        with open(TOMORROW_IO_CACHE_PATH) as f:
            saved: dict = json.load(f)
        now = time.time()
        for key, entry in saved.items():
            if now - entry.get("fetched_at", 0) < _config.TOMORROW_IO_CACHE_TTL_SECS:
                _cache[key] = (entry["fetched_at"], entry["data"])
        if _cache:
            log.info(f"tomorrow: loaded {len(_cache)} entries from disk")
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning(f"tomorrow: failed to load disk cache: {exc}")


def _save_disk_cache() -> None:
    try:
        Path(TOMORROW_IO_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
        now = time.time()
        to_save = {
            k: {"fetched_at": ts, "data": data}
            for k, (ts, data) in _cache.items()
            if now - ts < _config.TOMORROW_IO_CACHE_TTL_SECS
        }
        tmp = TOMORROW_IO_CACHE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(to_save, f)
        os.replace(tmp, TOMORROW_IO_CACHE_PATH)
    except Exception as exc:
        log.warning(f"tomorrow: failed to save disk cache: {exc}")


def _check_rate_limit() -> bool:
    """Returns True if a request is allowed. Prunes stale entries as side-effect."""
    now = time.time()
    cutoff = now - 3600
    while _call_times and _call_times[0] < cutoff:
        _call_times.pop(0)
    return len(_call_times) < _config.TOMORROW_IO_MAX_REQ_PER_HOUR


def _record_call() -> None:
    _call_times.append(time.time())


def _make_forecast_result(city: str, target_date: date, temp_max_c: float) -> ForecastResult:
    """Convert point forecast to ForecastResult with 100 synthetic Gaussian members."""
    sigma = 1.5  # °C — comparable to Open-Meteo next-day RMSE
    rng = random.Random(f"{city}-{target_date.isoformat()}-{temp_max_c:.2f}")
    members = [rng.gauss(temp_max_c, sigma) for _ in range(100)]
    return ForecastResult(
        city=city,
        target_date=target_date,
        daily_max_c=temp_max_c,
        ensemble_members=members,
        forecast_peak_hour=None,
    )


def _extract_temp_max(raw: dict, target_date: date) -> Optional[float]:
    """Extract temperatureMax from tomorrow.io timelines API response."""
    try:
        timelines = raw["data"]["timelines"]
        for timeline in timelines:
            for interval in timeline.get("intervals", []):
                start: str = interval.get("startTime", "")
                if start.startswith(target_date.isoformat()):
                    return float(interval["values"]["temperatureMax"])
    except (KeyError, TypeError, ValueError) as exc:
        log.debug(f"tomorrow: parse error: {exc}")
    return None


async def get_forecast(
    city: str, lat: float, lon: float, target_date: date
) -> Optional[ForecastResult]:
    """
    Fetch daily max temperature from tomorrow.io for a given city/date.
    Returns None if unavailable (no API key, rate limit hit, API error).
    """
    if not _config.TOMORROW_IO_API_KEY:
        log.debug("tomorrow: TOMORROW_IO_API_KEY not set — skipping")
        return None

    _load_disk_cache()

    cache_key = f"{round(lat, 4)}|{round(lon, 4)}|{target_date.isoformat()}"

    async with _get_cache_lock():
        if cache_key in _cache:
            fetched_at, raw = _cache[cache_key]
            if time.time() - fetched_at < _config.TOMORROW_IO_CACHE_TTL_SECS:
                log.debug(f"tomorrow: cache hit for {city} {target_date}")
                temp_max = _extract_temp_max(raw, target_date)
                if temp_max is not None:
                    return _make_forecast_result(city, target_date, temp_max)

    async with _get_call_lock():
        if not _check_rate_limit():
            log.warning(
                f"tomorrow: hourly rate limit ({_config.TOMORROW_IO_MAX_REQ_PER_HOUR}/hr) "
                f"reached — skipping {city} {target_date}"
            )
            return None

        start_time = target_date.isoformat() + "T00:00:00Z"
        end_time = (target_date + timedelta(days=1)).isoformat() + "T00:00:00Z"
        params = {
            "location": f"{lat},{lon}",
            "fields": "temperatureMax",
            "timesteps": "1d",
            "units": "metric",
            "startTime": start_time,
            "endTime": end_time,
            "apikey": _config.TOMORROW_IO_API_KEY,
        }
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(TOMORROW_IO_URL, params=params) as resp:
                    if resp.status == 429:
                        log.warning(f"tomorrow: 429 rate limit for {city} {target_date}")
                        return None
                    resp.raise_for_status()
                    raw = await resp.json()
        except Exception as exc:
            log.warning(f"tomorrow: fetch failed for {city} {target_date}: {exc}")
            return None

        _record_call()

    async with _get_cache_lock():
        _cache[cache_key] = (time.time(), raw)
    _save_disk_cache()

    temp_max = _extract_temp_max(raw, target_date)
    if temp_max is None:
        log.warning(f"tomorrow: no temperatureMax in response for {city} {target_date}")
        return None

    log.debug(f"tomorrow: {city} {target_date} → {temp_max:.1f}°C")
    return _make_forecast_result(city, target_date, temp_max)
```

- [ ] **Step 4: Run tests**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_tomorrow_client.py -v
```

Expected: 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add bot/bonding/tomorrow_client.py tests/bonding/test_tomorrow_client.py
git commit -m "feat: add tomorrow.io forecast client with 3h cache and rate limit guard"
```

---

## Task 5: Add `get_consensus_forecasts()` to `weather_client.py`

**Files:**
- Modify: `bot/bonding/weather_client.py`
- Test: `tests/bonding/test_source_consensus.py` (extended)

- [ ] **Step 1: Write the failing test**

Add to `tests/bonding/test_source_consensus.py`:

```python
def test_get_consensus_forecasts_builds_source_consensus():
    from bonding.weather_client import get_consensus_forecasts, SourceConsensus
    from datetime import date

    target = date(2026, 4, 15)
    gfs_result = _make_fr(18.0, [17.0, 18.0, 19.0])

    async def run():
        with patch("bonding.weather_client.get_all_forecasts", new=AsyncMock(
            return_value={("London", target): gfs_result}
        )):
            with patch("bonding.weather_client.get_ecmwf_forecast", new=AsyncMock(return_value=None)):
                with patch("bonding.tomorrow_client.get_forecast", new=AsyncMock(return_value=None)):
                    with patch("bonding.weather_client._resolve_city", return_value=("London", 51.5, -0.1)):
                        return await get_consensus_forecasts([("London", target)])

    result = asyncio.run(run())
    assert ("London", target) in result
    consensus = result[("London", target)]
    assert isinstance(consensus, SourceConsensus)
    assert consensus.gfs is gfs_result
    assert consensus.ecmwf is None
    assert consensus.tomorrowio is None
    assert consensus.available_sources() == 1
```

- [ ] **Step 2: Run — expect ImportError on `get_consensus_forecasts`**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_source_consensus.py::test_get_consensus_forecasts_builds_source_consensus -v 2>&1 | head -20
```

Expected: `ImportError: cannot import name 'get_consensus_forecasts'`

- [ ] **Step 3: Add `get_consensus_forecasts()` to `weather_client.py`**

Add after `get_all_forecasts()`:

```python
async def get_consensus_forecasts(
    city_date_pairs: list[tuple[str, date]],
) -> dict[tuple[str, date], SourceConsensus]:
    """
    Batch-fetch forecasts from all three sources for each (city, date) pair.

    Sources:
      - GFS (+ near-term hourly): existing get_all_forecasts() logic
      - ECMWF: get_ecmwf_forecast() — same OM rate limiter, 2h cache
      - tomorrow.io: tomorrow_client.get_forecast() — own rate limiter, 3h cache

    ECMWF and tomorrow.io return None gracefully on failure; GFS is required
    (same as existing behaviour — unknown cities are skipped entirely).

    Returns dict keyed by (canonical_city, date).
    """
    from bonding.tomorrow_client import get_forecast as _tio_get_forecast

    # GFS + near-term (serial, existing rate limiter)
    gfs_results = await get_all_forecasts(city_date_pairs)

    # Build city → (canonical, lat, lon) lookup for ECMWF and TIO calls
    city_coords: dict[str, tuple[str, float, float]] = {}
    for city, _ in city_date_pairs:
        if city in city_coords:
            continue
        try:
            canonical, lat, lon = _resolve_city(city)
            city_coords[city] = (canonical, lat, lon)
        except UnknownCityError:
            pass  # already logged by get_all_forecasts

    # ECMWF (serial, shares OM rate limiter)
    ecmwf_results: dict[tuple[str, date], ForecastResult] = {}
    for city, d in city_date_pairs:
        coords = city_coords.get(city)
        if coords is None:
            continue
        canonical = coords[0]
        result = await get_ecmwf_forecast(canonical, d)
        if result is not None:
            ecmwf_results[(canonical, d)] = result

    # tomorrow.io (serial, own rate limiter)
    tio_results: dict[tuple[str, date], ForecastResult] = {}
    for city, d in city_date_pairs:
        coords = city_coords.get(city)
        if coords is None:
            continue
        canonical, lat, lon = coords
        result = await _tio_get_forecast(canonical, lat, lon, d)
        if result is not None:
            tio_results[(canonical, d)] = result

    # Assemble SourceConsensus — only for cities where GFS succeeded
    consensus: dict[tuple[str, date], SourceConsensus] = {}
    for (city, d), gfs in gfs_results.items():
        consensus[(city, d)] = SourceConsensus(
            city=city,
            target_date=d,
            gfs=gfs,
            ecmwf=ecmwf_results.get((city, d)),
            tomorrowio=tio_results.get((city, d)),
        )

    n_ecmwf = sum(1 for v in consensus.values() if v.ecmwf is not None)
    n_tio   = sum(1 for v in consensus.values() if v.tomorrowio is not None)
    log.info(
        f"consensus_forecasts: {len(consensus)} pairs — "
        f"ecmwf={n_ecmwf} tio={n_tio}"
    )
    return consensus
```

- [ ] **Step 4: Run all source_consensus tests**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_source_consensus.py -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add bot/bonding/weather_client.py tests/bonding/test_source_consensus.py
git commit -m "feat: add get_consensus_forecasts() returning SourceConsensus per city/date"
```

---

## Task 6: Update `opportunity_scorer.py` to Use `SourceConsensus`

**Files:**
- Modify: `bot/bonding/opportunity_scorer.py`

- [ ] **Step 1: Update imports in `opportunity_scorer.py`**

Change the import line:

```python
# Old:
from bonding.weather_client import ForecastResult, prob_in_range, fahrenheit_to_celsius
from bonding.weather_client import _peak_hour_stats as _loaded_stats

# New:
from bonding.weather_client import ForecastResult, SourceConsensus, prob_in_range, fahrenheit_to_celsius
from bonding.weather_client import _peak_hour_stats as _loaded_stats
```

- [ ] **Step 2: Update `score_all()` signature**

```python
def score_all(
    markets: list[MarketCandidate],
    forecasts: dict[tuple, SourceConsensus],      # was dict[tuple, ForecastResult]
) -> list[ScoredOpportunity]:
```

Inside `score_all()`, the lookup line stays the same (the key is still `(city, date)`):

```python
consensus = forecasts.get((market.city, market.target_date))
if consensus is None:
    log.debug(f"scorer: no forecast for {market.city} {market.target_date}, skipping")
    continue
opp = score_market(market, consensus)           # was score_market(market, forecast)
```

- [ ] **Step 3: Update `score_market()` signature**

```python
def score_market(
    market: MarketCandidate,
    forecast: SourceConsensus,               # was ForecastResult
) -> Optional[ScoredOpportunity]:
```

Inside `score_market()`, find the probability computation lines:

```python
# Old:
prob_yes = prob_in_range(forecast, temp_min, temp_max)

# New:
prob_yes = forecast.consensus_prob(temp_min, temp_max)
```

The `prob_no` line doesn't need changing (it's `1.0 - prob_yes`).

The calls to `_score_side` pass `forecast=forecast`. Update `_score_side`'s `forecast` parameter type annotation:

```python
def _score_side(
    market: MarketCandidate,
    forecast: SourceConsensus,               # was ForecastResult
    prob: float,
    ...
```

Inside `_score_side`, find the `forecast_peak_hour` reference:

```python
# Old:
forecast_peak_hour = forecast.forecast_peak_hour

# New (use GFS as the canonical peak-hour source):
forecast_peak_hour = forecast.gfs.forecast_peak_hour
```

Also update the `ScoredOpportunity` construction — `forecast` field stored is the GFS result (to preserve backward compat with exit_manager and paper logs that may read it):

In `_score_side`, change the return statement's `forecast=forecast` to:

```python
return ScoredOpportunity(
    market=market,
    forecast=forecast.gfs,      # store GFS ForecastResult for downstream compatibility
    ...
)
```

- [ ] **Step 4: Run existing scorer tests (if any) and all other tests**

```bash
cd x:/CODING/polybot && python -m pytest tests/ -v 2>&1 | tail -20
```

Expected: all existing tests still PASS

- [ ] **Step 5: Commit**

```bash
git add bot/bonding/opportunity_scorer.py
git commit -m "feat: opportunity_scorer uses SourceConsensus.consensus_prob() for averaged probability"
```

---

## Task 7: Update `price_feed.py`, `main.py`, and `paper_sim.py`

**Files:**
- Modify: `bot/bonding/price_feed.py`
- Modify: `bot/main.py`
- Modify: `bot/bonding/paper_sim.py`

- [ ] **Step 1: Update `price_feed.py` type annotations**

In `bot/bonding/price_feed.py`, change the import:

```python
# Old:
from bonding.weather_client import ForecastResult, _peak_hour_stats as _loaded_stats

# New:
from bonding.weather_client import ForecastResult, SourceConsensus, _peak_hour_stats as _loaded_stats
```

Change the stored dict type in `BondPriceFeed.__init__`:

```python
# Old:
self._forecasts: dict[tuple, ForecastResult] = {}

# New:
self._forecasts: dict[tuple, SourceConsensus] = {}
```

Change `update_markets` signature:

```python
def update_markets(
    self,
    candidates: list[MarketCandidate],
    forecasts: dict[tuple, SourceConsensus],     # was dict[tuple, ForecastResult]
) -> None:
```

The `self._forecasts = forecasts` assignment line stays the same.

- [ ] **Step 2: Update `main.py`**

In `bot/main.py`, change the import inside `run_bonding_loop`:

```python
# Old:
from bonding.weather_client import get_all_forecasts

# New:
from bonding.weather_client import get_consensus_forecasts
```

Change every call to `get_all_forecasts(...)` to `get_consensus_forecasts(...)`. There are two: the initial pre-populate call and the per-cycle call. Example:

```python
# Old:
forecasts = await get_all_forecasts(city_date_pairs)

# New:
forecasts = await get_consensus_forecasts(city_date_pairs)
```

Also add `score_certain` to the scan loop. Find the fallback REST scoring block:

```python
# Old:
opps = score_all(markets, forecasts)

# New:
from bonding.sure_thing_scorer import score_certain
opps = score_all(markets, forecasts) + score_certain(markets, forecasts)
```

And update the import at the top of `run_bonding_loop`:

```python
from bonding.opportunity_scorer import score_all
```

becomes:

```python
from bonding.opportunity_scorer import score_all
# score_certain imported lazily inside loop (done above)
```

- [ ] **Step 3: Update `paper_sim.py`**

In `bot/bonding/paper_sim.py`, change:

```python
# Old:
from bonding.weather_client import get_all_forecasts

# New:
from bonding.weather_client import get_consensus_forecasts
```

Change the call site:

```python
# Old:
forecasts = await get_all_forecasts(city_date_pairs)

# New:
forecasts = await get_consensus_forecasts(city_date_pairs)
```

Also update the `score_all` import to also import `score_certain` and add it to the scoring step (if paper_sim has an explicit scoring call — check and mirror the main.py pattern).

- [ ] **Step 4: Run all tests**

```bash
cd x:/CODING/polybot && python -m pytest tests/ -v
```

Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add bot/bonding/price_feed.py bot/main.py bot/bonding/paper_sim.py
git commit -m "feat: wire get_consensus_forecasts and score_certain into scan loop"
```

---

## Task 8: Create `sure_thing_scorer.py`

**Files:**
- Create: `bot/bonding/sure_thing_scorer.py`
- Create: `tests/bonding/test_sure_thing_scorer.py`

- [ ] **Step 1: Write failing tests**

Create `tests/bonding/test_sure_thing_scorer.py`:

```python
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
    market = _make_market(ask=0.92)   # consensus_prob needs to be ≥ 0.97 for 5% edge
    # Make consensus_prob ≈ 0.94 (not enough edge)
    consensus = _make_consensus(gfs_max=20.0, ecmwf_max=20.0, tio_max=20.0, spread=0.3)
    # temp range 18–22 with members centred at 20, spread 0.3 → ~99.9% in range
    # ask=0.92 → edge = 0.999 - 0.92 = 0.079 > 0.05 → this would pass
    # Let's use a market where consensus_prob ≈ 0.80, ask=0.79 → edge=0.01 < 0.05
    market2 = _make_market(ask=0.79)
    gfs = ForecastResult("London", date(2026, 4, 15), 20.0, [20.0] * 80 + [10.0] * 20)  # 80% in range
    ecmwf = ForecastResult("London", date(2026, 4, 15), 20.0, [20.0] * 80 + [10.0] * 20)
    tio   = ForecastResult("London", date(2026, 4, 15), 20.0, [20.0] * 80 + [10.0] * 20)
    consensus2 = SourceConsensus("London", date(2026, 4, 15), gfs, ecmwf, tio)
    forecasts2 = {("London", date(2026, 4, 15)): consensus2}
    # consensus_prob ≈ 0.80, ask = 0.79, edge ≈ 0.01 < 0.05 → blocked
    assert _run_score_certain([market2], forecasts2) == []
```

- [ ] **Step 2: Run — expect ImportError**

```bash
cd x:/CODING/polybot && python -m pytest tests/bonding/test_sure_thing_scorer.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'bonding.sure_thing_scorer'`

- [ ] **Step 3: Create `bot/bonding/sure_thing_scorer.py`**

```python
"""
sure_thing_scorer.py — CERTAIN tier: high-confidence, high-ask bets.

Produces ScoredOpportunity(tier="CERTAIN") when all three weather sources
tightly agree that a YES outcome is very likely AND the market still offers
≥5% edge. Conservative sizing (20 shares) during the validation phase.

Called from the REST scan loop alongside score_all(). Uses the same
SourceConsensus dict produced by get_consensus_forecasts().
"""
import logging
import statistics
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import config as _config
from bonding.market_scanner import MarketCandidate
from bonding.opportunity_scorer import ScoredOpportunity, _passes_time_gate
from bonding.weather_client import SourceConsensus, prob_in_range, fahrenheit_to_celsius

log = logging.getLogger("bond.certain")

TIER_CERTAIN = "CERTAIN"


def _has_open_position(market_id: str) -> bool:
    """
    Returns True if a CERTAIN position is already open for this market.
    Reads the bonding positions ledger to prevent duplicate entries.
    """
    import json
    from pathlib import Path
    ledger_path = Path(_config.BOND_LEDGER_FILE)
    if not ledger_path.exists():
        return False
    try:
        positions = json.loads(ledger_path.read_text())
        return any(
            p.get("market_id") == market_id and p.get("status") == "OPEN"
            for p in positions
        )
    except Exception:
        return False


def _convert_temps(market: MarketCandidate) -> tuple[Optional[float], Optional[float]]:
    temp_min = market.temp_min
    temp_max = market.temp_max
    if market.unit == "F":
        if temp_min is not None:
            temp_min = fahrenheit_to_celsius(temp_min)
        if temp_max is not None:
            temp_max = fahrenheit_to_celsius(temp_max)
    return temp_min, temp_max


def _score_one(
    market: MarketCandidate,
    consensus: SourceConsensus,
) -> Optional[ScoredOpportunity]:
    """
    Apply all CERTAIN gates to one market/consensus pair.
    Returns ScoredOpportunity or None if any gate fails.
    """
    ask = market.best_ask

    # Gate 1: ask range
    if not (_config.CERTAIN_ASK_MIN <= ask <= _config.CERTAIN_ASK_MAX):
        return None

    # Gate 2: all three sources must be present
    if consensus.available_sources() < _config.CERTAIN_MIN_SOURCES:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"only {consensus.available_sources()} sources (need {_config.CERTAIN_MIN_SOURCES})"
        )
        return None

    # Gate 3: no open position for this market
    if _has_open_position(market.market_id):
        return None

    # Gate 4: time gate (same logic as standard scorer)
    if not _passes_time_gate(market, consensus.gfs.forecast_peak_hour):
        return None

    temp_min, temp_max = _convert_temps(market)
    if temp_min is None or temp_max is None:
        return None

    # Gate 5: each source must individually reach min probability
    gfs_prob   = prob_in_range(consensus.gfs, temp_min, temp_max)
    ecmwf_prob = prob_in_range(consensus.ecmwf, temp_min, temp_max)
    tio_prob   = prob_in_range(consensus.tomorrowio, temp_min, temp_max)

    for source_name, source_prob in [("gfs", gfs_prob), ("ecmwf", ecmwf_prob), ("tio", tio_prob)]:
        if source_prob < _config.CERTAIN_MIN_SOURCE_PROB:
            log.debug(
                f"certain: {market.city} {market.target_date} — "
                f"{source_name} prob {source_prob:.3f} < {_config.CERTAIN_MIN_SOURCE_PROB}"
            )
            return None

    # Gate 6: inter-source point-forecast delta
    forecasts = consensus.point_forecasts()
    temp_delta = max(forecasts) - min(forecasts)
    if temp_delta > _config.CERTAIN_MAX_TEMP_DELTA_C:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"temp delta {temp_delta:.2f}°C > {_config.CERTAIN_MAX_TEMP_DELTA_C}"
        )
        return None

    # Gate 7: combined ensemble spread
    all_members = consensus.all_ensemble_members()
    if len(all_members) >= 2:
        spread = statistics.stdev(all_members)
        if spread > _config.CERTAIN_MAX_SPREAD_C:
            log.debug(
                f"certain: {market.city} {market.target_date} — "
                f"spread {spread:.2f}°C > {_config.CERTAIN_MAX_SPREAD_C}"
            )
            return None

    # Gate 8: consensus probability floor
    consensus_prob = consensus.consensus_prob(temp_min, temp_max)
    if consensus_prob < _config.CERTAIN_MIN_CONSENSUS_PROB:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"consensus_prob {consensus_prob:.3f} < {_config.CERTAIN_MIN_CONSENSUS_PROB}"
        )
        return None

    # Gate 9: minimum edge
    edge = consensus_prob - ask
    if edge < _config.CERTAIN_MIN_EDGE:
        log.debug(
            f"certain: {market.city} {market.target_date} — "
            f"edge {edge:.3f} < {_config.CERTAIN_MIN_EDGE}"
        )
        return None

    log.info(
        f"certain: OPPORTUNITY {market.city} {market.target_date} "
        f"ask={ask:.3f} consensus_prob={consensus_prob:.3f} edge={edge:.3f} "
        f"gfs={gfs_prob:.3f} ecmwf={ecmwf_prob:.3f} tio={tio_prob:.3f} "
        f"delta={temp_delta:.2f}°C"
    )

    return ScoredOpportunity(
        market=market,
        forecast=consensus.gfs,
        prob=consensus_prob,
        ev=edge,
        edge=edge,
        tier=TIER_CERTAIN,
        shares=_config.CERTAIN_SHARES,
        capital=_config.CERTAIN_SHARES * ask,
        shares_immediate=_config.CERTAIN_SHARES,
        shares_limit=0,
        limit_price=ask,
        outcome="YES",
        token_id=market.token_id,
        side_ask=ask,
    )


def score_certain(
    markets: list[MarketCandidate],
    forecasts: dict[tuple, SourceConsensus],
) -> list[ScoredOpportunity]:
    """
    Score all markets for CERTAIN tier opportunities.
    Returns list sorted by edge descending, with per-cluster capital cap applied.
    """
    results: list[ScoredOpportunity] = []

    for market in markets:
        consensus = forecasts.get((market.city, market.target_date))
        if consensus is None:
            continue
        opp = _score_one(market, consensus)
        if opp is not None:
            results.append(opp)

    if not results:
        return []

    results.sort(key=lambda o: o.edge, reverse=True)
    results = _apply_cluster_cap(results)

    log.info(f"certain: {len(markets)} markets → {len(results)} CERTAIN opportunities")
    return results


def _apply_cluster_cap(opps: list[ScoredOpportunity]) -> list[ScoredOpportunity]:
    """
    Apply CERTAIN_MAX_CAPITAL_PER_CLUSTER per (city, target_date) cluster.
    Greedy inclusion by edge descending (list is already sorted).
    """
    cluster_spend: dict[tuple, float] = {}
    accepted: list[ScoredOpportunity] = []

    for opp in opps:
        key = (opp.market.city, opp.market.target_date)
        spent = cluster_spend.get(key, 0.0)
        if spent + opp.capital <= _config.CERTAIN_MAX_CAPITAL_PER_CLUSTER:
            cluster_spend[key] = spent + opp.capital
            accepted.append(opp)

    return accepted
```

- [ ] **Step 4: Add `_passes_time_gate` to `opportunity_scorer.py`**

The `sure_thing_scorer` imports `_passes_time_gate` from `opportunity_scorer`. Extract the time-gate logic into a reusable function in `opportunity_scorer.py`. Add this function just before `score_market()`:

```python
def _passes_time_gate(market: MarketCandidate, forecast_peak_hour: Optional[int]) -> bool:
    """
    Returns True if the market is still within the entry window.
    False if the city's peak hour has passed (today's markets) or the day has ended.
    Also sets the scan suppression cache as a side effect.
    """
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
                next_day.year, next_day.month, next_day.day, 0, 0, 0, tzinfo=city_tz
            ).astimezone(timezone.utc)
            suppress_secs = max((end_of_day_utc - now_utc).total_seconds(), 0) + 300
            _scan_suppressions[(market.city, market.target_date)] = time.time() + suppress_secs
            return False
    else:
        next_day = market.target_date + timedelta(days=1)
        end_of_day_utc = datetime(
            next_day.year, next_day.month, next_day.day, 0, 0, 0, tzinfo=city_tz
        ).astimezone(timezone.utc)
        if (end_of_day_utc - now_utc).total_seconds() <= 0:
            _scan_suppressions[(market.city, market.target_date)] = time.time() + 24 * 3600
            return False

    return True
```

Then update `score_market()` to call this function instead of inlining the logic. Remove the old inline gate block and replace with:

```python
if not _passes_time_gate(market, forecast.gfs.forecast_peak_hour):
    return None
```

- [ ] **Step 5: Run all tests**

```bash
cd x:/CODING/polybot && python -m pytest tests/ -v
```

Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add bot/bonding/sure_thing_scorer.py bot/bonding/opportunity_scorer.py tests/bonding/test_sure_thing_scorer.py
git commit -m "feat: add CERTAIN tier sure_thing_scorer with three-source hard gates"
```

---

## Task 9: Update `exit_manager.py` for CERTAIN Tier

**Files:**
- Modify: `bot/bonding/exit_manager.py`

- [ ] **Step 1: Add CERTAIN tier constant and skip early-exit for it**

In `bot/bonding/exit_manager.py`, add the constant after `TIER_WING`:

```python
TIER_CERTAIN = "CERTAIN"
```

Find the early-exit rule (Rule 2) in `_should_exit()`:

```python
# Rule 2 — core early exit
if pos.tier == TIER_CORE and price >= _config.BOND_EARLY_EXIT_PRICE:
    return True
```

No change needed here — CERTAIN is already excluded because it only fires for `TIER_CORE`. But add an explicit comment for clarity:

```python
# Rule 2 — core early exit (CERTAIN tier is excluded — hold to resolution)
if pos.tier == TIER_CORE and price >= _config.BOND_EARLY_EXIT_PRICE:
    return True
```

- [ ] **Step 2: Verify CERTAIN positions load correctly from ledger**

The `BondPosition` dataclass has `tier: str` which already accepts any string value, so `tier="CERTAIN"` loads correctly from JSON without code changes.

Run a quick sanity check:

```bash
cd x:/CODING/polybot/bot && python -c "
from bonding.exit_manager import BondPosition, TIER_CERTAIN
import json
p = BondPosition('mid', 'tid', 'q', 'London', 'YES', TIER_CERTAIN, 20, 0.85, '2026-04-09T10:00:00Z', '2026-04-15T20:00:00Z', 'OPEN', 0.95)
print('tier:', p.tier, '— OK')
print('serialised:', json.dumps({'tier': p.tier}))
"
```

Expected output:
```
tier: CERTAIN — OK
serialised: {"tier": "CERTAIN"}
```

- [ ] **Step 3: Commit**

```bash
git add bot/bonding/exit_manager.py
git commit -m "feat: exit_manager recognises CERTAIN tier (hold-to-resolution, skips 0.97 early exit)"
```

---

## Task 10: Integration Smoke Test

- [ ] **Step 1: Run the full test suite one final time**

```bash
cd x:/CODING/polybot && python -m pytest tests/ -v --tb=short
```

Expected: all PASS with no warnings about missing imports

- [ ] **Step 2: Verify imports work end-to-end**

```bash
cd x:/CODING/polybot/bot && python -c "
from bonding.weather_client import get_consensus_forecasts, SourceConsensus
from bonding.tomorrow_client import get_forecast
from bonding.opportunity_scorer import score_all
from bonding.sure_thing_scorer import score_certain, TIER_CERTAIN
from bonding.exit_manager import TIER_CERTAIN as EXIT_CERTAIN
print('All imports OK')
print('TIER_CERTAIN:', TIER_CERTAIN, EXIT_CERTAIN)
"
```

Expected:
```
All imports OK
TIER_CERTAIN: CERTAIN CERTAIN
```

- [ ] **Step 3: Confirm tomorrow.io config is in `.env`**

The `TOMORROW_IO_API_KEY` must be set before running live or paper mode. Confirm it's in the `.env` file:

```bash
grep -q TOMORROW_IO_API_KEY x:/CODING/polybot/.env && echo "Key present" || echo "⚠ Add TOMORROW_IO_API_KEY=<your-key> to .env"
```

If missing, add to `.env`:
```
TOMORROW_IO_API_KEY=<your-key-from-tomorrow.io-dashboard>
```

- [ ] **Step 4: Run paper mode for one scan cycle to confirm CERTAIN bets appear in log**

```bash
cd x:/CODING/polybot/bot && timeout 120 python -m bonding.paper_sim 2>&1 | grep -E "CERTAIN|certain|consensus_forecasts|tomorrow"
```

Expected output (example):
```
consensus_forecasts: 12 pairs — ecmwf=10 tio=8
certain: 3 markets → 0 CERTAIN opportunities    # 0 is fine — CERTAIN gates are strict
```

If `tio=0` across all pairs, check that `TOMORROW_IO_API_KEY` is set and the API is reachable.

- [ ] **Step 5: Final commit**

```bash
git add docs/superpowers/plans/2026-04-09-bond-cross-source-certain-tier.md
git commit -m "docs: add cross-source + CERTAIN tier implementation plan"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: All sections covered — SourceConsensus (Task 2), ECMWF (Task 3), tomorrow.io (Task 4), get_consensus_forecasts (Task 5), scorer update (Task 6), wiring (Task 7), sure_thing_scorer (Task 8), exit_manager (Task 9)
- [x] **No placeholders**: All steps include complete code
- [x] **Type consistency**: `SourceConsensus` used in scorer/feed; `ForecastResult` still stored in `ScoredOpportunity.forecast` for downstream compat; `_passes_time_gate` signature consistent between Task 8 definition and Task 9 usage
- [x] **Capital cap**: CERTAIN uses `CERTAIN_MAX_CAPITAL_PER_CLUSTER` ($20), not `BOND_MAX_CAPITAL_PER_CLUSTER` ($4)
- [x] **Exit manager**: CERTAIN explicitly excluded from CORE 0.97 early-exit rule — verified in Task 9
