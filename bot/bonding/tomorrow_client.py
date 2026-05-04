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
TOMORROW_IO_CALL_TIMES_PATH = os.environ.get(
    "TOMORROW_IO_CALL_TIMES_PATH", "/app/data/tomorrow_call_times.json"
)

# Free-tier forecast horizon: tomorrow.io Developer plan supports up to 5 days ahead.
# Dates beyond this always return 403; filtering prevents wasted credits.
TOMORROW_IO_MAX_FORECAST_DAYS = 5

_call_times: list[float] = []
_call_lock: Optional[asyncio.Lock] = None
_call_times_loaded: bool = False

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


def _load_call_times() -> None:
    """Load persisted call timestamps from disk, pruning entries older than 1 hour."""
    global _call_times_loaded
    if _call_times_loaded:
        return
    _call_times_loaded = True
    try:
        with open(TOMORROW_IO_CALL_TIMES_PATH) as f:
            saved: list = json.load(f)
        cutoff = time.time() - 3600
        recent = [t for t in saved if t > cutoff]
        _call_times.extend(recent)
        if recent:
            log.info(f"tomorrow: restored {len(recent)} call timestamps from disk")
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning(f"tomorrow: failed to load call times: {exc}")


def _save_call_times() -> None:
    """Persist recent call timestamps to disk for cross-restart rate limiting."""
    try:
        Path(TOMORROW_IO_CALL_TIMES_PATH).parent.mkdir(parents=True, exist_ok=True)
        cutoff = time.time() - 3600
        to_save = [t for t in _call_times if t > cutoff]
        tmp = TOMORROW_IO_CALL_TIMES_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(to_save, f)
        os.replace(tmp, TOMORROW_IO_CALL_TIMES_PATH)
    except Exception as exc:
        log.warning(f"tomorrow: failed to save call times: {exc}")


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
    _load_call_times()
    now = time.time()
    cutoff = now - 3600
    while _call_times and _call_times[0] < cutoff:
        _call_times.pop(0)
    return len(_call_times) < _config.TOMORROW_IO_MAX_REQ_PER_HOUR


def has_rate_limit_budget() -> bool:
    """Returns True if at least one more request is allowed this hour."""
    return _check_rate_limit()


def _record_call() -> None:
    _call_times.append(time.time())
    _save_call_times()


def _make_forecast_result(city: str, target_date: date, temp_max_c: float) -> ForecastResult:
    """Convert point forecast to ForecastResult with 100 synthetic Gaussian members."""
    sigma = 2.5  # °C — calibrated to real-world 24-48h daily max RMSE (~2-3°C)
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


async def get_forecasts_batch(
    city: str, lat: float, lon: float, dates: list[date]
) -> dict[date, ForecastResult]:
    """
    Fetch daily max temperatures for multiple dates in a single API call.
    Returns a dict of {date: ForecastResult} for dates where data was available.
    Cache key is per city+range so one call covers all requested dates.
    """
    if not _config.TOMORROW_IO_API_KEY or not dates:
        return {}

    _load_disk_cache()

    start_d = min(dates)
    end_d   = max(dates)
    cache_key = f"{round(lat, 4)}|{round(lon, 4)}|{start_d.isoformat()}|{end_d.isoformat()}"

    raw = None
    async with _get_cache_lock():
        if cache_key in _cache:
            fetched_at, cached_raw = _cache[cache_key]
            if time.time() - fetched_at < _config.TOMORROW_IO_CACHE_TTL_SECS:
                log.debug(f"tomorrow: batch cache hit for {city}")
                raw = cached_raw

    if raw is None:
        async with _get_call_lock():
            if not _check_rate_limit():
                log.warning(
                    f"tomorrow: hourly rate limit ({_config.TOMORROW_IO_MAX_REQ_PER_HOUR}/hr) "
                    f"reached — skipping {city} ({len(dates)} dates)"
                )
                return {}

            start_time = start_d.isoformat() + "T00:00:00Z"
            end_time   = (end_d + timedelta(days=1)).isoformat() + "T00:00:00Z"
            params = {
                "location":   f"{lat},{lon}",
                "fields":     "temperatureMax",
                "timesteps":  "1d",
                "units":      "metric",
                "startTime":  start_time,
                "endTime":    end_time,
                "apikey":     _config.TOMORROW_IO_API_KEY,
            }
            timeout = aiohttp.ClientTimeout(total=10)
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(TOMORROW_IO_URL, params=params) as resp:
                        if resp.status == 429:
                            log.warning(f"tomorrow: 429 rate limit for {city}")
                            return {}
                        resp.raise_for_status()
                        raw = await resp.json()
            except Exception as exc:
                log.warning(f"tomorrow: batch fetch failed for {city}: {exc}")
                return {}

            _record_call()

        async with _get_cache_lock():
            _cache[cache_key] = (time.time(), raw)
        _save_disk_cache()

    results: dict[date, ForecastResult] = {}
    for d in dates:
        temp_max = _extract_temp_max(raw, d)
        if temp_max is not None:
            results[d] = _make_forecast_result(city, d, temp_max)
    return results


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
