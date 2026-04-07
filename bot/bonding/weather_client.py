"""
weather_client.py — Fetch city temperature forecasts from Open-Meteo.

No API key required. Free tier limits:
  - 10,000 calls/day
  -  5,000 calls/hour
  -    600 calls/minute

Rate limiting strategy:
- Disk-persistent cache (2h TTL at /app/data/weather_cache.json) survives
  restarts, so a full city refresh only happens once every 2 hours.
- Batch all dates for a city into a single API call (date range fetch).
- Strict serial rate limiter: min 3s between requests (~20 req/min).
- Sliding-window counters enforce all three Open-Meteo limits hard.
- Together these reduce daily API calls from ~6,000 to ~300-400.
"""
import asyncio
import collections
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import aiohttp
import config as _config

log = logging.getLogger("bond.weather")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# In-memory TTL: re-check if data is older than 30 min within a session
MEM_CACHE_TTL_SECS  = 1800   # 30 minutes
# Disk TTL: don't re-fetch from API if disk entry is fresher than 2 hours
DISK_CACHE_TTL_SECS = 7200   # 2 hours

DISK_CACHE_PATH = os.environ.get("WEATHER_CACHE_PATH", "/app/data/weather_cache.json")

# Minimum seconds between consecutive API requests.
# 3s = ~20 req/min — well under the 600/min Open-Meteo limit.
_MIN_REQUEST_INTERVAL = 3.0

# Open-Meteo free-tier hard limits (enforced via sliding-window counters below)
_LIMIT_PER_MINUTE = 600
_LIMIT_PER_HOUR   = 5_000
_LIMIT_PER_DAY    = 10_000

# Timestamps of every successful API call in the last 24 hours.
# Entries older than 24 hours are pruned before each new request.
_api_call_log: collections.deque[float] = collections.deque()


class UnknownCityError(ValueError):
    """Raised when a city name cannot be resolved to coordinates."""


@dataclass
class ForecastResult:
    city: str
    target_date: date
    daily_max_c: float            # predicted daily high (°C)
    hourly_spread: list[float]    # hourly temps for the target day
    confidence_interval_c: float  # ±°C (std dev of hourly spread around daily max)


# ── In-memory cache: (lat, lon, start_date_str, end_date_str) → (fetched_at, raw) ─
_cache: dict[tuple, tuple[float, dict]] = {}
_cache_lock = asyncio.Lock()
_disk_cache_loaded = False

# Serial rate limiter
_rate_lock: Optional[asyncio.Lock] = None
_last_request_time: float = 0.0


def _get_rate_lock() -> asyncio.Lock:
    global _rate_lock
    if _rate_lock is None:
        _rate_lock = asyncio.Lock()
    return _rate_lock


def _check_limits() -> None:
    """
    Raise RuntimeError if any Open-Meteo sliding-window limit would be exceeded.
    Must be called while holding _get_rate_lock() (serial execution guaranteed).
    Prunes entries older than 24 hours as a side-effect.
    """
    now = time.time()
    cutoff_day = now - 86400
    # Drop entries outside the 24-hour window
    while _api_call_log and _api_call_log[0] < cutoff_day:
        _api_call_log.popleft()

    day_count  = len(_api_call_log)
    hour_count = sum(1 for t in _api_call_log if t > now - 3600)
    min_count  = sum(1 for t in _api_call_log if t > now - 60)

    if day_count >= _LIMIT_PER_DAY:
        raise RuntimeError(
            f"Open-Meteo daily limit reached ({day_count}/{_LIMIT_PER_DAY}). "
            "No further requests until the window resets."
        )
    if hour_count >= _LIMIT_PER_HOUR:
        raise RuntimeError(
            f"Open-Meteo hourly limit reached ({hour_count}/{_LIMIT_PER_HOUR}). "
            "Backing off until the window resets."
        )
    if min_count >= _LIMIT_PER_MINUTE:
        raise RuntimeError(
            f"Open-Meteo per-minute limit reached ({min_count}/{_LIMIT_PER_MINUTE}). "
            "Backing off."
        )

    # Log a warning when approaching limits (within 10%)
    if day_count  >= _LIMIT_PER_DAY   * 0.9:
        log.warning(f"Open-Meteo daily budget at {day_count}/{_LIMIT_PER_DAY} — approaching limit")
    elif hour_count >= _LIMIT_PER_HOUR * 0.9:
        log.warning(f"Open-Meteo hourly budget at {hour_count}/{_LIMIT_PER_HOUR} — approaching limit")


def _record_api_call() -> None:
    """Record a completed API call in the sliding-window log."""
    _api_call_log.append(time.time())


def _load_disk_cache() -> None:
    """Load persisted weather cache from disk into memory (called once on first use)."""
    global _disk_cache_loaded
    if _disk_cache_loaded:
        return
    _disk_cache_loaded = True
    try:
        with open(DISK_CACHE_PATH) as f:
            saved: dict = json.load(f)
        now = time.time()
        loaded = 0
        for key_str, entry in saved.items():
            age = now - entry.get("fetched_at", 0)
            if age < DISK_CACHE_TTL_SECS:
                parts = key_str.split("|")
                key = (float(parts[0]), float(parts[1]), parts[2], parts[3])
                _cache[key] = (entry["fetched_at"], entry["data"])
                loaded += 1
        if loaded:
            log.info(f"weather: loaded {loaded} entries from disk cache ({DISK_CACHE_PATH})")
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning(f"weather: failed to load disk cache: {exc}")


def _save_disk_cache() -> None:
    """Persist current in-memory cache to disk (best-effort, atomic write)."""
    try:
        os.makedirs(os.path.dirname(DISK_CACHE_PATH), exist_ok=True)
        now = time.time()
        to_save: dict = {}
        for key, (fetched_at, data) in _cache.items():
            if now - fetched_at < DISK_CACHE_TTL_SECS:
                key_str = f"{key[0]}|{key[1]}|{key[2]}|{key[3]}"
                to_save[key_str] = {"fetched_at": fetched_at, "data": data}
        tmp = DISK_CACHE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(to_save, f)
        os.replace(tmp, DISK_CACHE_PATH)
    except Exception as exc:
        log.warning(f"weather: failed to save disk cache: {exc}")


async def get_forecast(city: str, target_date: date) -> ForecastResult:
    """
    Return forecast for a single city on target_date.
    Resolves city aliases, fetches from Open-Meteo (cached 30 min).
    Raises UnknownCityError if city cannot be resolved.
    """
    canonical, lat, lon = _resolve_city(city)
    raw = await _fetch_open_meteo(lat, lon, target_date)
    return _parse_forecast(canonical, target_date, raw)


async def get_all_forecasts(
    city_date_pairs: list[tuple[str, date]],
) -> dict[tuple[str, date], ForecastResult]:
    """
    Batch-fetch forecasts for all (city, date) pairs.
    Groups dates by city so each city requires only ONE API call (date range).
    De-dupes identical requests. Returns dict keyed by (canonical_city, date).
    Unknown cities are logged and skipped (not raised).
    """
    # Open-Meteo free tier only supports forecasts up to 16 days ahead
    max_forecast_date = date.today() + timedelta(days=16)

    # Resolve cities and filter out-of-range dates
    # loc_key → (canonical, lat, lon, set of dates)
    loc_map: dict[tuple[float, float], tuple[str, float, float, set[date]]] = {}
    skipped_future = 0
    for city, d in city_date_pairs:
        if d > max_forecast_date:
            skipped_future += 1
            continue
        try:
            canonical, lat, lon = _resolve_city(city)
        except UnknownCityError:
            log.warning(f"weather: skipping unknown city '{city}'")
            continue
        loc_key = (round(lat, 4), round(lon, 4))
        if loc_key not in loc_map:
            loc_map[loc_key] = (canonical, lat, lon, set())
        loc_map[loc_key][3].add(d)

    if skipped_future:
        log.info(f"weather: skipped {skipped_future} pairs with dates beyond 16-day forecast window")

    unique_pairs = sum(len(v[3]) for v in loc_map.values())
    log.info(
        f"BOND_WEATHER_FETCH unique_pairs={unique_pairs} "
        f"city_requests={len(loc_map)} (serial, {_MIN_REQUEST_INTERVAL}s interval)"
    )

    # Fetch one range per city (serial to respect rate limits)
    results: dict[tuple[str, date], ForecastResult] = {}
    failed = 0

    for canonical, lat, lon, dates in loc_map.values():
        start_d = min(dates)
        end_d = max(dates)
        try:
            raw = await _fetch_open_meteo_range(lat, lon, start_d, end_d)
            for d in dates:
                try:
                    result = _parse_forecast_from_range(canonical, d, raw)
                    results[(canonical, d)] = result
                except Exception as exc:
                    log.warning(f"weather: failed to parse {canonical} {d}: {exc}")
                    failed += 1
        except Exception as exc:
            log.warning(f"weather: failed to fetch {canonical} {start_d}–{end_d}: {exc}")
            failed += len(dates)

    fetched = unique_pairs - failed
    log.info(f"BOND_WEATHER_DONE fetched={fetched} failed={failed}")
    return results


def prob_in_range(
    forecast: ForecastResult,
    temp_min: float,
    temp_max: float,
) -> float:
    """
    Return probability (0–1) that the day's high falls in [temp_min, temp_max].

    Uses a Gaussian approximation:
      mean = forecast.daily_max_c
      std  = forecast.confidence_interval_c  (or 1.0 if too tight)

    P(a < X < b) = 0.5 * [erf((b-mu)/(std*√2)) - erf((a-mu)/(std*√2))]
    Implemented via math.erf — no scipy dependency.
    """
    mu  = forecast.daily_max_c
    std = max(forecast.confidence_interval_c, 1.0)  # floor at 1°C
    sqrt2 = math.sqrt(2)

    def _phi(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / sqrt2))

    p = _phi((temp_max - mu) / std) - _phi((temp_min - mu) / std)
    return max(0.0, min(1.0, p))


def celsius_to_fahrenheit(c: float) -> float:
    return c * 9 / 5 + 32


def fahrenheit_to_celsius(f: float) -> float:
    return (f - 32) * 5 / 9


async def geocode_city(name: str) -> tuple[str, float, float]:
    """
    Resolve a city name to (display_name, lat, lon) using Open-Meteo geocoding.
    Free API, no key required.
    Raises UnknownCityError if no results found.
    Goes through the shared rate limiter so geocoding calls count against
    the same Open-Meteo budget as forecast calls.
    """
    global _last_request_time

    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": name, "count": 1, "language": "en", "format": "json"}
    timeout = aiohttp.ClientTimeout(total=10)

    async with _get_rate_lock():
        gap = time.time() - _last_request_time
        if gap < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - gap)

        _check_limits()

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

        _last_request_time = time.time()
        _record_api_call()

    results = data.get("results", [])
    if not results:
        raise UnknownCityError(f"No geocoding results for '{name}'")
    r = results[0]
    display = r.get("name", name)
    return display, float(r["latitude"]), float(r["longitude"])


# ── Internal helpers ──────────────────────────────────────────────

def _resolve_city(city_name: str) -> tuple[str, float, float]:
    """
    Resolve city name (including aliases) to (canonical_name, lat, lon).
    Raises UnknownCityError if not found.
    """
    cities  = _config.BOND_CITIES
    aliases = _config.BOND_CITY_ALIASES

    # Direct match
    if city_name in cities:
        lat, lon = cities[city_name]
        return city_name, lat, lon

    # Alias lookup
    canonical = aliases.get(city_name)
    if canonical and canonical in cities:
        lat, lon = cities[canonical]
        return canonical, lat, lon

    # Case-insensitive fallback
    lower = city_name.lower()
    for name, coords in cities.items():
        if name.lower() == lower:
            return name, coords[0], coords[1]
    for alias, can in aliases.items():
        if alias.lower() == lower and can in cities:
            lat, lon = cities[can]
            return can, lat, lon

    raise UnknownCityError(f"Cannot resolve city '{city_name}' to coordinates")


async def _fetch_open_meteo_range(lat: float, lon: float, start_date: date, end_date: date) -> dict:
    """
    Fetch a date range from Open-Meteo in a single API call.
    Checks disk cache on first call (DISK_CACHE_TTL_SECS), then in-memory
    (MEM_CACHE_TTL_SECS). Uses serial rate limiter between live requests.
    """
    global _last_request_time

    # Load disk cache once per process startup
    _load_disk_cache()

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    cache_key = (round(lat, 4), round(lon, 4), start_str, end_str)

    async with _cache_lock:
        if cache_key in _cache:
            fetched_at, data = _cache[cache_key]
            # In-memory: honour MEM_CACHE_TTL (wall clock)
            if time.time() - fetched_at < MEM_CACHE_TTL_SECS:
                return data

    params = {
        "latitude":   lat,
        "longitude":  lon,
        "daily":      "temperature_2m_max",
        "hourly":     "temperature_2m",
        "timezone":   "auto",
        "start_date": start_str,
        "end_date":   end_str,
    }

    timeout = aiohttp.ClientTimeout(total=20)

    # Serial rate limiter: enforce minimum gap and sliding-window budgets
    async with _get_rate_lock():
        gap = time.time() - _last_request_time
        if gap < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - gap)

        # Raises RuntimeError if any Open-Meteo limit would be exceeded
        _check_limits()

        for attempt in range(5):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(OPEN_METEO_URL, params=params) as resp:
                        if resp.status == 429:
                            wait = _MIN_REQUEST_INTERVAL * (2 ** attempt)
                            log.warning(
                                f"Open-Meteo 429 at ({lat:.2f},{lon:.2f}) "
                                f"{start_str}–{end_str} — retry in {wait:.1f}s (attempt {attempt+1}/5)"
                            )
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        data = await resp.json()
                        break
            except aiohttp.ClientResponseError as exc:
                if exc.status == 429 and attempt < 4:
                    wait = _MIN_REQUEST_INTERVAL * (2 ** attempt)
                    log.warning(f"Open-Meteo 429 (exc) — retry in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                raise
        else:
            raise RuntimeError(f"Open-Meteo rate limit: max retries exceeded (429) for ({lat:.2f},{lon:.2f})")

        _last_request_time = time.time()
        _record_api_call()

    now = time.time()
    async with _cache_lock:
        _cache[cache_key] = (now, data)

    _save_disk_cache()
    return data


async def _fetch_open_meteo(lat: float, lon: float, target_date: date) -> dict:
    """Single-date fetch (used by get_forecast). Wraps the range fetcher."""
    return await _fetch_open_meteo_range(lat, lon, target_date, target_date)


def _parse_forecast_from_range(city: str, target_date: date, raw: dict) -> ForecastResult:
    """
    Extract daily_max_c and hourly_spread for target_date from a (possibly
    multi-day) Open-Meteo range response. Computes confidence_interval_c as
    std dev of the day's hourly temps.
    """
    date_str = target_date.isoformat()

    try:
        daily_times: list[str] = raw["daily"]["time"]
        daily_maxes: list[float] = raw["daily"]["temperature_2m_max"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Unexpected Open-Meteo response shape for {city}: {exc}") from exc

    try:
        day_idx = daily_times.index(date_str)
        daily_max_c: float = daily_maxes[day_idx]
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Date {date_str} not found in response for {city}: {exc}") from exc

    hourly_times: list[str] = raw.get("hourly", {}).get("time", [])
    hourly_temps_all: list[float] = raw.get("hourly", {}).get("temperature_2m", [])

    # Extract only the 24 hourly values that belong to target_date
    hourly_temps = [
        t for ts, t in zip(hourly_times, hourly_temps_all)
        if ts.startswith(date_str)
    ]

    if not hourly_temps:
        return ForecastResult(
            city=city,
            target_date=target_date,
            daily_max_c=daily_max_c,
            hourly_spread=[daily_max_c],
            confidence_interval_c=1.5,
        )

    n = len(hourly_temps)
    mean = sum(hourly_temps) / n
    variance = sum((t - mean) ** 2 for t in hourly_temps) / n
    std = math.sqrt(variance) if variance > 0 else 1.0

    return ForecastResult(
        city=city,
        target_date=target_date,
        daily_max_c=daily_max_c,
        hourly_spread=hourly_temps,
        confidence_interval_c=max(std, 0.5),
    )


def _parse_forecast(city: str, target_date: date, raw: dict) -> ForecastResult:
    """Wrapper for single-date responses (backward compat with get_forecast)."""
    return _parse_forecast_from_range(city, target_date, raw)
