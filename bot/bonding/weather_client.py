"""
weather_client.py — Fetch city temperature forecasts from Open-Meteo ensemble API.

Uses the GFS Seamless ensemble model (30 members) to obtain a true empirical
probability distribution over daily maximum temperatures, replacing the previous
Gaussian approximation that was inflated by diurnal spread.

No API key required. Free tier limits:
  - 10,000 calls/day
  -  5,000 calls/hour
  -    600 calls/minute

Rate limiting notes:
- Ensemble requests fetch `temperature_2m_max` + 30 member variables = 31 vars.
  Open-Meteo counts this as ~3.1 API calls per request (>10 vars threshold).
- Disk-persistent cache (2h TTL) keeps daily usage well under 10k even at scale.
- Batch all dates for a city into a single API call (date range fetch).
- Strict serial rate limiter: min 3s between requests (~20 req/min).
"""
import asyncio
import collections
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import aiohttp
import config as _config

log = logging.getLogger("bond.weather")

OPEN_METEO_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
ENSEMBLE_MODEL = "gfs_seamless"  # 30 members, global coverage, free tier

# In-memory TTL: re-check if data is older than 30 min within a session
MEM_CACHE_TTL_SECS  = 1800   # 30 minutes
# Disk TTL: don't re-fetch from API if disk entry is fresher than 2 hours
DISK_CACHE_TTL_SECS = 7200   # 2 hours

DISK_CACHE_PATH = os.environ.get("WEATHER_CACHE_PATH", "/app/data/ensemble_cache.json")

# Minimum seconds between consecutive API requests.
# 3s = ~20 req/min — well under the 600/min Open-Meteo limit.
_MIN_REQUEST_INTERVAL = 3.0

# Open-Meteo free-tier hard limits (enforced via sliding-window counters below)
_LIMIT_PER_MINUTE = 600
_LIMIT_PER_HOUR   = 5_000
_LIMIT_PER_DAY    = 10_000

# Timestamps of every successful API call in the last 24 hours.
_api_call_log: collections.deque[float] = collections.deque()


class UnknownCityError(ValueError):
    """Raised when a city name cannot be resolved to coordinates."""


@dataclass
class ForecastResult:
    city: str
    target_date: date
    daily_max_c: float          # ensemble control run daily high (°C)
    ensemble_members: list[float]  # daily max from each ensemble member (°C)


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

    if day_count >= _LIMIT_PER_DAY * 0.9:
        log.warning(f"Open-Meteo daily budget at {day_count}/{_LIMIT_PER_DAY} — approaching limit")
    elif hour_count >= _LIMIT_PER_HOUR * 0.9:
        log.warning(f"Open-Meteo hourly budget at {hour_count}/{_LIMIT_PER_HOUR} — approaching limit")


def _record_api_call() -> None:
    _api_call_log.append(time.time())


def _load_disk_cache() -> None:
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
    Return ensemble forecast for a single city on target_date.
    Raises UnknownCityError if city cannot be resolved.
    """
    canonical, lat, lon = _resolve_city(city)
    raw = await _fetch_ensemble_range(lat, lon, target_date, target_date)
    return _parse_ensemble_from_range(canonical, target_date, raw)


async def get_all_forecasts(
    city_date_pairs: list[tuple[str, date]],
) -> dict[tuple[str, date], ForecastResult]:
    """
    Batch-fetch ensemble forecasts for all (city, date) pairs.
    Groups dates by city so each city requires only ONE API call (date range).
    De-dupes identical requests. Returns dict keyed by (canonical_city, date).
    Unknown cities are logged and skipped (not raised).
    """
    max_forecast_date = date.today() + timedelta(days=16)

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
        f"city_requests={len(loc_map)} model={ENSEMBLE_MODEL} (serial, {_MIN_REQUEST_INTERVAL}s interval)"
    )

    results: dict[tuple[str, date], ForecastResult] = {}
    failed = 0

    for canonical, lat, lon, dates in loc_map.values():
        start_d = min(dates)
        end_d = max(dates)
        try:
            raw = await _fetch_ensemble_range(lat, lon, start_d, end_d)
            for d in dates:
                try:
                    result = _parse_ensemble_from_range(canonical, d, raw)
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
    Return empirical probability (0–1) that the day's high falls in [temp_min, temp_max].

    Counts ensemble members whose predicted daily maximum falls within the range.
    With 30 GFS members the resolution is 1/30 ≈ 3.3 percentage points.
    Falls back to 0.0 if no members are available.
    """
    members = forecast.ensemble_members
    if not members:
        return 0.0
    count = sum(1 for m in members if temp_min <= m <= temp_max)
    return count / len(members)


def celsius_to_fahrenheit(c: float) -> float:
    return c * 9 / 5 + 32


def fahrenheit_to_celsius(f: float) -> float:
    return (f - 32) * 5 / 9


async def geocode_city(name: str) -> tuple[str, float, float]:
    """
    Resolve a city name to (display_name, lat, lon) using Open-Meteo geocoding.
    Free API, no key required. Goes through the shared rate limiter.
    Raises UnknownCityError if no results found.
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

    if city_name in cities:
        lat, lon = cities[city_name]
        return city_name, lat, lon

    canonical = aliases.get(city_name)
    if canonical and canonical in cities:
        lat, lon = cities[canonical]
        return canonical, lat, lon

    lower = city_name.lower()
    for name, coords in cities.items():
        if name.lower() == lower:
            return name, coords[0], coords[1]
    for alias, can in aliases.items():
        if alias.lower() == lower and can in cities:
            lat, lon = cities[can]
            return can, lat, lon

    raise UnknownCityError(f"Cannot resolve city '{city_name}' to coordinates")


async def _fetch_ensemble_range(
    lat: float, lon: float, start_date: date, end_date: date
) -> dict:
    """
    Fetch GFS ensemble daily max temperatures for a date range.
    Checks disk cache (DISK_CACHE_TTL_SECS), then in-memory (MEM_CACHE_TTL_SECS).
    Uses serial rate limiter between live requests.
    """
    global _last_request_time

    _load_disk_cache()

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    cache_key = (round(lat, 4), round(lon, 4), start_str, end_str)

    async with _cache_lock:
        if cache_key in _cache:
            fetched_at, data = _cache[cache_key]
            if time.time() - fetched_at < MEM_CACHE_TTL_SECS:
                return data

    params = {
        "latitude":   lat,
        "longitude":  lon,
        "daily":      "temperature_2m_max",
        "models":     ENSEMBLE_MODEL,
        "timezone":   "auto",
        "start_date": start_str,
        "end_date":   end_str,
    }

    timeout = aiohttp.ClientTimeout(total=20)

    async with _get_rate_lock():
        gap = time.time() - _last_request_time
        if gap < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - gap)

        _check_limits()

        for attempt in range(5):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(
                        OPEN_METEO_ENSEMBLE_URL, params=params
                    ) as resp:
                        if resp.status == 429:
                            wait = _MIN_REQUEST_INTERVAL * (2 ** attempt)
                            log.warning(
                                f"Open-Meteo ensemble 429 at ({lat:.2f},{lon:.2f}) "
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
                    log.warning(f"Open-Meteo ensemble 429 (exc) — retry in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                raise
        else:
            raise RuntimeError(
                f"Open-Meteo ensemble rate limit: max retries exceeded for ({lat:.2f},{lon:.2f})"
            )

        _last_request_time = time.time()
        _record_api_call()

    now = time.time()
    async with _cache_lock:
        _cache[cache_key] = (now, data)

    _save_disk_cache()
    return data


def _parse_ensemble_from_range(city: str, target_date: date, raw: dict) -> ForecastResult:
    """
    Extract ensemble daily max temperatures for target_date from an Open-Meteo
    ensemble response. Collects the control run value and all member values.

    Member keys follow the pattern: temperature_2m_max_member{N:02d}
    The control run is keyed as: temperature_2m_max
    """
    date_str = target_date.isoformat()
    daily = raw.get("daily", {})

    times: list[str] = daily.get("time", [])
    if date_str not in times:
        raise ValueError(f"Date {date_str} not found in ensemble response for {city}")

    day_idx = times.index(date_str)

    control_series = daily.get("temperature_2m_max")
    if not control_series:
        raise ValueError(f"Missing temperature_2m_max in ensemble response for {city}")
    daily_max_c: float = control_series[day_idx]

    # Collect all member series dynamically (member01, member02, ...)
    members: list[float] = []
    for key, series in daily.items():
        if key.startswith("temperature_2m_max_member") and isinstance(series, list):
            val = series[day_idx]
            if val is not None:
                members.append(float(val))

    if not members:
        log.warning(f"weather: no ensemble members found for {city} {date_str}, using control only")
        members = [daily_max_c]

    log.debug(
        f"weather: {city} {date_str} control={daily_max_c:.1f}°C "
        f"members={len(members)} range=[{min(members):.1f}, {max(members):.1f}]°C"
    )

    return ForecastResult(
        city=city,
        target_date=target_date,
        daily_max_c=daily_max_c,
        ensemble_members=members,
    )
