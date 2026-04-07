"""
weather_client.py — Fetch city temperature forecasts from Open-Meteo.

No API key required. Free tier covers 10,000 calls/day — sufficient for
polling 20 cities every 6 minutes.

Responses are cached for 30 minutes to avoid redundant calls within a
scan cycle.

Rate limiting strategy:
- Batch all dates for a city into a single API call (date range fetch)
- Strict serialized rate limiter: min 1.5s between requests (~40 req/min)
- This reduces ~130 individual requests to ~60 city-range requests
"""
import asyncio
import logging
import math
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import aiohttp
import config as _config

log = logging.getLogger("bond.weather")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CACHE_TTL_SECS = 1800  # 30 minutes

# Minimum seconds between consecutive API requests (serial rate limiter).
# 1.5s = ~40 req/min, well within Open-Meteo free tier.
_MIN_REQUEST_INTERVAL = 1.5


class UnknownCityError(ValueError):
    """Raised when a city name cannot be resolved to coordinates."""


@dataclass
class ForecastResult:
    city: str
    target_date: date
    daily_max_c: float            # predicted daily high (°C)
    hourly_spread: list[float]    # hourly temps for the target day
    confidence_interval_c: float  # ±°C (std dev of hourly spread around daily max)


# ── Cache: (lat, lon, start_date_str, end_date_str) → (fetched_at_unix, raw) ─
_cache: dict[tuple, tuple[float, dict]] = {}
_cache_lock = asyncio.Lock()

# Serial rate limiter: only one request in-flight at a time, with mandatory
# gap between releases. This prevents burst-triggering 429s.
_rate_lock: Optional[asyncio.Lock] = None
_last_request_time: float = 0.0


def _get_rate_lock() -> asyncio.Lock:
    global _rate_lock
    if _rate_lock is None:
        _rate_lock = asyncio.Lock()
    return _rate_lock


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
    """
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {"name": name, "count": 1, "language": "en", "format": "json"}
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
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
    Uses a serial rate limiter (min _MIN_REQUEST_INTERVAL between requests).
    Cached for CACHE_TTL_SECS. Retries on 429 with exponential backoff.
    """
    global _last_request_time

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    cache_key = (round(lat, 4), round(lon, 4), start_str, end_str)

    async with _cache_lock:
        if cache_key in _cache:
            fetched_at, data = _cache[cache_key]
            if time.monotonic() - fetched_at < CACHE_TTL_SECS:
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

    # Serial rate limiter: enforce minimum gap between requests
    async with _get_rate_lock():
        now = time.monotonic()
        gap = now - _last_request_time
        if gap < _MIN_REQUEST_INTERVAL:
            await asyncio.sleep(_MIN_REQUEST_INTERVAL - gap)

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

        _last_request_time = time.monotonic()

    async with _cache_lock:
        _cache[cache_key] = (time.monotonic(), data)

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
