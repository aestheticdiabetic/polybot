"""
weather_client.py — Fetch city temperature forecasts from Open-Meteo.

No API key required. Free tier covers 10,000 calls/day — sufficient for
polling 20 cities every 6 minutes.

Responses are cached for 30 minutes to avoid redundant calls within a
scan cycle.
"""
import asyncio
import logging
import math
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import aiohttp
import config as _config

log = logging.getLogger("bond.weather")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CACHE_TTL_SECS = 1800  # 30 minutes


class UnknownCityError(ValueError):
    """Raised when a city name cannot be resolved to coordinates."""


@dataclass
class ForecastResult:
    city: str
    target_date: date
    daily_max_c: float            # predicted daily high (°C)
    hourly_spread: list[float]    # hourly temps for the target day
    confidence_interval_c: float  # ±°C (std dev of hourly spread around daily max)


# ── Cache: (lat, lon, date_str) → (fetched_at_unix, raw_api_response) ─
_cache: dict[tuple, tuple[float, dict]] = {}
_cache_lock = asyncio.Lock()


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
    Batch-fetch forecasts for all (city, date) pairs concurrently.
    De-dupes identical requests. Returns dict keyed by (canonical_city, date).
    Unknown cities are logged and skipped (not raised).
    """
    # Resolve and de-dupe
    resolved: dict[tuple[str, date], tuple[str, float, float]] = {}
    for city, d in city_date_pairs:
        try:
            canonical, lat, lon = _resolve_city(city)
            resolved[(canonical, d)] = (canonical, lat, lon)
        except UnknownCityError:
            log.warning(f"weather: skipping unknown city '{city}'")

    # Fetch concurrently
    async def _fetch_one(key: tuple[str, date]) -> tuple[tuple[str, date], Optional[ForecastResult]]:
        canonical, d = key
        _, lat, lon = resolved[key]
        try:
            raw = await _fetch_open_meteo(lat, lon, d)
            return key, _parse_forecast(canonical, d, raw)
        except Exception as exc:
            log.warning(f"weather: failed to fetch {canonical} {d}: {exc}")
            return key, None

    tasks = [_fetch_one(k) for k in resolved]
    pairs = await asyncio.gather(*tasks)
    return {k: v for k, v in pairs if v is not None}


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


async def _fetch_open_meteo(lat: float, lon: float, target_date: date) -> dict:
    """
    GET https://api.open-meteo.com/v1/forecast
    Params: latitude, longitude, daily=temperature_2m_max,
            hourly=temperature_2m, timezone=auto,
            start_date, end_date (same day)
    Cached for CACHE_TTL_SECS.
    """
    date_str = target_date.isoformat()
    cache_key = (round(lat, 4), round(lon, 4), date_str)

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
        "start_date": date_str,
        "end_date":   date_str,
    }

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(OPEN_METEO_URL, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()

    async with _cache_lock:
        _cache[cache_key] = (time.monotonic(), data)

    return data


def _parse_forecast(city: str, target_date: date, raw: dict) -> ForecastResult:
    """
    Extract daily_max_c and hourly_spread from Open-Meteo response.
    Computes confidence_interval_c as std dev of hourly temps for the day.
    """
    try:
        daily_max_c: float = raw["daily"]["temperature_2m_max"][0]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"Unexpected Open-Meteo response shape for {city}: {exc}") from exc

    hourly_temps: list[float] = raw.get("hourly", {}).get("temperature_2m", [])
    if not hourly_temps:
        return ForecastResult(
            city=city,
            target_date=target_date,
            daily_max_c=daily_max_c,
            hourly_spread=[daily_max_c],
            confidence_interval_c=1.5,
        )

    # Use 6-hour window around daily max for confidence interval
    # Full 24 hourly values; std dev gives natural spread
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
