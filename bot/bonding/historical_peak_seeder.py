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

from bonding.peak_hour_stats import (
    compute_p75,
    load_stats,
    save_stats,
    _STATS_PATH,
    SEED_MIN_SAMPLES,
)

log = logging.getLogger("bond.seeder")

HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"
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
    where peak_hour is the local hour index (0-23) with the highest temperature.
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
            month_num = int(date_str[5:7])
            month_key = str(month_num)
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
