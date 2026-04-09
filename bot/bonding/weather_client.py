"""
weather_client.py — Fetch city temperature forecasts from Open-Meteo.

Two-source strategy based on time-to-resolution:

  ≥ 2 days out   → GFS Seamless ensemble (30 members, 6-hourly updates)
                    Proper probabilistic spread over daily maximum temperatures.

  Today/tomorrow → Open-Meteo hourly forecast API (1-2 hourly updates)
                   Past hours are model-analysis (observation-blended), giving a
                   hard running-maximum floor for same-day markets. Synthetic
                   ensemble members capture remaining forecast uncertainty.

No API key required. Free tier limits:
  - 10,000 calls/day
  -  5,000 calls/hour
  -    600 calls/minute

Rate limiting notes:
- Ensemble requests count as ~3.1 API calls (>10 vars). Hourly = 1 call.
- Both caches are disk-persistent (ensemble 2h TTL, near-term 1h TTL).
- Strict serial rate limiter: min 3s between requests (~20 req/min).
"""
import asyncio
import collections
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import aiohttp
import config as _config
from bonding import peak_hour_stats as _peak_stats

log = logging.getLogger("bond.weather")

OPEN_METEO_ENSEMBLE_URL  = "https://ensemble-api.open-meteo.com/v1/ensemble"
OPEN_METEO_FORECAST_URL  = "https://api.open-meteo.com/v1/forecast"
ENSEMBLE_MODEL           = "gfs_seamless"  # 30 members, global coverage, free tier

# Use hourly API when target date is today or tomorrow
NEARTERM_THRESHOLD_DAYS = 2

# ── Cache TTLs ────────────────────────────────────────────────────────────────
DISK_CACHE_TTL_SECS               = 7200   # 2 hours — ensemble data
NEARTERM_CACHE_TTL_SECS           = 3600   # 1 hour  — near-term hourly data (tomorrow)
NEARTERM_CACHE_TTL_SAME_DAY_SECS  = 900    # 15 min  — same-day (market underway; stale = false signals)

DISK_CACHE_PATH          = os.environ.get("WEATHER_CACHE_PATH",  "/app/data/ensemble_cache.json")
NEARTERM_DISK_CACHE_PATH = os.environ.get("NEARTERM_CACHE_PATH", "/app/data/nearterm_cache.json")

# Near-term forecast uncertainty: std dev of synthetic ensemble members (°C).
# Calibrated to typical Open-Meteo short-range RMSE for daily max temperature.
NEARTERM_SIGMA_SAME_DAY = 1.0   # same-day (observed hours give hard running-max floor)
NEARTERM_SIGMA_NEXT_DAY = 1.5   # next-day (full day still ahead)
NEARTERM_MEMBERS        = 100   # synthetic member count (finer resolution than 30)

# Post-peak decay: anchor hour is now dynamic per city/month (see get_gate_hour()).
# Decay starts at (gate_hour - 1) and converges fully over _POST_PEAK_DECAY_HOURS.
# At full decay forecast_max = running_max and sigma = 0, so markets where the
# target temp hasn't been reached yet get probability 0 (hard skip).
_POST_PEAK_DECAY_HOURS = 2.0

# Minimum seconds between consecutive API requests (~20 req/min, well under 600/min limit)
_MIN_REQUEST_INTERVAL = 3.0

# Open-Meteo free-tier hard limits
_LIMIT_PER_MINUTE = 600
_LIMIT_PER_HOUR   = 5_000
_LIMIT_PER_DAY    = 10_000

# Timestamps of every successful API call in the last 24 hours
_api_call_log: collections.deque[float] = collections.deque()


class UnknownCityError(ValueError):
    """Raised when a city name cannot be resolved to coordinates."""


@dataclass
class ForecastResult:
    city: str
    target_date: date
    daily_max_c: float            # ensemble control or hourly forecast daily high (°C)
    ensemble_members: list[float] # daily max from each member (real or synthetic) (°C)
    forecast_peak_hour: Optional[int] = None  # local hour (0-23) of forecast daily max; None for ensemble


# ── Ensemble cache (disk-persistent, 2h TTL) ─────────────────────────────────
_cache: dict[tuple, tuple[float, dict]] = {}
_cache_lock = asyncio.Lock()
_disk_cache_loaded = False

# ── Near-term cache (disk-persistent, 1h TTL) ────────────────────────────────
_nearterm_cache: dict[tuple, tuple[float, dict]] = {}
_nearterm_cache_lock = asyncio.Lock()
_nearterm_disk_cache_loaded = False

# In-memory peak hour stats — loaded once at startup via init_peak_stats()
_peak_hour_stats: dict = {}

# (city, date) pairs for which a peak-hour observation has already been recorded today.
# Cleared of stale entries on each call to _parse_nearterm_forecast.
_observation_recorded: set[tuple[str, date]] = set()

# Serial rate limiter (shared by all API calls)
_rate_lock: Optional[asyncio.Lock] = None
_last_request_time: float = 0.0


def _get_rate_lock() -> asyncio.Lock:
    global _rate_lock
    if _rate_lock is None:
        _rate_lock = asyncio.Lock()
    return _rate_lock


def init_peak_stats(path: Optional[str] = None) -> None:
    """Load peak hour stats into module-level cache. Call once at bot startup."""
    global _peak_hour_stats
    if path:
        _peak_hour_stats = _peak_stats.load_stats(path)
    else:
        _peak_hour_stats = _peak_stats.load_stats()
    log.info(f"weather: loaded peak hour stats for {len(_peak_hour_stats)} cities")


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


# ── Ensemble disk cache ───────────────────────────────────────────────────────

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
            log.info(f"weather: loaded {loaded} ensemble entries from disk ({DISK_CACHE_PATH})")
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


# ── Near-term disk cache ──────────────────────────────────────────────────────

def _load_nearterm_disk_cache() -> None:
    global _nearterm_disk_cache_loaded
    if _nearterm_disk_cache_loaded:
        return
    _nearterm_disk_cache_loaded = True
    try:
        with open(NEARTERM_DISK_CACHE_PATH) as f:
            saved: dict = json.load(f)
        now = time.time()
        loaded = 0
        for key_str, entry in saved.items():
            age = now - entry.get("fetched_at", 0)
            if age < NEARTERM_CACHE_TTL_SECS:
                parts = key_str.split("|")
                key = (float(parts[0]), float(parts[1]), parts[2])
                _nearterm_cache[key] = (entry["fetched_at"], entry["data"])
                loaded += 1
        if loaded:
            log.info(f"weather: loaded {loaded} nearterm entries from disk ({NEARTERM_DISK_CACHE_PATH})")
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning(f"weather: failed to load nearterm disk cache: {exc}")


def _save_nearterm_disk_cache() -> None:
    try:
        os.makedirs(os.path.dirname(NEARTERM_DISK_CACHE_PATH), exist_ok=True)
        now = time.time()
        to_save: dict = {}
        for key, (fetched_at, data) in _nearterm_cache.items():
            if now - fetched_at < NEARTERM_CACHE_TTL_SECS:
                key_str = f"{key[0]}|{key[1]}|{key[2]}"
                to_save[key_str] = {"fetched_at": fetched_at, "data": data}
        tmp = NEARTERM_DISK_CACHE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(to_save, f)
        os.replace(tmp, NEARTERM_DISK_CACHE_PATH)
    except Exception as exc:
        log.warning(f"weather: failed to save nearterm disk cache: {exc}")


# ── Public API ────────────────────────────────────────────────────────────────

async def get_forecast(city: str, target_date: date) -> ForecastResult:
    """
    Return forecast for a single city on target_date.
    Routes to the hourly API for today/tomorrow, ensemble for 2+ days out.
    Raises UnknownCityError if city cannot be resolved.
    """
    canonical, lat, lon = _resolve_city(city)
    today = date.today()
    nearterm_cutoff = today + timedelta(days=NEARTERM_THRESHOLD_DAYS - 1)

    if target_date <= nearterm_cutoff:
        raw = await _fetch_nearterm_hourly(lat, lon, target_date)
        return _parse_nearterm_forecast(canonical, target_date, raw)
    else:
        raw = await _fetch_ensemble_range(lat, lon, target_date, target_date)
        return _parse_ensemble_from_range(canonical, target_date, raw)


async def get_all_forecasts(
    city_date_pairs: list[tuple[str, date]],
) -> dict[tuple[str, date], ForecastResult]:
    """
    Batch-fetch forecasts for all (city, date) pairs.

    Routing:
      - today / tomorrow  → Open-Meteo hourly API (1-2h update cadence)
      - 2+ days out       → GFS ensemble (6h update cadence, 30 members)

    Ensemble dates are grouped by city into a single range request.
    Near-term dates are fetched individually (one request per city per date).
    Returns dict keyed by (canonical_city, date).
    Unknown cities are logged and skipped.
    """
    today = date.today()
    nearterm_cutoff = today + timedelta(days=NEARTERM_THRESHOLD_DAYS - 1)
    max_forecast_date = today + timedelta(days=16)

    nearterm_loc: dict[tuple, list] = {}  # loc_key -> [canonical, lat, lon, set[date]]
    ensemble_loc: dict[tuple, list] = {}

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
        target_map = nearterm_loc if d <= nearterm_cutoff else ensemble_loc

        if loc_key not in target_map:
            target_map[loc_key] = [canonical, lat, lon, set()]
        target_map[loc_key][3].add(d)

    if skipped_future:
        log.info(f"weather: skipped {skipped_future} pairs beyond 16-day forecast window")

    n_nearterm = sum(len(v[3]) for v in nearterm_loc.values())
    n_ensemble = sum(len(v[3]) for v in ensemble_loc.values())
    log.info(
        f"BOND_WEATHER_FETCH nearterm={n_nearterm} ensemble={n_ensemble} "
        f"model={ENSEMBLE_MODEL} (serial, {_MIN_REQUEST_INTERVAL}s interval)"
    )

    results: dict[tuple[str, date], ForecastResult] = {}
    failed = 0

    # Near-term: one API call per city per date
    for canonical, lat, lon, dates in nearterm_loc.values():
        for d in sorted(dates):
            try:
                raw = await _fetch_nearterm_hourly(lat, lon, d)
                results[(canonical, d)] = _parse_nearterm_forecast(canonical, d, raw)
            except Exception as exc:
                log.warning(f"weather: nearterm fetch failed {canonical} {d}: {exc}")
                failed += 1

    # Ensemble: one API call per city (date range)
    for canonical, lat, lon, dates in ensemble_loc.values():
        start_d, end_d = min(dates), max(dates)
        try:
            raw = await _fetch_ensemble_range(lat, lon, start_d, end_d)
            for d in dates:
                try:
                    results[(canonical, d)] = _parse_ensemble_from_range(canonical, d, raw)
                except Exception as exc:
                    log.warning(f"weather: ensemble parse failed {canonical} {d}: {exc}")
                    failed += 1
        except Exception as exc:
            log.warning(f"weather: ensemble fetch failed {canonical} {start_d}–{end_d}: {exc}")
            failed += len(dates)

    fetched = (n_nearterm + n_ensemble) - failed
    log.info(f"BOND_WEATHER_DONE fetched={fetched} failed={failed}")
    return results


def prob_in_range(
    forecast: ForecastResult,
    temp_min: float,
    temp_max: float,
) -> float:
    """
    Return empirical probability (0–1) that the day's high falls in [temp_min, temp_max].
    Works identically for real GFS ensemble members and synthetic near-term members.
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


# ── Internal helpers ──────────────────────────────────────────────────────────

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


async def _fetch_nearterm_hourly(lat: float, lon: float, target_date: date) -> dict:
    """
    Fetch hourly temperature_2m for a single day from Open-Meteo forecast API.
    Updated every 1-2 hours — much fresher than the 6-hourly GFS ensemble.
    Past hours in the response are model-analysis (observation-blended).
    Uses the shared serial rate limiter. Cache TTL: 15 min same-day, 1 hour tomorrow.
    """
    global _last_request_time

    _load_nearterm_disk_cache()

    cache_key = (round(lat, 4), round(lon, 4), target_date.isoformat())

    ttl = NEARTERM_CACHE_TTL_SAME_DAY_SECS if target_date == date.today() else NEARTERM_CACHE_TTL_SECS
    async with _nearterm_cache_lock:
        if cache_key in _nearterm_cache:
            fetched_at, data = _nearterm_cache[cache_key]
            if time.time() - fetched_at < ttl:
                return data

    params = {
        "latitude":   lat,
        "longitude":  lon,
        "hourly":     "temperature_2m",
        "current":    "temperature_2m",   # real-time reading (~15-min intervals)
        "timezone":   "auto",
        "start_date": target_date.isoformat(),
        "end_date":   target_date.isoformat(),
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
                    async with session.get(OPEN_METEO_FORECAST_URL, params=params) as resp:
                        if resp.status == 429:
                            wait = _MIN_REQUEST_INTERVAL * (2 ** attempt)
                            log.warning(
                                f"Open-Meteo forecast 429 at ({lat:.2f},{lon:.2f}) "
                                f"{target_date} — retry in {wait:.1f}s (attempt {attempt+1}/5)"
                            )
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        data = await resp.json()
                        break
            except aiohttp.ClientResponseError as exc:
                if exc.status == 429 and attempt < 4:
                    wait = _MIN_REQUEST_INTERVAL * (2 ** attempt)
                    await asyncio.sleep(wait)
                    continue
                raise
        else:
            raise RuntimeError(
                f"Open-Meteo forecast rate limit: max retries for ({lat:.2f},{lon:.2f})"
            )

        _last_request_time = time.time()
        _record_api_call()

    async with _nearterm_cache_lock:
        _nearterm_cache[cache_key] = (time.time(), data)

    _save_nearterm_disk_cache()
    return data


def _parse_nearterm_forecast(city: str, target_date: date, raw: dict) -> ForecastResult:
    """
    Parse Open-Meteo hourly forecast for a near-term market (today or tomorrow).

    Same-day markets:
      Past hours are model-analysis (observation-blended) and provide a hard
      running-maximum floor. If the running max already exceeds the temperature
      bucket being tested, probability collapses to zero automatically.
      Remaining hours use sigma=1.0°C uncertainty.

    Next-day markets:
      The full day is forecast; sigma=1.5°C reflects typical 24-48h RMSE.

    Returns NEARTERM_MEMBERS synthetic ensemble members drawn from
    N(forecast_max, sigma²), each floored at the observed running maximum.
    """
    hourly = raw.get("hourly", {})
    times: list[str] = hourly.get("time", [])
    temps: list      = hourly.get("temperature_2m", [])

    date_str = target_date.isoformat()
    day_temps = [
        float(t) for ts, t in zip(times, temps)
        if ts.startswith(date_str) and t is not None
    ]
    if not day_temps:
        raise ValueError(f"No hourly temps for {city} {date_str}")

    forecast_max = max(day_temps)
    today = date.today()
    running_max: Optional[float] = None

    if target_date == today:
        global _observation_recorded
        utc_offset_secs: int = raw.get("utc_offset_seconds", 0)
        local_ts = datetime.now(timezone.utc).timestamp() + utc_offset_secs
        current_hour_local = int((local_ts % 86400) // 3600)
        # Use city's local time for month (approximate via utc_offset)
        local_dt = datetime.now(timezone.utc) + timedelta(seconds=utc_offset_secs)
        current_month = local_dt.month

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

        # Extract forecast peak hour: local hour with highest forecast temp today.
        hour_temps = [
            (int(ts[11:13]), float(t))
            for ts, t in zip(times, temps)
            if ts.startswith(date_str) and t is not None
        ]
        forecast_peak_hour: Optional[int] = (
            max(hour_temps, key=lambda x: x[1])[0] if hour_temps else None
        )

        # Dynamic post-peak decay anchor using city's peak hour stats.
        # gate_hour = max(forecast_peak, p75_historical) + 1
        # Decay starts 1 hour before the gate (gate_hour - 1).
        gate_hour = _peak_stats.get_gate_hour(
            city, forecast_peak_hour, current_month, _peak_hour_stats
        )
        post_peak_hour = gate_hour - 1  # decay starts here

        # Post-peak decay: linearly weight forecast_max → running_max over 2 hours.
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
        if running_max is not None and current_hour_local >= gate_hour:
            today_date = date.today()
            # Clear stale entries from previous days
            _observation_recorded = {
                (c, d) for c, d in _observation_recorded if d >= today_date
            }
            obs_key = (city, target_date)
            if obs_key not in _observation_recorded:
                # Find the hour of observed peak temperature
                peak_obs_hour: Optional[int] = None
                peak_obs_temp = float("-inf")
                for ts, t in zip(times, temps):
                    if (ts.startswith(date_str) and t is not None
                            and int(ts[11:13]) <= current_hour_local):
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

    # Per-city bias correction (°C) derived from ERA5 calibration.
    # Shifts forecast_max before generating ensemble members.
    bias_c = _config.BOND_CITY_BIAS_CORRECTIONS.get(city, 0.0)
    if bias_c:
        forecast_max += bias_c
        if running_max is not None:
            running_max = max(running_max, forecast_max - abs(bias_c))
        log.debug(f"weather nearterm: {city} bias correction {bias_c:+.2f}°C applied")

    # Synthetic ensemble: N(forecast_max, sigma²), floored at observed running max
    members = [
        max(random.gauss(forecast_max, sigma), running_max)
        if running_max is not None
        else random.gauss(forecast_max, sigma)
        for _ in range(NEARTERM_MEMBERS)
    ]

    control = max(forecast_max, running_max) if running_max is not None else forecast_max

    log.debug(
        f"weather nearterm: {city} {date_str} "
        f"forecast_max={forecast_max:.1f}°C "
        f"running_max={f'{running_max:.1f}' if running_max is not None else 'n/a'}°C "
        f"sigma={sigma}"
    )

    return ForecastResult(
        city=city,
        target_date=target_date,
        daily_max_c=control,
        ensemble_members=members,
        forecast_peak_hour=forecast_peak_hour,
    )


async def _fetch_ensemble_range(
    lat: float, lon: float, start_date: date, end_date: date
) -> dict:
    """
    Fetch GFS ensemble daily max temperatures for a date range.
    Disk-persistent 2-hour cache. Uses shared serial rate limiter.
    """
    global _last_request_time

    _load_disk_cache()

    start_str = start_date.isoformat()
    end_str   = end_date.isoformat()
    cache_key = (round(lat, 4), round(lon, 4), start_str, end_str)

    async with _cache_lock:
        if cache_key in _cache:
            fetched_at, data = _cache[cache_key]
            # Use DISK_CACHE_TTL_SECS so disk-loaded entries survive restarts
            if time.time() - fetched_at < DISK_CACHE_TTL_SECS:
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

    async with _cache_lock:
        _cache[cache_key] = (time.time(), data)

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

    members: list[float] = []
    for key, series in daily.items():
        if key.startswith("temperature_2m_max_member") and isinstance(series, list):
            val = series[day_idx]
            if val is not None:
                members.append(float(val))

    if not members:
        log.warning(f"weather: no ensemble members found for {city} {date_str}, using control only")
        members = [daily_max_c]

    # Per-city bias correction (°C) derived from ERA5 calibration.
    bias_c = _config.BOND_CITY_BIAS_CORRECTIONS.get(city, 0.0)
    if bias_c:
        daily_max_c += bias_c
        members = [m + bias_c for m in members]
        log.debug(f"weather ensemble: {city} {date_str} bias correction {bias_c:+.2f}°C applied")

    log.debug(
        f"weather ensemble: {city} {date_str} control={daily_max_c:.1f}°C "
        f"members={len(members)} range=[{min(members):.1f}, {max(members):.1f}]°C"
    )

    return ForecastResult(
        city=city,
        target_date=target_date,
        daily_max_c=daily_max_c,
        ensemble_members=members,
    )
