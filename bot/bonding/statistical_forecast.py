"""
statistical_forecast.py — ARIMA(5,0,0) + Naïve temperature forecasts.

Provides a 4th source for SourceConsensus, trained on 2 years of observed
daily max temperatures from the Open-Meteo archive API.

Model blend:
  - Naïve:        yesterday's observed high (catches current regime)
  - ARIMA(5,0,0): AR(5) fitted on 2 years of history (captures autocorrelation)
  Both models are averaged equally when both are available.

Integration:
  SourceConsensus.statistical holds the returned ForecastResult.
  consensus_prob() weights this source at BOND_STATISTICAL_WEIGHT (< 1.0)
  vs 1.0 for each meteorological source (GFS, ECMWF, TIO).

Archive lag:
  Open-Meteo archive has ~5 day lag. The most recent available observation
  (which may be a few days old) is used for the Naïve model. ARIMA fits on
  all available history, so a small lag at the tail has minimal effect.

Seeding:
  seed_all_cities() is called once at BOND/PAPER startup. It fetches 2 years
  of daily max temps for every city in BOND_CITIES that hasn't been seeded.
  Already-seeded cities are skipped. Subsequent startups only fetch missing
  days (update_city), keeping API usage low.
"""
import asyncio
import json
import logging
import os
import time
from datetime import date, timedelta
from typing import Optional, TYPE_CHECKING

import aiohttp

if TYPE_CHECKING:
    from bonding.weather_client import ForecastResult

log = logging.getLogger("bond.statistical")

ARCHIVE_API_URL   = "https://archive-api.open-meteo.com/v1/archive"
_SEED_YEARS       = 2
_ARCHIVE_LAG_DAYS = 5    # archive typically lags ~5 days behind today
_MIN_HISTORY_DAYS = 10   # minimum observations to produce any forecast
_AR_ORDER         = 5    # ARIMA(5,0,0) — as per the NYC study
_REQUEST_INTERVAL = 3.0  # seconds between archive API calls (matches weather_client)
_MODEL_CACHE_TTL  = 86_400  # 24 h — refit ARIMA once per day per city

# In-memory stores populated at startup
_history: dict[str, dict[str, float]] = {}   # city → {date_str: daily_max_c}
_history_loaded = False

# ARIMA prediction cache: city → (computed_at, {date_str: point_forecast_c})
_prediction_cache: dict[str, tuple[float, dict[str, float]]] = {}

_request_lock: Optional[asyncio.Lock] = None
_last_request_time: float = 0.0


def _get_lock() -> asyncio.Lock:
    global _request_lock
    if _request_lock is None:
        _request_lock = asyncio.Lock()
    return _request_lock


def _cache_path() -> str:
    import config as _config
    return _config.BOND_STATISTICAL_CACHE_PATH


# ── Disk persistence ──────────────────────────────────────────────────────────

def load_history() -> None:
    """Load per-city daily max temp history from disk. Idempotent."""
    global _history_loaded
    if _history_loaded:
        return
    _history_loaded = True
    path = _cache_path()
    try:
        with open(path) as f:
            _history.update(json.load(f))
        total = sum(len(v) for v in _history.values())
        log.info(
            f"statistical: loaded history for {len(_history)} cities "
            f"({total} obs) from {path}"
        )
    except FileNotFoundError:
        pass
    except Exception as exc:
        log.warning(f"statistical: failed to load history cache: {exc}")


def _save_history() -> None:
    path = _cache_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_history, f)
        os.replace(tmp, path)
    except Exception as exc:
        log.warning(f"statistical: failed to save history cache: {exc}")


# ── Archive API ───────────────────────────────────────────────────────────────

async def _fetch_daily_max(
    lat: float,
    lon: float,
    start: date,
    end: date,
) -> dict[str, float]:
    """
    Fetch daily max temperatures from Open-Meteo archive API.
    Returns {date_str: max_temp_c} for dates where data is available.
    Uses its own rate limiter (separate from weather_client's limiter).
    """
    global _last_request_time
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
        "daily":      "temperature_2m_max",
        "timezone":   "UTC",
    }
    timeout = aiohttp.ClientTimeout(total=30)
    async with _get_lock():
        gap = time.time() - _last_request_time
        if gap < _REQUEST_INTERVAL:
            await asyncio.sleep(_REQUEST_INTERVAL - gap)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(ARCHIVE_API_URL, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
        _last_request_time = time.time()

    times = data.get("daily", {}).get("time", [])
    temps = data.get("daily", {}).get("temperature_2m_max", [])
    return {t: float(v) for t, v in zip(times, temps) if v is not None}


# ── Seeding & incremental updates ─────────────────────────────────────────────

async def _seed_city(city: str, lat: float, lon: float) -> None:
    """Fetch 2 years of history for a city that has no or partial coverage."""
    today      = date.today()
    seed_start = today - timedelta(days=_SEED_YEARS * 365)
    seed_end   = today - timedelta(days=_ARCHIVE_LAG_DAYS)

    log.info(f"statistical: seeding {city} ({seed_start} → {seed_end})")
    try:
        data = await _fetch_daily_max(lat, lon, seed_start, seed_end)
        if city not in _history:
            _history[city] = {}
        _history[city].update(data)
        _save_history()
        log.info(f"statistical: seeded {city} — {len(data)} days")
    except Exception as exc:
        log.warning(f"statistical: seed failed for {city}: {exc}")


async def _update_city(city: str, lat: float, lon: float) -> None:
    """Fetch any missing days since the last observation up to the archive lag cutoff."""
    today      = date.today()
    archive_end = today - timedelta(days=_ARCHIVE_LAG_DAYS)

    existing = _history.get(city, {})
    if not existing:
        await _seed_city(city, lat, lon)
        return

    latest     = date.fromisoformat(max(existing.keys()))
    fetch_from = latest + timedelta(days=1)
    if fetch_from > archive_end:
        return  # already up to date

    try:
        data = await _fetch_daily_max(lat, lon, fetch_from, archive_end)
        if data:
            _history[city].update(data)
            _save_history()
            log.info(
                f"statistical: updated {city} +{len(data)} days "
                f"(→ {max(data.keys())})"
            )
    except Exception as exc:
        log.warning(f"statistical: update failed for {city}: {exc}")


def _needs_seeding(city: str) -> bool:
    """
    Return True if the city has fewer than MIN_HISTORY_DAYS of data OR
    its earliest record is less than (SEED_YEARS - 0.1) years back.
    """
    existing = _history.get(city, {})
    if len(existing) < _MIN_HISTORY_DAYS:
        return True
    earliest = min(existing.keys())
    threshold = date.today() - timedelta(days=int(_SEED_YEARS * 365 * 0.9))
    return date.fromisoformat(earliest) > threshold


async def seed_all_cities(cities: dict[str, tuple[float, float]]) -> None:
    """
    Called once at BOND/PAPER startup. Seeds any city below coverage threshold,
    then fills recent gaps for already-seeded cities.

    cities: BOND_CITIES dict (name → (lat, lon))
    """
    load_history()

    to_seed   = [c for c in cities if _needs_seeding(c)]
    to_update = [c for c in cities if not _needs_seeding(c)]

    if to_seed:
        log.info(f"statistical: seeding {len(to_seed)} new cities (2-year history)")
    for city in to_seed:
        lat, lon = cities[city]
        await _seed_city(city, lat, lon)

    if to_update:
        log.info(f"statistical: updating {len(to_update)} existing cities")
    for city in to_update:
        lat, lon = cities[city]
        await _update_city(city, lat, lon)

    log.info(
        f"statistical: seeding complete — "
        f"{len(_history)} cities, "
        f"{sum(len(v) for v in _history.values())} total observations"
    )


# ── Model predictions ─────────────────────────────────────────────────────────

def _sorted_temps(city: str) -> list[tuple[date, float]]:
    """Return chronologically sorted (date, temp_c) pairs for a city."""
    items = [(date.fromisoformat(k), v) for k, v in _history.get(city, {}).items()]
    items.sort(key=lambda x: x[0])
    return items


def _naive_pred(sorted_temps: list[tuple[date, float]]) -> Optional[float]:
    """
    Return the most recent observed daily high as the Naïve forecast.
    Due to archive lag this is typically a few days old, but captures
    the current temperature regime better than seasonal averages.
    """
    if not sorted_temps:
        return None
    return sorted_temps[-1][1]


def _arima_pred(city: str, sorted_temps: list[tuple[date, float]]) -> Optional[float]:
    """
    ARIMA(5,0,0) one-step-ahead forecast fitted on the full 2-year history.
    Results are cached for _MODEL_CACHE_TTL (24 h) per city to avoid
    refitting on every 60-second scan cycle.

    Returns the predicted daily max temperature in °C, or None on failure.
    """
    try:
        from statsmodels.tsa.arima.model import ARIMA
        import numpy as np
    except ImportError:
        log.warning("statistical: statsmodels not installed — ARIMA unavailable")
        return None

    if len(sorted_temps) < _MIN_HISTORY_DAYS:
        return None

    now    = time.time()
    cached = _prediction_cache.get(city)
    if cached and (now - cached[0]) < _MODEL_CACHE_TTL:
        # Return cached forecast for the next day after the last training point
        _, forecasts = cached
        if forecasts:
            return next(iter(forecasts.values()))

    temps = np.array([v for _, v in sorted_temps], dtype=float)
    try:
        model  = ARIMA(temps, order=(_AR_ORDER, 0, 0))
        result = model.fit()
        pred   = float(result.forecast(steps=1)[0])
    except Exception as exc:
        log.warning(f"statistical: ARIMA fit failed for {city}: {exc}")
        return None

    next_date_str = (sorted_temps[-1][0] + timedelta(days=1)).isoformat()
    _prediction_cache[city] = (now, {next_date_str: pred})
    return pred


# ── Public interface ──────────────────────────────────────────────────────────

def get_statistical_forecast(
    city: str,
    target_date: date,
    sigma_c: float = 3.0,
    n_members: int = 100,
) -> Optional["ForecastResult"]:
    """
    Build a ForecastResult from the blended Naïve + ARIMA prediction.

    sigma_c:   std dev (°C) of synthetic ensemble members. ARIMA/Naïve models
               are less reliable than NWP ensembles; 3°C reflects higher
               uncertainty vs the ~2-3°C RMSE of meteorological sources.
    n_members: synthetic member count for probability resolution.

    Returns None if insufficient history exists for this city.
    """
    # Deferred import to avoid circular dependency at module load time
    from bonding.weather_client import ForecastResult
    import numpy as np

    load_history()
    sorted_temps = _sorted_temps(city)

    if len(sorted_temps) < _MIN_HISTORY_DAYS:
        log.debug(
            f"statistical: {city} — insufficient history "
            f"({len(sorted_temps)} days), skipping"
        )
        return None

    naive_p = _naive_pred(sorted_temps)
    arima_p = _arima_pred(city, sorted_temps)

    available = [p for p in [naive_p, arima_p] if p is not None]
    if not available:
        return None

    point_forecast = sum(available) / len(available)

    rng     = np.random.default_rng()
    members = rng.normal(loc=point_forecast, scale=sigma_c, size=n_members).tolist()

    log.debug(
        f"statistical: {city} {target_date} "
        f"naive={f'{naive_p:.1f}' if naive_p is not None else 'N/A'} "
        f"arima={f'{arima_p:.1f}' if arima_p is not None else 'N/A'} "
        f"blend={point_forecast:.1f}°C"
    )

    return ForecastResult(
        city=city,
        target_date=target_date,
        daily_max_c=point_forecast,
        ensemble_members=members,
    )


async def get_statistical_forecasts_batch(
    city_groups: dict[str, tuple[str, float, float, set]],
) -> dict[tuple, "ForecastResult"]:
    """
    Produce statistical forecasts for all (city, date) pairs in city_groups.
    city_groups mirrors the structure built in get_consensus_forecasts():
      canonical → (canonical, lat, lon, set[date])

    History is assumed already loaded/seeded at startup via seed_all_cities().
    This function only does in-memory lookups + ARIMA fits — no API calls
    during normal operation (updates happen once at startup).
    """
    load_history()
    results: dict[tuple, ForecastResult] = {}

    for canonical, lat, lon, dates in city_groups.values():
        for d in dates:
            fc = get_statistical_forecast(canonical, d)
            if fc is not None:
                results[(canonical, d)] = fc

    n_cities = len(city_groups)
    log.debug(
        f"statistical: batch — {len(results)} forecasts across {n_cities} cities"
    )
    return results
