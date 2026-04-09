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
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
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
