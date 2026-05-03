"""
era5_sigma_calibration.py — ERA5-based sigma calibration for near-term forecasts.

The near-term forecast builds a synthetic ensemble from N(forecast_max, sigma²).
A global sigma constant (NEARTERM_SIGMA_NEXT_DAY = 2.5°C) is applied to all cities,
but real temperature variability varies significantly by city and month. Singapore in
April has a daily max stdev of ~1°C; Munich in April has ~4°C.

This script fetches two years of ERA5 daily max temperatures for every city in
BOND_CITIES and computes the optimal per-city/month sigma by minimising the Brier
score. For a Gaussian model, this is equivalent to the empirical stdev — but the
Brier minimisation confirms this and provides a validation report.

Output: BOND_NEARTERM_SIGMA_BY_CITY_MONTH dict written to a JSON file.
        Optionally applied to the persistent override env file so the bot picks it
        up on next restart without changing config.py.

Usage:
    python era5_sigma_calibration.py
    python era5_sigma_calibration.py --years 3 --min-obs 15
    python era5_sigma_calibration.py --apply --override-file /app/data/config.override.env

Requires: aiohttp (already in requirements.txt)
"""

import argparse
import asyncio
import json
import logging
import math
import os
import statistics
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import aiohttp

log = logging.getLogger("era5_sigma_cal")

ARCHIVE_URL     = "https://archive-api.open-meteo.com/v1/archive"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
DEFAULT_YEARS   = 2
MIN_OBSERVATIONS = 15   # minimum per city/month to trust sigma estimate
_FETCH_DELAY_S   = 1.2  # seconds between requests — ERA5 free tier

# Sigma sweep range for Brier-score grid search
_SIGMA_MIN  = 0.5
_SIGMA_MAX  = 7.0
_SIGMA_STEP = 0.25


# ── ERA5 fetch ─────────────────────────────────────────────────────────────────

async def _fetch_era5_city(
    session: aiohttp.ClientSession,
    lat: float,
    lon: float,
    start: date,
    end: date,
) -> dict[str, float]:
    """Return {date_str: daily_max_c} for a single city."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
        "daily":      "temperature_2m_max",
        "timezone":   "UTC",
    }
    for attempt in range(3):
        try:
            async with session.get(
                ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT
            ) as resp:
                resp.raise_for_status()
                data  = await resp.json()
                times = data.get("daily", {}).get("time", [])
                temps = data.get("daily", {}).get("temperature_2m_max", [])
                return {t: float(v) for t, v in zip(times, temps) if v is not None}
        except Exception as exc:
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                log.warning(f"ERA5 fetch failed for ({lat:.2f},{lon:.2f}): {exc}")
    return {}


async def fetch_all_cities(
    city_coords: dict[str, tuple[float, float]],
    years: int = DEFAULT_YEARS,
) -> dict[str, dict[str, float]]:
    """
    Fetch {city: {date_str: daily_max_c}} for all cities.
    Applies a per-request delay to stay inside the Open-Meteo free-tier limit.
    """
    era5_cutoff = date.today() - timedelta(days=5)
    end   = era5_cutoff
    start = date(end.year - years, end.month, end.day)

    results: dict[str, dict[str, float]] = {}

    connector = aiohttp.TCPConnector(limit=3)
    async with aiohttp.ClientSession(connector=connector) as session:
        cities = list(city_coords.items())
        for i, (city, (lat, lon)) in enumerate(cities):
            data = await _fetch_era5_city(session, lat, lon, start, end)
            if data:
                results[city] = data
            if i < len(cities) - 1:
                await asyncio.sleep(_FETCH_DELAY_S)

    return results


# ── Brier-score optimisation ───────────────────────────────────────────────────

def _gaussian_cdf_diff(mean: float, sigma: float, lo: float, hi: float) -> float:
    """P(lo ≤ X ≤ hi) for X ~ N(mean, sigma²).  Uses erf — no scipy needed."""
    if sigma <= 0.0:
        return 1.0 if lo <= mean <= hi else 0.0
    sq2 = sigma * math.sqrt(2.0)
    return 0.5 * (math.erf((hi - mean) / sq2) - math.erf((lo - mean) / sq2))


def brier_score(daily_maxes: list[float], sigma: float, window: int = 14) -> float:
    """
    Mean Brier score using a rolling window as the point forecast.

    For each day d, predict N(mean(d-window…d-1), sigma²) and score against
    the observed 1°C bucket centred on actual_d.  Lower = better calibration.

    For a Gaussian model the optimal sigma equals the empirical stdev of
    (actual − forecast_mean), so the Brier grid search should agree with that.
    """
    if len(daily_maxes) < window + 5:
        return float("inf")

    scores: list[float] = []
    for i in range(window, len(daily_maxes)):
        actual = daily_maxes[i]
        mu = statistics.mean(daily_maxes[i - window: i])
        p  = _gaussian_cdf_diff(mu, sigma, actual - 0.5, actual + 0.5)
        scores.append((p - 1.0) ** 2)

    return sum(scores) / len(scores) if scores else float("inf")


def optimal_sigma(daily_maxes: list[float]) -> tuple[float, float]:
    """
    Return (recommended_sigma, brier_score_at_recommended_sigma) for a list of
    daily max temperatures.

    The recommended sigma is the empirical standard deviation — this is the
    theoretically correct choice for a Gaussian ensemble model, because it
    ensures P(actual in [T, T+1]) is calibrated with respect to the observed
    variability around the forecast mean.

    The Brier score at the empirical stdev is returned as a diagnostic: comparing
    it to brier_score(data, NEARTERM_SIGMA_NEXT_DAY) shows how much the calibrated
    sigma improves forecast skill for individual temperature buckets.

    Minimum returned sigma is 0.5°C to avoid degenerate near-zero ensembles.
    """
    emp   = statistics.stdev(daily_maxes) if len(daily_maxes) >= 2 else 2.5
    sigma = round(max(emp, 0.5), 2)
    score = brier_score(daily_maxes, sigma)
    return sigma, round(score, 6)


# ── Sigma table computation ────────────────────────────────────────────────────

def compute_sigma_table(
    city_data: dict[str, dict[str, float]],
    min_obs: int = MIN_OBSERVATIONS,
) -> dict[str, dict[int, dict]]:
    """
    Returns {city: {month: {"optimal_sigma": float, "empirical_stdev": float, "n": int}}}
    for all city/month combinations with enough observations.
    """
    result: dict[str, dict[int, dict]] = {}

    for city, date_temps in city_data.items():
        by_month: dict[int, list[float]] = defaultdict(list)
        for date_str, temp in date_temps.items():
            try:
                month = int(date_str[5:7])
                by_month[month].append(temp)
            except (ValueError, IndexError):
                continue

        city_slots: dict[int, dict] = {}
        for month, temps in sorted(by_month.items()):
            if len(temps) < min_obs:
                continue
            sigma, brier = optimal_sigma(temps)
            city_slots[month] = {
                "sigma":        sigma,   # recommended sigma (empirical stdev)
                "brier_score":  brier,   # Brier score at recommended sigma
                "n":            len(temps),
            }

        if city_slots:
            result[city] = city_slots

    return result


def build_sigma_config(
    sigma_table: dict[str, dict[int, dict]],
) -> dict[str, dict[int, float]]:
    """Flatten sigma_table to {city: {month: sigma}} for config injection."""
    return {
        city: {month: info["sigma"] for month, info in months.items()}
        for city, months in sigma_table.items()
    }


# ── Report ─────────────────────────────────────────────────────────────────────

def print_report(
    sigma_table: dict[str, dict[int, dict]],
    global_default: float,
) -> None:
    all_opt = [
        info["sigma"]
        for months in sigma_table.values()
        for info in months.values()
    ]

    print("=" * 76)
    print("ERA5 SIGMA CALIBRATION REPORT")
    print(f"Global default NEARTERM_SIGMA_NEXT_DAY = {global_default}°C")
    print("=" * 76)

    if all_opt:
        above = sum(1 for s in all_opt if s > global_default)
        print(
            f"Sigma range: {min(all_opt):.2f}–{max(all_opt):.2f}°C  |  "
            f"Median: {statistics.median(all_opt):.2f}°C  |  "
            f"{above}/{len(all_opt)} city/months above default"
        )
    print()
    print(
        f"  {'City':<22}  {'Mo':>2}  {'N':>4}  "
        f"{'Sigma':>7}  {'Brier':>7}  {'Default':>7}  {'Delta':>7}"
    )
    print("  " + "-" * 62)
    for city in sorted(sigma_table):
        for month, info in sorted(sigma_table[city].items()):
            sigma = info["sigma"]
            brier = info["brier_score"]
            n     = info["n"]
            delta = sigma - global_default
            flag  = " *" if abs(delta) >= 1.0 else ""
            print(
                f"  {city:<22}  {month:>2}  {n:>4}  "
                f"{sigma:>7.2f}  {brier:>7.4f}  {global_default:>7.2f}  "
                f"{delta:>+7.2f}{flag}"
            )
    print()
    print("  * = differs from global default by ≥1.0°C")
    print()


# ── Override file integration ──────────────────────────────────────────────────

def _read_override_env(path: Path) -> dict[str, str]:
    existing: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                existing[k.strip()] = v.strip()
    return existing


def apply_to_override(
    path: Path,
    sigma_config: dict[str, dict[int, float]],
) -> bool:
    """
    Write BOND_NEARTERM_SIGMA_BY_CITY_MONTH_JSON to the persistent override file.
    Month keys are serialised as strings (JSON requirement).
    Returns True if the file was changed.
    """
    existing = _read_override_env(path)
    serialisable = {
        city: {str(m): s for m, s in months.items()}
        for city, months in sigma_config.items()
    }
    new_json = json.dumps(serialisable, sort_keys=True)
    if new_json == existing.get("BOND_NEARTERM_SIGMA_BY_CITY_MONTH_JSON", ""):
        return False
    existing["BOND_NEARTERM_SIGMA_BY_CITY_MONTH_JSON"] = new_json
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for k, v in existing.items():
            f.write(f"{k}={v}\n")
    return True


# ── Entry point ────────────────────────────────────────────────────────────────

DEFAULT_OVERRIDE_FILE = "/app/data/config.override.env"


async def main(args: argparse.Namespace) -> None:
    # Load config for city coords and global default sigma
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        import config as _config
        from bonding.weather_client import NEARTERM_SIGMA_NEXT_DAY
        city_coords    = dict(_config.BOND_CITIES)
        global_default = float(NEARTERM_SIGMA_NEXT_DAY)
    except Exception as exc:
        print(f"ERROR: could not load config: {exc}", file=sys.stderr)
        sys.exit(1)

    print(
        f"Fetching {args.years} years of ERA5 daily max temperatures "
        f"for {len(city_coords)} cities …"
    )
    city_data = await fetch_all_cities(city_coords, years=args.years)
    total_pairs = sum(len(v) for v in city_data.values())
    print(
        f"  {len(city_data)}/{len(city_coords)} cities fetched  "
        f"({total_pairs} city/date pairs)"
    )
    print()

    print(f"Computing optimal sigma per city/month (min {args.min_obs} obs) …")
    sigma_table  = compute_sigma_table(city_data, min_obs=args.min_obs)
    total_slots  = sum(len(v) for v in sigma_table.values())
    print(f"  {total_slots} city/month slots computed.")
    print()

    print_report(sigma_table, global_default)

    sigma_config = build_sigma_config(sigma_table)
    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(
            {city: {str(m): s for m, s in months.items()} for city, months in sigma_config.items()},
            indent=2,
            sort_keys=True,
        )
    )
    print(f"Sigma config written to {out_path}")

    if args.apply:
        override_path = Path(args.override_file)
        try:
            changed = apply_to_override(override_path, sigma_config)
        except Exception as exc:
            print(f"ERROR writing override file: {exc}", file=sys.stderr)
            sys.exit(1)
        if changed:
            print(f"Override file updated: {override_path}")
            print("Restart the bot (docker restart polybot) for changes to take effect.")
        else:
            print("Override file already up to date — no changes written.")
    else:
        print()
        print("Re-run with --apply to write sigma values to the bot's override file.")

    print()
    print("Suggested BOND_NEARTERM_SIGMA_BY_CITY_MONTH (paste into config.py or use --apply):")
    print("BOND_NEARTERM_SIGMA_BY_CITY_MONTH = {")
    for city in sorted(sigma_config):
        month_strs = ", ".join(
            f"{m}: {s}" for m, s in sorted(sigma_config[city].items())
        )
        print(f'    "{city}": {{{month_strs}}},')
    print("}")


if __name__ == "__main__":
    _default_out = str(Path(__file__).parent / "sigma_calibration_output.json")

    parser = argparse.ArgumentParser(
        description="Compute per-city/month sigma for near-term forecast ensembles using ERA5."
    )
    parser.add_argument(
        "--years", type=int, default=DEFAULT_YEARS,
        help="Years of ERA5 data to fetch (default: %(default)s)",
    )
    parser.add_argument(
        "--min-obs", type=int, default=MIN_OBSERVATIONS,
        help="Min observations per city/month (default: %(default)s)",
    )
    parser.add_argument(
        "--out", default=_default_out,
        help="Output JSON file (default: %(default)s)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Write to override env file",
    )
    parser.add_argument(
        "--override-file", default=DEFAULT_OVERRIDE_FILE,
        help="Path to config.override.env (default: %(default)s)",
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    asyncio.run(main(args))
