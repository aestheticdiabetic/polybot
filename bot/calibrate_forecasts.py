#!/usr/bin/env python3
"""
calibrate_forecasts.py — Analyse forecast accuracy using ERA5 historical data.

Two analyses:

  1. Calibration curve
     Groups resolved paper trades by model-probability bucket and computes the
     actual win rate per bucket. A well-calibrated model should have a win rate
     close to the bucket midpoint (e.g. ~30% for the 0.25–0.35 bucket).

  2. Per-city bias
     For each resolved trade, fetches the ERA5 actual daily-max temperature and
     compares it to the centre of the market's target temperature range.
     A consistently positive residual means our ensemble is predicting too cold
     (actual is warmer than our target range centre); negative means too warm.

     Suggested BOND_CITY_BIAS_CORRECTIONS are written to --out (default:
     calibration_corrections.json).  Paste the non-zero entries into config.py.

Usage (from the bot/ directory on the VPS or locally with SSH-fetched trades):

    python calibrate_forecasts.py
    python calibrate_forecasts.py --trades /home/angus/polybot/logs/paper_trades.jsonl
    python calibrate_forecasts.py --days 60 --out my_corrections.json

Requires: aiohttp (already in requirements.txt)
"""

import argparse
import asyncio
import json
import logging
import re
import statistics
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger("calibrate")

import aiohttp

# ── Constants ─────────────────────────────────────────────────────────────────

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_TRADES = "/home/angus/polybot/logs/paper_trades.jsonl"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Minimum trades per city before we trust the bias estimate
MIN_TRADES_FOR_BIAS = 10

# ── Temperature range extraction (mirrors market_scanner._extract_temp_bucket) ─

_RANGE_RE     = re.compile(r"(\d+(?:\.\d+)?)[–\-](\d+(?:\.\d+)?)\s*°?\s*([CF])\b", re.I)
_ABOVE_RE     = re.compile(r"above\s+(\d+(?:\.\d+)?)\s*°?\s*([CF])\b", re.I)
_BELOW_RE     = re.compile(r"below\s+(\d+(?:\.\d+)?)\s*°?\s*([CF])\b", re.I)
_OR_HIGHER_RE = re.compile(
    r"(?:be\s+)?(\d+(?:\.\d+)?)\s*°?\s*([CF])\s+or\s+(?:higher|above|more)\b", re.I
)
_OR_LOWER_RE  = re.compile(
    r"(?:be\s+)?(\d+(?:\.\d+)?)\s*°?\s*([CF])\s+or\s+(?:below|lower|less)\b", re.I
)
_EXACT_RE     = re.compile(r"(?:be\s+)?(\d+(?:\.\d+)?)\s*°?\s*([CF])\b", re.I)


def _f_to_c(f: float) -> float:
    return (f - 32) * 5 / 9


def parse_temp_range_c(question: str) -> Optional[tuple[float, float]]:
    """
    Return (temp_min_c, temp_max_c) parsed from the question, or None.
    All values are converted to Celsius.
    """
    m = _RANGE_RE.search(question)
    if m:
        lo, hi, unit = float(m.group(1)), float(m.group(2)), m.group(3).upper()
        if unit == "F":
            lo, hi = _f_to_c(lo), _f_to_c(hi)
        return lo, hi

    m = _ABOVE_RE.search(question)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        if unit == "F":
            val = _f_to_c(val)
        return val, (45.0 if unit == "C" else _f_to_c(120.0))

    m = _BELOW_RE.search(question)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        if unit == "F":
            val = _f_to_c(val)
        return (-30.0 if unit == "C" else _f_to_c(-20.0)), val

    m = _OR_HIGHER_RE.search(question)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        if unit == "F":
            val = _f_to_c(val)
        return val, 45.0

    m = _OR_LOWER_RE.search(question)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        if unit == "F":
            val = _f_to_c(val)
        return -30.0, val

    m = _EXACT_RE.search(question)
    if m:
        val, unit = float(m.group(1)), m.group(2).upper()
        if unit == "F":
            val = _f_to_c(val)
        return val - 0.5, val + 0.5

    return None


# ── City coordinates (copy of BOND_CITIES from config) ───────────────────────

try:
    import sys, os
    sys.path.insert(0, os.path.dirname(__file__))
    import config as _config
    CITY_COORDS: dict[str, tuple[float, float]] = {
        **_config.BOND_CITIES,
        **{alias: _config.BOND_CITIES[canon]
           for alias, canon in _config.BOND_CITY_ALIASES.items()
           if canon in _config.BOND_CITIES},
    }
except Exception:
    # Fallback if running outside the bot environment
    CITY_COORDS: dict[str, tuple[float, float]] = {}
    print("Warning: could not import config — city coordinates unavailable.", file=sys.stderr)


# ── ERA5 fetch ────────────────────────────────────────────────────────────────

async def _fetch_era5(
    session: aiohttp.ClientSession,
    lat: float,
    lon: float,
    start: date,
    end: date,
) -> dict[str, float]:
    """Return {date_str: actual_max_c} for the given coordinate range."""
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": start.isoformat(),
        "end_date":   end.isoformat(),
        "daily":      "temperature_2m_max",
        "timezone":   "UTC",
    }
    async with session.get(ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT) as resp:
        resp.raise_for_status()
        data = await resp.json()

    times = data.get("daily", {}).get("time", [])
    temps = data.get("daily", {}).get("temperature_2m_max", [])
    return {t: float(v) for t, v in zip(times, temps) if v is not None}


async def fetch_era5_for_cities(
    city_dates: dict[str, set[date]],
) -> dict[tuple[str, str], float]:
    """
    Batch-fetch ERA5 daily-max temps.
    city_dates: {city_name: {date, ...}}
    Returns: {(city, date_str): actual_max_c}
    """
    results: dict[tuple[str, str], float] = {}
    missing: list[str] = []

    connector = aiohttp.TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        city_list: list[str] = []
        for city, dates in city_dates.items():
            coords = CITY_COORDS.get(city)
            if not coords:
                missing.append(city)
                continue
            lat, lon = coords
            start = min(dates)
            end   = max(dates)
            tasks.append(_fetch_era5(session, lat, lon, start, end))
            city_list.append(city)

        responses = await asyncio.gather(*tasks, return_exceptions=True)

    for city, resp in zip(city_list, responses):
        if isinstance(resp, Exception):
            print(f"  ERA5 fetch failed for {city}: {resp}", file=sys.stderr)
            continue
        for date_str, temp in resp.items():
            results[(city, date_str)] = temp

    if missing:
        print(f"  Skipped (no coords): {', '.join(sorted(missing))}", file=sys.stderr)

    return results


# ── Trade loading ─────────────────────────────────────────────────────────────

def load_resolved_trades(path: str) -> list[dict]:
    """Load only resolved (outcome known) trades from the JSONL log."""
    trades = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
            except json.JSONDecodeError:
                continue
            if t.get("outcome") is not None and t.get("pnl") is not None:
                trades.append(t)
    return trades


# ── Analysis ──────────────────────────────────────────────────────────────────

def calibration_curve(trades: list[dict]) -> list[dict]:
    """
    Group trades by model-probability bucket (width 0.1) and compute:
    - bucket midpoint
    - trade count
    - actual win rate
    - expected win rate = bucket midpoint
    - calibration error = |actual - expected|
    """
    buckets: dict[float, list[bool]] = defaultdict(list)
    for t in trades:
        side    = t.get("side", "YES")
        prob    = t.get("prob", 0.0)
        outcome = t.get("outcome", "")
        won = (side == "YES" and outcome == "YES") or (side == "NO" and outcome == "NO")
        bucket = round(round(prob * 10) / 10, 1)  # nearest 0.1
        buckets[bucket].append(won)

    rows = []
    for b in sorted(buckets):
        wins  = sum(buckets[b])
        total = len(buckets[b])
        actual_rate = wins / total
        rows.append({
            "prob_bucket":    b,
            "count":          total,
            "wins":           wins,
            "actual_rate":    round(actual_rate, 3),
            "expected_rate":  b,
            "calib_error":    round(abs(actual_rate - b), 3),
        })
    return rows


def per_city_bias(
    trades: list[dict],
    era5: dict[tuple[str, str], float],
) -> dict[str, dict]:
    """
    For each city, compute statistics comparing ERA5 actual temperatures against
    the centre of the temperature ranges targeted by our trades.

    residual = ERA5_actual - range_centre
      > 0 → actual was WARMER than our target range centre (ensemble too cold)
      < 0 → actual was COOLER than our target range centre (ensemble too warm)

    Returns dict keyed by city name.
    """
    city_residuals: dict[str, list[float]] = defaultdict(list)
    city_outcomes:  dict[str, list[bool]]  = defaultdict(list)

    for t in trades:
        city     = t.get("city", "")
        date_str = t.get("date", "")
        question = t.get("question", "")
        side     = t.get("side", "YES")
        outcome  = t.get("outcome", "")

        actual = era5.get((city, date_str))
        if actual is None:
            continue

        temp_range = parse_temp_range_c(question)
        if temp_range is None:
            continue

        tmin, tmax = temp_range
        # Ignore unbounded ranges (above/below) for bias analysis
        if tmax > 44 or tmin < -29:
            continue

        range_centre = (tmin + tmax) / 2.0
        residual = actual - range_centre
        city_residuals[city].append(residual)

        won = (side == "YES" and outcome == "YES") or (side == "NO" and outcome == "NO")
        city_outcomes[city].append(won)

    stats = {}
    for city in sorted(city_residuals):
        res  = city_residuals[city]
        wins = city_outcomes[city]
        n    = len(res)
        bias = statistics.mean(res)
        rmse = (sum(r ** 2 for r in res) / n) ** 0.5
        stats[city] = {
            "n":        n,
            "win_rate": round(sum(wins) / len(wins), 3) if wins else None,
            "bias_c":   round(bias, 2),   # + → actual warmer than target (correct upward)
            "rmse_c":   round(rmse, 2),
            "suggested_correction": round(bias, 1),  # round to 0.5°C granularity
        }

    return stats


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(
    trades: list[dict],
    calib: list[dict],
    bias_stats: dict[str, dict],
    era5_coverage: int,
) -> None:
    total   = len(trades)
    wins    = sum(1 for t in trades if t.get("pnl", 0) > 0)
    total_pnl = sum(t.get("pnl", 0) for t in trades)

    print("=" * 68)
    print("BOND FORECAST CALIBRATION REPORT")
    print("=" * 68)
    print(f"Resolved trades: {total}  |  Wins: {wins}  |  Win rate: {wins/total*100:.1f}%")
    print(f"Total PnL: {total_pnl:+.4f}")
    print(f"ERA5 actuals fetched: {era5_coverage} city/date pairs")
    print()

    print("-- Calibration curve -----------------------------------------------")
    print(f"  {'Bucket':>8}  {'Count':>6}  {'Actual%':>8}  {'Expected%':>10}  {'Error':>7}")
    for row in calib:
        print(
            f"  {row['prob_bucket']:>8.1f}  {row['count']:>6}  "
            f"{row['actual_rate']*100:>7.1f}%  {row['expected_rate']*100:>9.1f}%  "
            f"{row['calib_error']*100:>6.1f}%"
        )
    print()

    print("-- Per-city bias (ERA5 actual - target range centre) ---------------")
    print(f"  {'City':22}  {'N':>3}  {'WinRate':>8}  {'Bias C':>7}  {'RMSE C':>7}  {'Suggested':>10}")
    for city, s in bias_stats.items():
        if s["n"] < MIN_TRADES_FOR_BIAS:
            continue
        wr = f"{s['win_rate']*100:.1f}%" if s["win_rate"] is not None else "  n/a"
        corr = f"{s['suggested_correction']:+.1f}" if s["suggested_correction"] != 0 else "   0.0"
        print(
            f"  {city:22}  {s['n']:>3}  {wr:>8}  "
            f"{s['bias_c']:>+7.2f}  {s['rmse_c']:>7.2f}  {corr:>10}"
        )
    print()
    print("  Bias > 0 => actual warmer than our target => add positive correction")
    print("  Bias < 0 => actual cooler than our target => add negative correction")


def build_corrections(bias_stats: dict[str, dict]) -> dict[str, float]:
    """
    Return {city: bias_c} for cities with enough data and a meaningful bias.
    Only include corrections ≥ 0.5°C to avoid overfitting noise.
    """
    corrections = {}
    for city, s in bias_stats.items():
        if s["n"] >= MIN_TRADES_FOR_BIAS and abs(s["suggested_correction"]) >= 0.5:
            corrections[city] = s["suggested_correction"]
    return corrections


# ── Override file patching ────────────────────────────────────────────────────

# Default override file path (same as config_override.py uses inside Docker).
DEFAULT_OVERRIDE_FILE = "/app/data/config.override.env"


def apply_corrections_to_override(override_path: Path, corrections: dict[str, float]) -> bool:
    """
    Write BOND_CITY_BIAS_CORRECTIONS_JSON into the persistent override env file.
    Merges with any existing entries so other overrides are preserved.
    Returns True if the file was changed, False if already up to date.
    """
    existing: dict[str, str] = {}
    if override_path.exists():
        for line in override_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                existing[k.strip()] = v.strip()

    new_json = json.dumps(corrections, sort_keys=True)
    old_json = existing.get("BOND_CITY_BIAS_CORRECTIONS_JSON", "")

    if new_json == old_json:
        return False

    existing["BOND_CITY_BIAS_CORRECTIONS_JSON"] = new_json
    override_path.parent.mkdir(parents=True, exist_ok=True)
    with override_path.open("w", encoding="utf-8") as f:
        for k, v in existing.items():
            f.write(f"{k}={v}\n")
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    print(f"Loading trades from {args.trades} …")
    trades = load_resolved_trades(args.trades)
    if not trades:
        print("No resolved trades found. Exiting.")
        return
    print(f"  {len(trades)} resolved trades loaded.")

    # Collect city/date pairs
    city_dates: dict[str, set[date]] = defaultdict(set)
    for t in trades:
        city     = t.get("city", "")
        date_str = t.get("date", "")
        if city and date_str:
            try:
                city_dates[city].add(date.fromisoformat(date_str))
            except ValueError:
                pass

    print(f"Fetching ERA5 actuals for {len(city_dates)} cities …")
    era5 = await fetch_era5_for_cities(city_dates)
    print(f"  {len(era5)} city/date actuals retrieved.")

    calib      = calibration_curve(trades)
    bias_stats = per_city_bias(trades, era5)

    print_report(trades, calib, bias_stats, len(era5))

    corrections = build_corrections(bias_stats)
    out_path = Path(args.out)
    out_path.write_text(json.dumps(corrections, indent=2, sort_keys=True))

    if corrections:
        print(f"Corrections written to {out_path}")
        print()
        print("Suggested BOND_CITY_BIAS_CORRECTIONS:")
        print("  {")
        for city, val in sorted(corrections.items()):
            print(f'    "{city}": {val},')
        print("  }")
    else:
        print("No corrections exceed the 0.5°C threshold — model appears unbiased "
              "or insufficient data.")

    if args.apply:
        override_path = Path(args.override_file)
        try:
            changed = apply_corrections_to_override(override_path, corrections)
        except Exception as exc:
            print(f"ERROR writing override file: {exc}", file=sys.stderr)
            sys.exit(1)

        if changed:
            print()
            print(f"Override file updated: {override_path}")
            print(f"  Applied {len(corrections)} correction(s).")
            print("  Restart the bot (docker restart polybot) for changes to take effect.")
        else:
            print()
            print("Override file already up to date — no changes written.")


def run_with_apply(
    trades_path: str = DEFAULT_TRADES,
    override_path: str = DEFAULT_OVERRIDE_FILE,
) -> None:
    """Synchronous entry point for scheduled in-process calibration.
    Safe to call from a thread executor — creates its own event loop.
    Loads resolved trades, fetches ERA5 actuals, computes per-city bias
    corrections, and writes them to the override env file.
    """
    _out = str(Path(__file__).parent / "calibration_corrections.json")
    import argparse
    args = argparse.Namespace(
        trades=trades_path,
        days=90,
        out=_out,
        apply=True,
        override_file=override_path,
    )
    asyncio.run(main(args))


if __name__ == "__main__":
    _default_out = str(Path(__file__).parent / "calibration_corrections.json")

    parser = argparse.ArgumentParser(description="Calibrate bond forecast model against ERA5.")
    parser.add_argument(
        "--trades", default=DEFAULT_TRADES,
        help="Path to paper_trades.jsonl (default: %(default)s)",
    )
    parser.add_argument(
        "--days", type=int, default=90,
        help="Ignored (all resolved trades are used; kept for future filtering)",
    )
    parser.add_argument(
        "--out", default=_default_out,
        help="Output file for suggested BOND_CITY_BIAS_CORRECTIONS (default: %(default)s)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Write corrections to the override env file so the bot picks them up on next restart",
    )
    parser.add_argument(
        "--override-file", default=DEFAULT_OVERRIDE_FILE,
        help="Path to config.override.env (default: %(default)s)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    asyncio.run(main(args))
