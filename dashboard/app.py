"""
dashboard/app.py — aiohttp web dashboard with basic auth.
Accessible over SSH tunnel: ssh -L 8080:localhost:8080 user@vps
"""
import asyncio
import json
import logging
import os
import time
import base64
import secrets
from datetime import date as _date, datetime as _datetime, timezone as _tz
from pathlib import Path
from zoneinfo import ZoneInfo

import config as _config
from aiohttp import web

from config import DASHBOARD_SECRET, SIM, STRATEGY

log = logging.getLogger("dashboard")

DASHBOARD_DIR = Path(__file__).parent


PAPER_LOG = Path(os.getenv("PAPER_LOG", "/app/logs/paper_trades.jsonl"))


def _load_paper_trades(n: int = 10000) -> list[dict]:
    """Read paper trade records from JSONL file. Returns empty list on any error."""
    try:
        if not PAPER_LOG.exists():
            return []
        lines = PAPER_LOG.read_text(encoding="utf-8").splitlines()
        records = []
        for line in lines[-n:]:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass
        return records
    except Exception as exc:
        log.debug(f"dashboard: failed to read paper trades: {exc}")
        return []


def _end_of_day_utc(city: str, date_str: str) -> str | None:
    """Return ISO8601 UTC for end of market day in the city's local timezone.

    Gamma stores end_date_iso as midnight UTC at the START of the target date,
    not the actual resolution time. For weather markets, temperature outcomes are
    effectively determined by 6pm local time, so we use 18:00 as the cutoff.
    """
    tz_name = _config.BOND_CITY_TIMEZONES.get(city)
    if not tz_name or not date_str:
        return None
    try:
        d = _date.fromisoformat(date_str[:10])
        city_tz = ZoneInfo(tz_name)
        eod = _datetime(d.year, d.month, d.day, 18, 0, 0, tzinfo=city_tz)
        return eod.astimezone(_tz.utc).isoformat()
    except Exception:
        return None


def _extract_yes_outcome(market_data: dict) -> str | None:
    """
    Return "YES" or "NO" if the market has effectively resolved, None if still open.

    NegRisk weather markets (the majority here) never set resolved=True or
    tokens[].winner. Instead outcomePrices snaps to ~1.0/~0.0 once the result
    is known. We detect that: find the outcome whose price >= 0.99 and return
    whether it is the "Yes" outcome.

    Both 'outcomes' and 'outcomePrices' are JSON-encoded strings in the Gamma
    API response, not native lists.
    """
    # Shape 1: tokens list with explicit winner flag (non-NegRisk markets)
    tokens = market_data.get("tokens", [])
    for tok in tokens:
        if str(tok.get("outcome", "")).lower() in ("yes", "1"):
            winner = tok.get("winner")
            if winner is True:
                return "YES"
            if winner is False:
                return "NO"

    # Shape 2: top-level resolution string
    resolution = str(market_data.get("resolution", "")).upper()
    if resolution in ("YES", "NO"):
        return resolution

    # Shape 3: outcomePrices snapped to ~1.0 (NegRisk / weather markets)
    # Both fields arrive as JSON-encoded strings, e.g. '["Yes","No"]'
    try:
        raw_outcomes = market_data.get("outcomes", "[]")
        raw_prices   = market_data.get("outcomePrices", "[]")
        outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
        prices   = json.loads(raw_prices)   if isinstance(raw_prices,   str) else raw_prices
        for outcome, price in zip(outcomes, prices):
            if float(price) >= 0.99:
                return "YES" if str(outcome).lower() in ("yes", "1") else "NO"
    except Exception:
        pass

    return None


def _load_bond_ledger() -> list[dict]:
    """Read bond position ledger from disk. Returns empty list on any error."""
    try:
        ledger_path = Path(_config.BOND_LEDGER_FILE)
        if not ledger_path.exists():
            return []
        data = json.loads(ledger_path.read_text(encoding="utf-8"))
        return data.get("positions", [])
    except Exception as exc:
        log.debug(f"dashboard: failed to read bond ledger: {exc}")
        return []


async def _fetch_live_balance() -> float | None:
    """Fetch spendable USDC balance from Polymarket CLOB API."""
    from config import API_KEY, API_SECRET, API_PASSPHRASE, PRIVATE_KEY, FUNDER_ADDRESS, CLOB_HOST
    if not API_KEY or not PRIVATE_KEY:
        log.warning("No CLOB credentials configured — cannot fetch live balance")
        return None
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams, AssetType
        creds = ApiCreds(api_key=API_KEY, api_secret=API_SECRET, api_passphrase=API_PASSPHRASE)
        client = ClobClient(
            host=CLOB_HOST,
            chain_id=137,
            key=PRIVATE_KEY,
            creds=creds,
            signature_type=2,
            funder=FUNDER_ADDRESS,
        )
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: client.get_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
        )
        raw = float(result.get("balance", 0)) if isinstance(result, dict) else float(result.balance)
        return raw / 1e6  # USDC on Polygon uses 6 decimal places
    except Exception as e:
        log.warning(f"Live balance fetch failed: {e}")
        return None


# One-time session token generated at startup.
# The HTML page sets this as a cookie on first load (after Basic Auth).
# All subsequent API fetch() calls send the cookie automatically,
# avoiding 401 round-trips that cause Chrome's native auth dialog to flash.
_SESSION_TOKEN = secrets.token_hex(32)


def _check_auth(request: web.Request) -> bool:
    # Fast path: session cookie (used by all JS fetch() calls after page load)
    if request.cookies.get("polybotSession") == _SESSION_TOKEN:
        return True
    # Slow path: Basic Auth (used on initial page load)
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(auth[6:]).decode()
        _, password = decoded.split(":", 1)
        return password == DASHBOARD_SECRET
    except Exception:
        return False


def _auth_required(handler):
    async def wrapper(request):
        if not _check_auth(request):
            # No WWW-Authenticate header on API endpoints — that header triggers
            # Chrome's native auth dialog on every failed fetch(), causing the
            # entire browser UI to flash. Only the HTML page needs that header.
            return web.Response(status=401, text="Unauthorised")
        return await handler(request)
    return wrapper


async def create_app(state):
    app = web.Application()

    # ── API routes ───────────────────────────────────────────────

    @_auth_required
    async def api_status(request):
        return web.json_response(state.get_dashboard_data())

    @_auth_required
    async def api_start(request):
        if state.is_running():
            return web.json_response({"ok": False, "msg": "Already running"})
        data = await request.json() if request.content_length else {}
        # Allow changing sim/live mode and balance at start
        mode = data.get("mode", state.get_mode())
        state.set_mode(mode)
        if mode == "sim" and "balance" in data:
            state.set_balance(float(data["balance"]))
            SIM.starting_balance_usdc = float(data["balance"])
        elif mode == "live":
            balance = await _fetch_live_balance()
            if balance is not None:
                state.set_balance(balance)
                log.info(f"Live balance set: ${balance:.4f} USDC")
            else:
                log.warning("Could not fetch live balance — balance will show $0")
        SIM.enabled = (mode == "sim")
        state.set_running(True)
        log.info(f"Bot START requested via dashboard — mode={mode}")
        return web.json_response({"ok": True, "mode": mode})

    @_auth_required
    async def api_stop(request):
        state.set_running(False)
        log.info("Bot STOP requested via dashboard")
        return web.json_response({"ok": True})

    @_auth_required
    async def api_config(request):
        """Return current strategy config."""
        return web.json_response({
            "bracket_threshold": STRATEGY.bracket_threshold,
            "position_size_usdc": STRATEGY.position_size_usdc,
            "max_concurrent_brackets": STRATEGY.max_concurrent_brackets,
            "max_wallet_exposure_pct": STRATEGY.max_wallet_exposure_pct,
            "target_windows": STRATEGY.target_windows,
            "target_assets": STRATEGY.target_assets,
            "taker_fee_pct": STRATEGY.taker_fee_pct,
            "cancel_unfilled_after_s": STRATEGY.cancel_unfilled_after_s,
        })

    @_auth_required
    async def api_update_config(request):
        """Live-update strategy parameters (non-credential only)."""
        data = await request.json()
        safe_keys = {
            "bracket_threshold", "position_size_usdc",
            "max_concurrent_brackets", "cancel_unfilled_after_s",
        }
        updated = {}
        for k, v in data.items():
            if k in safe_keys:
                setattr(STRATEGY, k, type(getattr(STRATEGY, k))(v))
                updated[k] = v

        # Persist overrides to .env file so they survive restarts
        if updated:
            from config_override import save_overrides
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: save_overrides(updated))

        log.info(f"Config updated: {updated}")
        return web.json_response({"ok": True, "updated": updated})

    @_auth_required
    async def api_live_balance(request):
        """Fetch current spendable USDC balance from Polymarket."""
        balance = await _fetch_live_balance()
        if balance is not None:
            state.set_balance(balance)
            return web.json_response({"ok": True, "balance": balance})
        return web.json_response({"ok": False, "balance": state.get_balance()})

    @_auth_required
    async def api_trades(request):
        d = state.get_dashboard_data()
        return web.json_response(d["recent_trades"])

    @_auth_required
    async def api_markets(request):
        return web.json_response(state.get_markets())

    @_auth_required
    async def api_logs(request):
        """Return last N lines of trade log."""
        n = int(request.rel_url.query.get("n", 100))
        try:
            from config import TRADE_LOG
            with open(TRADE_LOG) as f:
                lines = f.readlines()
            records = [json.loads(l) for l in lines[-n:] if l.strip()]
            return web.json_response(records)
        except FileNotFoundError:
            return web.json_response([])

    # ── Bond API routes ──────────────────────────────────────────

    @_auth_required
    async def api_bond_status(request):
        """Bond mode status: cycle stats + position summary from ledger."""
        bond_stats = state.get_bond_stats()
        positions  = _load_bond_ledger()
        open_pos   = [p for p in positions if p["status"] == "OPEN"]
        sold_pos   = [p for p in positions if p["status"] == "SOLD"]

        capital_deployed = sum(p["shares"] * p["entry_price"] for p in open_pos)
        realised_pnl = sum(
            (p["exit_price"] - p["entry_price"]) * p["shares"]
            for p in sold_pos
            if p.get("exit_price") is not None
        )

        tier_stats: dict = {}
        for tier in ("CHEAP", "CORE", "CERTAIN"):
            tier_sold = [p for p in sold_pos if p["tier"] == tier and p.get("exit_price") is not None]
            wins      = [p for p in tier_sold if p["exit_price"] > p["entry_price"]]
            tier_stats[tier] = {
                "open":     len([p for p in open_pos   if p["tier"] == tier]),
                "sold":     len(tier_sold),
                "wins":     len(wins),
                "win_rate": round(len(wins) / len(tier_sold) * 100, 1) if tier_sold else None,
                "pnl":      round(sum((p["exit_price"] - p["entry_price"]) * p["shares"] for p in tier_sold), 4),
            }

        return web.json_response({
            "running":          state.is_running(),
            "uptime_s":         state.get_dashboard_data().get("uptime_s", 0),
            "cycle":            bond_stats,
            "total_positions":  len(positions),
            "open_positions":   len(open_pos),
            "sold_positions":   len(sold_pos),
            "capital_deployed": round(capital_deployed, 4),
            "realised_pnl":     round(realised_pnl, 4),
            "tier_stats":       tier_stats,
        })

    @_auth_required
    async def api_bond_positions(request):
        """Full position ledger."""
        positions = _load_bond_ledger()
        status_filter = request.rel_url.query.get("status")
        if status_filter:
            positions = [p for p in positions if p["status"] == status_filter.upper()]
        # Sort newest first
        positions.sort(key=lambda p: p.get("entry_time", ""), reverse=True)
        return web.json_response(positions[:200])

    @_auth_required
    async def api_bond_config_get(request):
        """Return current BOND_* config values."""
        return web.json_response({
            "BOND_MIN_EDGE_CHEAP":          _config.BOND_MIN_EDGE_CHEAP,
            "BOND_MIN_EDGE_CORE":           _config.BOND_MIN_EDGE_CORE,
            "BOND_SHARES_CHEAP_MAX":        _config.BOND_SHARES_CHEAP_MAX,
            "BOND_SHARES_CORE":             _config.BOND_SHARES_CORE,
            "BOND_MAX_CAPITAL_PER_CLUSTER": _config.BOND_MAX_CAPITAL_PER_CLUSTER,
            "BOND_EARLY_EXIT_PRICE":        _config.BOND_EARLY_EXIT_PRICE,
            "BOND_CHEAP_EXIT_MULTIPLIER":   _config.BOND_CHEAP_EXIT_MULTIPLIER,
            "BOND_CHEAP_MIN_ABS_GAIN":      _config.BOND_CHEAP_MIN_ABS_GAIN,
            "BOND_GAS_FLOOR_HOURS":         _config.BOND_GAS_FLOOR_HOURS,
            "BOND_POLL_INTERVAL_SECS":      _config.BOND_POLL_INTERVAL_SECS,
            "BOND_MAX_MARKETS_PER_RUN":     _config.BOND_MAX_MARKETS_PER_RUN,
        })

    @_auth_required
    async def api_bond_config_set(request):
        """Live-update BOND_* numeric parameters."""
        data = await request.json()
        _float_keys = {
            "BOND_MIN_EDGE_CHEAP", "BOND_MIN_EDGE_CORE",
            "BOND_MAX_CAPITAL_PER_CLUSTER",
            "BOND_EARLY_EXIT_PRICE", "BOND_CHEAP_EXIT_MULTIPLIER", "BOND_CHEAP_MIN_ABS_GAIN",
        }
        _int_keys = {
            "BOND_GAS_FLOOR_HOURS", "BOND_SHARES_CORE", "BOND_SHARES_CHEAP_MAX",
            "BOND_POLL_INTERVAL_SECS", "BOND_MAX_MARKETS_PER_RUN",
        }
        updated = {}
        for k, v in data.items():
            if k in _float_keys:
                val = float(v)
                setattr(_config, k, val)
                updated[k] = val
            elif k in _int_keys:
                val = int(v)
                setattr(_config, k, val)
                updated[k] = val
        if updated:
            from config_override import save_overrides
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: save_overrides(updated))
        log.info(f"Bond config updated: {updated}")
        return web.json_response({"ok": True, "updated": updated})

    @_auth_required
    async def api_bond_cities_get(request):
        """Return current city list and aliases."""
        return web.json_response({
            "cities":  {k: list(v) for k, v in _config.BOND_CITIES.items()},
            "aliases": _config.BOND_CITY_ALIASES,
        })

    @_auth_required
    async def api_bond_cities_set(request):
        """Replace city list and/or aliases in-memory and persist."""
        data = await request.json()
        updated = {}

        if "cities" in data:
            new_cities = {k: tuple(v) for k, v in data["cities"].items()}
            _config.BOND_CITIES = new_cities
            updated["BOND_CITIES"] = new_cities

        if "aliases" in data:
            _config.BOND_CITY_ALIASES = data["aliases"]
            updated["BOND_CITY_ALIASES"] = data["aliases"]

        if updated:
            from config_override import save_overrides
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: save_overrides(updated))

        log.info(f"Bond cities/aliases updated — cities={len(_config.BOND_CITIES)} aliases={len(_config.BOND_CITY_ALIASES)}")
        return web.json_response({"ok": True})

    @_auth_required
    async def api_bond_discover_cities(request):
        """
        Fetch Polymarket weather markets, extract city names that appear in questions
        but aren't in BOND_CITIES, geocode them via Open-Meteo, return as candidates.
        """
        try:
            from bonding.market_scanner import _fetch_gamma_markets, extract_unknown_cities
            from bonding.weather_client import geocode_city, UnknownCityError

            raw = await _fetch_gamma_markets()
            unknown = extract_unknown_cities(raw)

            # Top 25 by frequency
            top_cities = sorted(unknown, key=lambda c: -unknown[c])[:25]

            results = []
            for city_name in top_cities:
                try:
                    display, lat, lon = await geocode_city(city_name)
                    results.append({
                        "name":          display,
                        "query":         city_name,
                        "lat":           round(lat, 4),
                        "lon":           round(lon, 4),
                        "market_count":  unknown[city_name],
                        "geocoded":      True,
                    })
                except UnknownCityError:
                    results.append({
                        "name":         city_name,
                        "query":        city_name,
                        "lat":          None,
                        "lon":          None,
                        "market_count": unknown[city_name],
                        "geocoded":     False,
                    })

            log.info(f"discover-cities: found {len(top_cities)} unknown cities, geocoded {sum(1 for r in results if r['geocoded'])}")
            return web.json_response({"ok": True, "candidates": results})
        except Exception as e:
            log.error(f"discover-cities failed: {e}")
            return web.json_response({"ok": False, "error": str(e), "candidates": []})

    @_auth_required
    async def api_bond_logs(request):
        """Return last N lines of bond log file."""
        n = int(request.rel_url.query.get("n", 200))
        try:
            log_path = Path(_config.BOND_LOG_FILE)
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            return web.json_response({"lines": lines[-n:]})
        except FileNotFoundError:
            return web.json_response({"lines": []})

    @_auth_required
    async def api_bond_paper_trades(request):
        """Return last N paper trade records from paper_trades.jsonl."""
        n = int(request.rel_url.query.get("n", 5000))
        all_records = _load_paper_trades(n)
        # Build a fallback map from WOULD_SELL events: market_id → sell record.
        # Used when _patch_would_buy failed or lost a file-write race — the WOULD_SELL
        # event is the only source of exit_price/pnl in that case.
        sell_map: dict = {}
        for r in all_records:
            if r.get("event") == "WOULD_SELL" and r.get("market_id"):
                sell_map[r["market_id"]] = r
        # Return only WOULD_BUY records — WOULD_SELL are supplementary sell-side logs.
        # Patch any WOULD_BUY record whose WOULD_BUY patch was incomplete using sell_map,
        # preserving all original fields (prob, ev, capital, etc.) for display.
        records = []
        for r in all_records:
            if r.get("event") == "WOULD_SELL":
                continue
            mid = r.get("market_id")
            if mid and mid in sell_map and r.get("outcome") != "SOLD":
                sell = sell_map[mid]
                r = {**r, "outcome": "SOLD", "exit_price": sell.get("exit_price"), "pnl": sell.get("pnl")}
            records.append(r)
        records.sort(key=lambda r: r.get("ts", ""), reverse=True)
        # Override resolution_time with accurate end-of-day UTC for display.
        # Gamma's stored end_date_iso is start-of-day UTC, not actual resolution.
        result = []
        for rec in records[:n]:
            corrected = _end_of_day_utc(rec.get("city", ""), rec.get("date", ""))
            if corrected:
                rec = {**rec, "resolution_time": corrected}
            result.append(rec)
        return web.json_response(result)

    @_auth_required
    async def api_bond_paper_stats(request):
        """Aggregated paper sim stats with P&L projection for a given starting balance."""
        from datetime import datetime as _dt, timezone as _tz
        starting_balance = float(request.rel_url.query.get("balance", 1000))
        all_records = _load_paper_trades()
        # Build a fallback map from WOULD_SELL events: market_id → sell record.
        # Used to handle cases where _patch_would_buy failed or lost a file-write
        # race with paper-check (both rewrite the whole JSONL atomically).
        sell_map: dict = {}
        for r in all_records:
            if r.get("event") == "WOULD_SELL" and r.get("market_id"):
                sell_map[r["market_id"]] = r
        # Exclude WOULD_SELL events — they are supplementary sell-side logs.
        # The corresponding WOULD_BUY record is patched with outcome='SOLD' and
        # pnl by PaperExitManager._patch_would_buy, so it is the canonical record.
        # Including WOULD_SELL would double-count capital, pnl, and resolved counts.
        # If sell_map has an entry for a market, the early-exit ALWAYS takes priority:
        # paper-check may have raced and overwritten the patch with a market-resolution
        # outcome (e.g. 'NO'), but the actual realized trade was the early sell.
        raw_records = [r for r in all_records if r.get("event") != "WOULD_SELL"]
        records = []
        for r in raw_records:
            mid = r.get("market_id")
            if mid and mid in sell_map:
                sell = sell_map[mid]
                r = {**r, "outcome": "SOLD", "exit_price": sell.get("exit_price"), "pnl": sell.get("pnl")}
            records.append(r)

        if not records:
            _empty_tier_bd = {t: {"count": 0, "avg_conf": None} for t in ("CHEAP", "CORE", "CERTAIN")}
            _empty_et = {lbl: {"count": 0, "resolved": 0, "wins": 0, "win_rate": None,
                               "actual_pnl": 0, "tier_breakdown": _empty_tier_bd}
                         for lbl in ("0-10h", "10-20h", "20-30h", "30-48h", "48h+")}
            return web.json_response({
                "total": 0, "cycles": 0, "tier_stats": {},
                "total_capital": 0, "total_projected_profit": 0,
                "scaled_projected_profit": 0, "starting_balance": starting_balance,
                "scale_factor": 0, "actual_pnl": 0, "resolved_count": 0,
                "entry_time_stats": _empty_et, "side_stats": {},
            })

        def _is_win(r: dict) -> bool:
            """A bet wins if the market resolved in the direction we bet,
            or if we sold early with a profit. Records without a 'side' field
            pre-date NO bets and were always YES bets."""
            outcome = r.get("outcome")
            if outcome == "SOLD":
                return (r.get("pnl") or 0) > 0
            side = r.get("side") or "YES"
            return outcome is not None and outcome == side

        def _is_resolved(r: dict) -> bool:
            return r.get("outcome") is not None

        def _hours_before_resolution(r: dict) -> float | None:
            try:
                entry = _dt.fromisoformat(r["ts"].replace("Z", "+00:00"))
                res   = _dt.fromisoformat(r["resolution_time"].replace("Z", "+00:00"))
                return max(0.0, (res - entry).total_seconds() / 3600)
            except Exception:
                return None

        # Approximate cycle count by distinct timestamp values
        cycles = len(set(r.get("ts", "") for r in records))

        # ── Tier stats ────────────────────────────────────────────────
        tier_stats: dict = {}
        for tier in ("CHEAP", "CORE", "CERTAIN"):
            tr_list = [r for r in records if r.get("tier") == tier]
            if not tr_list:
                tier_stats[tier] = {
                    "count": 0, "capital": 0, "avg_ask": None,
                    "avg_prob": None, "avg_ev": None, "projected_profit": 0,
                    "resolved": 0, "wins": 0, "win_rate": None, "actual_pnl": 0,
                }
                continue
            total_cap   = sum(r.get("capital", 0) for r in tr_list)
            proj_profit = sum(r.get("ev", 0) * r.get("shares", 0) for r in tr_list)
            resolved    = [r for r in tr_list if _is_resolved(r)]
            wins        = [r for r in resolved if _is_win(r)]
            tier_actual = sum(r.get("pnl", 0) for r in tr_list if r.get("pnl") is not None)
            tier_stats[tier] = {
                "count":            len(tr_list),
                "capital":          round(total_cap, 4),
                "avg_ask":          round(sum(r.get("ask", 0) for r in tr_list) / len(tr_list), 4),
                "avg_prob":         round(sum(r.get("prob", 0) for r in tr_list) / len(tr_list), 4),
                "avg_ev":           round(sum(r.get("ev", 0) for r in tr_list) / len(tr_list), 4),
                "projected_profit": round(proj_profit, 4),
                "resolved":         len(resolved),
                "wins":             len(wins),
                "win_rate":         round(len(wins) / len(resolved) * 100, 1) if resolved else None,
                "actual_pnl":       round(tier_actual, 4),
            }

        # ── Entry time stats (bucketed by hours before resolution) ─────
        _BUCKETS = [
            ("0-10h",  0,   10),
            ("10-20h", 10,  20),
            ("20-30h", 20,  30),
            ("30-48h", 30,  48),
            ("48h+",   48,  float("inf")),
        ]
        entry_time_stats: dict = {}
        for label, lo, hi in _BUCKETS:
            bucket = []
            for r in records:
                h = _hours_before_resolution(r)
                if h is not None and lo <= h < hi:
                    bucket.append(r)
            resolved_b = [r for r in bucket if _is_resolved(r)]
            wins_b     = [r for r in resolved_b if _is_win(r)]
            tier_breakdown: dict = {}
            for tier in ("CHEAP", "CORE", "CERTAIN"):
                tb = [r for r in bucket if r.get("tier") == tier]
                probs = [r["prob"] for r in tb if r.get("prob") is not None]
                tier_breakdown[tier] = {
                    "count":    len(tb),
                    "avg_conf": round(sum(probs) / len(probs), 4) if probs else None,
                }
            entry_time_stats[label] = {
                "count":          len(bucket),
                "resolved":       len(resolved_b),
                "wins":           len(wins_b),
                "win_rate":       round(len(wins_b) / len(resolved_b) * 100, 1) if resolved_b else None,
                "actual_pnl":     round(sum(r.get("pnl", 0) for r in bucket if r.get("pnl") is not None), 4),
                "tier_breakdown": tier_breakdown,
            }

        # ── YES vs NO side stats ──────────────────────────────────────
        side_stats: dict = {}
        for side_val in ("YES", "NO"):
            side_list = [r for r in records if r.get("side") == side_val]
            resolved_s = [r for r in side_list if _is_resolved(r)]
            wins_s     = [r for r in resolved_s if _is_win(r)]
            side_stats[side_val] = {
                "count":      len(side_list),
                "resolved":   len(resolved_s),
                "wins":       len(wins_s),
                "win_rate":   round(len(wins_s) / len(resolved_s) * 100, 1) if resolved_s else None,
                "actual_pnl": round(sum(r.get("pnl", 0) for r in side_list if r.get("pnl") is not None), 4),
            }

        total_capital          = sum(r.get("capital", 0) for r in records)
        total_projected_profit = sum(r.get("ev", 0) * r.get("shares", 0) for r in records)
        scale                  = min(1.0, starting_balance / total_capital) if total_capital > 0 else 0
        resolved_records       = [r for r in records if r.get("pnl") is not None]
        active_records         = [r for r in records if r.get("outcome") is None]
        active_capital         = sum(r.get("capital", 0) for r in active_records)
        actual_pnl             = sum(r.get("pnl", 0) for r in resolved_records)

        # ── Order book depth stats ────────────────────────────────────
        # Only count WOULD_BUY records that have shares_wanted — these are the
        # depth-aware records logged after the depth-check was introduced.
        # Old records (no shares_wanted field) are excluded so they don't
        # inflate fill rates. DEPTH_MISS events track confirmed no-depth cases.
        miss_records = [r for r in all_records if r.get("event") == "DEPTH_MISS"]
        depth_stats: dict = {}
        for tier in ("CHEAP", "CORE"):
            buy_recs  = [r for r in records if r.get("tier") == tier and "shares_wanted" in r]
            miss_recs = [r for r in miss_records if r.get("tier") == tier]
            fillable_ids  = {r["market_id"] for r in buy_recs  if r.get("market_id")}
            miss_only_ids = {r["market_id"] for r in miss_recs if r.get("market_id")} - fillable_ids
            n_fillable = len(fillable_ids)
            n_miss     = len(miss_only_ids)
            n_total    = n_fillable + n_miss
            fill_rate  = round(n_fillable / n_total * 100, 1) if n_total > 0 else None
            avg_depth  = round(sum(r.get("shares", 0) for r in buy_recs) / len(buy_recs), 1) if buy_recs else None
            avg_wanted = round(sum(r["shares_wanted"] for r in buy_recs) / len(buy_recs), 1) if buy_recs else None
            depth_stats[tier] = {
                "fillable":   n_fillable,
                "no_depth":   n_miss,
                "total":      n_total,
                "fill_rate":  fill_rate,
                "avg_depth":  avg_depth,
                "avg_wanted": avg_wanted,
            }

        return web.json_response({
            "total":                    len(records),
            "cycles":                   cycles,
            "total_capital":            round(total_capital, 4),
            "total_projected_profit":   round(total_projected_profit, 4),
            "scaled_projected_profit":  round(total_projected_profit * scale, 4),
            "starting_balance":         starting_balance,
            "scale_factor":             round(scale, 4),
            "actual_pnl":               round(actual_pnl, 4),
            "resolved_count":           len(resolved_records),
            "active_capital":           round(active_capital, 4),
            "active_count":             len(active_records),
            "tier_stats":               tier_stats,
            "entry_time_stats":         entry_time_stats,
            "side_stats":               side_stats,
            "depth_stats":              depth_stats,
        })

    @_auth_required
    async def api_bond_real_stats(request):
        """Aggregated REAL bond stats (tier analysis + entry time stats) from the ledger."""
        from datetime import datetime as _dt, timezone as _tz

        positions = _load_bond_ledger()

        _empty_tier_bd = {t: {"count": 0, "avg_conf": None} for t in ("CHEAP", "CORE", "CERTAIN")}
        _empty_et = {lbl: {"count": 0, "resolved": 0, "wins": 0, "win_rate": None,
                            "actual_pnl": 0, "tier_breakdown": _empty_tier_bd}
                     for lbl in ("0-10h", "10-20h", "20-30h", "30-48h", "48h+")}

        if not positions:
            return web.json_response({
                "total": 0, "tier_stats": {}, "entry_time_stats": _empty_et,
            })

        def _is_win(p: dict) -> bool:
            ep = p.get("exit_price")
            return ep is not None and ep > p.get("entry_price", 0)

        def _is_resolved(p: dict) -> bool:
            return p.get("exit_price") is not None or p.get("status") in ("SOLD", "RESOLVED")

        def _pnl(p: dict) -> float:
            ep = p.get("exit_price")
            if ep is None:
                return 0.0
            return (ep - p.get("entry_price", 0)) * p.get("shares", 0)

        def _hours_before_resolution(p: dict) -> float | None:
            try:
                entry = _dt.fromisoformat(p["entry_time"].replace("Z", "+00:00"))
                res   = _dt.fromisoformat(p["resolution_time"].replace("Z", "+00:00"))
                return max(0.0, (res - entry).total_seconds() / 3600)
            except Exception:
                return None

        # ── Tier stats ────────────────────────────────────────────────
        tier_stats: dict = {}
        for tier in ("CHEAP", "CORE", "CERTAIN"):
            tp = [p for p in positions if p.get("tier") == tier]
            if not tp:
                tier_stats[tier] = {
                    "count": 0, "capital": 0, "avg_entry": None,
                    "avg_prob": None, "resolved": 0, "wins": 0,
                    "win_rate": None, "actual_pnl": 0,
                }
                continue
            resolved = [p for p in tp if _is_resolved(p)]
            wins     = [p for p in resolved if _is_win(p)]
            probs    = [p["prob"] for p in tp if p.get("prob")]
            tier_stats[tier] = {
                "count":      len(tp),
                "capital":    round(sum(p.get("shares", 0) * p.get("entry_price", 0) for p in tp), 4),
                "avg_entry":  round(sum(p.get("entry_price", 0) for p in tp) / len(tp), 4),
                "avg_prob":   round(sum(probs) / len(probs), 4) if probs else None,
                "resolved":   len(resolved),
                "wins":       len(wins),
                "win_rate":   round(len(wins) / len(resolved) * 100, 1) if resolved else None,
                "actual_pnl": round(sum(_pnl(p) for p in tp), 4),
            }

        # ── Entry time stats (bucketed by hours before resolution) ─────
        _BUCKETS = [
            ("0-10h",  0,   10),
            ("10-20h", 10,  20),
            ("20-30h", 20,  30),
            ("30-48h", 30,  48),
            ("48h+",   48,  float("inf")),
        ]
        entry_time_stats: dict = {}
        for label, lo, hi in _BUCKETS:
            bucket = []
            for p in positions:
                h = _hours_before_resolution(p)
                if h is not None and lo <= h < hi:
                    bucket.append(p)
            resolved_b = [p for p in bucket if _is_resolved(p)]
            wins_b     = [p for p in resolved_b if _is_win(p)]
            tier_breakdown: dict = {}
            for tier in ("CHEAP", "CORE", "CERTAIN"):
                tb = [p for p in bucket if p.get("tier") == tier]
                probs = [p["prob"] for p in tb if p.get("prob")]
                tier_breakdown[tier] = {
                    "count":    len(tb),
                    "avg_conf": round(sum(probs) / len(probs), 4) if probs else None,
                }
            entry_time_stats[label] = {
                "count":          len(bucket),
                "resolved":       len(resolved_b),
                "wins":           len(wins_b),
                "win_rate":       round(len(wins_b) / len(resolved_b) * 100, 1) if resolved_b else None,
                "actual_pnl":     round(sum(_pnl(p) for p in bucket), 4),
                "tier_breakdown": tier_breakdown,
            }

        return web.json_response({
            "total":            len(positions),
            "tier_stats":       tier_stats,
            "entry_time_stats": entry_time_stats,
        })

    @_auth_required
    async def api_bond_paper_check_resolutions(request):
        """
        Check Gamma API for outcomes on:
          - Paper trades whose resolution_time has passed and outcome is still null
          - Live bonding positions that are still OPEN but past their resolution_time

        Uses tokens[].winner for reliable YES/NO detection (same logic as ExitManager).
        Updates both the paper trades JSONL and the bonding ledger in-place.
        """
        import aiohttp as _aiohttp
        from datetime import datetime, timedelta, timezone as _tz
        from pathlib import Path as _Path

        now = datetime.now(_tz.utc)

        from zoneinfo import ZoneInfo as _ZoneInfo

        def _resolution_deadline(city: str, resolution_time_str: str) -> datetime:
            """Return the UTC time after which a market's outcome can be scored.

            Uses 18:00 local time in the city's timezone — the same convention as
            _end_of_day_utc() in paper_sim.py and the peak-hour gate in the scorer.
            Falls back to midnight UTC of date+1 if the city is unknown, ensuring we
            never score a market that is still running on its calendar day.

            This replaces the old approach of comparing resolution_time < now directly,
            which caused midnight-start-of-day timestamps (the raw Gamma end_date_iso
            value) to be treated as past deadlines for current-day markets.
            """
            date_str = resolution_time_str[:10]  # "YYYY-MM-DD"
            year, month, day = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
            tz_name = _config.BOND_CITY_TIMEZONES.get(city)
            if tz_name:
                try:
                    city_tz = _ZoneInfo(tz_name)
                    eod = datetime(year, month, day, 18, 0, 0, tzinfo=city_tz)
                    return eod.astimezone(_tz.utc)
                except Exception:
                    pass
            # Fallback: midnight UTC of following day
            return datetime.fromisoformat(date_str + "T00:00:00+00:00") + timedelta(days=1)

        # ── Collect market IDs that need resolution ───────────────────
        paper_records = _load_paper_trades()
        pending_paper = [
            r for r in paper_records
            if r.get("outcome") is None
            and r.get("resolution_time")
            and _resolution_deadline(r.get("city", ""), r["resolution_time"]) < now
        ]

        live_positions = _load_bond_ledger()
        pending_live = [
            p for p in live_positions
            if p.get("status") == "OPEN"
            and p.get("resolution_time")
            and _resolution_deadline(p.get("city", ""), p["resolution_time"]) < now
        ]

        all_market_ids = {r["market_id"] for r in pending_paper if r.get("market_id")}
        all_market_ids |= {p["market_id"] for p in pending_live if p.get("market_id")}

        if not all_market_ids:
            return web.json_response({"ok": True, "checked": 0,
                                      "paper_resolved": 0, "live_resolved": 0})

        # ── Fetch outcomes from Gamma API ─────────────────────────────
        # outcome_map: market_id -> "YES" | "NO" (only present if fully resolved)
        outcome_map: dict[str, str] = {}
        timeout = _aiohttp.ClientTimeout(total=10)
        async with _aiohttp.ClientSession(timeout=timeout) as session:
            for mid in list(all_market_ids)[:50]:  # cap at 50 per call
                try:
                    async with session.get(
                        f"https://gamma-api.polymarket.com/markets/{mid}"
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                        outcome = _extract_yes_outcome(data)
                        if outcome is not None:
                            outcome_map[mid] = outcome
                except Exception:
                    pass

        # ── Update paper trades ───────────────────────────────────────
        paper_resolved = 0
        updated_paper = []
        for r in paper_records:
            mid = r.get("market_id", "")
            if mid in outcome_map and r.get("outcome") is None:
                r = dict(r)
                r["outcome"] = outcome_map[mid]
                shares     = r.get("shares", 0)
                ask        = r.get("ask", 0)
                side       = r.get("side") or "YES"  # pre-NO-bet records had no side field
                market_won = outcome_map[mid] == side  # did the market resolve in our direction?
                r["pnl"] = round(
                    (1.0 - ask) * shares if market_won else -ask * shares, 4
                )
                paper_resolved += 1
            updated_paper.append(r)

        if paper_resolved:
            try:
                tmp = PAPER_LOG.with_suffix(".tmp")
                tmp.write_text(
                    "\n".join(json.dumps(rec) for rec in updated_paper) + "\n",
                    encoding="utf-8",
                )
                tmp.replace(PAPER_LOG)
            except Exception as exc:
                log.error(f"paper trades file update failed: {exc}")
                return web.json_response({"ok": False, "error": str(exc)})

        # ── Update live bonding ledger ────────────────────────────────
        live_resolved = 0
        ledger_path = _Path(_config.BOND_LEDGER_FILE)
        if pending_live and ledger_path.exists():
            try:
                ledger_data = json.loads(ledger_path.read_text(encoding="utf-8"))
                positions = ledger_data.get("positions", [])
                for p in positions:
                    mid = p.get("market_id", "")
                    if (
                        p.get("status") == "OPEN"
                        and mid in outcome_map
                        and p.get("resolution_time")
                        and _resolution_deadline(p.get("city", ""), p["resolution_time"]) < now
                    ):
                        exit_price = 1.0 if outcome_map[mid] == "YES" else 0.0
                        p["status"]     = "RESOLVED"
                        p["exit_price"] = exit_price
                        p["exit_time"]  = now.isoformat()
                        live_resolved += 1
                        log.info(
                            f"dashboard-check-resolutions: RESOLVED market={mid[:8]} "
                            f"outcome={outcome_map[mid]} exit_price={exit_price:.1f} "
                            f"pnl={round((exit_price - p.get('entry_price', 0)) * p.get('shares', 0), 4):+.4f}"
                        )
                if live_resolved:
                    tmp = ledger_path.with_suffix(".tmp")
                    tmp.write_text(json.dumps(ledger_data, indent=2), encoding="utf-8")
                    tmp.replace(ledger_path)
            except Exception as exc:
                log.error(f"bond ledger update failed: {exc}")
                return web.json_response({"ok": False, "error": str(exc)})

        total_checked = len(pending_paper) + len(pending_live)
        log.info(
            f"check-resolutions: checked={total_checked} "
            f"paper_resolved={paper_resolved} live_resolved={live_resolved}"
        )
        return web.json_response({
            "ok":            True,
            "checked":       total_checked,
            "paper_resolved": paper_resolved,
            "live_resolved":  live_resolved,
        })

    @_auth_required
    async def api_bond_paper_revert_resolutions(request):
        """
        Revert paper-trade WOULD_BUY records whose outcome was set prematurely —
        i.e., where the calendar day has not yet ended (end-of-day = midnight UTC
        of date+1 is still in the future).

        Accepts optional query param ?date=YYYY-MM-DD to target a specific date,
        otherwise reverts any record whose date's end-of-day is still in the future.
        """
        from datetime import datetime, timedelta, timezone as _tz
        from pathlib import Path as _Path
        from zoneinfo import ZoneInfo as _ZoneInfo

        now = datetime.now(_tz.utc)
        target_date = request.rel_url.query.get("date")  # optional filter

        def _eod(city: str, date_str: str) -> datetime:
            tz_name = _config.BOND_CITY_TIMEZONES.get(city)
            year, month, day = int(date_str[:4]), int(date_str[5:7]), int(date_str[8:10])
            if tz_name:
                try:
                    eod = datetime(year, month, day, 18, 0, 0, tzinfo=_ZoneInfo(tz_name))
                    return eod.astimezone(_tz.utc)
                except Exception:
                    pass
            return datetime.fromisoformat(date_str + "T00:00:00+00:00") + timedelta(days=1)

        paper_records = _load_paper_trades()
        reverted = 0
        updated = []

        for r in paper_records:
            rec = dict(r)
            res_time = rec.get("resolution_time", "")
            if not res_time:
                updated.append(rec)
                continue

            date_str = res_time[:10]
            if target_date and date_str != target_date:
                updated.append(rec)
                continue

            eod = _eod(rec.get("city", ""), date_str)
            outcome = rec.get("outcome")

            # Only revert if: outcome was set (YES/NO) AND the day hasn't ended yet
            if outcome in ("YES", "NO") and eod > now:
                log.info(
                    f"revert-resolutions: clearing outcome={outcome} "
                    f"city={rec.get('city')} tier={rec.get('tier')} "
                    f"date={date_str} market={rec.get('market_id','')[:8]}"
                )
                rec["outcome"] = None
                rec["pnl"]     = None
                reverted += 1

            updated.append(rec)

        if reverted:
            try:
                tmp = PAPER_LOG.with_suffix(".tmp")
                tmp.write_text(
                    "\n".join(json.dumps(rec) for rec in updated) + "\n",
                    encoding="utf-8",
                )
                tmp.replace(PAPER_LOG)
            except Exception as exc:
                log.error(f"revert-resolutions: file update failed: {exc}")
                return web.json_response({"ok": False, "error": str(exc)})

        log.info(f"revert-resolutions: reverted={reverted}")
        return web.json_response({"ok": True, "reverted": reverted})

    # ── Static dashboard HTML ────────────────────────────────────

    async def dashboard_html(request):
        if not _check_auth(request):
            return web.Response(
                status=401,
                headers={"WWW-Authenticate": 'Basic realm="PolyBot"'},
                text="Unauthorised",
            )
        html_path = DASHBOARD_DIR / "index.html"
        # Serve HTML and set session cookie so subsequent API fetch() calls
        # are authenticated via cookie (no more 401/WWW-Authenticate cycles
        # that trigger Chrome's native auth dialog flash).
        content = html_path.read_bytes()
        resp = web.Response(body=content, content_type="text/html")
        resp.set_cookie(
            "polybotSession", _SESSION_TOKEN,
            httponly=True, samesite="Strict", path="/"
        )
        return resp

    app.router.add_get("/",           dashboard_html)
    app.router.add_get("/api/status", api_status)
    app.router.add_post("/api/start", api_start)
    app.router.add_post("/api/stop",  api_stop)
    app.router.add_get("/api/config", api_config)
    app.router.add_post("/api/config", api_update_config)
    app.router.add_get("/api/live-balance", api_live_balance)
    app.router.add_get("/api/trades",   api_trades)
    app.router.add_get("/api/markets",  api_markets)
    app.router.add_get("/api/logs",     api_logs)
    # Bond routes
    app.router.add_get("/api/bond/status",    api_bond_status)
    app.router.add_get("/api/bond/positions", api_bond_positions)
    app.router.add_get("/api/bond/config",    api_bond_config_get)
    app.router.add_post("/api/bond/config",   api_bond_config_set)
    app.router.add_get("/api/bond/cities",    api_bond_cities_get)
    app.router.add_post("/api/bond/cities",   api_bond_cities_set)
    app.router.add_get("/api/bond/logs",              api_bond_logs)
    app.router.add_get("/api/bond/discover-cities",  api_bond_discover_cities)
    app.router.add_get("/api/bond/paper-trades",     api_bond_paper_trades)
    app.router.add_get("/api/bond/paper-stats",      api_bond_paper_stats)
    app.router.add_post("/api/bond/paper-check",      api_bond_paper_check_resolutions)
    app.router.add_post("/api/bond/paper-revert",     api_bond_paper_revert_resolutions)
    app.router.add_get("/api/bond/real-stats",        api_bond_real_stats)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    return app, runner
