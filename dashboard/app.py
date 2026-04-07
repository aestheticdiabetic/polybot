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
from pathlib import Path

import config as _config
from aiohttp import web

from config import DASHBOARD_SECRET, SIM, STRATEGY

log = logging.getLogger("dashboard")

DASHBOARD_DIR = Path(__file__).parent


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
        for tier in ("CORE", "SECONDARY", "WING"):
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
            "BOND_MIN_EV_CORE":            _config.BOND_MIN_EV_CORE,
            "BOND_MIN_EV_SECONDARY":       _config.BOND_MIN_EV_SECONDARY,
            "BOND_CONFIDENCE_FLOOR":       _config.BOND_CONFIDENCE_FLOOR,
            "BOND_EDGE_FLOOR":             _config.BOND_EDGE_FLOOR,
            "BOND_SHARES_CORE":            _config.BOND_SHARES_CORE,
            "BOND_SHARES_SECONDARY":       _config.BOND_SHARES_SECONDARY,
            "BOND_SHARES_WING":            _config.BOND_SHARES_WING,
            "BOND_MAX_CAPITAL_PER_CLUSTER": _config.BOND_MAX_CAPITAL_PER_CLUSTER,
            "BOND_EARLY_EXIT_PRICE":       _config.BOND_EARLY_EXIT_PRICE,
            "BOND_WING_EXIT_MULTIPLIER":   _config.BOND_WING_EXIT_MULTIPLIER,
            "BOND_WING_MIN_ABS_GAIN":      _config.BOND_WING_MIN_ABS_GAIN,
            "BOND_GAS_FLOOR_HOURS":        _config.BOND_GAS_FLOOR_HOURS,
            "BOND_POLL_INTERVAL_SECS":     _config.BOND_POLL_INTERVAL_SECS,
            "BOND_MAX_MARKETS_PER_RUN":    _config.BOND_MAX_MARKETS_PER_RUN,
        })

    @_auth_required
    async def api_bond_config_set(request):
        """Live-update BOND_* numeric parameters."""
        data = await request.json()
        _float_keys = {
            "BOND_MIN_EV_CORE", "BOND_MIN_EV_SECONDARY", "BOND_CONFIDENCE_FLOOR",
            "BOND_EDGE_FLOOR", "BOND_MAX_CAPITAL_PER_CLUSTER",
            "BOND_EARLY_EXIT_PRICE", "BOND_WING_EXIT_MULTIPLIER", "BOND_WING_MIN_ABS_GAIN",
        }
        _int_keys = {
            "BOND_GAS_FLOOR_HOURS", "BOND_SHARES_CORE", "BOND_SHARES_SECONDARY",
            "BOND_SHARES_WING", "BOND_POLL_INTERVAL_SECS", "BOND_MAX_MARKETS_PER_RUN",
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
    app.router.add_get("/api/bond/logs",             api_bond_logs)
    app.router.add_get("/api/bond/discover-cities",  api_bond_discover_cities)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    return app, runner
