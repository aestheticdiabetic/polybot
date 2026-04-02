"""
dashboard/app.py — aiohttp web dashboard with basic auth.
Accessible over SSH tunnel: ssh -L 8080:localhost:8080 user@vps
"""
import json
import logging
import os
import time
import base64
import secrets
from pathlib import Path

from aiohttp import web

from config import DASHBOARD_SECRET, SIM, STRATEGY

log = logging.getLogger("dashboard")

DASHBOARD_DIR = Path(__file__).parent

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
        log.info(f"Config updated: {updated}")
        return web.json_response({"ok": True, "updated": updated})

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
    app.router.add_get("/api/trades",   api_trades)
    app.router.add_get("/api/markets",  api_markets)
    app.router.add_get("/api/logs",     api_logs)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    return app, runner
