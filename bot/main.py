"""
main.py — Bot entrypoint. Orchestrates scanner, trader, redeemer, and dashboard.
"""
import asyncio
import logging
import os
import signal
import sys
import time

from config import LOG_LEVEL, LOG_FILE, SIM, STRATEGY
from state import StateManager
from scanner import Scanner
from trader import Trader
from redeemer import Redeemer
from config_override import load_overrides

# Load persisted config overrides from dashboard before logging
load_overrides()

# ── Logging setup ────────────────────────────────────────────────

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)
log = logging.getLogger("main")


async def run_bot(state: StateManager):
    """Main bot loop."""
    trader   = Trader(state)
    scanner  = Scanner(on_bracket=trader.on_bracket, on_near_bracket=trader.on_near_bracket)
    redeemer = Redeemer(state)

    await trader.start()
    await redeemer.start()

    state.set_running(True)
    log.info(
        f"PolyBot starting — "
        f"mode={'SIMULATION' if SIM.enabled else 'LIVE'} | "
        f"windows={STRATEGY.target_windows} | "
        f"assets={STRATEGY.target_assets} | "
        f"size=${STRATEGY.position_size_usdc}/leg | "
        f"threshold={STRATEGY.bracket_threshold}"
    )

    # Stats sync task
    async def sync_stats():
        while state.is_running():
            state.update_scanner_stats(scanner.stats)
            state.update_trader_stats(trader.stats)
            state.update_redeemer_stats(redeemer.stats)
            state.update_markets(scanner.get_markets_snapshot())
            await asyncio.sleep(2)

    asyncio.get_running_loop().create_task(sync_stats())

    # Start scanner (blocks until stopped)
    try:
        await scanner.start()
    except asyncio.CancelledError:
        pass
    finally:
        await scanner.stop()
        state.set_running(False)
        log.info("Bot stopped cleanly")


async def main():
    state = StateManager()

    # Import dashboard here to avoid circular imports
    from dashboard.app import create_app
    app, runner = await create_app(state)

    # Start dashboard server
    from aiohttp import web
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("DASHBOARD_PORT", "8080")))
    await site.start()
    log.info(f"Dashboard running on http://0.0.0.0:{os.getenv('DASHBOARD_PORT', '8080')}")

    # Handle graceful shutdown
    loop = asyncio.get_running_loop()
    bot_task = None

    def handle_stop():
        log.info("Shutdown signal received")
        if bot_task and not bot_task.done():
            bot_task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_stop)

    # Start bot if AUTO_START env var is set
    if os.getenv("AUTO_START", "false").lower() == "true":
        bot_task = loop.create_task(run_bot(state))

    # Keep running — dashboard controls bot start/stop
    try:
        while True:
            await asyncio.sleep(1)
            # Check if dashboard requested bot start
            if state.is_running() and (bot_task is None or bot_task.done()):
                bot_task = loop.create_task(run_bot(state))
            elif not state.is_running() and bot_task and not bot_task.done():
                bot_task.cancel()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
