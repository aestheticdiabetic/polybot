"""
main.py — Bot entrypoint. Orchestrates scanner, trader, redeemer, and dashboard.
"""
import asyncio
import logging
import os
import signal
import sys
import time

from config import LOG_LEVEL, LOG_FILE, SIM, STRATEGY, BOT_MODE, BOND_LOG_FILE
from state import StateManager
from scanner import Scanner
from trader import Trader
from redeemer import Redeemer
from config_override import load_overrides

# Load persisted config overrides from dashboard before logging
load_overrides()

# ── Logging setup ────────────────────────────────────────────────

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

_log_handlers = [
    logging.StreamHandler(sys.stdout),
    logging.FileHandler(LOG_FILE),
]
if BOT_MODE in ("BOND", "PAPER"):
    os.makedirs(os.path.dirname(BOND_LOG_FILE), exist_ok=True)
    _log_handlers.append(logging.FileHandler(BOND_LOG_FILE))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=_log_handlers,
)
log = logging.getLogger("main")


async def run_bonding_loop(state: StateManager) -> None:
    """BOND mode main loop — weather market bonding strategy."""
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
    from bonding.weather_client import get_all_forecasts
    from bonding.market_scanner import scan_weather_markets
    from bonding.opportunity_scorer import score_all
    from bonding.exit_manager import ExitManager, BondPosition
    from config import (
        CLOB_HOST, CHAIN_ID, PRIVATE_KEY, FUNDER_ADDRESS,
        API_KEY, API_SECRET, API_PASSPHRASE,
        BOND_POLL_INTERVAL_SECS, BOND_MAX_MARKETS_PER_RUN,
    )

    # Dedicated CLOB client for bonding mode (same credentials, isolated instance)
    creds = ApiCreds(
        api_key=API_KEY,
        api_secret=API_SECRET,
        api_passphrase=API_PASSPHRASE,
    )
    bond_client = ClobClient(
        host=CLOB_HOST,
        chain_id=CHAIN_ID,
        key=PRIVATE_KEY,
        creds=creds,
        signature_type=2,
        funder=FUNDER_ADDRESS,
    )

    from bonding.order_tracker import PendingOrderTracker

    exit_mgr     = ExitManager(bond_client)
    order_tracker = PendingOrderTracker(bond_client, exit_mgr)
    asyncio.get_running_loop().create_task(exit_mgr.run())
    asyncio.get_running_loop().create_task(order_tracker.run())

    state.set_running(True)
    log.info(
        f"PolyBot BOND mode starting — "
        f"poll_interval={BOND_POLL_INTERVAL_SECS}s | "
        f"max_per_run={BOND_MAX_MARKETS_PER_RUN}"
    )

    import time as _time
    cycle = 0
    while state.is_running():
        cycle += 1
        cycle_start = _time.time()
        try:
            markets = await scan_weather_markets()

            city_date_pairs = list({(m.city, m.target_date) for m in markets})
            forecasts = await get_all_forecasts(city_date_pairs)

            opps = score_all(markets, forecasts)

            placed = 0
            for opp in opps[:BOND_MAX_MARKETS_PER_RUN]:
                await _place_bond_order(bond_client, exit_mgr, order_tracker, opp, OrderArgs, OrderType)
                placed += 1

            state.update_bond_stats({
                "cycle": cycle,
                "last_cycle_at": _time.time(),
                "cycle_duration_s": round(_time.time() - cycle_start, 1),
                "markets_scanned": len(markets),
                "opportunities_found": len(opps),
                "orders_placed": placed,
            })

        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.error(f"Bonding loop error: {exc}", exc_info=True)

        await asyncio.sleep(BOND_POLL_INTERVAL_SECS)

    state.set_running(False)
    log.info("BOND mode stopped cleanly")


async def _place_bond_order(client, exit_mgr, order_tracker, opp, OrderArgs, OrderType) -> None:
    """
    Execute a bonding opportunity using a two-phase fill:

    1. FOK for shares_immediate — buys what's available in the book right now.
    2. GTC limit for shares_limit — queues a resting buy at limit_price for
       the remainder. The PendingOrderTracker monitors these and cancels them
       if the edge deteriorates.
    """
    from datetime import datetime, timezone
    from bonding.exit_manager import BondPosition
    from bonding.order_tracker import PendingOrder
    loop = asyncio.get_running_loop()

    # ── Phase 1: immediate FOK fill ───────────────────────────────
    if opp.shares_immediate > 0:
        fok_args = OrderArgs(
            token_id=opp.market.token_id,
            price=opp.market.best_ask,
            size=opp.shares_immediate,
            side="BUY",
        )
        try:
            signed = await loop.run_in_executor(None, client.create_order, fok_args)
            await loop.run_in_executor(
                None, lambda: client.post_order(signed, OrderType.FOK)
            )
            log.info(
                f"BOND_FOK_PLACED city={opp.market.city} date={opp.market.target_date} "
                f"tier={opp.tier} shares={opp.shares_immediate} "
                f"price={opp.market.best_ask:.4f} ev={opp.ev:.4f}"
            )
            pos = BondPosition(
                market_id=opp.market.market_id,
                token_id=opp.market.token_id,
                question=opp.market.question,
                city=opp.market.city,
                outcome="YES",
                tier=opp.tier,
                shares=opp.shares_immediate,
                entry_price=opp.market.best_ask,
                entry_time=datetime.now(timezone.utc).isoformat(),
                resolution_time=opp.market.resolution_time.isoformat(),
                status="OPEN",
            )
            await exit_mgr.add_position(pos)
        except Exception as exc:
            log.warning(
                f"BOND_FOK_FAILED city={opp.market.city} tier={opp.tier} "
                f"market={opp.market.market_id[:8]} error={exc}"
            )

    # ── Phase 2: GTC limit for remainder ─────────────────────────
    if opp.shares_limit > 0:
        gtc_args = OrderArgs(
            token_id=opp.market.token_id,
            price=opp.limit_price,
            size=opp.shares_limit,
            side="BUY",
        )
        try:
            signed = await loop.run_in_executor(None, client.create_order, gtc_args)
            result = await loop.run_in_executor(
                None, lambda: client.post_order(signed, OrderType.GTC)
            )
            order_id = (result or {}).get("orderID") or (result or {}).get("order_id", "")
            if order_id:
                pending = PendingOrder(
                    order_id=order_id,
                    market_id=opp.market.market_id,
                    token_id=opp.market.token_id,
                    question=opp.market.question,
                    city=opp.market.city,
                    tier=opp.tier,
                    shares=opp.shares_limit,
                    limit_price=opp.limit_price,
                    prob_at_placement=opp.prob,
                    placed_at=datetime.now(timezone.utc).isoformat(),
                    resolution_time=opp.market.resolution_time.isoformat(),
                    status="PENDING",
                )
                await order_tracker.add_order(pending)
            else:
                log.warning(
                    f"BOND_GTC_NO_ORDER_ID city={opp.market.city} "
                    f"result={result}"
                )
        except Exception as exc:
            log.warning(
                f"BOND_GTC_FAILED city={opp.market.city} tier={opp.tier} "
                f"market={opp.market.market_id[:8]} error={exc}"
            )


async def run_paper_loop(state: StateManager) -> None:
    """PAPER mode — runs paper_sim scan cycles, controlled via dashboard Start/Stop."""
    from bonding.paper_sim import run_cycle
    from config import BOND_POLL_INTERVAL_SECS

    state.set_running(True)
    log.info(f"PolyBot PAPER mode starting — poll_interval={BOND_POLL_INTERVAL_SECS}s")

    cycle = 0
    while state.is_running():
        cycle += 1
        cycle_start = time.time()
        try:
            found = await run_cycle()
            state.update_bond_stats({
                "cycle":              cycle,
                "last_cycle_at":      time.time(),
                "cycle_duration_s":   round(time.time() - cycle_start, 1),
                "markets_scanned":    found,
                "opportunities_found": found,
                "orders_placed":      0,  # paper mode — no real orders
            })
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.error(f"Paper sim loop error: {exc}", exc_info=True)
        await asyncio.sleep(BOND_POLL_INTERVAL_SECS)

    state.set_running(False)
    log.info("PAPER mode stopped cleanly")


async def run_bot(state: StateManager):
    """Main bot loop."""
    trader   = Trader(state)
    scanner  = Scanner(on_bracket=trader.on_bracket, on_near_bracket=trader.on_near_bracket, on_new_markets=trader.on_new_markets)
    redeemer = Redeemer(state)

    await trader.start()
    scanner.set_client(trader._client)  # Provide client for fresh metadata fetches on bracket detection
    trader.set_scanner(scanner)         # Provide live WS price state for order book refresh at submission
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

    # Select loop based on BOT_MODE
    if BOT_MODE == "BOND":
        _bot_runner = run_bonding_loop
    elif BOT_MODE == "PAPER":
        _bot_runner = run_paper_loop
    else:
        _bot_runner = run_bot
    _mode_name = {"BOND": "bonding", "PAPER": "paper sim"}.get(BOT_MODE, "arbitrage")
    log.info(f"BOT_MODE={BOT_MODE} — using {_mode_name} loop")

    # Start bot if AUTO_START env var is set
    if os.getenv("AUTO_START", "false").lower() == "true":
        bot_task = loop.create_task(_bot_runner(state))

    # Keep running — dashboard controls bot start/stop
    try:
        while True:
            await asyncio.sleep(1)
            # Check if dashboard requested bot start
            if state.is_running() and (bot_task is None or bot_task.done()):
                bot_task = loop.create_task(_bot_runner(state))
            elif not state.is_running() and bot_task and not bot_task.done():
                bot_task.cancel()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
