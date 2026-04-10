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
    """BOND mode main loop — weather market bonding strategy.

    Architecture:
    - REST scan every BOND_POLL_INTERVAL_SECS (60s) discovers new/closed markets
      and refreshes weather forecasts (cached 1-2h, so no extra meteo calls).
    - A persistent WebSocket connection (BondPriceFeed) subscribes to all weather
      market token IDs and scores opportunities on every price tick — no polling delay.
    - REST scan also does a scoring pass as a fallback for markets with no recent WS events.
    - Per-token cooldown (5 min) prevents duplicate orders from both paths firing at once.
    """
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
    from bonding.weather_client import get_consensus_forecasts
    from bonding.market_scanner import scan_weather_markets
    from bonding.opportunity_scorer import score_all
    from bonding.exit_manager import ExitManager, BondPosition
    from bonding.price_feed import BondPriceFeed
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

    exit_mgr      = ExitManager(bond_client)
    order_tracker = PendingOrderTracker(bond_client, exit_mgr)
    asyncio.get_running_loop().create_task(exit_mgr.run())
    asyncio.get_running_loop().create_task(order_tracker.run())

    # WS price feed — fires on_opportunity whenever a price tick creates an edge
    async def _on_ws_opportunity(opp):
        await _place_bond_order(bond_client, exit_mgr, order_tracker, opp, OrderArgs, OrderType)

    # Load peak hour stats and seed any cities that haven't been bootstrapped yet.
    # Seeding fetches 2 years of archive data per unseeded city (1 API call/city).
    # Only runs for cities below SEED_MIN_SAMPLES — already-seeded cities are skipped.
    from bonding.weather_client import init_peak_stats
    from bonding.historical_peak_seeder import seed_missing_cities
    import bonding.weather_client as _wc
    from config import BOND_CITIES

    init_peak_stats()  # load existing stats into shared in-memory dict
    await seed_missing_cities(BOND_CITIES, _wc._peak_hour_stats)
    init_peak_stats()  # reload after seeding to incorporate newly written data
    log.info("BOND mode: peak hour stats loaded (%d cities)", len(_wc._peak_hour_stats))

    # Seed/update 2-year daily max temp history for statistical (ARIMA/Naïve) source.
    # First run fetches ~2 years per city (~3 min for all cities); subsequent runs
    # only fill the last few missing days and complete in seconds.
    from bonding.statistical_forecast import seed_all_cities as _seed_statistical
    log.info("BOND mode: seeding statistical temp history...")
    await _seed_statistical(BOND_CITIES)

    # Initial scan to pre-populate the feed before the WS connects.
    # This ensures the WebSocket subscribes to all markets immediately on connect
    # rather than starting with 0 subscriptions and resubscribing ~60s later.
    log.info("BOND mode: running initial scan to pre-populate WS feed...")
    markets = await scan_weather_markets()
    city_date_pairs = list({(m.city, m.target_date) for m in markets})
    forecasts = await get_consensus_forecasts(city_date_pairs)

    feed = BondPriceFeed(on_opportunity=_on_ws_opportunity)
    feed.update_markets(markets, forecasts)  # pre-populate; WS not connected yet so no resubscribe
    exit_mgr.set_price_feed(feed)            # wire feed so confidence exits can read live forecasts
    feed_task = asyncio.get_running_loop().create_task(feed.run())  # now connects subscribed

    state.set_running(True)
    log.info(
        f"PolyBot BOND mode starting — "
        f"discovery_interval={BOND_POLL_INTERVAL_SECS}s (WS for real-time prices) | "
        f"max_per_run={BOND_MAX_MARKETS_PER_RUN}"
    )

    import time as _time
    cycle = 0
    while state.is_running():
        cycle += 1
        cycle_start = _time.time()
        try:
            # Cycle 1 reuses the pre-fetched markets/forecasts; subsequent cycles rescan
            if cycle > 1:
                markets = await scan_weather_markets()
                city_date_pairs = list({(m.city, m.target_date) for m in markets})
                forecasts = await get_consensus_forecasts(city_date_pairs)
                feed.update_markets(markets, forecasts)

            # Fallback REST scoring pass: catches markets with no recent WS events
            from bonding.sure_thing_scorer import score_certain
            opps = score_all(markets, forecasts) + score_certain(markets, forecasts)
            placed = 0
            for opp in opps[:BOND_MAX_MARKETS_PER_RUN]:
                if not feed.is_on_cooldown(opp.token_id):
                    await _place_bond_order(
                        bond_client, exit_mgr, order_tracker, opp, OrderArgs, OrderType
                    )
                    feed.mark_cooldown(opp.token_id)
                    placed += 1

            state.update_bond_stats({
                "cycle": cycle,
                "last_cycle_at": _time.time(),
                "cycle_duration_s": round(_time.time() - cycle_start, 1),
                "markets_scanned": len(markets),
                "opportunities_found": len(opps),
                "orders_placed": placed,
                "ws_price_events": feed.stats["price_events"],
                "ws_opportunities": feed.stats["opportunities_fired"],
                "ws_reconnects": feed.stats["ws_reconnects"],
            })

        except asyncio.CancelledError:
            feed_task.cancel()
            break
        except Exception as exc:
            log.error(f"Bonding loop error: {exc}", exc_info=True)

        await asyncio.sleep(BOND_POLL_INTERVAL_SECS)

    await feed.stop()
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

    # Convert temp range to Celsius once — used by both FOK position and GTC pending order
    from bonding.weather_client import fahrenheit_to_celsius
    _temp_min_c = opp.market.temp_min
    _temp_max_c = opp.market.temp_max
    if opp.market.unit == "F":
        if _temp_min_c is not None:
            _temp_min_c = fahrenheit_to_celsius(_temp_min_c)
        if _temp_max_c is not None:
            _temp_max_c = fahrenheit_to_celsius(_temp_max_c)

    # ── Phase 1: immediate FOK fill ───────────────────────────────
    if opp.shares_immediate > 0:
        fok_args = OrderArgs(
            token_id=opp.token_id,
            price=opp.side_ask,
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
                f"outcome={opp.outcome} tier={opp.tier} shares={opp.shares_immediate} "
                f"price={opp.side_ask:.4f} ev={opp.ev:.4f}"
            )
            pos = BondPosition(
                market_id=opp.market.market_id,
                token_id=opp.token_id,
                question=opp.market.question,
                city=opp.market.city,
                outcome=opp.outcome,
                tier=opp.tier,
                shares=opp.shares_immediate,
                entry_price=opp.side_ask,
                entry_time=datetime.now(timezone.utc).isoformat(),
                resolution_time=opp.market.resolution_time.isoformat(),
                status="OPEN",
                prob=opp.prob,
                temp_min_c=_temp_min_c,
                temp_max_c=_temp_max_c,
            )
            await exit_mgr.add_position(pos)
        except Exception as exc:
            log.warning(
                f"BOND_FOK_FAILED city={opp.market.city} tier={opp.tier} "
                f"market={opp.market.market_id[:8]} error={exc}"
            )

    # ── Phase 2: GTC limit for remainder ─────────────────────────
    from config import BOND_MIN_GTC_ORDER_USDC
    gtc_capital = opp.shares_limit * opp.limit_price
    if opp.shares_limit > 0 and gtc_capital < BOND_MIN_GTC_ORDER_USDC:
        log.debug(
            f"BOND_GTC_SKIP city={opp.market.city} tier={opp.tier} "
            f"shares_limit={opp.shares_limit} capital=${gtc_capital:.3f} < min=${BOND_MIN_GTC_ORDER_USDC}"
        )
    if opp.shares_limit > 0 and gtc_capital >= BOND_MIN_GTC_ORDER_USDC:
        gtc_args = OrderArgs(
            token_id=opp.token_id,
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
                    token_id=opp.token_id,
                    question=opp.market.question,
                    city=opp.market.city,
                    tier=opp.tier,
                    shares=opp.shares_limit,
                    limit_price=opp.limit_price,
                    prob_at_placement=opp.prob,
                    placed_at=datetime.now(timezone.utc).isoformat(),
                    resolution_time=opp.market.resolution_time.isoformat(),
                    status="PENDING",
                    outcome=opp.outcome,
                    temp_min_c=_temp_min_c,
                    temp_max_c=_temp_max_c,
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
    """PAPER mode — mirrors live bonding loop with WS prices, but logs instead of placing orders.

    Architecture matches run_bonding_loop exactly:
    - Initial REST scan pre-populates the WS feed before it connects, so the
      WebSocket subscribes to all weather token IDs immediately on connect.
    - REST scan every BOND_POLL_INTERVAL_SECS discovers new/closed markets.
    - BondPriceFeed subscribes via WS and calls back on every qualifying price tick.
    - Per-market deduplication via seen_ids (loaded from JSONL at startup) prevents
      logging the same market opportunity more than once across restarts.
    - REST fallback pass catches markets with no recent WS events.
    """
    from bonding.weather_client import get_consensus_forecasts
    from bonding.market_scanner import scan_weather_markets
    from bonding.opportunity_scorer import score_all
    from bonding.price_feed import BondPriceFeed
    from bonding.paper_sim import log_opportunity, _load_seen_market_ids, PaperExitManager, PAPER_LOG
    from config import BOND_POLL_INTERVAL_SECS, BOND_MAX_MARKETS_PER_RUN

    # Load already-logged market IDs so we never double-log across restarts
    seen_ids: set[str] = _load_seen_market_ids()
    log.info(f"PAPER mode: loaded {len(seen_ids)} previously logged market IDs")

    exit_mgr = PaperExitManager(PAPER_LOG, seen_ids=seen_ids)

    async def _on_ws_opportunity(opp):
        if not exit_mgr.has_open_position(opp.token_id):
            if log_opportunity(opp, seen_ids):
                exit_mgr.add_position(opp)

    async def _on_price_tick(token_id: str, price: float) -> None:
        await exit_mgr.on_price_tick(token_id, price)

    # Seed/update statistical temp history (shared with BOND mode cache on disk).
    from bonding.statistical_forecast import seed_all_cities as _seed_statistical
    from config import BOND_CITIES
    log.info("PAPER mode: seeding statistical temp history...")
    await _seed_statistical(BOND_CITIES)

    # Initial scan to pre-populate the feed before the WS connects.
    # This ensures the WebSocket subscribes to all markets immediately on connect
    # rather than starting with 0 subscriptions and resubscribing ~60s later.
    log.info("PAPER mode: running initial scan to pre-populate WS feed...")
    markets = await scan_weather_markets()
    city_date_pairs = list({(m.city, m.target_date) for m in markets})
    forecasts = await get_consensus_forecasts(city_date_pairs)

    feed = BondPriceFeed(on_opportunity=_on_ws_opportunity, on_price_tick=_on_price_tick)
    feed.update_markets(markets, forecasts)  # pre-populate; WS not connected yet so no resubscribe
    feed_task = asyncio.get_running_loop().create_task(feed.run())  # now connects subscribed

    state.set_running(True)
    log.info(
        f"PolyBot PAPER mode starting — "
        f"discovery_interval={BOND_POLL_INTERVAL_SECS}s (WS for real-time prices)"
    )

    cycle = 0
    while state.is_running():
        cycle += 1
        cycle_start = time.time()
        try:
            # Cycle 1 reuses the pre-fetched markets/forecasts; subsequent cycles rescan
            if cycle > 1:
                markets = await scan_weather_markets()
                city_date_pairs = list({(m.city, m.target_date) for m in markets})
                forecasts = await get_consensus_forecasts(city_date_pairs)
                feed.update_markets(markets, forecasts)

            # Fallback REST scoring pass: catches markets with no recent WS events
            from bonding.sure_thing_scorer import score_certain
            opps = (score_all(markets, forecasts) + score_certain(markets, forecasts))[:BOND_MAX_MARKETS_PER_RUN]
            logged = 0
            for opp in opps:
                if not feed.is_on_cooldown(opp.token_id):
                    if log_opportunity(opp, seen_ids):
                        feed.mark_cooldown(opp.token_id)
                        exit_mgr.add_position(opp)
                        logged += 1

            state.update_bond_stats({
                "cycle":               cycle,
                "last_cycle_at":       time.time(),
                "cycle_duration_s":    round(time.time() - cycle_start, 1),
                "markets_scanned":     len(markets),
                "opportunities_found": len(opps),
                "orders_placed":       logged,  # "placed" = logged in paper mode
                "ws_price_events":     feed.stats["price_events"],
                "ws_opportunities":    feed.stats["opportunities_fired"],
                "ws_reconnects":       feed.stats["ws_reconnects"],
            })

        except asyncio.CancelledError:
            feed_task.cancel()
            break
        except Exception as exc:
            log.error(f"Paper sim loop error: {exc}", exc_info=True)
        await asyncio.sleep(BOND_POLL_INTERVAL_SECS)

    await feed.stop()
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
