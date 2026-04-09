"""
price_feed.py — Real-time WebSocket price feed for weather bond markets.

Maintains a persistent WS connection to the Polymarket CLOB, subscribing to
the YES-outcome token IDs for all tracked weather markets. On each qualifying
price update, scores the opportunity and fires an async callback.

Usage:
    feed = BondPriceFeed(on_opportunity=callback)
    feed.update_markets(candidates, forecasts)   # call after each REST scan
    asyncio.create_task(feed.run())              # start WS listener
"""
import asyncio
import config as _config
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import websockets

from bonding import peak_hour_stats as _peak_stats
from bonding.market_scanner import MarketCandidate
from bonding.opportunity_scorer import score_market
from bonding.weather_client import ForecastResult, SourceConsensus, _peak_hour_stats as _loaded_stats
from config import CLOB_WS

log = logging.getLogger("bond.feed")

# Minimum seconds between orders for the same token to avoid re-firing on small ticks.
COOLDOWN_SECS = 300  # 5 minutes


class BondPriceFeed:
    """
    WebSocket price listener for weather bond markets.

    Lifecycle:
      1. Call update_markets() after each REST scan to refresh the tracked set.
      2. Call run() as a background asyncio task to maintain the WS connection.
      3. On each qualifying price event, on_opportunity(ScoredOpportunity) is awaited.
    """

    def __init__(self, on_opportunity, on_price_tick=None):
        self._on_opportunity = on_opportunity
        self._on_price_tick = on_price_tick  # async cb(token_id, price) fired on every WS tick
        self._markets: dict[str, MarketCandidate] = {}        # token_id → candidate
        self._forecasts: dict[tuple, SourceConsensus] = {}     # (city, date) → consensus
        self._cooldowns: dict[str, float] = {}                # token_id → last_order_ts
        self._ws = None
        self._running = False
        self._last_msg_at: float = 0.0
        self.stats = {
            "price_events": 0,
            "opportunities_fired": 0,
            "ws_reconnects": 0,
        }

    def update_markets(
        self,
        candidates: list[MarketCandidate],
        forecasts: dict[tuple, SourceConsensus],
    ) -> None:
        """
        Refresh the tracked market set and forecasts from the latest REST scan.
        Preserves WS-updated prices for markets already in the feed.
        Registers both YES and NO token IDs so both get WS price events.
        Triggers a WS resubscription if the token set changed.
        """
        new_ids: set[str] = set()
        old_ids = set(self._markets.keys())

        updated: dict[str, MarketCandidate] = {}
        for m in candidates:
            # Preserve WS-updated YES prices
            existing = self._markets.get(m.token_id)
            if existing is not None:
                m.best_ask = existing.best_ask
                m.ask_book = existing.ask_book
            updated[m.token_id] = m
            new_ids.add(m.token_id)

            # Also register NO token — same MarketCandidate object, separate key
            if m.no_token_id:
                existing_no = self._markets.get(m.no_token_id)
                if existing_no is not None:
                    m.no_best_ask = existing_no.no_best_ask
                    m.no_ask_book = existing_no.no_ask_book
                updated[m.no_token_id] = m
                new_ids.add(m.no_token_id)

        self._markets = updated
        self._forecasts = forecasts

        if new_ids != old_ids:
            added   = new_ids - old_ids
            removed = old_ids - new_ids
            log.info(
                f"feed: market set changed +{len(added)} -{len(removed)} tokens, resubscribing"
            )
            asyncio.create_task(self._resubscribe())

    def mark_cooldown(self, token_id: str) -> None:
        """Record that an order was just placed for this token."""
        self._cooldowns[token_id] = time.time()

    def is_on_cooldown(self, token_id: str) -> bool:
        return time.time() - self._cooldowns.get(token_id, 0) < COOLDOWN_SECS

    async def run(self) -> None:
        """Maintain a persistent WS connection with exponential backoff."""
        self._running = True
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(
                    CLOB_WS,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    backoff = 1
                    await self._subscribe_all(ws)
                    log.info(
                        f"feed: WS connected, subscribed to {len(self._markets)} weather tokens"
                    )
                    async for raw in ws:
                        if not raw:
                            continue
                        try:
                            parsed = json.loads(raw)
                        except (json.JSONDecodeError, ValueError):
                            continue
                        await self._handle_message(parsed)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.stats["ws_reconnects"] += 1
                log.warning(f"feed: WS error ({exc}), reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        self._running = False

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    # ── WS internals ──────────────────────────────────────────────────────────

    async def _subscribe_all(self, ws) -> None:
        token_ids = list(self._markets.keys())
        if not token_ids:
            return
        for i in range(0, len(token_ids), 500):
            chunk = token_ids[i:i + 500]
            await ws.send(json.dumps({
                "type": "subscribe",
                "channel": "market",
                "assets_ids": chunk,
            }))

    async def _resubscribe(self) -> None:
        if self._ws:
            try:
                await self._subscribe_all(self._ws)
            except Exception:
                pass

    async def _handle_message(self, msg) -> None:
        self.stats["price_events"] += 1
        self._last_msg_at = time.time()
        if isinstance(msg, list):
            for item in msg:
                await self._process_event(item)
        elif isinstance(msg, dict):
            await self._process_event(msg)

    async def _process_event(self, event: dict) -> None:
        event_type = event.get("event_type", "")
        asset_id   = event.get("asset_id", "")

        if event_type not in ("book", "price_change"):
            return
        if asset_id not in self._markets:
            return

        market = self._markets[asset_id]
        is_no_side = (asset_id == market.no_token_id)
        updated_ask: float | None = None

        if event.get("asks"):
            # Full orderbook snapshot — build sorted ask book
            levels: list[tuple[float, float]] = []
            for a in event["asks"]:
                try:
                    p = float(a["price"])
                    s = float(a.get("size", 0.0))
                    if s > 0:
                        levels.append((p, s))
                except Exception:
                    continue
            if levels:
                levels.sort(key=lambda x: x[0])
                if is_no_side:
                    market.no_ask_book = levels
                    market.no_best_ask = levels[0][0]
                else:
                    market.ask_book = levels
                    market.best_ask = levels[0][0]
                updated_ask = levels[0][0]

        elif event_type == "price_change":
            try:
                updated_ask = float(event["price"])
            except (KeyError, ValueError, TypeError):
                return
            if is_no_side:
                market.no_best_ask = updated_ask
            else:
                market.best_ask = updated_ask

        if updated_ask is None or not (0.0 < updated_ask < 1.0):
            return

        # Notify price-tick subscribers (e.g. paper exit manager) before cooldown gate.
        if self._on_price_tick is not None:
            await self._on_price_tick(asset_id, updated_ask)

        if self.is_on_cooldown(asset_id):
            return

        forecast = self._forecasts.get((market.city, market.target_date))
        if forecast is None:
            return

        # Dynamic peak-hour gate — mirrors opportunity_scorer logic exactly.
        tz_name = _config.BOND_CITY_TIMEZONES.get(market.city)
        if not tz_name:
            log.warning(
                f"feed: {market.city} {market.target_date} — no timezone configured, skipping"
            )
            self._cooldowns[asset_id] = time.time() - COOLDOWN_SECS + 300
            return
        try:
            city_tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            log.warning(
                f"feed: {market.city} {market.target_date} — invalid timezone '{tz_name}', skipping"
            )
            self._cooldowns[asset_id] = time.time() - COOLDOWN_SECS + 300
            return

        now_utc   = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(city_tz)

        if market.target_date == now_local.date():
            current_local_hour = now_local.hour
            current_month      = now_local.month
            forecast_peak_hour = forecast.gfs.forecast_peak_hour
            gate_hour = _peak_stats.get_gate_hour(
                market.city, forecast_peak_hour, current_month, _loaded_stats
            )
            if current_local_hour >= gate_hour:
                next_day = market.target_date + timedelta(days=1)
                end_of_day_utc = datetime(
                    next_day.year, next_day.month, next_day.day, 0, 0, 0,
                    tzinfo=city_tz,
                ).astimezone(timezone.utc)
                suppress_secs = max(
                    (end_of_day_utc - now_utc).total_seconds(), 0
                ) + 300
                self._cooldowns[asset_id] = time.time() - COOLDOWN_SECS + suppress_secs
                log.info(
                    f"feed: {market.city} {market.target_date} — "
                    f"past gate hour {gate_hour} (current={current_local_hour}): "
                    f"suppressed {suppress_secs/3600:.1f}h"
                )
                return

        opp = score_market(market, forecast)
        # Always cooldown after scoring — prevents re-evaluating the same token on every tick.
        self.mark_cooldown(asset_id)
        # Only fire if scored side matches the token that triggered this event.
        # Prevents a YES price tick from firing a NO order (or vice versa).
        if opp is None or opp.token_id != asset_id:
            return
        self.stats["opportunities_fired"] += 1
        log.info(
            f"feed: WS opportunity city={market.city} date={market.target_date} "
            f"outcome={opp.outcome} ask={updated_ask:.4f} ev={opp.ev:.4f} tier={opp.tier}"
        )
        await self._on_opportunity(opp)
