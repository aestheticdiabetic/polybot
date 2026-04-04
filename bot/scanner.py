"""
scanner.py — WebSocket price listener and bracket detector.
Connects to Polymarket CLOB WebSocket, subscribes to all active
Up/Down markets, and emits bracket opportunities when found.
"""
import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple
import aiohttp
import websockets

from config import CLOB_HOST, CLOB_WS, STRATEGY, SIM

log = logging.getLogger("scanner")


@dataclass
class MarketInfo:
    token_id_up: str
    token_id_down: str
    condition_id: str
    title: str
    window: str       # "5M", "15M", "1H"
    asset: str        # "ETH", "BTC"
    end_time: float   # unix timestamp


@dataclass
class PriceState:
    """Price state for a single token (one side of a bracket)."""
    ask: float = 1.0
    bid: float = 0.0
    ask_size: float = 0.0   # shares at best ask (top-of-book, for log display)
    ask_book: list = field(default_factory=list)  # [(price, size), ...] ascending
    last_update: float = 0.0

    def depth_up_to(self, limit_price: float) -> float:
        """Cumulative ask depth (shares) for all levels at or below limit_price.

        A FOK order with this limit sweeps every resting ask ≤ limit_price, so
        this measures the actual fillable quantity — unlike ask_size which only
        counts the single best-ask level and under-counts on thin multi-level books.
        Returns 0.0 when no book snapshot has been received yet.
        """
        return sum(size for price, size in self.ask_book if price <= limit_price)


@dataclass
class BracketOpportunity:
    market: MarketInfo
    ask_up: float
    ask_down: float
    combined_ask: float     # ask_up + ask_down
    spread: float           # 1.0 - combined_ask = gross profit per $1 staked
    gross_profit_usdc: float
    net_profit_usdc: float
    detected_at: float      # unix timestamp
    depth_up: float = 0.0   # shares visible at ask_up (single level — may be partial)
    depth_down: float = 0.0 # shares visible at ask_down
    bid_up: float = 0.0     # best bid on UP token at detection time (emergency exit)
    bid_down: float = 0.0   # best bid on DOWN token at detection time (emergency exit)
    # FOK limit prices — set to max price we'd pay while remaining profitable.
    # Splitting available margin evenly lets the FOK sweep through price levels
    # above best ask (e.g. 2 @ 0.53 + 20 @ 0.54) instead of cancelling on thin
    # depth at a single level.  Worst case: both legs fill at their limits →
    # limit_up + limit_down = bracket_threshold → still profitable.
    limit_up: float = 0.0
    limit_down: float = 0.0
    sim_mode: bool = False
    metadata_age_ms: float = 0.0  # age of market metadata used in detection (cache vs. fresh)
    ask_book_up: list = field(default_factory=list)  # Full order book for UP token [(price, size), ...]
    ask_book_down: list = field(default_factory=list)  # Full order book for DOWN token [(price, size), ...]


class Scanner:
    def __init__(
        self,
        on_bracket: Callable[[BracketOpportunity], None],
        on_near_bracket: Optional[Callable[[BracketOpportunity], None]] = None,
        client=None,
    ):
        self.on_bracket = on_bracket
        self.on_near_bracket = on_near_bracket
        self._markets: Dict[str, MarketInfo] = {}          # condition_id → MarketInfo
        self._prices:  Dict[str, PriceState] = {}          # token_id → PriceState
        self._market_added_at: Dict[str, float] = {}       # condition_id → unix ts added
        self._recent_brackets: Dict[str, float] = {}       # condition_id → last bracket ts
        self._recent_near_brackets: Dict[str, float] = {}  # condition_id → last near-bracket ts
        self._running = False
        self._ws = None
        self._last_ws_msg_at: float = 0.0
        self._metadata_cache: Optional[List[MarketInfo]] = None  # cache for market metadata
        self._metadata_cache_time: float = time.time()  # Initialize to now to avoid stale metadata_age before first fetch
        self._metadata_cache_ttl: float = 10.0  # cache valid for 10 seconds (was 30s)
        self._last_http_fetch_at: float = 0.0  # timestamp of last HTTP call
        self._client = client  # PolyClient for fetching fresh metadata on bracket detection
        self.stats = {
            "markets_tracked": 0,
            "markets_active": 0,   # subset with live WS price data on both sides
            "price_updates": 0,
            "brackets_detected": 0,
            "brackets_throttled": 0,
            "near_brackets_detected": 0,
            "ws_reconnects": 0,
            "metadata_fetches_http": 0,
            "metadata_fetches_cache": 0,
        }

    # ── Public API ────────────────────────────────────────────────

    async def start(self):
        self._running = True
        await asyncio.gather(
            self._market_discovery_loop(),  # HTTP polling in background (Option 2)
            self._ws_loop(),                 # WS price updates (real-time, high priority)
            self._market_refresh_loop(),     # Market maintenance (add/remove from local cache)
        )

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    def set_client(self, client):
        """Set the PolyClient after trader initialization."""
        self._client = client

    def invalidate_metadata_cache(self):
        """Force next metadata fetch to be fresh (not cached)."""
        self._metadata_cache_time = 0.0

    @property
    def tracked_market_count(self) -> int:
        return len(self._markets)

    def get_markets_snapshot(self) -> list:
        """Return current price state for all tracked markets (for dashboard)."""
        now = time.time()
        result = []
        for cid, m in self._markets.items():
            ps_up   = self._prices.get(m.token_id_up)
            ps_down = self._prices.get(m.token_id_down)
            ask_up   = ps_up.ask   if ps_up   else None
            ask_down = ps_down.ask if ps_down else None
            combined = round(ask_up + ask_down, 4) if (ask_up and ask_down) else None
            # "no_data" means we've never received a price event for this token
            no_data = (
                not ps_up or not ps_down or
                ps_up.last_update == 0 or ps_down.last_update == 0
            )
            stale = no_data or (
                now - ps_up.last_update > 30 or
                now - ps_down.last_update > 30
            )
            time_left = round(m.end_time - now) if m.end_time else None
            result.append({
                "condition_id": cid[:10] + "…",
                "asset": m.asset,
                "window": m.window,
                "title": m.title,
                "ask_up": round(ask_up, 4) if ask_up else None,
                "ask_down": round(ask_down, 4) if ask_down else None,
                "combined": combined,
                "spread_pct": round((1.0 - combined) * 100, 3) if combined else None,
                "time_left_s": time_left if time_left and time_left > 0 else None,
                "stale": stale,
            })
        result.sort(key=lambda x: (x["asset"], x["window"]))
        return result

    # ── Market discovery ─────────────────────────────────────────

    # Full name → ticker mapping for Polymarket event titles
    _ASSET_NAME_MAP = {
        "BITCOIN": "BTC", "ETHEREUM": "ETH", "SOLANA": "SOL",
        "BNB": "BNB", "XRP": "XRP", "RIPPLE": "XRP",
        "CARDANO": "ADA", "AVALANCHE": "AVAX", "DOGECOIN": "DOGE",
        "POLYGON": "MATIC", "POL": "POL", "CHAINLINK": "LINK",
        "POLKADOT": "DOT", "UNISWAP": "UNI", "LITECOIN": "LTC",
        "COSMOS": "ATOM",
    }

    async def _fetch_active_markets_http(self) -> List[MarketInfo]:
        """Fetch all active Up/Down markets from the Gamma events API via HTTP."""
        markets: List[MarketInfo] = []
        seen: set = set()
        url = "https://gamma-api.polymarket.com/events"
        params = {
            "active": "true",
            "closed": "false",
            "tag_slug": "up-or-down",
            "limit": 500,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    events = await r.json()

            for event in events:
                for market in event.get("markets", []):
                    cid = market.get("conditionId") or market.get("condition_id", "")
                    if not cid or cid in seen:
                        continue
                    info = self._parse_event_market(event, market)
                    if info:
                        seen.add(cid)
                        markets.append(info)
        except Exception as e:
            log.error(f"Market fetch failed: {e}")
            # Return cached data if HTTP fails
            if self._metadata_cache is not None:
                log.warning(f"Using cached market metadata ({len(self._metadata_cache)} markets)")
                return self._metadata_cache

        now = time.time()
        log.info(
            f"Market fetch complete: {len(markets)} up/down markets found "
            f"(fresh HTTP fetch)"
        )
        self._metadata_cache = markets
        self._metadata_cache_time = now
        self._last_http_fetch_at = now
        self.stats["metadata_fetches_http"] += 1
        return markets

    async def _fetch_active_markets(self) -> List[MarketInfo]:
        """Fetch markets with TTL-based caching.

        Returns cached metadata if available and fresh (< 30 seconds old).
        Otherwise makes a fresh HTTP call and caches the result.
        """
        now = time.time()
        # Use cache if it's fresh
        if (self._metadata_cache is not None
            and (now - self._metadata_cache_time) < self._metadata_cache_ttl):
            self.stats["metadata_fetches_cache"] += 1
            return self._metadata_cache

        # Fetch fresh metadata from HTTP
        return await self._fetch_active_markets_http()

    def _parse_event_market(self, event: dict, market: dict) -> Optional[MarketInfo]:
        """Parse a market nested inside a Gamma API event.

        Gamma API field notes (verified against live API 2026-04):
        - tags: array of objects with 'slug' field (e.g. {"slug": "5M", ...})
        - conditionId: camelCase only — no condition_id variant exists
        - clobTokenIds: JSON-encoded string of token ID array (positionally
          aligned with the 'outcomes' JSON string)
        - outcomes: JSON-encoded string e.g. '["Up", "Down"]'
        - endDate: camelCase on both event and market — no end_date variant
        """
        try:
            title = event.get("title", "")
            title_up = title.upper()

            # Tags are objects with a 'slug' field — never plain strings.
            # Slugs are mixed-case (e.g. "5M", "up-or-down"), so compare
            # case-insensitively.
            tags = [t.get("slug", "").upper() if isinstance(t, dict) else t.upper()
                    for t in event.get("tags", [])]
            window = None
            for w in STRATEGY.target_windows:
                if w.upper() in tags:
                    window = w
                    break
            if not window:
                return None

            # Detect asset — check full names first, then ticker symbols
            asset = None
            for name, ticker in self._ASSET_NAME_MAP.items():
                if name in title_up and ticker in STRATEGY.target_assets:
                    asset = ticker
                    break
            if not asset:
                for a in STRATEGY.target_assets:
                    if a.upper() in title_up:
                        asset = a
                        break
            if not asset:
                return None

            # Extract token IDs from clobTokenIds (JSON string) and outcomes
            # (JSON string). Positions are aligned: clobTokenIds[i] belongs to
            # outcomes[i].
            raw_clob = market.get("clobTokenIds")
            raw_outcomes = market.get("outcomes")
            if not raw_clob or not raw_outcomes:
                return None

            clob_ids = json.loads(raw_clob) if isinstance(raw_clob, str) else raw_clob
            outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

            if len(clob_ids) < 2 or len(outcomes) < 2:
                return None

            token_up = token_down = None
            for i, outcome in enumerate(outcomes):
                if outcome.lower() == "up":
                    token_up = clob_ids[i]
                elif outcome.lower() == "down":
                    token_down = clob_ids[i]

            if not token_up or not token_down:
                return None

            # Parse end time — endDate is camelCase on both event and market
            end_time = 0
            raw_end = market.get("endDate") or event.get("endDate", "")
            if raw_end:
                try:
                    from datetime import datetime
                    end_time = datetime.fromisoformat(
                        raw_end.replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    pass

            cid = market.get("conditionId", "")
            if not cid:
                return None

            # Reject markets that have already ended
            if end_time and end_time < time.time():
                return None

            # Reject markets that haven't started yet (allow 60s grace)
            raw_start = market.get("startDate") or event.get("startDate", "")
            if raw_start:
                try:
                    start_time = datetime.fromisoformat(
                        raw_start.replace("Z", "+00:00")
                    ).timestamp()
                    if start_time > time.time() + 60:
                        return None
                except Exception:
                    pass

            return MarketInfo(
                token_id_up=token_up,
                token_id_down=token_down,
                condition_id=cid,
                title=title,
                window=window,
                asset=asset,
                end_time=end_time,
            )
        except Exception as e:
            log.debug(f"Market parse error: {e}")
            return None

    async def _market_discovery_loop(self):
        """Background HTTP polling for market discovery (Option 2).

        Runs independently from WS processing, so HTTP latency doesn't block
        real-time price updates. Fetches every 60s (cache expires every 30s).
        Updates are processed by _market_refresh_loop, not here.
        """
        while self._running:
            try:
                # Fetch metadata (hits cache if fresh ~30s, makes HTTP call when stale)
                await self._fetch_active_markets()
            except Exception as e:
                log.warning(f"Background market discovery error: {e}")

            # Poll every 60s, but cache expires every 30s → HTTP calls ~every 30-60s
            await asyncio.sleep(60)

    async def _market_refresh_loop(self):
        """Refresh market list every 10s, add new markets, prune expired ones.

        With TTL-based caching (30s), HTTP calls happen roughly every 30-60s.
        Cache hits (every 10s when < 30s old) are near-instant.
        """
        while self._running:
            markets = await self._fetch_active_markets()
            now = time.time()

            # Prune markets that have expired
            expired = [
                cid for cid, m in self._markets.items()
                if m.end_time and m.end_time < now
            ]
            for cid in expired:
                m = self._markets.pop(cid)
                self._prices.pop(m.token_id_up, None)
                self._prices.pop(m.token_id_down, None)
                self._market_added_at.pop(cid, None)
            if expired:
                log.info(f"Markets: pruned {len(expired)} expired, {len(self._markets)} remaining")

            # Prune markets that have never received a WS price event after the
            # grace period.  These are markets listed by the Gamma API that are not
            # yet open or have no active order book — subscribing to them wastes WS
            # bandwidth and subscription slots.
            _INACTIVE_GRACE_S = 120
            inactive = [
                cid for cid, m in self._markets.items()
                if cid not in expired
                and now - self._market_added_at.get(cid, now) > _INACTIVE_GRACE_S
                and self._prices.get(m.token_id_up,   PriceState()).last_update == 0
                and self._prices.get(m.token_id_down, PriceState()).last_update == 0
            ]
            for cid in inactive:
                m = self._markets.pop(cid)
                self._prices.pop(m.token_id_up, None)
                self._prices.pop(m.token_id_down, None)
                self._market_added_at.pop(cid, None)
            if inactive:
                log.info(
                    f"Markets: pruned {len(inactive)} inactive (no price data after "
                    f"{_INACTIVE_GRACE_S}s), {len(self._markets)} remaining"
                )

            new_count = 0
            for m in markets:
                if m.condition_id not in self._markets:
                    self._markets[m.condition_id] = m
                    self._market_added_at[m.condition_id] = now
                    if m.token_id_up not in self._prices:
                        self._prices[m.token_id_up] = PriceState()
                    if m.token_id_down not in self._prices:
                        self._prices[m.token_id_down] = PriceState()
                    new_count += 1

            # Count markets with live price data on both sides
            active_count = sum(
                1 for cid, m in self._markets.items()
                if self._prices.get(m.token_id_up,   PriceState()).last_update > 0
                and self._prices.get(m.token_id_down, PriceState()).last_update > 0
            )
            self.stats["markets_tracked"] = len(self._markets)
            self.stats["markets_active"]  = active_count

            needs_resub = new_count > 0 or bool(inactive)
            if new_count:
                log.info(f"Markets: +{new_count} new, {len(self._markets)} tracked "
                         f"({active_count} active)")
            if needs_resub:
                await self._resubscribe()

            # Stale WS watchdog: if we have markets but no price updates for 90s,
            # force-close the WS connection to trigger a fresh reconnect + resubscribe.
            if (self._markets and self._last_ws_msg_at > 0
                    and time.time() - self._last_ws_msg_at > 90):
                log.warning("WS stale: no price updates for 90s — forcing reconnect")
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                self._last_ws_msg_at = time.time()  # reset to avoid tight reconnect loops

            # Reduced polling interval: 10s for market list checks, caching reduces HTTP load
            # With TTL cache (5 min), only ~1 HTTP call per 5 minutes instead of per 60s
            await asyncio.sleep(10)

    # ── WebSocket ─────────────────────────────────────────────────

    async def _ws_loop(self):
        """Maintain WebSocket connection with exponential backoff."""
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(
                    CLOB_WS,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024,  # 10 MB — default 1 MB is too small for full book snapshots
                ) as ws:
                    self._ws = ws
                    backoff = 1
                    await self._subscribe_all(ws)
                    log.info("WebSocket connected, subscribed to price feeds")
                    async for msg in ws:
                        if not msg:
                            continue
                        try:
                            parsed = json.loads(msg)
                        except (json.JSONDecodeError, ValueError):
                            continue
                        await self._handle_message(parsed)
            except Exception as e:
                self.stats["ws_reconnects"] += 1
                log.warning(f"WebSocket error ({e}), reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _subscribe_all(self, ws):
        """Subscribe to all tracked token IDs."""
        token_ids = []
        for m in self._markets.values():
            token_ids.extend([m.token_id_up, m.token_id_down])

        # CLOB WS accepts up to 500 assets per subscription
        for i in range(0, len(token_ids), 500):
            chunk = token_ids[i:i+500]
            await ws.send(json.dumps({
                "type": "subscribe",
                "channel": "market",
                "assets_ids": chunk,
            }))

    async def _resubscribe(self):
        """Re-subscribe after market list update."""
        if self._ws:
            try:
                await self._subscribe_all(self._ws)
            except Exception:
                pass

    async def _handle_message(self, msg):
        """Process incoming WebSocket message."""
        self.stats["price_updates"] += 1
        self._last_ws_msg_at = time.time()

        if isinstance(msg, list):
            for item in msg:
                await self._process_event(item)
        elif isinstance(msg, dict):
            await self._process_event(msg)

    async def _process_event(self, event: dict):
        event_type = event.get("event_type", "")
        asset_id   = event.get("asset_id", "")

        if event_type not in ("book", "price_change", "tick_size_change"):
            return
        if asset_id not in self._prices:
            return

        ps = self._prices[asset_id]

        # Book snapshot (sent immediately on subscribe and on orderbook changes).
        # asks are sorted ascending — asks[0] is the best (lowest) ask.
        if event.get("asks"):
            try:
                # WS sends asks sorted descending (highest price first).
                # Build a sorted-ascending book so depth_up_to() can sum cheaply.
                levels: list = []
                for a in event["asks"]:
                    try:
                        p = float(a["price"])
                        s = float(a.get("size", 0.0))
                        if s > 0:
                            levels.append((p, s))
                    except Exception:
                        pass
                if levels:
                    levels.sort(key=lambda x: x[0])  # ascending by price
                    ps.ask_book = levels
                    ps.ask      = levels[0][0]   # best (lowest) ask
                    ps.ask_size = levels[0][1]   # top-of-book size (for logging)
            except Exception:
                pass
        if event.get("bids"):
            try:
                # Bids are sorted ascending (0.01 → worst); bids[-1] = highest = best bid
                ps.bid = float(event["bids"][-1]["price"])
            except Exception:
                pass

        # price_change events carry a top-level "price" field.
        # Use it as the ask if we haven't received a book snapshot yet.
        if "price" in event and ps.ask == 1.0:
            try:
                ps.ask = float(event["price"])
            except Exception:
                pass

        ps.last_update = time.time()
        self._prices[asset_id] = ps

        # Check for bracket opportunity on any market containing this token
        for m in self._markets.values():
            if asset_id in (m.token_id_up, m.token_id_down):
                await self._check_bracket(m)

    async def _check_bracket(self, m: MarketInfo):
        """Evaluate bracket opportunity for a market."""
        ps_up   = self._prices.get(m.token_id_up)
        ps_down = self._prices.get(m.token_id_down)
        if not ps_up or not ps_down:
            return

        ask_up   = ps_up.ask
        ask_down = ps_down.ask

        # Skip stale prices
        if time.time() - ps_up.last_update > 30 or time.time() - ps_down.last_update > 30:
            return

        # Skip if either side has no active sellers (empty order book).
        # ask defaults to 1.0 on init; a live market with real offers will be < 1.0.
        if ps_up.ask >= 1.0 or ps_down.ask >= 1.0:
            return

        # Skip near-expiry markets (< 3 minutes to close)
        if m.end_time and (m.end_time - time.time()) < 180:
            return

        combined = ask_up + ask_down

        # Compute FOK limit prices first — needed for accurate multi-level depth
        # calculation below and for the near-bracket presign.
        # Limit prices are deterministic from current asks so this is cheap.
        tick       = 0.01
        margin     = STRATEGY.bracket_threshold - combined
        limit_up   = round(ask_up + tick, 2)
        limit_down = round(ask_down + max(margin - tick, 0.0) + STRATEGY.down_extra_ticks * tick, 2)
        # Profitability cap: worst-case (both legs fill at limit) must still be profitable.
        # CRITICAL: limit_up + limit_down MUST remain below 1.0 - fee.
        max_limit_sum = round(1.0 - STRATEGY.taker_fee_pct - tick, 2)
        if limit_up + limit_down > max_limit_sum:
            limit_down = round(max_limit_sum - limit_up, 2)
            limit_down = max(limit_down, ask_down)  # never below current ask

        # Near-bracket hook: when combined is between near_threshold and bracket_threshold,
        # fire on_near_bracket so the trader can pre-warm the cache and pre-sign orders
        # while prices are still moving toward the entry threshold.  Throttled to once
        # per 5s per market so we don't flood the thread pool.
        if (self.on_near_bracket is not None and
                STRATEGY.bracket_threshold <= combined < STRATEGY.near_bracket_threshold):
            last_near = self._recent_near_brackets.get(m.condition_id, 0)
            if time.time() - last_near > 5.0:
                self._recent_near_brackets[m.condition_id] = time.time()
                self.stats["near_brackets_detected"] += 1
                metadata_age_ms = (time.time() - self._metadata_cache_time) * 1000
                # Use total visible ask depth (all levels) for near-bracket sizing.
                nb_depth_up   = sum(s for _, s in ps_up.ask_book)   if ps_up.ask_book   else ps_up.ask_size
                nb_depth_down = sum(s for _, s in ps_down.ask_book) if ps_down.ask_book else ps_down.ask_size
                near_opp = BracketOpportunity(
                    market=m,
                    ask_up=ask_up,
                    ask_down=ask_down,
                    combined_ask=combined,
                    spread=1.0 - combined,
                    gross_profit_usdc=0.0,
                    net_profit_usdc=0.0,
                    detected_at=time.time(),
                    depth_up=nb_depth_up,
                    depth_down=nb_depth_down,
                    bid_up=ps_up.bid,
                    bid_down=ps_down.bid,
                    limit_up=limit_up,
                    limit_down=limit_down,
                    sim_mode=SIM.enabled,
                    metadata_age_ms=metadata_age_ms,
                    ask_book_up=ps_up.ask_book.copy() if ps_up.ask_book else [],
                    ask_book_down=ps_down.ask_book.copy() if ps_down.ask_book else [],
                )
                log.debug(
                    f"NEAR_BRACKET {m.asset} {m.window} | "
                    f"combined={combined:.3f} | metadata_age={metadata_age_ms:.0f}ms"
                )
                self.on_near_bracket(near_opp)

        if combined >= STRATEGY.bracket_threshold:
            return

        # Multi-level depth: count cumulative shares available up to each FOK limit price.
        # A FOK sweeps every resting ask ≤ limit, so this directly measures actual
        # fillable quantity.  Single-level ask_size under-counts when depth spans
        # multiple price levels, causing phantom-depth bracket approvals.
        depth_up   = ps_up.depth_up_to(limit_up)
        depth_down = ps_down.depth_up_to(limit_down)
        max_fillable_down = depth_down * 0.80

        # UP must have comparable depth to DOWN
        if depth_up < max_fillable_down * 0.80:
            log.debug(
                f"DEPTH SKIP {m.asset} {m.window} | "
                f"UP depth={depth_up:.1f} insufficient for DOWN depth={depth_down:.1f}"
            )
            self.stats["brackets_depth_skipped"] = self.stats.get("brackets_depth_skipped", 0) + 1
            return

        # Size bracket to what DOWN can actually provide
        size = STRATEGY.position_size_usdc
        total_spend = size * 2
        max_shares_by_budget = total_spend / combined
        n_shares = min(max_fillable_down, max_shares_by_budget)

        if n_shares < 1.0:  # Minimum viable order size
            log.debug(
                f"DEPTH SKIP {m.asset} {m.window} | "
                f"n_shares={n_shares:.2f} too small (budget limits or thin depth)"
            )
            self.stats["brackets_depth_skipped"] = self.stats.get("brackets_depth_skipped", 0) + 1
            return

        # Throttle: skip if we detected a bracket on this market recently
        last = self._recent_brackets.get(m.condition_id, 0)
        throttle_window = 60.0 / STRATEGY.pause_if_bracket_hz
        if (time.time() - last) < throttle_window:
            self.stats["brackets_throttled"] += 1
            return

        self._recent_brackets[m.condition_id] = time.time()
        self.stats["brackets_detected"] += 1

        # Profitability based on ACTUAL shares we'll fill
        actual_spend = n_shares * (ask_up + ask_down)
        gross = n_shares - actual_spend
        fee   = actual_spend * STRATEGY.taker_fee_pct
        gas   = STRATEGY.gas_fee_live_usdc
        net   = gross - fee - gas
        metadata_age_ms = (time.time() - self._metadata_cache_time) * 1000

        opp = BracketOpportunity(
            market=m,
            ask_up=ask_up,
            ask_down=ask_down,
            combined_ask=combined,
            spread=1.0 - combined,
            gross_profit_usdc=gross,
            net_profit_usdc=net,
            detected_at=time.time(),
            depth_up=depth_up,
            depth_down=depth_down,
            bid_up=ps_up.bid,
            bid_down=ps_down.bid,
            limit_up=limit_up,
            limit_down=limit_down,
            sim_mode=SIM.enabled,
            metadata_age_ms=metadata_age_ms,
            ask_book_up=ps_up.ask_book.copy() if ps_up.ask_book else [],
            ask_book_down=ps_down.ask_book.copy() if ps_down.ask_book else [],
        )

        log.info(
            f"BRACKET {m.asset} {m.window} | "
            f"Up={ask_up:.3f}({depth_up:.1f}) lim={limit_up:.3f} "
            f"Down={ask_down:.3f}({depth_down:.1f}) lim={limit_down:.3f} "
            f"Sum={combined:.3f} Fillable={n_shares:.2f}sh Net=+${net:.3f} "
            f"metadata_age={metadata_age_ms:.0f}ms"
        )
        # Invalidate metadata cache when bracket detected so trader gets fresh data
        self.invalidate_metadata_cache()
        self.on_bracket(opp)
