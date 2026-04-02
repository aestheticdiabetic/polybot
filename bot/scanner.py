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
    ask_up: float   = 1.0
    ask_down: float = 1.0
    bid_up: float   = 0.0
    bid_down: float = 0.0
    last_update: float = 0.0


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
    sim_mode: bool = False


class Scanner:
    def __init__(self, on_bracket: Callable[[BracketOpportunity], None]):
        self.on_bracket = on_bracket
        self._markets: Dict[str, MarketInfo] = {}       # condition_id → MarketInfo
        self._prices:  Dict[str, PriceState] = {}       # token_id → PriceState
        self._recent_brackets: Dict[str, float] = {}   # condition_id → last bracket ts
        self._running = False
        self._ws = None
        self.stats = {
            "markets_tracked": 0,
            "price_updates": 0,
            "brackets_detected": 0,
            "brackets_throttled": 0,
            "ws_reconnects": 0,
        }

    # ── Public API ────────────────────────────────────────────────

    async def start(self):
        self._running = True
        await asyncio.gather(
            self._market_refresh_loop(),
            self._ws_loop(),
        )

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    @property
    def tracked_market_count(self) -> int:
        return len(self._markets)

    # ── Market discovery ─────────────────────────────────────────

    async def _fetch_active_markets(self) -> List[MarketInfo]:
        """Fetch all active Up/Down markets from Gamma API by searching per asset."""
        markets: List[MarketInfo] = []
        seen: set = set()
        url = "https://gamma-api.polymarket.com/markets"
        base_params = {"active": "true", "closed": "false", "limit": 100}
        try:
            async with aiohttp.ClientSession() as session:
                for asset in STRATEGY.target_assets:
                    params = {**base_params, "search": asset}
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        data = await r.json()
                    for m in data:
                        cid = m.get("condition_id", "")
                        if cid in seen:
                            continue
                        info = self._parse_market(m)
                        if info:
                            seen.add(cid)
                            markets.append(info)
        except Exception as e:
            log.error(f"Market fetch failed: {e}")
        log.info(f"Market fetch complete: {len(markets)} up/down markets found across {len(STRATEGY.target_assets)} assets")
        return markets

    # Map various title phrases to canonical window keys
    _WINDOW_PATTERNS = [
        ("15M",  [r"\b15[\s-]?min", r"\b15m\b"]),
        ("1H",   [r"\b1[\s-]?hour", r"\b60[\s-]?min", r"\b1h\b"]),
        ("24H",  [r"\b24[\s-]?hour", r"\b1[\s-]?day", r"\b24h\b"]),
    ]

    def _parse_market(self, m: dict) -> Optional[MarketInfo]:
        """Extract relevant fields from a Gamma API market object."""
        try:
            title = m.get("question", "") or m.get("title", "")
            title_up = title.upper()

            # Must be an up/down style market
            if not any(kw in title_up for kw in ("UP OR DOWN", "HIGHER OR LOWER", "HIGHER OR LOWER", "UP/DOWN")):
                return None

            # Detect window size — try configured keys first, then regex patterns
            window = None
            for w in STRATEGY.target_windows:
                if w in title_up:
                    window = w
                    break
            if not window:
                for key, patterns in self._WINDOW_PATTERNS:
                    if key in STRATEGY.target_windows:
                        if any(re.search(p, title, re.IGNORECASE) for p in patterns):
                            window = key
                            break
            if not window:
                return None

            # Detect asset
            asset = None
            for a in STRATEGY.target_assets:
                if a.upper() in title_up:
                    asset = a
                    break
            if not asset:
                return None

            tokens = m.get("tokens", [])
            if len(tokens) < 2:
                return None

            # Identify Up/Down tokens
            token_up = token_down = None
            for t in tokens:
                outcome = t.get("outcome", "").lower()
                if outcome == "up":
                    token_up = t["token_id"]
                elif outcome == "down":
                    token_down = t["token_id"]

            if not token_up or not token_down:
                return None

            end_time = 0
            if m.get("end_date_iso"):
                from datetime import datetime, timezone
                try:
                    end_time = datetime.fromisoformat(
                        m["end_date_iso"].replace("Z", "+00:00")
                    ).timestamp()
                except Exception:
                    pass

            return MarketInfo(
                token_id_up=token_up,
                token_id_down=token_down,
                condition_id=m.get("condition_id", ""),
                title=title,
                window=window,
                asset=asset,
                end_time=end_time,
            )
        except Exception as e:
            log.debug(f"Market parse error: {e}")
            return None

    async def _market_refresh_loop(self):
        """Refresh market list every 60s, re-subscribe new markets."""
        while self._running:
            markets = await self._fetch_active_markets()
            new_count = 0
            for m in markets:
                if m.condition_id not in self._markets:
                    self._markets[m.condition_id] = m
                    if m.token_id_up not in self._prices:
                        self._prices[m.token_id_up] = PriceState()
                    if m.token_id_down not in self._prices:
                        self._prices[m.token_id_down] = PriceState()
                    new_count += 1

            self.stats["markets_tracked"] = len(self._markets)
            if new_count:
                log.info(f"Markets: +{new_count} new, {len(self._markets)} total tracked")
                await self._resubscribe()

            await asyncio.sleep(60)

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
                ) as ws:
                    self._ws = ws
                    backoff = 1
                    await self._subscribe_all(ws)
                    log.info("WebSocket connected, subscribed to price feeds")
                    async for msg in ws:
                        await self._handle_message(json.loads(msg))
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
        if self._ws and not self._ws.closed:
            await self._subscribe_all(self._ws)

    async def _handle_message(self, msg):
        """Process incoming WebSocket message."""
        self.stats["price_updates"] += 1

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

        # Update ask/bid from orderbook snapshot or price change
        if "asks" in event and event["asks"]:
            try:
                ps.ask_up = float(event["asks"][0]["price"]) if asset_id.endswith("_up") else ps.ask_up
                ps.ask_down = float(event["asks"][0]["price"]) if asset_id.endswith("_down") else ps.ask_down
            except Exception:
                pass

        if "best_ask" in event:
            try:
                ps.ask_up   = float(event["best_ask"])
            except Exception:
                pass

        # Simpler: use last_trade_price as proxy when ask not available
        if "price" in event:
            try:
                price = float(event["price"])
                if ps.ask_up == 1.0:
                    ps.ask_up = price
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

        ask_up   = ps_up.ask_up
        ask_down = ps_down.ask_down

        # Skip stale prices
        if time.time() - ps_up.last_update > 30 or time.time() - ps_down.last_update > 30:
            return

        # Skip near-expiry markets (< 3 minutes to close)
        if m.end_time and (m.end_time - time.time()) < 180:
            return

        combined = ask_up + ask_down
        if combined >= STRATEGY.bracket_threshold:
            return

        # Throttle: skip if we detected a bracket on this market recently
        last = self._recent_brackets.get(m.condition_id, 0)
        throttle_window = 60.0 / STRATEGY.pause_if_bracket_hz
        if (time.time() - last) < throttle_window:
            self.stats["brackets_throttled"] += 1
            return

        self._recent_brackets[m.condition_id] = time.time()
        self.stats["brackets_detected"] += 1

        size = STRATEGY.position_size_usdc
        gross = size * (1.0 - combined)
        fee   = size * 2 * STRATEGY.taker_fee_pct
        net   = gross - fee

        opp = BracketOpportunity(
            market=m,
            ask_up=ask_up,
            ask_down=ask_down,
            combined_ask=combined,
            spread=1.0 - combined,
            gross_profit_usdc=gross,
            net_profit_usdc=net,
            detected_at=time.time(),
            sim_mode=SIM.enabled,
        )

        log.info(
            f"BRACKET {m.asset} {m.window} | "
            f"Up={ask_up:.3f} Down={ask_down:.3f} Sum={combined:.3f} "
            f"Net=+${net:.3f}"
        )
        await asyncio.get_event_loop().run_in_executor(None, self.on_bracket, opp)
