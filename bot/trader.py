"""
trader.py — Order placement, position tracking, and risk controls.
Handles both live trading and simulation mode.
"""
import asyncio
import json
import logging
import math
import random
import re
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
from enum import Enum

from config import (
    CLOB_HOST, PRIVATE_KEY, FUNDER_ADDRESS,
    API_KEY, API_SECRET, API_PASSPHRASE,
    STRATEGY, SIM, TRADE_LOG, MAKER
)
from scanner import BracketOpportunity

log = logging.getLogger("trader")


class TokenMetadataCache:
    """Cache tick-size, fee-rate, and neg-risk for all tokens.

    Fetches metadata once at startup and on first encounter with new tokens.
    Prevents repeated REST calls for the same token metadata.
    """

    def __init__(self, client):
        self._client = client
        self._cache: Dict[str, dict] = {}  # token_id -> {tick_size, fee_rate_bps, neg_risk}
        self._fetching: Dict[str, asyncio.Task] = {}  # in-flight fetches to deduplicate

    async def get_or_fetch(self, token_id: str) -> Optional[dict]:
        """Get metadata for a token, fetching if not cached.

        Returns None if the fetch failed (caller should fall back to REST).
        """
        if token_id in self._cache:
            return self._cache[token_id]

        # If already fetching, wait for that fetch to complete
        if token_id in self._fetching:
            try:
                await self._fetching[token_id]
            except Exception:
                pass
            return self._cache.get(token_id)

        # Start fetch, store task so concurrent callers deduplicate
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._fetch_metadata(token_id))
        self._fetching[token_id] = task

        try:
            await task
        except Exception:
            pass
        finally:
            self._fetching.pop(token_id, None)

        return self._cache.get(token_id)

    async def _fetch_metadata(self, token_id: str) -> None:
        """Fetch tick-size, fee-rate, neg-risk for a token."""
        loop = asyncio.get_running_loop()
        try:
            tick_size, fee_rate_bps, neg_risk = await asyncio.gather(
                loop.run_in_executor(None, self._client.get_tick_size, token_id),
                loop.run_in_executor(None, self._client.get_fee_rate_bps, token_id),
                loop.run_in_executor(None, self._client.get_neg_risk, token_id),
                return_exceptions=False,
            )
            self._cache[token_id] = {
                "tick_size": tick_size,
                "fee_rate_bps": fee_rate_bps,
                "neg_risk": neg_risk,
            }
            log.debug(f"[METADATA] Cached {token_id[:16]}…: tick={tick_size}, fee={fee_rate_bps}bps, neg_risk={neg_risk}")
        except Exception as e:
            log.warning(f"[METADATA] Failed to fetch {token_id[:16]}…: {e}")


def _ask_book_depth_to(ask_book: list, limit_price: float) -> float:
    """Calculate cumulative ask depth up to a limit price from an order book snapshot.

    ask_book is a list of (price, size) tuples sorted by price (ascending).
    Returns the sum of all sizes where price <= limit_price.
    """
    return sum(size for price, size in ask_book if price <= limit_price)


def _clob_valid_shares(target: float, price: float) -> float:
    """Return the largest share count ≤ target such that the CLOB maker-amount
    constraint is satisfied: floor(shares×100)/100 × price must have ≤ 2
    decimal places (i.e. be a whole number of cents).

    py_clob_client floors shares to 2 dp internally (raw_taker_amt), then
    multiplies by price to get the USDC maker amount.  The Polymarket API
    rejects orders where that product has more than 2 decimal places.

    The required share granularity depends on the price fraction.  Treating
    price as a 0.001-tick value (covers 0.01 and 0.1 tick too), the valid
    step is 1000 / gcd(round(price×1000), 1000) share-hundredths.

    Accounts for float representation: py_clob_client computes floor(shares*100)
    internally. Due to IEEE 754, shares like 10.2 are stored as 10.199999...,
    causing floor(shares*100) to give 1019 instead of 1020. This function ensures
    the value py_clob_client ACTUALLY computes still yields a valid maker amount.
    """
    p_int = round(price * 1000)
    step  = 1000 // math.gcd(p_int, 1000)   # divisor for s = floor(shares×100)
    max_s = math.floor(target * 100 + 1e-9)  # epsilon for target float imprecision
    valid_s = (max_s // step) * step
    if valid_s <= 0:
        valid_s = step   # ensure at least one step

    # Predict what py_clob_client will compute (floor of float representation)
    # and verify the resulting maker amount is valid (≤ 2 decimal places).
    p_int_100 = round(price * 100)  # price in cents
    while valid_s > 0:
        shares = valid_s / 100.0
        # What py_clob_client actually computes: floor(shares * 100)
        # Due to float imprecision, this may be valid_s - 1.
        actual_taker = math.floor(shares * 100)
        # Verify maker amount precision: (actual_taker / 100) * price must have ≤ 2dp
        # Check using integer math: actual_taker * p_int_100 must be divisible by 100
        if (actual_taker * p_int_100) % 100 == 0:
            return shares
        valid_s -= step
    return step / 100.0


class OrderStatus(Enum):
    PENDING   = "pending"
    FILLED    = "filled"
    PARTIAL   = "partial"
    CANCELLED = "cancelled"
    FAILED    = "failed"


@dataclass
class Leg:
    token_id: str
    side: str         # "UP" or "DOWN"
    price: float
    size_usdc: float
    shares: float
    order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.PENDING
    fill_price: Optional[float] = None
    placed_at: Optional[float] = None
    filled_at: Optional[float] = None


@dataclass
class Bracket:
    id: str
    market_condition_id: str
    market_title: str
    asset: str
    window: str
    leg_up: Leg
    leg_down: Leg
    detected_spread: float
    expected_net_usdc: float
    actual_net_usdc: Optional[float] = None
    opened_at: float = field(default_factory=time.time)
    closed_at: Optional[float] = None
    status: str = "open"    # open, won, lost, cancelled, partial, stranded
    sim_mode: bool = False
    latency_ms: Optional[float] = None   # time for HTTP order placement
    age_ms: Optional[float] = None       # time from scanner detection to submission
    bid_up: float = 0.0     # bid at detection time, for emergency exit pricing
    bid_down: float = 0.0
    limit_up: float = 0.0   # FOK limit price for UP leg (max profitable price)
    limit_down: float = 0.0 # FOK limit price for DOWN leg
    submitted_shares_up: float = 0.0     # actual shares submitted in order (after revalidation)
    submitted_shares_down: float = 0.0   # actual shares submitted in order (after revalidation)
    depth_up: float = 0.0   # WS-observed depth at detection time (single best-ask level)
    depth_down: float = 0.0 # WS-observed depth at detection time (single best-ask level)
    ask_book_up: list = field(default_factory=list)  # Order book snapshot for UP token at detection
    ask_book_down: list = field(default_factory=list)  # Order book snapshot for DOWN token at detection


class RiskGuard:
    """Enforces all risk limits before orders are placed."""

    def __init__(self):
        self._open_by_market: Dict[str, int] = {}
        self._total_open: int = 0
        self._deployed_usdc: float = 0.0
        # Markets cooling down after a partial fill — maps condition_id → unblock timestamp
        self._partial_fill_cooldown: Dict[str, float] = {}

    def can_open(self, condition_id: str, wallet_balance: float) -> tuple[bool, str]:
        if self._total_open >= STRATEGY.max_concurrent_brackets:
            return False, f"Max concurrent brackets ({STRATEGY.max_concurrent_brackets}) reached"

        if self._open_by_market.get(condition_id, 0) >= STRATEGY.max_brackets_per_market:
            return False, f"Already have bracket open on this market"

        # Block re-entry after a partial fill until the cooldown expires
        unblock_at = self._partial_fill_cooldown.get(condition_id, 0)
        if time.time() < unblock_at:
            remaining = unblock_at - time.time()
            return False, f"Partial fill cooldown: {remaining:.0f}s remaining on this market"

        max_deploy = wallet_balance * STRATEGY.max_wallet_exposure_pct
        cost = STRATEGY.position_size_usdc * 2
        if self._deployed_usdc + cost > max_deploy:
            return False, f"Exposure limit: deployed ${self._deployed_usdc:.0f}, max ${max_deploy:.0f}"

        return True, "ok"

    def open(self, condition_id: str, cost_usdc: float):
        self._open_by_market[condition_id] = self._open_by_market.get(condition_id, 0) + 1
        self._total_open += 1
        self._deployed_usdc += cost_usdc

    def close(self, condition_id: str, cost_usdc: float):
        self._open_by_market[condition_id] = max(0, self._open_by_market.get(condition_id, 0) - 1)
        self._total_open = max(0, self._total_open - 1)
        self._deployed_usdc = max(0.0, self._deployed_usdc - cost_usdc)

    def mark_partial_fill(self, condition_id: str):
        """Block this market from re-entry for partial_fill_cooldown_s seconds."""
        self._partial_fill_cooldown[condition_id] = (
            time.time() + STRATEGY.partial_fill_cooldown_s
        )

    @property
    def total_open(self): return self._total_open
    @property
    def deployed_usdc(self): return self._deployed_usdc


class Trader:
    def __init__(self, state_manager):
        self.state = state_manager
        self.risk  = RiskGuard()
        self._client = None
        self._metadata_cache: Optional[TokenMetadataCache] = None
        self._open_brackets: Dict[str, Bracket] = {}
        self._cancel_tasks: Dict[str, asyncio.Task] = {}
        # Pre-signed orders keyed by condition_id.  Populated by _presign_orders when
        # the scanner fires on_near_bracket; consumed (and cleared) by _live_place.
        self._presigned: Dict[str, dict] = {}
        # Resting DOWN GTC orders posted by maker positioning, keyed by condition_id
        # Value: {order_id, shares, limit_down_maker, posted_at}
        self._pending_down_gtc: Dict[str, dict] = {}
        self._scanner = None  # set via set_scanner() after scanner is created
        self.stats = {
            "brackets_attempted": 0,
            "brackets_stale_skipped": 0,
            "brackets_opened": 0,
            "brackets_won": 0,
            "brackets_lost": 0,
            "brackets_cancelled": 0,
            "emergency_exits_attempted": 0,
            "emergency_exits_succeeded": 0,
            "emergency_exits_failed": 0,
            "total_gross_usdc": 0.0,
            "total_fees_usdc": 0.0,
            "total_net_usdc": 0.0,
            "total_gas_usdc": 0.0,
        }

    async def start(self):
        if not SIM.enabled:
            await self._init_client()
        log.info(f"Trader started — {'SIMULATION' if SIM.enabled else 'LIVE'} mode")
        if not SIM.enabled and MAKER.enabled:
            asyncio.get_running_loop().create_task(self._cleanup_down_gtc())

    async def _init_client(self):
        """Initialise py-clob-client with metadata cache."""
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            creds = ApiCreds(
                api_key=API_KEY,
                api_secret=API_SECRET,
                api_passphrase=API_PASSPHRASE,
            )
            self._client = ClobClient(
                host=CLOB_HOST,
                chain_id=137,
                key=PRIVATE_KEY,
                creds=creds,
                signature_type=2,
                funder=FUNDER_ADDRESS,
            )
            log.info("CLOB client initialised")

            # Initialize metadata cache and monkey-patch client methods
            # to use cached metadata instead of making REST calls
            self._metadata_cache = TokenMetadataCache(self._client)
            self._patch_client_metadata_methods()

        except Exception as e:
            log.error(f"Failed to init CLOB client: {e}")
            raise

    def _patch_client_metadata_methods(self) -> None:
        """Monkey-patch client's metadata methods to use cache-first approach.

        This intercepts get_tick_size, get_fee_rate_bps, and get_neg_risk calls
        to check the cache first before making REST calls. The cached values are
        fetched asynchronously to avoid blocking order creation.
        """
        if self._client is None or self._metadata_cache is None:
            return

        original_tick_size = self._client.get_tick_size
        original_fee_rate = self._client.get_fee_rate_bps
        original_neg_risk = self._client.get_neg_risk

        def get_tick_size_cached(token_id: str) -> float:
            """Return cached tick-size or fetch synchronously as fallback."""
            if token_id in self._metadata_cache._cache:
                return self._metadata_cache._cache[token_id]["tick_size"]
            # Fallback to original (will make REST call)
            return original_tick_size(token_id)

        def get_fee_rate_bps_cached(token_id: str) -> int:
            """Return cached fee-rate, or fetch via REST and write back to cache."""
            cached = self._metadata_cache._cache.get(token_id)
            if cached is not None and "fee_rate_bps" in cached:
                return cached["fee_rate_bps"]
            result = original_fee_rate(token_id)
            # Write back so subsequent calls are served from our cache (no REST)
            if token_id in self._metadata_cache._cache:
                self._metadata_cache._cache[token_id]["fee_rate_bps"] = result
            else:
                self._metadata_cache._cache[token_id] = {"fee_rate_bps": result}
            log.debug(f"[METADATA] fee_rate_bps={result} fetched and cached for {token_id[:16]}…")
            return result

        def get_neg_risk_cached(token_id: str) -> dict:
            """Return cached neg-risk or fetch synchronously as fallback."""
            if token_id in self._metadata_cache._cache:
                return self._metadata_cache._cache[token_id]["neg_risk"]
            return original_neg_risk(token_id)

        self._client.get_tick_size = get_tick_size_cached
        self._client.get_fee_rate_bps = get_fee_rate_bps_cached
        self._client.get_neg_risk = get_neg_risk_cached

        log.info("Client metadata methods patched to use cache")

    def set_scanner(self, scanner) -> None:
        """Set scanner reference for live WS price state lookups at order submission."""
        self._scanner = scanner

    def on_new_markets(self, markets) -> None:
        """Called by scanner when new markets are discovered.

        Populates the metadata cache synchronously from Gamma API data already
        embedded in MarketInfo — zero REST calls required.
        """
        if self._metadata_cache is None:
            return
        count = 0
        for m in markets:
            # fee_rate_bps intentionally omitted: Gamma API does not reliably expose
            # the CLOB taker fee. The monkey-patched get_fee_rate_bps will fetch it
            # via REST on first use and write it back here so subsequent calls are cached.
            entry = {"tick_size": m.tick_size, "neg_risk": m.neg_risk}
            for token_id in (m.token_id_up, m.token_id_down):
                if token_id not in self._metadata_cache._cache:
                    self._metadata_cache._cache[token_id] = entry
                    count += 1
        if count:
            log.debug(f"[METADATA] Pre-populated {count} tokens (tick_size+neg_risk) from Gamma API (0 REST calls)")

    async def _ensure_token_metadata(self, token_id_up: str, token_id_down: str, wait: bool = False) -> None:
        """Fetch and cache metadata for token pair if not already cached.

        Args:
            token_id_up: UP token ID
            token_id_down: DOWN token ID
            wait: If True, wait for metadata to be cached before returning.
                  If False, spawn background tasks and return immediately.
        """
        if self._metadata_cache is None:
            return

        if wait:
            # Wait for both tokens to be cached
            await asyncio.gather(
                self._metadata_cache.get_or_fetch(token_id_up),
                self._metadata_cache.get_or_fetch(token_id_down),
                return_exceptions=True,
            )
        else:
            # Fire and forget in background
            loop = asyncio.get_running_loop()
            for token_id in [token_id_up, token_id_down]:
                if token_id not in self._metadata_cache._cache:
                    loop.create_task(self._metadata_cache.get_or_fetch(token_id))

    # ── Main entry point ─────────────────────────────────────────

    def on_bracket(self, opp: BracketOpportunity):
        """Called by scanner when a bracket opportunity is found."""
        if not SIM.enabled:
            # Pre-fetch metadata for this market in background (fire-and-forget)
            asyncio.get_running_loop().create_task(
                self._ensure_token_metadata(opp.market.token_id_up, opp.market.token_id_down, wait=False)
            )
        asyncio.get_running_loop().create_task(self._handle_opportunity(opp))

    def on_near_bracket(self, opp: BracketOpportunity):
        """Called by scanner when combined ask is within near_bracket_threshold.

        Pre-fetches metadata for this market (waiting briefly if needed), then:
        1. Pre-sign both orders (uses cached metadata)
        2. Post a resting GTC DOWN order if maker positioning is enabled
        """
        if not SIM.enabled and self._client is not None:
            asyncio.get_running_loop().create_task(
                self._presign_and_post(opp)
            )

    async def _presign_and_post(self, opp: BracketOpportunity) -> None:
        """Ensure metadata is cached, then presign and post GTC."""
        # Wait briefly for metadata to be cached (usually <50ms)
        await self._ensure_token_metadata(opp.market.token_id_up, opp.market.token_id_down, wait=True)

        # Now presign and post GTC (metadata cache is populated)
        await self._presign_orders(opp)
        if MAKER.enabled:
            await self._post_down_maker_gtc(opp)

    async def _presign_orders(self, opp: BracketOpportunity) -> None:
        """Pre-sign both legs for a near-threshold opportunity.

        Pre-signed orders are stored in _presigned[condition_id] and consumed by
        _live_place when the real threshold is crossed.  They expire after 8s because
        prices move significantly beyond that window, making precomputed limits invalid.

        Limit prices are set 1 tick above the near-threshold ask so the signed
        orders remain valid even if prices drift slightly upward before the actual
        threshold crossing.  FOK still executes at the real market price (≤ limit).
        """
        cid = opp.market.condition_id

        # Don't re-sign if we already have a fresh pre-sign for this market
        # AND current asks are still within the presigned limits.
        # If prices have moved (e.g. ask_down rose above limit_down), the presign
        # is invalid and we need to re-sign with updated limits immediately.
        existing = self._presigned.get(cid)
        if (existing
                and time.time() - existing["ts"] < 10.0
                and opp.ask_up <= existing["limit_up"]
                and opp.ask_down <= existing["limit_down"]):
            return

        # Limits: match the scanner's limit calculation so presigned orders have the
        # same sweep room as freshly-signed ones.  Previously presigned DOWN was only
        # +0.01 while the scanner computes +0.05 (down_extra_ticks=5) — that caused
        # presigned FOK failures on thin books that fresh signing would have swept.
        tick     = 0.01
        combined = opp.ask_up + opp.ask_down
        margin   = STRATEGY.bracket_threshold - combined   # may be ≤ 0 at near-threshold
        limit_up   = round(opp.ask_up   + tick, 2)
        # Use max(margin, 0) so DOWN always gets at least down_extra_ticks of headroom
        # even when presigning at near-bracket (where margin is negative).  Without this,
        # DOWN limit collapses to ask_down+0.05 and is easily breached before the bracket
        # actually fires — causing presign rejection and forcing expensive fresh signing.
        limit_down = round(opp.ask_down + max(margin, 0.0) + STRATEGY.down_extra_ticks * tick, 2)
        # Profitability cap: must match scanner's invariant — limit_up + limit_down must
        # stay below 1.0 - fee so a worst-case fill (both at limit) is still profitable.
        max_limit_sum = round(1.0 - STRATEGY.taker_fee_pct - tick, 2)
        if limit_up + limit_down > max_limit_sum:
            limit_down = round(max_limit_sum - limit_up, 2)
            limit_down = max(limit_down, opp.ask_down)

        n_shares   = (STRATEGY.position_size_usdc * 2) / combined
        shares_up  = _clob_valid_shares(n_shares, limit_up)
        shares_dn  = _clob_valid_shares(n_shares, limit_down)

        try:
            from py_clob_client.clob_types import OrderArgs
            loop = asyncio.get_running_loop()
            signed_up, signed_dn = await asyncio.gather(
                loop.run_in_executor(
                    None, self._client.create_order,
                    OrderArgs(token_id=opp.market.token_id_up,
                              price=limit_up, size=shares_up, side="BUY"),
                ),
                loop.run_in_executor(
                    None, self._client.create_order,
                    OrderArgs(token_id=opp.market.token_id_down,
                              price=limit_down, size=shares_dn, side="BUY"),
                ),
                return_exceptions=True,
            )
            if isinstance(signed_up, Exception) or isinstance(signed_dn, Exception):
                log.debug(f"[PRESIGN] {opp.market.asset} {opp.market.window} failed: "
                          f"{signed_up if isinstance(signed_up, Exception) else signed_dn}")
                return

            self._presigned[cid] = {
                "signed_up":  signed_up,
                "signed_dn":  signed_dn,
                "limit_up":   limit_up,
                "limit_down": limit_down,
                "shares_up":  shares_up,
                "shares_dn":  shares_dn,
                "ask_up":     opp.ask_up,
                "ask_dn":     opp.ask_down,
                "ts":         time.time(),
            }
            log.info(
                f"[PRESIGN] {opp.market.asset} {opp.market.window} ready | "
                f"lim_up={limit_up:.3f} lim_dn={limit_down:.3f} sh={shares_up}/{shares_dn} | "
                f"book_age={opp.metadata_age_ms:.0f}ms"
            )
        except Exception as e:
            log.debug(f"[PRESIGN] {opp.market.asset} {opp.market.window} error: {e}")

    async def _post_down_maker_gtc(self, opp: BracketOpportunity) -> None:
        """Post a resting GTC DOWN order at a price that locks in profitability if filled.

        Called by on_near_bracket when combined ask is near threshold (0.985-0.98).
        The DOWN order rests on the book and gains queue priority while we wait for the
        actual bracket threshold to fire. If it fills, we immediately post UP.
        """
        cid = opp.market.condition_id

        # Check if we already have a pending DOWN GTC for this market
        if cid in self._pending_down_gtc:
            existing = self._pending_down_gtc[cid]
            if time.time() - existing["posted_at"] < 5.0:
                return  # Still fresh, don't re-post
            # Old pending DOWN GTC — fall through and post a new one

        # Skip if current DOWN depth is already healthy relative to our target size
        depth_check = opp.depth_down / ((STRATEGY.position_size_usdc * 2) / opp.combined_ask)
        if depth_check >= MAKER.min_down_depth_for_maker_x:
            log.debug(
                f"[MAKER] {opp.market.asset} {opp.market.window} | DOWN depth={opp.depth_down:.1f} "
                f"is {depth_check:.1f}x our size — skipping maker GTC"
            )
            return

        # Limit price for maker DOWN: such that DOWN fill + current UP ask = bracket_threshold
        limit_down_maker = round(
            STRATEGY.bracket_threshold - opp.ask_up + MAKER.maker_margin_pct, 2
        )
        limit_down_maker = max(limit_down_maker, opp.ask_down)  # never below current ask

        # Size: use same sizing as the bracket would use
        n_shares = (STRATEGY.position_size_usdc * 2) / opp.combined_ask
        shares_down = _clob_valid_shares(n_shares, limit_down_maker)

        if shares_down < 1.0:
            log.debug(f"[MAKER] {opp.market.asset} {opp.market.window} | shares too small")
            return

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            loop = asyncio.get_running_loop()

            # Post the GTC
            signed_down = await loop.run_in_executor(
                None, self._client.create_order,
                OrderArgs(
                    token_id=opp.market.token_id_down,
                    price=limit_down_maker,
                    size=shares_down,
                    side="BUY",
                ),
            )

            resp = await loop.run_in_executor(
                None, self._client.post_order,
                signed_down, OrderType.GTC, False  # GTC, not FOK
            )

            order_id = (resp.get("orderID") or "") if isinstance(resp, dict) else ""
            status = (resp.get("status") or "").lower() if isinstance(resp, dict) else ""

            if status == "open" and order_id:
                self._pending_down_gtc[cid] = {
                    "order_id": order_id,
                    "shares": shares_down,
                    "limit_down_maker": limit_down_maker,
                    "posted_at": time.time(),
                }
                log.info(
                    f"[MAKER] {opp.market.asset} {opp.market.window} | "
                    f"DOWN GTC posted id={order_id} lim={limit_down_maker:.3f} sh={shares_down:.2f}"
                )
            else:
                log.debug(
                    f"[MAKER] {opp.market.asset} {opp.market.window} | "
                    f"DOWN GTC post failed: status={status!r}"
                )
        except Exception as e:
            log.debug(
                f"[MAKER] {opp.market.asset} {opp.market.window} | DOWN GTC exception: {e}"
            )

    async def _handle_opportunity(self, opp: BracketOpportunity):
        self.stats["brackets_attempted"] += 1

        # Reject if opportunity is stale — prices may have moved since detection
        age_ms = (time.time() - opp.detected_at) * 1000
        if age_ms > 500:
            log.debug(f"Stale opportunity: {age_ms:.0f}ms old, skipping")
            self.stats["brackets_stale_skipped"] += 1
            return

        balance = self.state.get_balance()
        ok, reason = self.risk.can_open(opp.market.condition_id, balance)
        if not ok:
            log.debug(f"Risk block: {reason}")
            return

        size = STRATEGY.position_size_usdc
        # Equal-shares sizing: buy the same number of shares on each leg so
        # payout is identical regardless of which side wins.
        # total_budget = 2*size; n_shares = total_budget / combined
        combined = opp.ask_up + opp.ask_down
        total_budget = size * 2
        n_shares = total_budget / combined

        # Depth-first sizing: cap shares to 80% of visible depth on each leg,
        # matching the scanner's sizing logic.  The previous 50% guard only triggered
        # when depth was severely thin, causing the trader to submit more shares than
        # the scanner computed as fillable (e.g. scanner: 9.60sh, trader: 10.50sh),
        # which caused systematic FOK failures on orders the scanner had approved.
        if opp.depth_up > 0:
            n_shares = min(n_shares, opp.depth_up * 0.80)
        if opp.depth_down > 0:
            n_shares = min(n_shares, opp.depth_down * 0.80)
        # Skip if the position is now too small to be worth the transaction cost.
        # risk.open() hasn't been called yet so no cleanup needed — just return.
        if n_shares * combined / 2 < STRATEGY.min_position_size_usdc:
            log.debug(
                f"SIZE SKIP after depth cap: effective=${n_shares * combined / 2:.2f} "
                f"< min=${STRATEGY.min_position_size_usdc}"
            )
            return

        # CLOB constraint: floor(shares×100)/100 × price must have ≤ 2dp
        # (the Polymarket API rejects maker amounts with > 2 decimal places).
        # _clob_valid_shares() finds the largest valid share count ≤ n_shares.
        shares_up   = _clob_valid_shares(n_shares, opp.ask_up)
        shares_down = _clob_valid_shares(n_shares, opp.ask_down)
        size_up   = round(shares_up   * opp.ask_up,   2)
        size_down = round(shares_down * opp.ask_down, 2)

        bracket = Bracket(
            id=str(uuid.uuid4())[:8],
            market_condition_id=opp.market.condition_id,
            market_title=opp.market.title,
            asset=opp.market.asset,
            window=opp.market.window,
            leg_up=Leg(
                token_id=opp.market.token_id_up,
                side="UP",
                price=opp.ask_up,
                size_usdc=size_up,
                shares=shares_up,
            ),
            leg_down=Leg(
                token_id=opp.market.token_id_down,
                side="DOWN",
                price=opp.ask_down,
                size_usdc=size_down,
                shares=shares_down,
            ),
            detected_spread=opp.spread,
            expected_net_usdc=opp.net_profit_usdc,
            sim_mode=SIM.enabled,
            bid_up=opp.bid_up,
            bid_down=opp.bid_down,
            limit_up=opp.limit_up,
            limit_down=opp.limit_down,
        )

        bracket.age_ms = age_ms
        bracket.depth_up   = opp.depth_up
        bracket.depth_down = opp.depth_down
        bracket.ask_book_up = opp.ask_book_up
        bracket.ask_book_down = opp.ask_book_down
        self.risk.open(opp.market.condition_id, total_budget)
        self._open_brackets[bracket.id] = bracket

        t0 = time.time()
        try:
            success = await asyncio.wait_for(self._place_bracket(bracket), timeout=10.0)
        except asyncio.TimeoutError:
            log.warning(f"[{bracket.id}] Order placement timed out after 10s")
            self.risk.close(opp.market.condition_id, total_budget)
            del self._open_brackets[bracket.id]
            return
        bracket.latency_ms = (time.time() - t0) * 1000

        if success:
            self.stats["brackets_opened"] += 1
            self.state.add_bracket(bracket)
            if not SIM.enabled and STRATEGY.order_type == "GTC":
                # GTC only: poll for fills; FOK results are resolved inline in _live_place
                task = asyncio.get_running_loop().create_task(
                    self._poll_fills(bracket)
                )
                self._cancel_tasks[bracket.id] = task
            self._log_trade(bracket, "opened")
        else:
            if bracket.status == "partial_fill":
                # Emergency exit is running as a background task and owns cleanup.
                # Don't close risk here — the cooldown is already set and risk will
                # be released when _emergency_exit finishes.
                pass
            else:
                # Both legs cancelled — no exposure, release risk immediately.
                self.risk.close(opp.market.condition_id, size * 2)
                self._open_brackets.pop(bracket.id, None)

    # ── Order placement ──────────────────────────────────────────

    async def _place_bracket(self, b: Bracket) -> bool:
        """Place both legs. Returns True if both submitted."""
        if SIM.enabled:
            return await self._sim_place(b)
        return await self._live_place(b)

    async def _live_place(self, b: Bracket) -> bool:
        """Place both legs: sequential or parallel depending on order book depth.

        Hybrid execution strategy:
        - Parallel (low latency, ~360ms): Submit both legs simultaneously when both sides
          have sufficient depth (>= 150% of requested shares). Reduces latency from ~723ms
          but increases partial fill risk window from ~0ms to ~5-10ms.
        - Sequential (high safety, ~723ms): POST DOWN first, then UP only if DOWN fills.
          Eliminates ~90% of partial fills but incurs full latency penalty.

        Decision logic: if STRATEGY.parallel_submission_enabled and both sides have
        depth >= (shares × threshold_multiplier), use parallel; otherwise sequential.

        FOK: each order either fills completely or is auto-cancelled — no open exposure.

        Returns True only if BOTH legs filled. Single-leg fills trigger emergency exit.
        """
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType, PostOrdersArgs
            loop = asyncio.get_running_loop()

            order_type = (
                OrderType.FOK if STRATEGY.order_type == "FOK" else OrderType.GTC
            )

            # Refresh order book snapshots from live WS data before any depth calculations.
            # The snapshots in b.ask_book_up/down were captured at detection time; by the
            # time we reach here they may be stale. Scanner._prices is updated continuously
            # by the WS — pull the latest copy so depth checks reflect current liquidity.
            if self._scanner is not None:
                live_up = self._scanner.get_price_state(b.leg_up.token_id)
                live_dn = self._scanner.get_price_state(b.leg_down.token_id)
                if live_up is not None and live_up.ask_book:
                    b.ask_book_up = live_up.ask_book.copy()
                if live_dn is not None and live_dn.ask_book:
                    b.ask_book_down = live_dn.ask_book.copy()

            # Use limit prices (not just best ask) so the FOK can sweep through
            # multiple price levels up to the max profitable price.
            # Shares are re-derived for each limit price to satisfy CLOB precision.
            limit_up   = b.limit_up   or b.leg_up.price
            limit_down = b.limit_down or b.leg_down.price
            shares_up   = _clob_valid_shares(b.leg_up.shares,   limit_up)
            shares_down = _clob_valid_shares(b.leg_down.shares, limit_down)

            # Use pre-signed orders if available, fresh (< 8s), and still valid
            # (current ask hasn't risen above the pre-signed limit on either leg).
            # Presigned limit prices remain valid across depth changes; however, presigned
            # shares were calculated with budget-first sizing and may exceed current depth.
            # Recalculate shares based on actual depth at submission time (depth-first).
            presigned = self._presigned.pop(b.market_condition_id, None)
            used_presigned = False
            presigned_depth_rejected = False
            # Presigned orders expire after 8 seconds. Beyond that, prices have moved
            # too much and the precomputed limits are invalid. Force fresh signing.
            if (presigned is not None
                    and time.time() - presigned["ts"] < 8.0
                    and b.leg_up.price   <= presigned["limit_up"]
                    and b.leg_down.price <= presigned["limit_down"]):
                signed_up   = presigned["signed_up"]
                signed_dn   = presigned["signed_dn"]
                limit_up    = presigned["limit_up"]
                limit_down  = presigned["limit_down"]
                # Recalculate shares based on CURRENT order book depth from snapshots.
                # Presigned shares assumed certain depth; actual depth may have shrunk.
                # Use depth-first sizing: limit to min(presigned_size, actual_fillable_depth).
                current_depth_down = _ask_book_depth_to(b.ask_book_down, limit_down)
                current_depth_up = _ask_book_depth_to(b.ask_book_up, limit_up)
                max_fillable_down = current_depth_down * 0.80  # 80% safety margin
                max_fillable_up   = current_depth_up * 0.80
                # Ensure balanced depth on both sides (80% rule)
                if max_fillable_up < max_fillable_down * 0.80:
                    # UP depth too shallow relative to DOWN — reject and re-sign fresh
                    presigned_depth_rejected = True
                    presigned = None
                else:
                    shares_up   = _clob_valid_shares(min(presigned["shares_up"], max_fillable_up), limit_up)
                    shares_down = _clob_valid_shares(min(presigned["shares_dn"], max_fillable_down), limit_down)
                    used_presigned = True
                    presign_age = (time.time() - presigned['ts']) * 1000
                    log.info(f"[{b.id}] Using pre-signed orders (age={presign_age:.0f}ms) "
                             f"| shares resized for current depth: up={shares_up:.2f} (was {presigned['shares_up']:.2f}), "
                             f"dn={shares_down:.2f} (was {presigned['shares_dn']:.2f})")
            if not used_presigned:
                # Presigned was either not available, expired, price-changed, or rejected due to depth
                if presigned_depth_rejected:
                    log.info(
                        f"[{b.id}] Pre-signed depth-rejected — signing fresh "
                        f"(up={b.depth_up:.1f} insufficient vs down={b.depth_down:.1f})"
                    )
                elif presigned is not None:
                    # Presigned failed outer validation (expired or price changed)
                    log.info(
                        f"[{b.id}] Pre-signed stale/invalid — signing fresh "
                        f"(age={(time.time()-presigned['ts'])*1000:.0f}ms "
                        f"ask_up={b.leg_up.price:.3f}>lim={presigned['limit_up']:.3f}? "
                        f"ask_dn={b.leg_down.price:.3f}>lim={presigned['limit_down']:.3f}?)"
                    )
                else:
                    log.debug(
                        f"[{b.id}] No presign — signing fresh "
                        f"(near-bracket fired on a different {b.asset} {b.window} window)"
                    )
                # Sign both orders concurrently (ECDSA crypto, pure CPU, no I/O)
                signed_up, signed_dn = await asyncio.gather(
                    loop.run_in_executor(
                        None, self._client.create_order,
                        OrderArgs(token_id=b.leg_up.token_id, price=limit_up,
                                  size=shares_up, side="BUY"),
                    ),
                    loop.run_in_executor(
                        None, self._client.create_order,
                        OrderArgs(token_id=b.leg_down.token_id, price=limit_down,
                                  size=shares_down, side="BUY"),
                    ),
                )

            # Track actual submitted shares (after limit price revalidation)
            # for use in emergency exit. These may differ from b.leg_up/down.shares
            # if the limit prices differ from ask prices.
            b.submitted_shares_up = shares_up
            b.submitted_shares_down = shares_down

            def _parse(resp):
                if not isinstance(resp, dict):
                    return "", "", "", ""
                return (
                    (resp.get("orderID") or ""),
                    (resp.get("status") or "").lower(),
                    (resp.get("errorCode") or resp.get("error_code") or ""),
                    (resp.get("message") or resp.get("errorMsg") or ""),
                )

            # ── Check for resting DOWN maker GTC from on_near_bracket ──
            cid = b.market_condition_id
            down_gtc_filled = False
            down_gtc_shares = 0.0
            down_gtc_limit = 0.0

            if cid in self._pending_down_gtc:
                pending = self._pending_down_gtc[cid]
                gtc_order_id = pending["order_id"]

                # Fetch fresh order status from REST
                try:
                    order_status = await loop.run_in_executor(
                        None, self._client.get_order, gtc_order_id
                    )
                    if isinstance(order_status, dict):
                        gtc_status = (order_status.get("status") or "").lower()
                        if gtc_status == "filled" or gtc_status == "matched":
                            down_gtc_filled = True
                            down_gtc_shares = pending["shares"]
                            down_gtc_limit = pending["limit_down_maker"]
                            log.info(
                                f"[{b.id}] DOWN maker GTC filled: id={gtc_order_id} "
                                f"sh={down_gtc_shares:.2f} lim={down_gtc_limit:.3f}"
                            )
                        elif gtc_status == "open" or gtc_status == "pending":
                            # Still resting — cancel it before submitting new orders
                            try:
                                await loop.run_in_executor(
                                    None, self._client.cancel_order, gtc_order_id
                                )
                                log.debug(f"[{b.id}] Cancelled pending DOWN GTC {gtc_order_id}")
                            except:
                                pass
                except Exception as e:
                    log.debug(f"[{b.id}] Error checking DOWN GTC status: {e}")

                # Clear the pending dict regardless of outcome
                del self._pending_down_gtc[cid]

            # ── Decide: parallel or sequential submission ──

            # Calculate actual available depth from order book snapshots (not stale WS-at-detection time)
            current_depth_down_check = _ask_book_depth_to(b.ask_book_down, limit_down)
            current_depth_up_check = _ask_book_depth_to(b.ask_book_up, limit_up)

            use_parallel = (
                STRATEGY.parallel_submission_enabled
                and current_depth_up_check >= (shares_up * STRATEGY.parallel_depth_threshold_multiplier)
                and current_depth_down_check >= (shares_down * STRATEGY.parallel_depth_threshold_multiplier)
            )

            # No need for pre-flight REST check — we have current depth from ask_book snapshots

            if down_gtc_filled:
                # DOWN maker GTC filled — submit UP with the same share count
                log.info(
                    f"[{b.id}] Submitting UP to match DOWN GTC fill (sh={down_gtc_shares:.2f})"
                )
                shares_up = _clob_valid_shares(down_gtc_shares, limit_up)

                try:
                    signed_up = await loop.run_in_executor(
                        None, self._client.create_order,
                        OrderArgs(token_id=b.leg_up.token_id, price=limit_up,
                                  size=shares_up, side="BUY"),
                    )
                    resp_up = await loop.run_in_executor(
                        None, self._client.post_order, signed_up, order_type, False
                    )
                    up_id, up_status, up_err, up_msg = _parse(resp_up)

                    # Mark both legs as filled/matched
                    now = time.time()
                    b.leg_down.order_id = pending["order_id"]
                    b.leg_down.status = OrderStatus.FILLED
                    b.leg_down.fill_price = down_gtc_limit
                    b.leg_down.shares = down_gtc_shares
                    b.leg_down.filled_at = now
                    b.leg_down.placed_at = now

                    if up_status == "matched":
                        b.leg_up.order_id = up_id
                        b.leg_up.status = OrderStatus.FILLED
                        b.leg_up.fill_price = limit_up
                        b.leg_up.shares = shares_up
                        b.leg_up.filled_at = now
                        b.leg_up.placed_at = now

                        b.status = "matched"
                        log.info(
                            f"[{b.id}] Maker DOWN + FOK UP both filled | "
                            f"DOWN={down_gtc_shares:.2f}@{down_gtc_limit:.3f} "
                            f"UP={shares_up:.2f}@{limit_up:.3f}"
                        )
                        await self._live_resolve(b)
                        return True
                    else:
                        # DOWN filled but UP failed — emergency exit (same as normal partial fill)
                        log.warning(f"[{b.id}] DOWN GTC filled but UP FOK failed — emergency exit")
                        b.leg_up.status = OrderStatus.CANCELLED
                        b.status = "partial_fill"
                        self._log_trade(b, "partial_fill")
                        self.risk.mark_partial_fill(cid)
                        asyncio.get_running_loop().create_task(self._emergency_exit(b))
                        return True

                except Exception as e:
                    log.warning(f"[{b.id}] Exception submitting UP after DOWN GTC fill: {e}")
                    # Mark DOWN as filled for emergency exit
                    b.leg_down.status = OrderStatus.FILLED
                    b.status = "partial_fill"
                    self._log_trade(b, "partial_fill")
                    self.risk.mark_partial_fill(cid)
                    asyncio.get_running_loop().create_task(self._emergency_exit(b))
                    return True

            if use_parallel:
                dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg = (
                    await self._submit_orders_parallel(b, signed_up, signed_dn, limit_up,
                                                       limit_down, shares_up, shares_down,
                                                       order_type, loop, _parse)
                )
                dn_filled = dn_status == "matched"
                up_filled = up_status == "matched"
            else:
                dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg = (
                    await self._submit_orders_sequential(b, signed_up, signed_dn, limit_up,
                                                         limit_down, shares_up, shares_down,
                                                         order_type, loop, _parse)
                )
                dn_filled = dn_status == "matched"
                up_filled = up_status == "matched"

                # In sequential mode, if DOWN missed, attempt retry with reduced size
                # to handle race conditions where depth has changed slightly.
                if not dn_filled and dn_status != "matched":
                    retry_result = await self._retry_down_reduced(
                        b, signed_up, limit_up, limit_down, order_type, loop
                    )
                    if retry_result:
                        # Retry succeeded — already logged and resolved
                        return True
                    # Retry failed — continue to result handling with original miss state

            # ── Result handling (identical for both paths) ──

            now = time.time()
            for leg, oid, filled in (
                (b.leg_up,   up_id, up_filled),
                (b.leg_down, dn_id, dn_filled),
            ):
                leg.order_id = oid or None
                leg.placed_at = now
                if filled:
                    leg.status     = OrderStatus.FILLED
                    leg.filled_at  = now
                    leg.fill_price = leg.price
                else:
                    leg.status = OrderStatus.CANCELLED

            if up_filled and dn_filled:
                log.info(
                    f"[{b.id}] Both legs filled (FOK){'[presigned]' if used_presigned else ''} "
                    f"{'[parallel]' if use_parallel else '[sequential]'} | "
                    f"Up={up_id} Down={dn_id} | "
                    f"ask={b.leg_up.price}/lim={limit_up} "
                    f"ask={b.leg_down.price}/lim={limit_down} "
                    f"shares_up={shares_up} shares_dn={shares_down}"
                )
                await self._live_resolve(b)
                return True

            if up_filled or dn_filled:
                filled_side = "UP" if up_filled else "DOWN"
                missed_side = "DOWN" if up_filled else "UP"
                missed_err  = dn_err  if up_filled else up_err
                missed_msg  = dn_msg  if up_filled else up_msg
                log.warning(
                    f"[{b.id}] Partial fill — {filled_side} filled, {missed_side} cancelled "
                    f"{'[parallel]' if use_parallel else '[sequential]'} "
                    f"[err={missed_err or 'none'} msg={missed_msg or 'none'}] | "
                    f"up: ask={b.leg_up.price:.3f} lim={limit_up:.3f} sh={shares_up:.4f} | "
                    f"dn: ask={b.leg_down.price:.3f} lim={limit_down:.3f} sh={shares_down:.4f} | "
                    f"age={b.age_ms:.0f}ms. Initiating emergency exit."
                )
                b.status = "partial_fill"
                self._log_trade(b, "partial_fill")
                self.risk.mark_partial_fill(b.market_condition_id)
                asyncio.get_running_loop().create_task(self._emergency_exit(b))
                return False

            log.info(
                f"[{b.id}] Unexpected state: no fills {'[parallel]' if use_parallel else '[sequential]'} | "
                f"up: status={up_status!r} dn: status={dn_status!r} age={b.age_ms:.0f}ms"
            )
            return False

        except Exception as e:
            log.error(f"[{b.id}] Order placement failed: {e}")
            return False

    async def _submit_orders_parallel(
        self, b: Bracket, signed_up, signed_dn, limit_up: float, limit_down: float,
        shares_up: float, shares_down: float, order_type, loop, _parse
    ) -> tuple:
        """Submit both DOWN and UP orders concurrently.

        Returns: (dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg)

        Parallel submission reduces latency from ~723ms → ~360ms by posting both legs
        simultaneously. Risk: if one fills and the other doesn't, partial fill window
        is ~5-10ms (versus ~0ms with sequential). Only attempted when order book depth
        is sufficient (>= 150% of requested shares on each side).
        """
        resp_dn, resp_up = await asyncio.gather(
            loop.run_in_executor(
                None, self._client.post_order,
                signed_dn, order_type, False
            ),
            loop.run_in_executor(
                None, self._client.post_order,
                signed_up, order_type, False
            ),
            return_exceptions=True,
        )

        if isinstance(resp_dn, Exception):
            dn_id, dn_status, dn_err, dn_msg = "", "cancelled", "exception", str(resp_dn)
        else:
            dn_id, dn_status, dn_err, dn_msg = _parse(resp_dn)

        if isinstance(resp_up, Exception):
            up_id, up_status, up_err, up_msg = "", "cancelled", "exception", str(resp_up)
        else:
            up_id, up_status, up_err, up_msg = _parse(resp_up)

        if isinstance(resp_dn, Exception) or isinstance(resp_up, Exception):
            log.warning(
                f"[{b.id}] Parallel submission: one or both legs raised exception | "
                f"dn={resp_dn if isinstance(resp_dn, Exception) else dn_status} "
                f"up={resp_up if isinstance(resp_up, Exception) else up_status}"
            )

        return dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg

    async def _submit_orders_sequential(
        self, b: Bracket, signed_up, signed_dn, limit_up: float, limit_down: float,
        shares_up: float, shares_down: float, order_type, loop, _parse
    ) -> tuple:
        """Submit DOWN first, then UP only if DOWN fills (classical sequential).

        Returns: (dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg)

        Sequential execution (Option 2.1) eliminates ~90% of partial fills:
        - POST DOWN order via /order endpoint (FOK)
        - If DOWN fills → POST UP order immediately (FOK)
        - If DOWN doesn't fill → abort early (zero exposure, zero loss)

        Reasoning: DOWN is thin/competitive (consensus side, fewer sellers).
        UP is deep/liquid (underdog side, many sellers). If we can fill the
        hard side (DOWN), the easy side (UP) almost always fills even if price
        drifts 1-2 ticks in the 100-150ms submission gap.

        Tradeoff: full latency penalty (~723ms) but max safety on partial fills.
        """
        dn_id = up_id = dn_err = up_err = dn_msg = up_msg = ""
        dn_status = up_status = "cancelled"

        # ── POST DOWN ──

        try:
            resp_dn = await loop.run_in_executor(
                None, self._client.post_order,
                signed_dn, order_type, False
            )
            dn_id, dn_status, dn_err, dn_msg = _parse(resp_dn)
        except Exception as e:
            # DOWN threw (400 FOK reject) — no exposure, clean abort.
            log.info(
                f"[{b.id}] DOWN FOK exception (no fill) — "
                f"ask={b.leg_down.price:.3f} lim={limit_down:.3f} sh={shares_down:.2f} "
                f"depth={b.depth_down:.1f} | {e}"
            )
            dn_status = "cancelled"
            dn_err = "exception"
            dn_msg = str(e)
            return dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg

        dn_filled = dn_status == "matched"

        if not dn_filled:
            log.info(
                f"[{b.id}] DOWN FOK miss — "
                f"ask={b.leg_down.price:.3f} lim={limit_down:.3f} sh={shares_down:.2f} "
                f"depth={b.depth_down:.1f} status={dn_status!r} err={dn_err!r}"
            )
            return dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg

        # ── DOWN filled — now post UP ──

        try:
            resp_up = await loop.run_in_executor(
                None, self._client.post_order,
                signed_up, order_type, False
            )
            up_id, up_status, up_err, up_msg = _parse(resp_up)
        except Exception as e:
            # DOWN filled but UP threw (400 FOK reject) — partial fill, need emergency exit.
            log.warning(
                f"[{b.id}] UP FOK exception after DOWN fill — partial exposure! "
                f"ask={b.leg_up.price:.3f} lim={limit_up:.3f} sh={shares_up:.2f} "
                f"depth={b.depth_up:.1f} | {e}. Initiating emergency exit."
            )
            up_status = "cancelled"
            up_err = "exception"
            up_msg = str(e)
            # Mark DOWN as filled (it did fill) so result handler triggers emergency exit
            dn_status = "matched"
            return dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg

        return dn_id, dn_status, dn_err, dn_msg, up_id, up_status, up_err, up_msg

    async def _retry_down_reduced(
        self, b: Bracket, signed_up, limit_up: float, limit_down: float,
        order_type, loop
    ) -> bool:
        """After a full-size DOWN FOK miss, attempt a reduced-size bracket.

        Uses WS-observed depth (b.depth_down) as the target share count, capped
        at the original shares and floored at min_position_size_usdc / ask_down.
        If the smaller DOWN fills, immediately posts UP at the same share count.

        This lets us capture bracketing opportunities where only part of the
        target depth is available at submission time (race condition between
        detection and order arrival).  Returns True only if both reduced legs fill.
        """
        from py_clob_client.clob_types import OrderArgs

        ask_down = b.leg_down.price
        ask_up   = b.leg_up.price

        # Floor: shares worth at least min_position_size_usdc per leg
        min_shares = STRATEGY.min_position_size_usdc / ask_down if ask_down > 0 else 2.0
        # Target: smaller of current ask_book depth and original shares, with a 10% safety haircut
        # to account for depth consumed since the snapshot.
        current_down_depth = _ask_book_depth_to(b.ask_book_down, limit_down)
        if current_down_depth > 0:
            target = min(current_down_depth * 0.9, b.leg_down.shares)
        else:
            target = b.leg_down.shares * 0.5   # no depth data — try half

        if target < min_shares:
            log.debug(
                f"[{b.id}] DOWN reduced-size retry skipped — "
                f"target={target:.2f} < min={min_shares:.2f}"
            )
            return False

        retry_shares = _clob_valid_shares(target, limit_down)

        log.info(
            f"[{b.id}] DOWN reduced-size retry — "
            f"target={target:.2f}sh (depth={b.depth_down:.1f}) → "
            f"retry={retry_shares:.2f}sh lim={limit_down:.3f}"
        )

        try:
            signed_dn_r = await loop.run_in_executor(
                None, self._client.create_order,
                OrderArgs(token_id=b.leg_down.token_id, price=limit_down,
                          size=retry_shares, side="BUY"),
            )
            resp_dn_r = await loop.run_in_executor(
                None, self._client.post_order, signed_dn_r, order_type, False
            )
        except Exception as e:
            log.info(f"[{b.id}] DOWN reduced-size retry exception: {e}")
            return False

        def _parse_r(resp):
            if not isinstance(resp, dict):
                return "", "", ""
            return (
                (resp.get("orderID") or ""),
                (resp.get("status") or "").lower(),
                (resp.get("errorCode") or resp.get("error_code") or ""),
            )

        dn_r_id, dn_r_status, dn_r_err = _parse_r(resp_dn_r)
        if dn_r_status != "matched":
            log.info(
                f"[{b.id}] DOWN reduced-size retry miss — "
                f"retry_sh={retry_shares:.2f} lim={limit_down:.3f} status={dn_r_status!r} err={dn_r_err!r}"
            )
            return False

        # DOWN filled with reduced size — post UP at same share count
        up_retry_shares = _clob_valid_shares(retry_shares, limit_up)
        try:
            signed_up_r = await loop.run_in_executor(
                None, self._client.create_order,
                OrderArgs(token_id=b.leg_up.token_id, price=limit_up,
                          size=up_retry_shares, side="BUY"),
            )
            resp_up_r = await loop.run_in_executor(
                None, self._client.post_order, signed_up_r, order_type, False
            )
        except Exception as e:
            # DOWN filled but UP threw — partial fill with reduced size
            log.warning(
                f"[{b.id}] Reduced-size DOWN filled but UP exception: {e}. Emergency exit."
            )
            now = time.time()
            b.leg_down.order_id  = dn_r_id or None
            b.leg_down.placed_at = now
            b.leg_down.status    = OrderStatus.FILLED
            b.leg_down.filled_at = now
            b.leg_down.fill_price = ask_down
            b.leg_down.shares    = retry_shares
            b.submitted_shares_down = retry_shares
            b.leg_up.status = OrderStatus.CANCELLED
            b.status = "partial_fill"
            self._log_trade(b, "partial_fill")
            self.risk.mark_partial_fill(b.market_condition_id)
            asyncio.get_running_loop().create_task(self._emergency_exit(b))
            return False

        up_r_id, up_r_status, _ = _parse_r(resp_up_r)
        if up_r_status == "matched":
            now = time.time()
            b.leg_down.order_id = dn_r_id or None
            b.leg_up.order_id   = up_r_id or None
            for leg, filled in ((b.leg_down, True), (b.leg_up, True)):
                leg.placed_at  = now
                leg.status     = OrderStatus.FILLED
                leg.filled_at  = now
                leg.fill_price = leg.price
            b.leg_down.shares    = retry_shares
            b.leg_up.shares      = up_retry_shares
            b.submitted_shares_down = retry_shares
            b.submitted_shares_up   = up_retry_shares
            log.info(
                f"[{b.id}] Both legs filled (reduced-size FOK) — "
                f"Down={dn_r_id} Up={up_r_id} | "
                f"sh={retry_shares:.2f}/{up_retry_shares:.2f} "
                f"lim_dn={limit_down:.3f} lim_up={limit_up:.3f}"
            )
            await self._live_resolve(b)
            return True

        # UP also missed at reduced size — DOWN is stranded
        log.warning(
            f"[{b.id}] Reduced-size DOWN filled but UP miss — "
            f"up_status={up_r_status!r}. Emergency exit."
        )
        now = time.time()
        b.leg_down.order_id  = dn_r_id or None
        b.leg_down.placed_at = now
        b.leg_down.status    = OrderStatus.FILLED
        b.leg_down.filled_at = now
        b.leg_down.fill_price = ask_down
        b.leg_down.shares    = retry_shares
        b.submitted_shares_down = retry_shares
        b.leg_up.status = OrderStatus.CANCELLED
        b.status = "partial_fill"
        self._log_trade(b, "partial_fill")
        self.risk.mark_partial_fill(b.market_condition_id)
        asyncio.get_running_loop().create_task(self._emergency_exit(b))
        return False

    async def _fetch_book_depth(self, token_id: str, limit_price: float) -> float:
        """REST fetch of cumulative ask depth up to limit_price for token_id.

        Hits GET /book?token_id=... which always returns the live order book,
        guaranteeing freshness regardless of WS snapshot age.

        Returns total fillable shares (sum of all ask levels ≤ limit_price), or
        -1.0 on any error so the caller can fall back to cached WS depth gracefully.
        Called only on the parallel submission path to validate DOWN book depth
        immediately before committing both legs simultaneously (~50ms round-trip).
        """
        import aiohttp
        url = f"{CLOB_HOST}/book"
        params = {"token_id": token_id}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, params=params,
                    timeout=aiohttp.ClientTimeout(total=2.0),
                ) as resp:
                    data = await resp.json()
            asks = data.get("asks", [])
            total = sum(
                float(a.get("size", 0))
                for a in asks
                if float(a.get("price", 1.0)) <= limit_price
            )
            return total
        except Exception as e:
            log.debug(f"Book depth REST fetch failed for {token_id[:12]}: {e}")
            return -1.0

    async def _emergency_exit(self, b: Bracket):
        """One leg filled, the other was FOK-cancelled.  Sell the filled leg at
        bid minus slippage buffer to exit the one-sided position.

        Runs as a background task (not awaited by _handle_opportunity) so it is
        not subject to the 10s placement timeout.  Owns all cleanup: risk.close,
        _open_brackets removal, and trade log entry on final outcome.

        Polymarket batches on-chain CTF token delivery, so a SELL placed in the
        same millisecond as the fill will see balance=0.  Retry up to 4 times
        with increasing waits to allow the token settlement to land.

        Fix 1 — balance parsing: on "not enough balance" errors with a non-zero
        actual balance, parse the settled token count from the error message and
        shrink exit_shares to match.  Handles FOK sweep-fill rounding where the
        on-chain CTF delivery is fractionally less than the nominal order size.

        Fix 2 — GTC instead of FOK: a GTC sell at bid-2% immediately crosses
        against any existing buy at or above that price (filling at the better
        bid price), and rests on the book if no immediate match exists.  Either
        way the position is exited — unlike FOK which simply cancels with no fill
        when the bid has moved.  Status "live" is treated as success since the
        order will fill independently without further intervention.
        """
        self.stats["emergency_exits_attempted"] += 1

        filled_leg = b.leg_up if b.leg_up.status == OrderStatus.FILLED else b.leg_down
        bid_price  = b.bid_up  if filled_leg is b.leg_up else b.bid_down

        exit_price = round(bid_price * (1.0 - STRATEGY.emergency_exit_slippage_pct), 2)
        exit_price = max(exit_price, 0.01)
        # Use the ACTUAL submitted shares (after limit revalidation in _live_place),
        # not the original bracket shares which may differ.
        submitted_shares = b.submitted_shares_up if filled_leg is b.leg_up else b.submitted_shares_down
        exit_shares = submitted_shares if submitted_shares > 0 else _clob_valid_shares(filled_leg.shares, exit_price)

        from py_clob_client.clob_types import OrderArgs, OrderType
        loop = asyncio.get_running_loop()

        _RETRY_DELAYS = [3.0, 5.0, 8.0, 12.0]
        last_exc: Optional[Exception] = None

        for attempt, delay in enumerate([0.0] + _RETRY_DELAYS, start=1):
            if delay:
                log.info(f"[{b.id}] Emergency exit retry {attempt} in {delay:.0f}s "
                         f"(waiting for token settlement)")
                await asyncio.sleep(delay)

            try:
                signed_exit = await loop.run_in_executor(
                    None, self._client.create_order,
                    OrderArgs(
                        token_id=filled_leg.token_id,
                        price=exit_price,
                        size=exit_shares,
                        side="SELL",
                    ),
                )
                # Fix 2: GTC so the order crosses immediately against any resting bid,
                # or rests on the book if no match — never silently cancels like FOK.
                resp = await loop.run_in_executor(
                    None, self._client.post_order,
                    signed_exit, OrderType.GTC,
                )
                status   = (resp.get("status")  or "").lower() if isinstance(resp, dict) else ""
                order_id = (resp.get("orderID") or "")          if isinstance(resp, dict) else ""

                if status == "matched":
                    realised_loss = (filled_leg.price - exit_price) * exit_shares
                    log.info(
                        f"[{b.id}] Emergency exit filled @ {exit_price:.2f} "
                        f"(attempt {attempt}) | loss=${realised_loss:.4f}"
                    )
                    self.stats["emergency_exits_succeeded"] += 1
                    b.actual_net_usdc = -realised_loss
                    b.status = "emergency_exited"
                    b.closed_at = time.time()
                    self.state.update_balance(-realised_loss)
                    self.state.close_bracket(b.id, -realised_loss)
                    self._log_trade(b, "emergency_exited")
                    self.risk.close(b.market_condition_id, STRATEGY.position_size_usdc * 2)
                    self._open_brackets.pop(b.id, None)
                    return

                elif status in ("live", "open"):
                    # GTC order is resting on the book — will fill when a buyer crosses.
                    # Release risk and cleanup now; the position resolves independently.
                    realised_loss = (filled_leg.price - exit_price) * exit_shares
                    log.info(
                        f"[{b.id}] Emergency exit GTC live on book @ {exit_price:.2f} "
                        f"(attempt {attempt}) order={order_id} | est_loss=${realised_loss:.4f}"
                    )
                    self.stats["emergency_exits_succeeded"] += 1
                    b.actual_net_usdc = -realised_loss
                    b.status = "emergency_exited"
                    b.closed_at = time.time()
                    self.state.update_balance(-realised_loss)
                    self.state.close_bracket(b.id, -realised_loss)
                    self._log_trade(b, "emergency_exited")
                    self.risk.close(b.market_condition_id, STRATEGY.position_size_usdc * 2)
                    self._open_brackets.pop(b.id, None)
                    return

                else:
                    log.warning(
                        f"[{b.id}] Emergency exit attempt {attempt} unexpected status "
                        f"(status={status!r} order={order_id})"
                    )
                    last_exc = None

            except Exception as e:
                err_str = str(e)
                log.warning(f"[{b.id}] Emergency exit attempt {attempt} failed: {e}")

                # Fix 1: parse actual settled token balance from the CLOB error message.
                # Format: "balance: 9820140, order amount: 10000000"
                # If balance > 0 but < exit_shares, the CTF delivered fewer tokens than
                # the nominal order size (sweep-fill rounding).  Shrink exit_shares so
                # the next attempt sells exactly what settled, not the nominal amount.
                if "not enough balance" in err_str.lower():
                    m = re.search(r'balance:\s*(\d+)', err_str)
                    if m:
                        actual_micro = int(m.group(1))
                        if actual_micro == 0:
                            log.info(f"[{b.id}] Token not yet settled (balance=0) — waiting")
                        elif actual_micro < round(exit_shares * 1_000_000):
                            actual_shares = actual_micro / 1_000_000
                            log.info(
                                f"[{b.id}] Adjusting exit_shares {exit_shares:.6f} → "
                                f"{actual_shares:.6f} (actual settled balance)"
                            )
                            exit_shares = _clob_valid_shares(actual_shares, exit_price)
                last_exc = e

        # All attempts exhausted — position is stranded until market resolution
        log.error(
            f"[{b.id}] EMERGENCY EXIT FAILED after {len(_RETRY_DELAYS)+1} attempts "
            f"(last error: {last_exc}) — stranded {filled_leg.side} position "
            f"({exit_shares:.2f} shares @ {filled_leg.price:.3f}). "
            f"Will resolve at market close."
        )
        self.stats["emergency_exits_failed"] += 1
        b.status = "stranded"
        b.closed_at = time.time()
        self._log_trade(b, "stranded")
        # Release risk so the guard doesn't stay permanently blocked.
        # The cooldown already prevents re-entry for partial_fill_cooldown_s.
        self.risk.close(b.market_condition_id, STRATEGY.position_size_usdc * 2)
        self._open_brackets.pop(b.id, None)

    async def _cleanup_down_gtc(self) -> None:
        """Periodically cancel resting DOWN GTCs that haven't been claimed."""
        while True:
            await asyncio.sleep(5.0)
            now = time.time()
            expired = []

            for cid, pending in self._pending_down_gtc.items():
                age = now - pending["posted_at"]
                if age > MAKER.down_gtc_timeout_s:
                    expired.append(cid)
                    try:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(
                            None, self._client.cancel_order, pending["order_id"]
                        )
                        log.info(f"[MAKER] Cancelled expired DOWN GTC {pending['order_id']}")
                    except Exception as e:
                        log.debug(f"[MAKER] Error cancelling expired DOWN GTC: {e}")

            for cid in expired:
                del self._pending_down_gtc[cid]

    async def _sim_place(self, b: Bracket) -> bool:
        """Simulate order placement with realistic latency and fill probability."""
        # Simulate CLOB round-trip latency
        latency = random.gauss(SIM.simulated_latency_ms_p50, 12) / 1000
        await asyncio.sleep(max(0.005, latency))

        # Simulate partial fill failures
        if random.random() > SIM.fill_probability:
            log.info(f"[SIM][{b.id}] Simulated fill failure")
            return False

        b.leg_up.order_id   = f"sim-up-{b.id}"
        b.leg_down.order_id = f"sim-dn-{b.id}"
        b.leg_up.placed_at   = time.time()
        b.leg_down.placed_at = time.time()
        b.leg_up.status   = OrderStatus.FILLED
        b.leg_down.status = OrderStatus.FILLED
        b.leg_up.fill_price   = b.leg_up.price
        b.leg_down.fill_price = b.leg_down.price
        b.leg_up.filled_at   = time.time()
        b.leg_down.filled_at = time.time()

        # Simulate resolution after window closes (random 15-900s)
        resolution_delay = random.uniform(15, 120)
        asyncio.get_running_loop().create_task(
            self._sim_resolve(b, resolution_delay)
        )
        log.info(f"[SIM][{b.id}] Both legs filled | resolves in {resolution_delay:.0f}s")
        return True

    async def _sim_resolve(self, b: Bracket, delay: float):
        """Simulate market resolution."""
        await asyncio.sleep(delay)
        # With equal-shares sizing both legs hold the same number of shares,
        # so payout is identical regardless of which side wins.
        total_cost = b.leg_up.size_usdc + b.leg_down.size_usdc
        gross = b.leg_up.shares - total_cost   # $1/share payout minus cost
        fee   = total_cost * STRATEGY.taker_fee_pct
        gas   = SIM.gas_fee_usdc_per_redemption if SIM.include_gas_fees else 0
        net   = gross - fee - gas

        b.actual_net_usdc = net
        b.status = "won" if net > 0 else "lost"
        b.closed_at = time.time()

        self.stats["brackets_won" if net > 0 else "brackets_lost"] += 1
        self.stats["total_gross_usdc"] += gross
        self.stats["total_fees_usdc"]  += fee
        self.stats["total_gas_usdc"]   += gas
        self.stats["total_net_usdc"]   += net

        self.risk.close(b.market_condition_id, total_cost)
        self.state.update_balance(net)
        self.state.close_bracket(b.id, net)
        self._log_trade(b, "resolved")
        log.info(f"[SIM][{b.id}] Resolved {b.status} | net=${net:.4f}")

    # ── Fill tracking ────────────────────────────────────────────

    async def _poll_fills(self, b: Bracket):
        """Poll CLOB every 3s until both legs fill or the timeout expires."""
        deadline = time.time() + STRATEGY.cancel_unfilled_after_s
        loop = asyncio.get_running_loop()

        while time.time() < deadline:
            await asyncio.sleep(3.0)

            if b.id not in self._open_brackets:
                return  # already cancelled or resolved by another path

            for leg in (b.leg_up, b.leg_down):
                if leg.order_id and leg.status == OrderStatus.PENDING:
                    try:
                        order = await loop.run_in_executor(
                            None, self._client.get_order, leg.order_id
                        )
                        status = (order.get("status") or "").lower() if order else ""
                        if status == "matched":
                            leg.status     = OrderStatus.FILLED
                            leg.filled_at  = time.time()
                            leg.fill_price = float(order.get("price", leg.price))
                            log.info(f"[{b.id}] {leg.side} leg filled @ {leg.fill_price}")
                        elif status in ("cancelled", "unmatched"):
                            leg.status = OrderStatus.CANCELLED
                    except Exception as e:
                        log.warning(f"[{b.id}] Fill poll error ({leg.side}): {e}")

            if (b.leg_up.status  == OrderStatus.FILLED and
                    b.leg_down.status == OrderStatus.FILLED):
                await self._live_resolve(b)
                return

        # Timeout — cancel any legs still pending
        if b.id in self._open_brackets:
            if (b.leg_up.status  == OrderStatus.PENDING or
                    b.leg_down.status == OrderStatus.PENDING):
                await self._cancel_bracket(b)

    async def _live_resolve(self, b: Bracket):
        """Both legs confirmed filled — hand off to redeemer for on-chain resolution."""
        # Use actual submitted shares (set in _live_place after limit-price revalidation
        # and presign selection).  These may differ from b.leg_*.shares, which were
        # computed at detection-time ask prices — presigned orders use limit-price
        # quantization and can produce different valid share counts.
        shares_up = b.submitted_shares_up or b.leg_up.shares
        shares_dn = b.submitted_shares_down or b.leg_down.shares
        price_up  = b.leg_up.fill_price  or b.leg_up.price
        price_dn  = b.leg_down.fill_price or b.leg_down.price
        total_cost = round(shares_up * price_up + shares_dn * price_dn, 4)
        # Gross profit: winning leg pays out shares × $1 (whichever side wins).
        # Use the average as a point estimate since we don't know which side wins yet.
        avg_shares = (shares_up + shares_dn) / 2
        gross = avg_shares - total_cost
        fee   = total_cost * STRATEGY.taker_fee_pct
        gas   = STRATEGY.gas_fee_live_usdc
        net   = gross - fee - gas
        b.actual_net_usdc = net
        b.status = "filled"   # redeemer will close it as won/lost after on-chain settlement
        self.risk.close(b.market_condition_id, total_cost)
        if b.id in self._cancel_tasks:
            self._cancel_tasks.pop(b.id).cancel()
        if b.id in self._open_brackets:
            del self._open_brackets[b.id]
        self._log_trade(b, "filled")
        log.info(f"[{b.id}] Both legs filled — expected net=${net:.4f}, awaiting on-chain resolution")

    async def _cancel_bracket(self, b: Bracket):
        if not SIM.enabled and self._client:
            for leg in [b.leg_up, b.leg_down]:
                if leg.order_id and leg.status == OrderStatus.PENDING:
                    try:
                        await asyncio.get_running_loop().run_in_executor(
                            None, self._client.cancel, leg.order_id
                        )
                    except Exception as e:
                        log.warning(f"Cancel failed for {leg.order_id}: {e}")

        b.status = "cancelled"
        b.closed_at = time.time()
        self.stats["brackets_cancelled"] += 1
        self.risk.close(b.market_condition_id, STRATEGY.position_size_usdc * 2)
        self.state.close_bracket(b.id, 0)
        self._log_trade(b, "cancelled")
        if b.id in self._open_brackets:
            del self._open_brackets[b.id]

    # ── Trade logging ────────────────────────────────────────────

    def _log_trade(self, b: Bracket, event: str):
        record = {
            "event": event,
            "ts": time.time(),
            "bracket_id": b.id,
            "market": b.market_title,
            "asset": b.asset,
            "window": b.window,
            "spread": b.detected_spread,
            "expected_net": b.expected_net_usdc,
            "actual_net": b.actual_net_usdc,
            "status": b.status,
            "latency_ms": b.latency_ms,
            "age_ms": b.age_ms,
            "sim": b.sim_mode,
            # Detailed leg info for debugging partial fills and execution details
            "leg_up": {
                "status": b.leg_up.status.value,
                "order_id": b.leg_up.order_id,
                "price": b.leg_up.price,
                "shares": b.leg_up.shares,
                "fill_price": b.leg_up.fill_price,
            },
            "leg_down": {
                "status": b.leg_down.status.value,
                "order_id": b.leg_down.order_id,
                "price": b.leg_down.price,
                "shares": b.leg_down.shares,
                "fill_price": b.leg_down.fill_price,
            },
        }
        try:
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass
