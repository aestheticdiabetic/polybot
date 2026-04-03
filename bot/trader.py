"""
trader.py — Order placement, position tracking, and risk controls.
Handles both live trading and simulation mode.
"""
import asyncio
import json
import logging
import math
import random
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
from enum import Enum

from config import (
    CLOB_HOST, PRIVATE_KEY, FUNDER_ADDRESS,
    API_KEY, API_SECRET, API_PASSPHRASE,
    STRATEGY, SIM, TRADE_LOG
)
from scanner import BracketOpportunity

log = logging.getLogger("trader")


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
        self._open_brackets: Dict[str, Bracket] = {}
        self._cancel_tasks: Dict[str, asyncio.Task] = {}
        # Pre-signed orders keyed by condition_id.  Populated by _presign_orders when
        # the scanner fires on_near_bracket; consumed (and cleared) by _live_place.
        self._presigned: Dict[str, dict] = {}
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

    async def _init_client(self):
        """Initialise py-clob-client."""
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
        except Exception as e:
            log.error(f"Failed to init CLOB client: {e}")
            raise

    # ── Main entry point ─────────────────────────────────────────

    def on_bracket(self, opp: BracketOpportunity):
        """Called by scanner when a bracket opportunity is found."""
        asyncio.get_running_loop().create_task(self._handle_opportunity(opp))

    def on_near_bracket(self, opp: BracketOpportunity):
        """Called by scanner when combined ask is within near_bracket_threshold.

        Fires a background task that warms the CLOB metadata cache and pre-signs
        both orders so the critical path at actual threshold is just one POST call.
        """
        if not SIM.enabled and self._client is not None:
            asyncio.get_running_loop().create_task(self._presign_orders(opp))

    async def _presign_orders(self, opp: BracketOpportunity) -> None:
        """Warm cache + pre-sign both legs for a near-threshold opportunity.

        Pre-signed orders are stored in _presigned[condition_id] and consumed by
        _live_place when the real threshold is crossed.  They expire after 15s to
        avoid submitting stale orders if the opportunity dissolves and re-appears.

        Limit prices are set 1 tick above the near-threshold ask so the signed
        orders remain valid even if prices drift slightly upward before the actual
        threshold crossing.  FOK still executes at the real market price (≤ limit).
        """
        cid = opp.market.condition_id

        # Don't re-sign if we already have a fresh pre-sign for this market
        existing = self._presigned.get(cid)
        if existing and time.time() - existing["ts"] < 10.0:
            return

        # Warm cache first (no-op if already warm within TTL)
        await self._warm_token_cache(opp.market.token_id_up, opp.market.token_id_down)

        # Limits: 1 tick above near-threshold ask gives headroom for asymmetric moves.
        # FOK executes at market price (≤ limit), so profitability is determined by
        # the actual ask at submission time, not these limits.
        tick = 0.01
        limit_up   = round(opp.ask_up   + tick, 2)
        limit_down = round(opp.ask_down + tick, 2)

        combined   = opp.ask_up + opp.ask_down
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
                f"metadata_age={opp.metadata_age_ms:.0f}ms"
            )
        except Exception as e:
            log.debug(f"[PRESIGN] {opp.market.asset} {opp.market.window} error: {e}")

    async def _warm_token_cache(self, token_id_up: str, token_id_down: str) -> None:
        """Pre-populate ClobClient's tick-size / neg-risk / fee-rate caches for both
        tokens *concurrently*.

        ClobClient fetches these values lazily inside create_order() — three
        sequential HTTP GETs per token (~200 ms total when cold).  By firing all six
        GETs in parallel here, we reduce the cold-path cost to roughly one round-trip
        (~60 ms) and leave create_order() with nothing to fetch, so it only does
        ECDSA signing (~5 ms).

        return_exceptions=True means a failed warm (e.g. transient 500) is silently
        ignored; create_order() will simply re-fetch that value itself.
        """
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            loop.run_in_executor(None, self._client.get_tick_size,    token_id_up),
            loop.run_in_executor(None, self._client.get_neg_risk,     token_id_up),
            loop.run_in_executor(None, self._client.get_fee_rate_bps, token_id_up),
            loop.run_in_executor(None, self._client.get_tick_size,    token_id_down),
            loop.run_in_executor(None, self._client.get_neg_risk,     token_id_down),
            loop.run_in_executor(None, self._client.get_fee_rate_bps, token_id_down),
            return_exceptions=True,
        )

    async def _handle_opportunity(self, opp: BracketOpportunity):
        # Fire cache warm immediately, then yield so the warm task actually starts
        # running before we spend time on synchronous checks below.  Without the
        # yield the task is scheduled but stays queued until the first I/O await,
        # which is `await _warm` — meaning the warm doesn't overlap with the checks.
        if not SIM.enabled and self._client is not None:
            _warm = asyncio.get_running_loop().create_task(
                self._warm_token_cache(
                    opp.market.token_id_up, opp.market.token_id_down
                )
            )
            await asyncio.sleep(0)   # yield → warm GETs start immediately
        else:
            _warm = None

        self.stats["brackets_attempted"] += 1

        # Reject if opportunity is stale — prices may have moved since detection
        age_ms = (time.time() - opp.detected_at) * 1000
        if age_ms > 500:
            log.debug(f"Stale opportunity: {age_ms:.0f}ms old, skipping")
            self.stats["brackets_stale_skipped"] += 1
            # Let _warm finish in background — result cached for next opportunity
            return

        balance = self.state.get_balance()
        ok, reason = self.risk.can_open(opp.market.condition_id, balance)
        if not ok:
            log.debug(f"Risk block: {reason}")
            return  # same — warm result still useful

        # Ensure cache is populated before signing (should already be done or near-done)
        if _warm is not None:
            await _warm

        size = STRATEGY.position_size_usdc
        # Equal-shares sizing: buy the same number of shares on each leg so
        # payout is identical regardless of which side wins.
        # total_budget = 2*size; n_shares = total_budget / combined
        combined = opp.ask_up + opp.ask_down
        total_budget = size * 2
        n_shares = total_budget / combined

        # Depth-proportional sizing: if visible depth on either leg is less than
        # 50% of the target, cap shares to that depth.  WS depth is single-level
        # (best ask only), so we only apply this guard when the book looks genuinely
        # thin — not just because depth is spread across levels above best ask.
        # This prevents FOK attempts on markets where the book is clearly empty.
        if opp.depth_up > 0 and opp.depth_up < n_shares * 0.5:
            n_shares = min(n_shares, opp.depth_up)
        if opp.depth_down > 0 and opp.depth_down < n_shares * 0.5:
            n_shares = min(n_shares, opp.depth_down)
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
        """Place both legs as FOK via a single batch HTTP call.

        Both orders are signed locally (fast, CPU-bound) then submitted together
        in one post_orders() request to minimise the time delta between them
        landing on the matching engine.  FOK means each order either fills
        completely in the same millisecond or is auto-cancelled — no 30-second
        GTC exposure window.

        Returns True only if BOTH legs filled.  Partial fills trigger an
        emergency exit before returning False.
        """
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType, PostOrdersArgs
            loop = asyncio.get_running_loop()

            order_type = (
                OrderType.FOK if STRATEGY.order_type == "FOK" else OrderType.GTC
            )

            # Use limit prices (not just best ask) so the FOK can sweep through
            # multiple price levels up to the max profitable price.
            # Shares are re-derived for each limit price to satisfy CLOB precision.
            limit_up   = b.limit_up   or b.leg_up.price
            limit_down = b.limit_down or b.leg_down.price
            shares_up   = _clob_valid_shares(b.leg_up.shares,   limit_up)
            shares_down = _clob_valid_shares(b.leg_down.shares, limit_down)

            # Use pre-signed orders if available, fresh (< 55s), and still valid
            # (current ask hasn't risen above the pre-signed limit on either leg).
            # 55s TTL covers the typical gap between near-bracket and real bracket.
            # This skips ~12ms of signing and, crucially, avoids any thread-pool
            # contention — the critical path becomes a single POST call.
            presigned = self._presigned.pop(b.market_condition_id, None)
            used_presigned = False
            if (presigned is not None
                    and time.time() - presigned["ts"] < 55.0
                    and b.leg_up.price   <= presigned["limit_up"]
                    and b.leg_down.price <= presigned["limit_down"]):
                signed_up   = presigned["signed_up"]
                signed_dn   = presigned["signed_dn"]
                limit_up    = presigned["limit_up"]
                limit_down  = presigned["limit_down"]
                shares_up   = presigned["shares_up"]
                shares_down = presigned["shares_dn"]
                used_presigned = True
                log.info(f"[{b.id}] Using pre-signed orders "
                         f"(age={(time.time()-presigned['ts'])*1000:.0f}ms)")
            else:
                if presigned is not None:
                    log.info(
                        f"[{b.id}] Pre-signed stale/invalid — signing fresh "
                        f"(age={(time.time()-presigned['ts'])*1000:.0f}ms "
                        f"ask_up={b.leg_up.price:.3f}>lim={presigned['limit_up']:.3f}? "
                        f"ask_dn={b.leg_down.price:.3f}>lim={presigned['limit_down']:.3f}?)"
                    )
                else:
                    log.debug(
                        f"[{b.id}] No presign for this condition — signing fresh "
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

            # Submit both in a single HTTP request.
            # DOWN goes in slot 0: if the CLOB processes batch entries sequentially,
            # DOWN lands first and competes before other bots' DOWN orders.
            # UP books are deeper (underdog side has more sellers) so UP can afford
            # the fractional delay of being in slot 1.
            results = await loop.run_in_executor(
                None, self._client.post_orders,
                [
                    PostOrdersArgs(order=signed_dn, orderType=order_type),
                    PostOrdersArgs(order=signed_up, orderType=order_type),
                ],
            )

            # Parse response — index 0 = DOWN, index 1 = UP (batch order above)
            resp_dn = results[0] if results and len(results) > 0 else {}
            resp_up = results[1] if results and len(results) > 1 else {}

            log.debug(f"[{b.id}] raw batch response: {results!r}")

            def _parse(resp):
                if not isinstance(resp, dict):
                    return "", "", "", ""
                return (
                    (resp.get("orderID") or ""),
                    (resp.get("status") or "").lower(),
                    (resp.get("errorCode") or resp.get("error_code") or ""),
                    (resp.get("message") or resp.get("errorMsg") or ""),
                )

            up_id,  up_status,  up_err,  up_msg  = _parse(resp_up)
            dn_id,  dn_status,  dn_err,  dn_msg  = _parse(resp_dn)

            up_filled = up_status == "matched"
            dn_filled = dn_status == "matched"

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
                    f"[{b.id}] Both legs filled (FOK){'[presigned]' if used_presigned else ''} | "
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
                    f"[err={missed_err or 'none'} msg={missed_msg or 'none'}] | "
                    f"up: ask={b.leg_up.price:.3f} lim={limit_up:.3f} sh={shares_up:.4f} | "
                    f"dn: ask={b.leg_down.price:.3f} lim={limit_down:.3f} sh={shares_down:.4f} | "
                    f"age={b.age_ms:.0f}ms. Initiating emergency exit."
                )
                b.status = "partial_fill"
                self._log_trade(b, "partial_fill")
                # Mark cooldown immediately so the market is blocked during the full
                # emergency exit window (up to ~28s of retries), not just until the
                # 10s placement timeout fires.
                self.risk.mark_partial_fill(b.market_condition_id)
                # Fire emergency exit as a background task so it runs outside the
                # 10s wait_for timeout in _handle_opportunity.  _emergency_exit owns
                # its own risk.close() + _open_brackets cleanup.
                asyncio.get_running_loop().create_task(self._emergency_exit(b))
                return False

            # Neither filled — clean miss, no exposure
            log.info(
                f"[{b.id}] Both legs cancelled (FOK) — no fill | "
                f"up: status={up_status!r} err={up_err!r} msg={up_msg!r} "
                f"ask={b.leg_up.price:.3f} lim={limit_up:.3f} sh={shares_up:.4f} | "
                f"dn: status={dn_status!r} err={dn_err!r} msg={dn_msg!r} "
                f"ask={b.leg_down.price:.3f} lim={limit_down:.3f} sh={shares_down:.4f} | "
                f"spread={b.detected_spread:.4f} age={b.age_ms:.0f}ms"
            )
            return False

        except Exception as e:
            log.error(f"[{b.id}] Order placement failed: {e}")
            return False

    async def _emergency_exit(self, b: Bracket):
        """One leg filled, the other was FOK-cancelled.  Sell the filled leg at
        bid minus slippage buffer to exit the one-sided position.

        Runs as a background task (not awaited by _handle_opportunity) so it is
        not subject to the 10s placement timeout.  Owns all cleanup: risk.close,
        _open_brackets removal, and trade log entry on final outcome.

        Polymarket batches on-chain CTF token delivery, so a SELL placed in the
        same millisecond as the fill will see balance=0.  Retry up to 4 times
        with increasing waits to allow the token settlement to land.
        """
        self.stats["emergency_exits_attempted"] += 1

        filled_leg = b.leg_up if b.leg_up.status == OrderStatus.FILLED else b.leg_down
        bid_price  = b.bid_up  if filled_leg is b.leg_up else b.bid_down

        exit_price = round(bid_price * (1.0 - STRATEGY.emergency_exit_slippage_pct), 2)
        exit_price = max(exit_price, 0.01)
        # Use the ACTUAL submitted shares (after limit revalidation in _live_place),
        # not the original bracket shares which may differ.
        # Revalidating here with a lower (exit) price could compute a larger share
        # count than was actually filled, causing "not enough balance" errors.
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
                resp = await loop.run_in_executor(
                    None, self._client.post_order,
                    signed_exit, OrderType.FOK,
                )
                status = (resp.get("status") or "").lower() if isinstance(resp, dict) else ""
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
                    self._log_trade(b, "emergency_exited")
                    self.risk.close(b.market_condition_id, STRATEGY.position_size_usdc * 2)
                    self._open_brackets.pop(b.id, None)
                    return
                else:
                    log.warning(
                        f"[{b.id}] Emergency exit attempt {attempt} not matched "
                        f"(status={status!r})"
                    )
                    last_exc = None
            except Exception as e:
                log.warning(f"[{b.id}] Emergency exit attempt {attempt} failed: {e}")
                last_exc = e

        # All attempts exhausted — position is stranded until market resolution
        if last_exc:
            log.error(
                f"[{b.id}] EMERGENCY EXIT FAILED after {len(_RETRY_DELAYS)+1} attempts "
                f"(last error: {last_exc}) — stranded {filled_leg.side} position "
                f"({exit_shares:.2f} shares @ {filled_leg.price:.3f}). "
                f"Will resolve at market close."
            )
        else:
            log.error(
                f"[{b.id}] EMERGENCY EXIT FAILED — no FOK match after "
                f"{len(_RETRY_DELAYS)+1} attempts — stranded {filled_leg.side} position. "
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
        total_cost = b.leg_up.size_usdc + b.leg_down.size_usdc
        # Gross profit: winning leg pays out shares × $1.  With nearly-equal sizing
        # the two share counts differ by <0.001 shares; use the average as an estimate.
        avg_shares = (b.leg_up.shares + b.leg_down.shares) / 2
        gross = avg_shares - total_cost
        fee   = total_cost * STRATEGY.taker_fee_pct
        net   = gross - fee
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
        }
        try:
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass
