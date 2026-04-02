"""
trader.py — Order placement, position tracking, and risk controls.
Handles both live trading and simulation mode.
"""
import asyncio
import json
import logging
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
    status: str = "open"    # open, won, lost, cancelled, partial
    sim_mode: bool = False
    latency_ms: Optional[float] = None


class RiskGuard:
    """Enforces all risk limits before orders are placed."""

    def __init__(self):
        self._open_by_market: Dict[str, int] = {}
        self._total_open: int = 0
        self._deployed_usdc: float = 0.0

    def can_open(self, condition_id: str, wallet_balance: float) -> tuple[bool, str]:
        if self._total_open >= STRATEGY.max_concurrent_brackets:
            return False, f"Max concurrent brackets ({STRATEGY.max_concurrent_brackets}) reached"

        if self._open_by_market.get(condition_id, 0) >= STRATEGY.max_brackets_per_market:
            return False, f"Already have bracket open on this market"

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
        self.stats = {
            "brackets_attempted": 0,
            "brackets_opened": 0,
            "brackets_won": 0,
            "brackets_lost": 0,
            "brackets_cancelled": 0,
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

    async def _handle_opportunity(self, opp: BracketOpportunity):
        self.stats["brackets_attempted"] += 1

        balance = self.state.get_balance()
        ok, reason = self.risk.can_open(opp.market.condition_id, balance)
        if not ok:
            log.debug(f"Risk block: {reason}")
            return

        size = STRATEGY.position_size_usdc
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
                size_usdc=size,
                shares=round(size / opp.ask_up, 2),
            ),
            leg_down=Leg(
                token_id=opp.market.token_id_down,
                side="DOWN",
                price=opp.ask_down,
                size_usdc=size,
                shares=round(size / opp.ask_down, 2),
            ),
            detected_spread=opp.spread,
            expected_net_usdc=opp.net_profit_usdc,
            sim_mode=SIM.enabled,
        )

        self.risk.open(opp.market.condition_id, size * 2)
        self._open_brackets[bracket.id] = bracket

        t0 = time.time()
        success = await self._place_bracket(bracket)
        bracket.latency_ms = (time.time() - t0) * 1000

        if success:
            self.stats["brackets_opened"] += 1
            self.state.add_bracket(bracket)
            # Schedule cancel task for unfilled orders
            task = asyncio.get_running_loop().create_task(
                self._auto_cancel(bracket)
            )
            self._cancel_tasks[bracket.id] = task
            self._log_trade(bracket, "opened")
        else:
            self.risk.close(opp.market.condition_id, size * 2)
            del self._open_brackets[bracket.id]

    # ── Order placement ──────────────────────────────────────────

    async def _place_bracket(self, b: Bracket) -> bool:
        """Place both legs. Returns True if both submitted."""
        if SIM.enabled:
            return await self._sim_place(b)
        return await self._live_place(b)

    async def _live_place(self, b: Bracket) -> bool:
        """Place real orders via CLOB API."""
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            orders = [
                OrderArgs(
                    token_id=b.leg_up.token_id,
                    price=b.leg_up.price,
                    size=b.leg_up.shares,
                    side="BUY",
                    order_type=OrderType.GTC,
                ),
                OrderArgs(
                    token_id=b.leg_down.token_id,
                    price=b.leg_down.price,
                    size=b.leg_down.shares,
                    side="BUY",
                    order_type=OrderType.GTC,
                ),
            ]
            resp = await asyncio.get_running_loop().run_in_executor(
                None, self._client.create_and_post_orders, orders
            )
            if resp and len(resp) >= 2:
                b.leg_up.order_id   = resp[0].get("orderID")
                b.leg_down.order_id = resp[1].get("orderID")
                b.leg_up.placed_at   = time.time()
                b.leg_down.placed_at = time.time()
                b.leg_up.status   = OrderStatus.PENDING
                b.leg_down.status = OrderStatus.PENDING
                log.info(f"[{b.id}] Orders placed | Up={b.leg_up.order_id} Down={b.leg_down.order_id}")
                return True
            return False
        except Exception as e:
            log.error(f"[{b.id}] Order placement failed: {e}")
            return False

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
        # One leg wins, one loses — always net positive if bracket was valid
        winning_side = random.choice(["UP", "DOWN"])
        size = STRATEGY.position_size_usdc
        winning_shares = b.leg_up.shares if winning_side == "UP" else b.leg_down.shares
        gross_return = winning_shares * 1.0  # $1 per share at resolution
        fee = size * 2 * STRATEGY.taker_fee_pct
        gas = SIM.gas_fee_usdc_per_redemption if SIM.include_gas_fees else 0
        net = gross_return - (size * b.leg_up.price + size * b.leg_down.price) / 1 - fee - gas
        # Simplified: net = size*(1 - combined_ask) - fees
        net = size * b.detected_spread - fee - gas

        b.actual_net_usdc = net
        b.status = "won" if net > 0 else "lost"
        b.closed_at = time.time()

        self.stats["brackets_won" if net > 0 else "brackets_lost"] += 1
        self.stats["total_gross_usdc"] += size * b.detected_spread
        self.stats["total_fees_usdc"]  += fee + gas
        self.stats["total_net_usdc"]   += net

        self.risk.close(b.market_condition_id, size * 2)
        self.state.update_balance(net)
        self.state.close_bracket(b.id, net)
        self._log_trade(b, "resolved")
        log.info(f"[SIM][{b.id}] Resolved {b.status} | net=${net:.4f}")

    # ── Auto-cancel stale orders ─────────────────────────────────

    async def _auto_cancel(self, b: Bracket):
        await asyncio.sleep(STRATEGY.cancel_unfilled_after_s)
        if b.id in self._open_brackets:
            if b.leg_up.status == OrderStatus.PENDING or b.leg_down.status == OrderStatus.PENDING:
                await self._cancel_bracket(b)

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
            "sim": b.sim_mode,
        }
        try:
            with open(TRADE_LOG, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            pass
