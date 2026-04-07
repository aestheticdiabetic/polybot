"""
order_tracker.py — Tracks GTC limit buy orders placed for partial-fill opportunities.

When a bonding opportunity has insufficient depth to fill the full target shares
immediately, we place a GTC limit order for the remainder. This module monitors
those pending orders and:
  - Registers fills as BondPositions with the ExitManager
  - Cancels orders whose edge has deteriorated since placement
"""
import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import config as _config
from py_clob_client.client import ClobClient
from bonding.exit_manager import ExitManager, BondPosition

log = logging.getLogger("bond.tracker")

CLOB_API = "https://clob.polymarket.com"

STATUS_PENDING   = "PENDING"
STATUS_FILLED    = "FILLED"
STATUS_CANCELLED = "CANCELLED"


@dataclass
class PendingOrder:
    order_id: str
    market_id: str
    token_id: str
    question: str
    city: str
    tier: str
    shares: int
    limit_price: float
    prob_at_placement: float   # forecast probability stored so we can reassess edge
    placed_at: str             # ISO8601
    resolution_time: str       # ISO8601
    status: str                # PENDING | FILLED | CANCELLED


class PendingOrderTracker:
    """
    Background asyncio task.  Polls open GTC buy limit orders every 60 seconds.

    For each PENDING order:
      1. Fetch order status from the CLOB.
      2. If filled  → create a BondPosition and hand off to ExitManager.
      3. If still open → check whether edge still holds (current ask vs stored prob).
         If edge is gone, cancel the order.
    """

    def __init__(self, client: ClobClient, exit_mgr: ExitManager):
        self._client   = client
        self._exit_mgr = exit_mgr
        self._ledger   = Path(_config.BOND_LEDGER_FILE).parent / "pending_orders.json"

    # ── Public API ────────────────────────────────────────────────

    async def run(self) -> None:
        log.info("PendingOrderTracker started")
        while True:
            try:
                await self._check_orders()
            except asyncio.CancelledError:
                log.info("PendingOrderTracker shutting down")
                return
            except Exception as exc:
                log.error(f"PendingOrderTracker error: {exc}", exc_info=True)
            await asyncio.sleep(60)

    async def add_order(self, order: PendingOrder) -> None:
        """Register a newly placed GTC limit buy order."""
        orders = self._load()
        # Guard against duplicates on restart
        if any(o.order_id == order.order_id for o in orders):
            return
        orders.append(order)
        self._save(orders)
        log.info(
            f"BOND_LIMIT_QUEUED city={order.city} tier={order.tier} "
            f"shares={order.shares} price={order.limit_price:.4f} "
            f"order_id={order.order_id[:8]}"
        )

    # ── Check loop ────────────────────────────────────────────────

    async def _check_orders(self) -> None:
        orders = self._load()
        pending = [o for o in orders if o.status == STATUS_PENDING]
        if not pending:
            return
        log.debug(f"tracker: checking {len(pending)} pending order(s)")

        loop = asyncio.get_running_loop()
        for order in pending:
            try:
                await self._assess_order(order, loop)
            except Exception as exc:
                log.warning(
                    f"tracker: error assessing {order.order_id[:8]}: {exc}"
                )

    async def _assess_order(self, order: PendingOrder, loop) -> None:
        # 1. Fetch current CLOB status
        try:
            clob_order = await loop.run_in_executor(
                None, self._client.get_order, order.order_id
            )
        except Exception as exc:
            log.debug(f"tracker: get_order failed for {order.order_id[:8]}: {exc}")
            return

        clob_status = (clob_order or {}).get("status", "").upper()

        if clob_status in ("MATCHED", "FILLED"):
            await self._handle_fill(order)
            return

        if clob_status in ("CANCELLED", "EXPIRED"):
            self._mark_status(order.order_id, STATUS_CANCELLED)
            log.info(
                f"BOND_LIMIT_CANCELLED_EXTERNALLY order={order.order_id[:8]} "
                f"city={order.city}"
            )
            return

        # 2. Order still open — reassess edge
        current_ask = await self._fetch_best_ask(order.token_id)
        if current_ask <= 0.0:
            return  # Can't assess; leave the order open

        max_profitable = order.prob_at_placement - _config.BOND_EDGE_FLOOR
        if current_ask > max_profitable:
            await self._cancel_order(order, current_ask, max_profitable)

    # ── Actions ───────────────────────────────────────────────────

    async def _handle_fill(self, order: PendingOrder) -> None:
        self._mark_status(order.order_id, STATUS_FILLED)
        pos = BondPosition(
            market_id=order.market_id,
            token_id=order.token_id,
            question=order.question,
            city=order.city,
            outcome="YES",
            tier=order.tier,
            shares=order.shares,
            entry_price=order.limit_price,
            entry_time=datetime.now(timezone.utc).isoformat(),
            resolution_time=order.resolution_time,
            status="OPEN",
        )
        await self._exit_mgr.add_position(pos)
        log.info(
            f"BOND_LIMIT_FILLED city={order.city} tier={order.tier} "
            f"shares={order.shares} price={order.limit_price:.4f} "
            f"order_id={order.order_id[:8]}"
        )

    async def _cancel_order(
        self, order: PendingOrder, current_ask: float, max_profitable: float
    ) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, self._client.cancel_orders, [order.order_id]
            )
            self._mark_status(order.order_id, STATUS_CANCELLED)
            log.info(
                f"BOND_LIMIT_CANCELLED city={order.city} tier={order.tier} "
                f"current_ask={current_ask:.4f} max_profitable={max_profitable:.4f} "
                f"order_id={order.order_id[:8]}"
            )
        except Exception as exc:
            log.warning(
                f"tracker: cancel failed for {order.order_id[:8]}: {exc}"
            )

    # ── Helpers ───────────────────────────────────────────────────

    async def _fetch_best_ask(self, token_id: str) -> float:
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"{CLOB_API}/book", params={"token_id": token_id}
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    asks = data.get("asks", [])
                    if asks:
                        return float(asks[0]["price"])
        except Exception as exc:
            log.debug(f"tracker: ask fetch failed for {token_id[:12]}: {exc}")
        return 0.0

    # ── Ledger ────────────────────────────────────────────────────

    def _load(self) -> list[PendingOrder]:
        if not self._ledger.exists():
            return []
        try:
            data = json.loads(self._ledger.read_text(encoding="utf-8"))
            return [PendingOrder(**o) for o in data.get("orders", [])]
        except Exception as exc:
            log.error(f"tracker: failed to read ledger: {exc}")
            return []

    def _save(self, orders: list[PendingOrder]) -> None:
        self._ledger.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(self._ledger) + ".tmp")
        tmp.write_text(
            json.dumps({"orders": [asdict(o) for o in orders]}, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._ledger)

    def _mark_status(self, order_id: str, status: str) -> None:
        orders = self._load()
        for o in orders:
            if o.order_id == order_id:
                o.status = status
        self._save(orders)
