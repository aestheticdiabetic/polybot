"""
exit_manager.py — Async background task that monitors open bonding positions.

Fires limit sell orders when exit criteria are met (price targets, multipliers,
or gas-cost floors). Reads/writes a persistent JSON position ledger atomically.
"""
import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import config as _config
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

log = logging.getLogger("bond.exit")

GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
BOND_EVENT_LOG = Path(_config.BOND_EVENT_LOG_FILE)


def _append_bond_record(record: dict) -> bool:
    """Append a record to the live bond event JSONL log. Returns True on success."""
    try:
        BOND_EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with BOND_EVENT_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
        return True
    except Exception as exc:
        log.error(
            f"exit_mgr: failed to append event record ({exc}); "
            f"event={record.get('event')} market={record.get('market_id', '')[:8]}"
        )
        return False


def _patch_bond_buy(market_id: str, exit_price: float, pnl: float) -> None:
    """
    Rewrite the event JSONL so the original BOND_BUY entry for this market shows
    the resolved outcome, exit_price, and pnl — keeping the log self-contained.
    Only patches the most recent BOND_BUY with outcome=None (allows re-entry).
    """
    if not BOND_EVENT_LOG.exists():
        return
    try:
        lines = BOND_EVENT_LOG.read_text(encoding="utf-8").splitlines()
        patched = []
        found = False
        for line in reversed(lines):
            if not found and line.strip():
                try:
                    rec = json.loads(line)
                    if (
                        rec.get("event") == "BOND_BUY"
                        and rec.get("market_id") == market_id
                        and rec.get("outcome") is None
                    ):
                        rec["outcome"]    = "SOLD"
                        rec["exit_price"] = round(exit_price, 4)
                        rec["pnl"]        = round(pnl, 4)
                        line = json.dumps(rec)
                        found = True
                except Exception:
                    pass
            patched.append(line)
        patched.reverse()
        tmp = BOND_EVENT_LOG.with_suffix(".tmp")
        tmp.write_text("\n".join(patched) + "\n", encoding="utf-8")
        tmp.replace(BOND_EVENT_LOG)
    except Exception as exc:
        log.error(f"exit_mgr: failed to patch BOND_BUY record for {market_id[:8]}: {exc}")

STATUS_OPEN     = "OPEN"
STATUS_SOLD     = "SOLD"
STATUS_RESOLVED = "RESOLVED"

TIER_CHEAP   = "CHEAP"
TIER_CORE    = "CORE"
TIER_CERTAIN = "CERTAIN"

@dataclass
class BondPosition:
    market_id: str
    token_id: str
    question: str
    city: str
    outcome: str              # always "YES"
    tier: str                 # CHEAP | CORE | CERTAIN
    shares: int
    entry_price: float
    entry_time: str           # ISO8601
    resolution_time: str      # ISO8601
    status: str               # OPEN | SOLD | RESOLVED
    prob: float               = 0.0     # weather-model P(YES) at placement time
    exit_price: Optional[float] = None  # filled on SOLD
    exit_time: Optional[str]   = None   # filled on SOLD (ISO8601)


class ExitManager:
    """
    Background asyncio task. Polls open positions every 60 seconds.
    Applies exit decision tree and places limit sell orders via CLOB.
    """

    def __init__(self, client: ClobClient):
        self._client             = client
        self._ledger_path        = Path(_config.BOND_LEDGER_FILE)
        self._stop_loss_strikes: dict[str, int] = {}  # market_id → consecutive hit count

    async def run(self) -> None:
        log.info("ExitManager started")
        while True:
            try:
                await self._check_exits()
            except asyncio.CancelledError:
                log.info("ExitManager shutting down")
                return
            except Exception as exc:
                log.error(f"ExitManager error: {exc}", exc_info=True)
            await asyncio.sleep(60)

    async def add_position(self, pos: BondPosition) -> None:
        """Called by main loop after a confirmed buy fill."""
        positions = self._load_positions()
        # Only block re-entry if an OPEN position already exists for this market.
        # SOLD/RESOLVED positions allow re-entry when a new profitable opportunity appears.
        if any(p.market_id == pos.market_id for p in positions if p.status == STATUS_OPEN):
            log.debug(f"exit_mgr: open position {pos.market_id[:8]} already in ledger, skipping add")
            return
        positions.append(pos)
        self._save_positions(positions)
        log.info(
            f"BOND_LEDGER_ADD city={pos.city} tier={pos.tier} "
            f"shares={pos.shares} entry={pos.entry_price:.4f} "
            f"market={pos.market_id[:8]}"
        )
        _append_bond_record({
            "ts":             pos.entry_time,
            "event":          "BOND_BUY",
            "market_id":      pos.market_id,
            "question":       pos.question,
            "city":           pos.city,
            "date":           pos.resolution_time[:10],
            "resolution_time": pos.resolution_time,
            "tier":           pos.tier,
            "shares":         pos.shares,
            "side":           pos.outcome,
            "ask":            pos.entry_price,
            "prob":           round(pos.prob, 4),
            "capital":        round(pos.shares * pos.entry_price, 4),
            "outcome":        None,
            "pnl":            None,
        })

    # ── Core exit check ───────────────────────────────────────────

    async def _check_exits(self) -> None:
        positions = self._load_positions()
        open_pos  = [p for p in positions if p.status == STATUS_OPEN]
        if not open_pos:
            return

        for pos in open_pos:
            market_data = await self._fetch_market_data(pos.market_id)
            hours_left  = self._hours_to_resolution(pos.resolution_time)

            # Market has already passed its end date — record actual P&L and mark resolved
            if hours_left <= 0:
                if pos.outcome == "NO":
                    exit_price = self._no_resolution_price(market_data)
                else:
                    exit_price = self._yes_resolution_price(market_data)
                pnl = (exit_price - pos.entry_price) * pos.shares
                log.info(
                    f"BOND_RESOLVED market={pos.market_id[:8]} city={pos.city} "
                    f"tier={pos.tier} entry={pos.entry_price:.4f} "
                    f"exit={exit_price:.4f} pnl={pnl:+.2f}"
                )
                now_ts = datetime.now(timezone.utc).isoformat()
                _append_bond_record({
                    "ts":          now_ts,
                    "event":       "BOND_RESOLVED",
                    "market_id":   pos.market_id,
                    "question":    pos.question,
                    "city":        pos.city,
                    "date":        pos.resolution_time[:10],
                    "tier":        pos.tier,
                    "shares":      pos.shares,
                    "side":        pos.outcome,
                    "entry_price": pos.entry_price,
                    "exit_price":  round(exit_price, 4),
                    "pnl":         round(pnl, 4),
                    "reason":      "RESOLVED",
                })
                _patch_bond_buy(pos.market_id, exit_price, pnl)
                self._mark_resolved(pos.market_id, exit_price)
                continue

            # Stop-loss: uses bid price (what we'd actually receive selling now).
            # Runs before profit-exit check; has its own hours gate separate from gas floor.
            if await self._check_stop_loss(pos, hours_left):
                continue

            current_price = await self._get_current_price(pos.token_id)
            if self._should_exit(pos, current_price, hours_left):
                await self._execute_sell(pos, current_price)
            else:
                log.debug(
                    f"BOND_HOLDING market={pos.market_id[:8]} tier={pos.tier} "
                    f"price={current_price:.4f} hours={hours_left:.1f}"
                )

    def _should_exit(self, pos: BondPosition, price: float, hours: float) -> bool:
        """
        Profit-based exit rules:

        1. Hours to resolution < BOND_GAS_FLOOR_HOURS → HOLD (gas not worth it)
        2. CORE: price >= BOND_EARLY_EXIT_PRICE → SELL (near-certainty)
        3. Any tier: price >= entry * 10 → SELL (10× windfall)
        4. CHEAP: price >= entry * BOND_CHEAP_EXIT_MULTIPLIER
                  AND gain >= BOND_CHEAP_MIN_ABS_GAIN → SELL
        CERTAIN is held to resolution (excluded from rules 2 and 4).
        """
        if price <= 0.0:
            return False

        # Rule 1 — gas floor: don't exit near resolution
        if hours < _config.BOND_GAS_FLOOR_HOURS:
            log.debug(
                f"BOND_EXIT_SKIPPED market={pos.market_id[:8]} reason=GAS_FLOOR "
                f"hours={hours:.1f}"
            )
            return False

        # Rule 2 — CORE near-certainty exit (CERTAIN held to resolution)
        if pos.tier == TIER_CORE and price >= _config.BOND_EARLY_EXIT_PRICE:
            return True

        # Rule 3 — 10× return on any tier
        if price >= pos.entry_price * 10:
            return True

        # Rule 4 — CHEAP multiplier + absolute gain floor
        if pos.tier == TIER_CHEAP:
            gain = (price - pos.entry_price) * pos.shares
            if (
                price >= pos.entry_price * _config.BOND_CHEAP_EXIT_MULTIPLIER
                and gain >= _config.BOND_CHEAP_MIN_ABS_GAIN
            ):
                return True

        return False

    # ── Stop-loss ─────────────────────────────────────────────────

    async def _check_stop_loss(self, pos: BondPosition, hours: float) -> bool:
        """
        Check stop-loss condition using bid-side order book depth.
        Returns True (and fires sell) if the stop is confirmed.

        Guards against false triggers from thin/manipulative bids:
        - Requires fillable bid depth >= BOND_STOP_LOSS_MIN_FILL_FRACTION of our shares
        - Requires condition true in BOND_STOP_LOSS_CONFIRM_POLLS consecutive polls
        - Only fires when >BOND_STOP_LOSS_HOURS remain (position can still be saved)
        """
        if _config.BOND_STOP_LOSS_RATIO <= 0 or hours <= _config.BOND_STOP_LOSS_HOURS:
            return False
        stop_price = pos.entry_price * _config.BOND_STOP_LOSS_RATIO
        bid, fillable = await self._get_bid_depth(pos.token_id, stop_price)
        min_fill = pos.shares * _config.BOND_STOP_LOSS_MIN_FILL_FRACTION
        if bid > 0 and bid <= stop_price and fillable >= min_fill:
            strikes = self._stop_loss_strikes.get(pos.market_id, 0) + 1
            self._stop_loss_strikes[pos.market_id] = strikes
            if strikes >= _config.BOND_STOP_LOSS_CONFIRM_POLLS:
                self._stop_loss_strikes.pop(pos.market_id, None)
                await self._execute_sell(pos, bid, reason="STOP_LOSS")
                return True
            log.debug(
                f"BOND_STOP_LOSS_STRIKE [{strikes}/{_config.BOND_STOP_LOSS_CONFIRM_POLLS}] "
                f"market={pos.market_id[:8]} bid={bid:.4f} stop={stop_price:.4f} "
                f"fillable={fillable:.0f}/{pos.shares}"
            )
        else:
            self._stop_loss_strikes.pop(pos.market_id, None)
        return False

    async def _get_bid_depth(self, token_id: str, stop_price: float) -> tuple[float, float]:
        """
        Fetch bid-side order book and return (best_bid, fillable_shares_at_or_above_stop_price).
        Using bids prevents false signals from ask-side illusions (exhausted asks showing 0.99).
        """
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"{CLOB_API}/book", params={"token_id": token_id}
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    bids = data.get("bids", [])
                    if not bids:
                        return 0.0, 0.0
                    # Sort descending so bids[0] is always highest (best) bid.
                    bids_sorted = sorted(bids, key=lambda b: float(b["price"]), reverse=True)
                    best_bid = float(bids_sorted[0]["price"])
                    fillable = sum(
                        float(b.get("size", 0))
                        for b in bids_sorted
                        if float(b["price"]) >= stop_price
                    )
                    return best_bid, fillable
        except Exception as exc:
            log.debug(f"exit_mgr: bid depth fetch failed for {token_id[:12]}: {exc}")
        return 0.0, 0.0

    # ── Order placement ───────────────────────────────────────────

    async def _execute_sell(self, pos: BondPosition, current_price: float, reason: str = "PROFIT_EXIT") -> None:
        """Place a limit GTC sell order one tick below current price to ensure fill."""
        limit_price = max(round(current_price - 0.01, 2), 0.01)
        order_args  = OrderArgs(
            token_id=pos.token_id,
            price=limit_price,
            size=pos.shares,
            side="SELL",
        )
        try:
            signed = await asyncio.get_running_loop().run_in_executor(
                None, self._client.create_order, order_args
            )
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: self._client.post_order(signed, OrderType.GTC)
            )
            pnl = (current_price - pos.entry_price) * pos.shares
            log.info(
                f"BOND_EXIT_TRIGGERED [{reason}] market={pos.market_id[:8]} tier={pos.tier} "
                f"current_price={current_price:.4f} limit={limit_price:.4f} "
                f"pnl={pnl:+.2f}"
            )
            now_ts = datetime.now(timezone.utc).isoformat()
            _append_bond_record({
                "ts":          now_ts,
                "event":       "BOND_SELL",
                "market_id":   pos.market_id,
                "question":    pos.question,
                "city":        pos.city,
                "date":        pos.resolution_time[:10],
                "tier":        pos.tier,
                "shares":      pos.shares,
                "side":        pos.outcome,
                "entry_price": pos.entry_price,
                "exit_price":  round(limit_price, 4),
                "pnl":         round(pnl, 4),
                "reason":      reason,
            })
            _patch_bond_buy(pos.market_id, limit_price, pnl)
            self._mark_sold(pos.market_id, limit_price)
        except Exception as exc:
            log.error(
                f"BOND_EXIT_FAILED market={pos.market_id[:8]} error={exc}", exc_info=True
            )

    # ── Ledger helpers ────────────────────────────────────────────

    def _load_positions(self) -> list[BondPosition]:
        if not self._ledger_path.exists():
            return []
        try:
            data = json.loads(self._ledger_path.read_text(encoding="utf-8"))
            return [BondPosition(**p) for p in data.get("positions", [])]
        except (json.JSONDecodeError, TypeError) as exc:
            log.error(f"exit_mgr: failed to read ledger: {exc}")
            return []

    def _save_positions(self, positions: list[BondPosition]) -> None:
        """Atomic write: write to .tmp then rename (prevents corruption on crash)."""
        self._ledger_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(self._ledger_path) + ".tmp")
        payload = json.dumps(
            {"positions": [asdict(p) for p in positions]}, indent=2
        )
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(self._ledger_path)

    def _mark_sold(self, market_id: str, exit_price: float) -> None:
        positions = self._load_positions()
        for p in positions:
            if p.market_id == market_id and p.status == STATUS_OPEN:
                p.status     = STATUS_SOLD
                p.exit_price = exit_price
                p.exit_time  = datetime.now(timezone.utc).isoformat()
        self._save_positions(positions)

    def _mark_resolved(self, market_id: str, exit_price: float) -> None:
        positions = self._load_positions()
        for p in positions:
            if p.market_id == market_id and p.status == STATUS_OPEN:
                p.status     = STATUS_RESOLVED
                p.exit_price = exit_price
                p.exit_time  = datetime.now(timezone.utc).isoformat()
        self._save_positions(positions)

    # ── External data fetches ─────────────────────────────────────

    async def _get_current_price(self, token_id: str) -> float:
        """Fetch best ask from CLOB order book. Returns 0.0 on failure."""
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
            log.debug(f"exit_mgr: price fetch failed for {token_id[:12]}: {exc}")
        return 0.0

    async def _fetch_market_data(self, market_id: str) -> dict:
        """Fetch raw Gamma market dict. Returns {} on failure."""
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"{GAMMA_API}/markets/{market_id}", timeout=timeout
                ) as resp:
                    resp.raise_for_status()
                    return await resp.json()
        except Exception as exc:
            log.debug(f"exit_mgr: market data fetch failed for {market_id[:8]}: {exc}")
        return {}

    def _hours_to_resolution(self, resolution_time: str) -> float:
        """Hours until the market's resolution time (ISO8601 string stored at entry)."""
        try:
            resolution = datetime.fromisoformat(resolution_time)
            return max(0.0, (resolution - datetime.now(timezone.utc)).total_seconds() / 3600)
        except Exception:
            return 999.0  # unknown — don't gate on gas floor

    def _yes_resolution_price(self, market_data: dict) -> float:
        """
        Return the actual payout price for the YES token (1.0 = win, 0.0 = loss).

        NegRisk weather markets never set tokens[].winner or resolved=True.
        Instead outcomePrices snaps to ~1.0/~0.0 once the result is known.
        Both 'outcomes' and 'outcomePrices' arrive as JSON-encoded strings.
        """
        # Shape 1: tokens list with explicit winner flag
        tokens = market_data.get("tokens", [])
        for tok in tokens:
            if str(tok.get("outcome", "")).lower() in ("yes", "1"):
                winner = tok.get("winner")
                if winner is True:
                    return 1.0
                if winner is False:
                    return 0.0

        # Shape 2: top-level resolution string
        resolution = str(market_data.get("resolution", "")).upper()
        if resolution == "YES":
            return 1.0
        if resolution == "NO":
            return 0.0

        # Shape 3: outcomePrices snapped to ~1.0 (NegRisk / weather markets)
        try:
            raw_outcomes = market_data.get("outcomes", "[]")
            raw_prices   = market_data.get("outcomePrices", "[]")
            outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
            prices   = json.loads(raw_prices)   if isinstance(raw_prices,   str) else raw_prices
            for outcome, price in zip(outcomes, prices):
                if float(price) >= 0.99:
                    return 1.0 if str(outcome).lower() in ("yes", "1") else 0.0
        except Exception:
            pass

        log.debug("exit_mgr: could not determine YES winner from market data, defaulting to 0.0")
        return 0.0

    def _no_resolution_price(self, market_data: dict) -> float:
        """
        Return the actual payout for the NO token (1.0 = win, 0.0 = loss).
        Exact mirror of _yes_resolution_price() targeting the NO/0 outcome.
        """
        # Shape 1: tokens list with explicit winner flag
        tokens = market_data.get("tokens", [])
        for tok in tokens:
            if str(tok.get("outcome", "")).lower() in ("no", "0"):
                winner = tok.get("winner")
                if winner is True:
                    return 1.0
                if winner is False:
                    return 0.0

        # Shape 2: top-level resolution string
        resolution = str(market_data.get("resolution", "")).upper()
        if resolution == "NO":
            return 1.0
        if resolution == "YES":
            return 0.0

        # Shape 3: outcomePrices snapped to ~1.0 (NegRisk / weather markets)
        try:
            raw_outcomes = market_data.get("outcomes", "[]")
            raw_prices   = market_data.get("outcomePrices", "[]")
            outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
            prices   = json.loads(raw_prices)   if isinstance(raw_prices,   str) else raw_prices
            for outcome, price in zip(outcomes, prices):
                if float(price) >= 0.99:
                    return 1.0 if str(outcome).lower() in ("no", "0") else 0.0
        except Exception:
            pass

        log.debug("exit_mgr: could not determine NO winner from market data, defaulting to 0.0")
        return 0.0
