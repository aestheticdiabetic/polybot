"""
exit_manager.py — Async background task that monitors open bonding positions.

Fires limit sell orders when exit criteria are met (price targets, multipliers,
or gas-cost floors). Reads/writes a persistent JSON position ledger atomically.
"""
import asyncio
import json
import logging
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import config as _config
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

log = logging.getLogger("bond.exit")

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"

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
    temp_min_c: Optional[float] = None  # lower bound of temperature bucket (°C)
    temp_max_c: Optional[float] = None  # upper bound of temperature bucket (°C)
    exit_price: Optional[float] = None  # filled on SOLD
    exit_time: Optional[str]   = None   # filled on SOLD (ISO8601)


class ExitManager:
    """
    Background asyncio task. Polls open positions every 60 seconds.
    Applies exit decision tree and places limit sell orders via CLOB.
    """

    def __init__(self, client: ClobClient):
        self._client      = client
        self._ledger_path = Path(_config.BOND_LEDGER_FILE)
        self._feed        = None   # set via set_price_feed() after feed construction

    def set_price_feed(self, feed) -> None:
        """Wire the price feed so confidence exits can read live forecasts."""
        self._feed = feed

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
        # Avoid duplicates on restart
        if any(p.market_id == pos.market_id for p in positions):
            log.debug(f"exit_mgr: position {pos.market_id[:8]} already in ledger, skipping add")
            return
        positions.append(pos)
        self._save_positions(positions)
        log.info(
            f"BOND_LEDGER_ADD city={pos.city} tier={pos.tier} "
            f"shares={pos.shares} entry={pos.entry_price:.4f} "
            f"market={pos.market_id[:8]}"
        )

    # ── Core exit check ───────────────────────────────────────────

    async def _check_exits(self) -> None:
        positions = self._load_positions()
        open_pos  = [p for p in positions if p.status == STATUS_OPEN]
        if not open_pos:
            return

        for pos in open_pos:
            market_data = await self._fetch_market_data(pos.market_id)
            hours_left  = self._hours_to_resolution(market_data)

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
                self._mark_resolved(pos.market_id, exit_price)
                continue

            current_price = await self._get_current_price(pos.token_id)
            # Stop loss: only within BOND_STOP_LOSS_HOURS of market closure (gate hour).
            if 0 < current_price <= pos.entry_price * _config.BOND_STOP_LOSS_RATIO:
                if self._hours_until_closure(pos) <= _config.BOND_STOP_LOSS_HOURS:
                    await self._execute_sell(pos, current_price, reason="STOP_LOSS")
            elif self._should_exit(pos, current_price, hours_left):
                await self._execute_sell(pos, current_price)
            elif await self._check_confidence_exits(pos, market_data, current_price, hours_left):
                await self._execute_sell(pos, current_price, reason="CONF_EXIT")
            else:
                log.debug(
                    f"BOND_EXIT_SKIPPED market={pos.market_id[:8]} tier={pos.tier} "
                    f"price={current_price:.4f} hours={hours_left:.1f}"
                )

    def _should_exit(
        self, pos: BondPosition, price: float, hours: float
    ) -> bool:
        """
        Exit decision tree:

        1. Hours to resolution < BOND_GAS_FLOOR_HOURS → HOLD (gas not worth it)
        2. CORE: price >= BOND_EARLY_EXIT_PRICE → SELL (near-certainty)
        3. Any tier: price >= entry * 10 → SELL (10× windfall)
        4. CHEAP: price >= entry * BOND_CHEAP_EXIT_MULTIPLIER
                  AND gain >= BOND_CHEAP_MIN_ABS_GAIN → SELL
        5. CERTAIN is held to resolution (excluded from rules 2 and 4)
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

    async def _check_confidence_exits(
        self,
        pos: BondPosition,
        market_data: dict,
        current_price: float,
        hours_left: float,
    ) -> bool:
        """
        Weather-confidence exit check using real-time Open-Meteo observations.

        Only fires for same-day markets within the monitoring window (10:00–gate_hour local).
        Gate sequence:
          0. Feed available and position has temp bounds stored
          1. Gas floor: skip if hours_left < BOND_GAS_FLOOR_HOURS
          2. Same-day: skip if market resolves on a future date
          3. Monitoring window: 10 ≤ local_hour < get_gate_hour(city, ...)
          4. Min proceeds: skip if current_price × shares < BOND_CONF_EXIT_MIN_PROCEEDS
          5a. Profit-lock (any tier): price ≥ entry × MULT and prob drop ≥ PROFIT_DROP
          5b. Confidence drop (CERTAIN/CORE): drop ≥ threshold and current_prob < abs_floor
        """
        import zoneinfo
        import bonding.weather_client as _wc
        from bonding.peak_hour_stats import get_gate_hour

        # Gate 0: prerequisites
        if self._feed is None:
            return False
        if pos.temp_min_c is None or pos.temp_max_c is None:
            return False

        # Gate 1: gas floor
        if hours_left < _config.BOND_GAS_FLOOR_HOURS:
            return False

        # Gate 2: same-day only
        tz_name = _config.BOND_CITY_TIMEZONES.get(pos.city)
        if not tz_name:
            return False
        try:
            tz = zoneinfo.ZoneInfo(tz_name)
        except Exception:
            return False

        now_local   = datetime.now(tz)
        target_date = date.fromisoformat(pos.resolution_time[:10])
        if target_date != now_local.date():
            return False

        # Gate 3: monitoring window [BOND_CONF_MONITORING_START_HOUR, gate_hour)
        local_hour = now_local.hour
        if local_hour < _config.BOND_CONF_MONITORING_START_HOUR:
            return False

        consensus = self._feed._forecasts.get((pos.city, target_date))
        if consensus is None:
            return False

        gate_hour = get_gate_hour(
            pos.city,
            consensus.gfs.forecast_peak_hour,
            now_local.month,
            _wc._peak_hour_stats,
        )
        if local_hour >= gate_hour:
            return False

        # Gate 4: minimum proceeds
        if current_price * pos.shares < _config.BOND_CONF_EXIT_MIN_PROCEEDS:
            return False

        # Fetch real-time observation
        coords = _config.BOND_CITIES.get(pos.city)
        if not coords:
            return False
        lat, lon = coords

        current_temp = await _wc.get_current_observation(pos.city, lat, lon)
        if current_temp is None:
            return False

        current_prob = _wc.prob_with_current_obs(
            consensus.gfs, current_temp, pos.temp_min_c, pos.temp_max_c
        )
        prob_drop = pos.prob - current_prob

        log.debug(
            f"BOND_CONF_CHECK market={pos.market_id[:8]} city={pos.city} "
            f"tier={pos.tier} entry_prob={pos.prob:.3f} current_prob={current_prob:.3f} "
            f"drop={prob_drop:.3f} obs_temp={current_temp:.1f}°C"
        )

        # Gate 5a: profit-lock (any tier)
        if (
            current_price >= pos.entry_price * _config.BOND_CONF_PROFIT_MULT
            and prob_drop >= _config.BOND_CONF_PROFIT_DROP
        ):
            log.info(
                f"BOND_CONF_PROFITLOCK market={pos.market_id[:8]} city={pos.city} "
                f"tier={pos.tier} entry={pos.entry_price:.4f} current={current_price:.4f} "
                f"prob_drop={prob_drop:.3f} obs_temp={current_temp:.1f}°C"
            )
            return True

        # Gate 5b: confidence drop (CERTAIN/CORE only — CHEAP held without confidence exit)
        if pos.tier == TIER_CERTAIN:
            threshold = _config.BOND_CONF_CERTAIN_DROP
            abs_floor  = _config.BOND_CONF_CERTAIN_ABS
        elif pos.tier == TIER_CORE:
            threshold = _config.BOND_CONF_CORE_DROP
            abs_floor  = _config.BOND_CONF_CORE_ABS
        else:
            return False

        if prob_drop >= threshold and current_prob < abs_floor:
            log.info(
                f"BOND_CONF_EXIT market={pos.market_id[:8]} city={pos.city} "
                f"tier={pos.tier} entry_prob={pos.prob:.3f} current_prob={current_prob:.3f} "
                f"drop={prob_drop:.3f} threshold={threshold:.2f} "
                f"abs_floor={abs_floor:.2f} obs_temp={current_temp:.1f}°C"
            )
            return True

        return False

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

    def _hours_until_closure(self, pos: BondPosition) -> float:
        """
        Hours until market closure = gate_hour (hottest hour + 1) in city local time.
        Uses forecast peak hour from price feed when available; falls back to P75 only.
        Returns 0.0 if gate has already passed today, 999.0 if city/timezone unknown.
        """
        import zoneinfo
        import bonding.weather_client as _wc
        from bonding.peak_hour_stats import get_gate_hour

        tz_name = _config.BOND_CITY_TIMEZONES.get(pos.city)
        if not tz_name:
            return 999.0
        try:
            tz = zoneinfo.ZoneInfo(tz_name)
        except Exception:
            return 999.0

        now_local = datetime.now(tz)
        stats = getattr(_wc, "_peak_hour_stats", {}) or {}

        # Use forecast peak hour from feed if available (same as confidence exits)
        forecast_peak_hour = None
        if self._feed is not None:
            try:
                target_date = date.fromisoformat(pos.resolution_time[:10])
                consensus = self._feed._forecasts.get((pos.city, target_date))
                if consensus is not None and consensus.gfs is not None:
                    forecast_peak_hour = consensus.gfs.forecast_peak_hour
            except Exception:
                pass

        gate_hour = get_gate_hour(pos.city, forecast_peak_hour, now_local.month, stats)

        # Fractional hours until gate_hour (whole-hour boundary) in local time
        hours_until = gate_hour - now_local.hour - now_local.minute / 60
        return max(0.0, hours_until)

    def _hours_to_resolution(self, market_data: dict) -> float:
        """Hours until end of the target calendar day (UTC midnight of day+1).

        Gamma's end_date_iso is the date label at midnight start-of-day UTC, not the
        actual resolution timestamp. We extract the date portion only and compute hours
        to end-of-day (midnight of the following day UTC).
        """
        for key in ("end_date_iso", "endDateIso", "endDate", "end_date"):
            val = market_data.get(key)
            if val:
                try:
                    date_str = str(val)[:10]  # "2026-04-08"
                    end_of_day = datetime.fromisoformat(
                        date_str + "T00:00:00+00:00"
                    ) + timedelta(days=1)
                    return max(0.0, (end_of_day - datetime.now(timezone.utc)).total_seconds() / 3600)
                except ValueError:
                    continue
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
