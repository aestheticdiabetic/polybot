"""
state.py — Shared in-memory state manager.
Single source of truth for balance, open brackets, and performance metrics.
Thread-safe via asyncio locks.
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

from config import SIM, STRATEGY

log = logging.getLogger("state")


@dataclass
class PerformanceMetrics:
    # Totals
    total_brackets: int = 0
    won: int = 0
    lost: int = 0
    cancelled: int = 0
    # PnL
    total_gross_usdc: float = 0.0
    total_fees_usdc: float = 0.0
    total_gas_usdc: float = 0.0
    total_net_usdc: float = 0.0
    # Win rate
    win_rate: float = 0.0
    # Avg metrics
    avg_spread: float = 0.0
    avg_net_per_bracket: float = 0.0
    avg_latency_ms: float = 0.0
    # Session
    session_start: float = field(default_factory=time.time)
    uptime_s: float = 0.0
    # Rolling 1h
    brackets_last_1h: int = 0
    net_last_1h: float = 0.0


class StateManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._balance: float = SIM.starting_balance_usdc if SIM.enabled else 0.0
        self._open_brackets: Dict[str, dict] = {}
        self._closed_brackets: List[dict] = []
        self._redemptions: List[dict] = []
        self._bot_running: bool = False
        self._mode: str = "sim" if SIM.enabled else "live"
        self._metrics = PerformanceMetrics()
        self._latencies: List[float] = []
        self._spreads: List[float] = []
        self._hourly_buckets: List[dict] = []   # rolling hourly stats
        self._session_start = time.time()
        self._scanner_stats: dict = {}
        self._trader_stats: dict = {}
        self._redeemer_stats: dict = {}

    # ── Balance ──────────────────────────────────────────────────

    def get_balance(self) -> float:
        return self._balance

    def set_balance(self, amount: float):
        self._balance = amount

    def update_balance(self, delta: float):
        self._balance += delta

    # ── Brackets ─────────────────────────────────────────────────

    def add_bracket(self, bracket):
        d = {
            "id": bracket.id,
            "title": bracket.market_title,
            "asset": bracket.asset,
            "window": bracket.window,
            "spread": bracket.detected_spread,
            "expected_net": bracket.expected_net_usdc,
            "ask_up": bracket.leg_up.price,
            "ask_down": bracket.leg_down.price,
            "size_usdc": STRATEGY.position_size_usdc,
            "opened_at": bracket.opened_at,
            "status": "open",
            "latency_ms": bracket.latency_ms,
            "sim": bracket.sim_mode,
        }
        self._open_brackets[bracket.id] = d
        self._metrics.total_brackets += 1
        if bracket.latency_ms:
            self._latencies.append(bracket.latency_ms)
        if bracket.detected_spread:
            self._spreads.append(bracket.detected_spread)
        self._update_rolling_metrics()

    def close_bracket(self, bracket_id: str, net_usdc: float):
        if bracket_id not in self._open_brackets:
            return
        b = self._open_brackets.pop(bracket_id)
        b["actual_net"] = net_usdc
        b["closed_at"] = time.time()
        b["status"] = "won" if net_usdc > 0 else ("lost" if net_usdc < 0 else "cancelled")

        self._closed_brackets.append(b)
        # Keep last 500 closed brackets in memory
        if len(self._closed_brackets) > 500:
            self._closed_brackets = self._closed_brackets[-500:]

        if net_usdc > 0:
            self._metrics.won += 1
        elif net_usdc < 0:
            self._metrics.lost += 1
        else:
            self._metrics.cancelled += 1

        self._metrics.total_net_usdc += net_usdc
        self._update_rolling_metrics()

    def update_bracket_status(self, bracket_id: str, status: str):
        if bracket_id in self._open_brackets:
            self._open_brackets[bracket_id]["status"] = status

    def record_redemption(self, condition_id: str, amount: float):
        self._redemptions.append({
            "condition_id": condition_id,
            "amount": amount,
            "ts": time.time(),
        })

    # ── Bot control ──────────────────────────────────────────────

    def set_running(self, running: bool):
        self._bot_running = running

    def is_running(self) -> bool:
        return self._bot_running

    def get_mode(self) -> str:
        return self._mode

    def set_mode(self, mode: str):
        self._mode = mode

    # ── Component stats ─────────────────────────────────────────

    def update_scanner_stats(self, stats: dict):
        self._scanner_stats = stats

    def update_trader_stats(self, stats: dict):
        self._trader_stats = stats

    def update_redeemer_stats(self, stats: dict):
        self._redeemer_stats = stats

    # ── Metrics snapshot for dashboard ──────────────────────────

    def get_dashboard_data(self) -> dict:
        self._update_rolling_metrics()
        resolved = self._metrics.won + self._metrics.lost
        win_rate = (self._metrics.won / resolved * 100) if resolved > 0 else 0

        # Last 1h brackets
        cutoff = time.time() - 3600
        recent = [b for b in self._closed_brackets if b.get("closed_at", 0) > cutoff]
        net_1h = sum(b.get("actual_net", 0) for b in recent)

        # Recent trades list for dashboard
        recent_trades = sorted(
            list(self._open_brackets.values()) + self._closed_brackets[-50:],
            key=lambda x: x.get("opened_at", 0),
            reverse=True
        )[:50]

        return {
            "mode": self._mode,
            "running": self._bot_running,
            "balance": round(self._balance, 4),
            "uptime_s": round(time.time() - self._session_start),
            "metrics": {
                "total_brackets": self._metrics.total_brackets,
                "open_brackets": len(self._open_brackets),
                "won": self._metrics.won,
                "lost": self._metrics.lost,
                "cancelled": self._metrics.cancelled,
                "win_rate": round(win_rate, 1),
                "total_net_usdc": round(self._metrics.total_net_usdc, 4),
                "total_fees_usdc": round(self._metrics.total_fees_usdc, 4),
                "total_gas_usdc": round(self._metrics.total_gas_usdc, 4),
                "avg_spread": round(sum(self._spreads[-100:]) / max(len(self._spreads[-100:]), 1) * 100, 3),
                "avg_latency_ms": round(sum(self._latencies[-100:]) / len(self._latencies[-100:]), 1) if self._latencies else None,
                "brackets_last_1h": len(recent),
                "net_last_1h": round(net_1h, 4),
            },
            "scanner": self._scanner_stats,
            "trader": self._trader_stats,
            "redeemer": self._redeemer_stats,
            "open_brackets": list(self._open_brackets.values()),
            "recent_trades": recent_trades,
        }

    def _update_rolling_metrics(self):
        resolved = self._metrics.won + self._metrics.lost
        if resolved > 0:
            self._metrics.win_rate = self._metrics.won / resolved
        if self._latencies:
            self._metrics.avg_latency_ms = sum(self._latencies[-100:]) / len(self._latencies[-100:])
        if self._spreads:
            self._metrics.avg_spread = sum(self._spreads[-100:]) / len(self._spreads[-100:])
