"""
config.py — All tunable parameters in one place.
Edit this file before deploying. Never commit credentials.
"""
import os
from dataclasses import dataclass, field
from typing import Optional

# ─── Polymarket CLOB ───────────────────────────────────────────────
CLOB_HOST = "https://clob.polymarket.com"
CLOB_WS   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAIN_ID  = 137  # Polygon mainnet

# ─── Credentials (set via .env or environment variables) ──────────
PRIVATE_KEY      = os.getenv("PRIVATE_KEY", "")
FUNDER_ADDRESS   = os.getenv("FUNDER_ADDRESS", "")   # Polymarket proxy wallet
API_KEY          = os.getenv("POLY_API_KEY", "")
API_SECRET       = os.getenv("POLY_API_SECRET", "")
API_PASSPHRASE   = os.getenv("POLY_API_PASSPHRASE", "")
ALCHEMY_API_KEY  = os.getenv("ALCHEMY_API_KEY", "")
ALCHEMY_WS       = f"wss://polygon-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY', '')}"
ALCHEMY_RPC      = f"https://polygon-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY', '')}"

# ─── Strategy parameters ──────────────────────────────────────────
@dataclass
class StrategyConfig:
    # Markets to watch — Polymarket tags use "5M", "1H", "24H"
    target_windows: list = field(default_factory=lambda: ["5M", "1H", "24H"])
    # All major crypto assets tracked by Polymarket up/down markets
    target_assets:  list = field(default_factory=lambda: [
        "BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "DOGE",
        "MATIC", "POL", "LINK", "DOT", "UNI", "LTC", "ATOM",
    ])

    # Bracket detection threshold
    # Buy bracket when: ask(Up) + ask(Down) < bracket_threshold
    # Fees = 1% taker × 2 legs = 2% total. Break-even = 0.98.
    # 0.97 → 3% spread → ~1% net margin after fees.
    bracket_threshold: float = 0.97

    # Position sizing
    position_size_usdc: float = 10.0   # USDC per leg (both legs = 2x this)
    max_position_size_usdc: float = 50.0
    min_position_size_usdc: float = 5.0

    # Risk controls
    max_concurrent_brackets: int = 20
    max_wallet_exposure_pct: float = 0.60    # never deploy >60% of wallet
    cancel_unfilled_after_s: int = 30        # cancel stale orders after 30s
    max_brackets_per_market: int = 1         # one bracket per market at a time
    pause_if_bracket_hz: float = 5.0         # pause if >5 brackets/min on same market

    # Fees (Polymarket taker fee)
    taker_fee_pct: float = 0.01   # 1%
    polygon_gas_gwei: float = 30  # estimated gas for redemption tx

    # Order type
    order_type: str = "GTC"   # Good Till Cancelled limit orders

STRATEGY = StrategyConfig()

# ─── Simulation parameters ────────────────────────────────────────
@dataclass
class SimConfig:
    enabled: bool = False
    starting_balance_usdc: float = 1000.0
    # Latency model — adds realistic delay to simulated CLOB calls
    # Helsinki → London p50 latency
    simulated_latency_ms_p50: float = 32.0
    simulated_latency_ms_p99: float = 85.0
    # Polygon confirmation time simulation
    simulated_polygon_confirm_s: float = 2.5
    # Slippage model — occasionally miss fills
    fill_probability: float = 0.92     # 92% of brackets actually fill both legs
    # Fee simulation
    include_gas_fees: bool = True
    gas_fee_usdc_per_redemption: float = 0.002  # ~$0.002 on Polygon at 30 gwei

SIM = SimConfig()

# ─── Dashboard ────────────────────────────────────────────────────
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 8080
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "changeme")  # basic auth password

# ─── Logging ──────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE  = "/app/logs/polybot.log"
TRADE_LOG = "/app/logs/trades.jsonl"
