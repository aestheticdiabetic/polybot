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
    # Markets to watch — Polymarket tags use "5M", "15M", "1H", "24H"
    target_windows: list = field(default_factory=lambda: ["5M", "15M", "1H", "24H"])
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

    # Near-bracket threshold — when combined ask crosses this value (heading down
    # toward bracket_threshold), the trader pre-warms the CLOB metadata cache and
    # pre-signs both orders so the critical path at actual threshold is just one POST.
    near_bracket_threshold: float = 0.985

    # Position sizing
    position_size_usdc: float = 5.0    # USDC per leg; total deployed per bracket = 2x this
    max_position_size_usdc: float = 50.0
    min_position_size_usdc: float = 5.0

    # Risk controls
    max_concurrent_brackets: int = 20
    max_wallet_exposure_pct: float = 0.60    # never deploy >60% of wallet
    cancel_unfilled_after_s: int = 30        # cancel stale orders after 30s
    max_brackets_per_market: int = 1         # one bracket per market at a time
    pause_if_bracket_hz: float = 10.0        # min gap between brackets on same market = 60/hz seconds

    # Fees (Polymarket taker fee)
    # WARNING: crypto up/down markets appear to charge 1000bps (10%), not 100bps (1%).
    # This value drives the profitability cap (max_limit_sum = 1 - fee - tick) AND the
    # net profit estimate in the scanner.  If set too low, limits can allow fills at a
    # loss and net profit will be overstated.  Changing this also requires updating
    # bracket_threshold (break-even = 1 / (1 + fee); at 10% that's ~0.909, not 0.97).
    # Verify per-market via: GET /fee-rate?token_id=<id>
    taker_fee_pct: float = 0.01   # TODO: confirm 1% vs 10% before adjusting
    polygon_gas_gwei: float = 30  # estimated gas for redemption tx
    # Estimated gas cost per on-chain redemption (MATIC → USDC conversion).
    # Polygon at 30 gwei, ~150k gas, MATIC ~$0.50: ≈ $0.002.  Used in live net estimate.
    gas_fee_live_usdc: float = 0.002

    # Order type for live entry orders
    order_type: str = "FOK"   # Fill-Or-Kill: fills immediately or auto-cancels

    # Emergency exit: if one FOK leg fills but the other cancels, sell the
    # filled leg at bid minus this slippage buffer to exit cleanly.
    emergency_exit_slippage_pct: float = 0.02   # accept up to 2% below bid

    # After a partial fill (one leg filled, other cancelled), block re-entry on
    # that market for this many seconds — covers the full emergency exit window
    # plus a safety margin so we don't compound losses on a broken book.
    partial_fill_cooldown_s: int = 90

    # Extra ticks of limit headroom given to the DOWN leg beyond the equal-split.
    # DOWN books are structurally thinner than UP (consensus side has fewer sellers),
    # so the FOK needs more room to sweep through additional price levels.
    # Each tick = $0.01/share. Presigned orders age 10-15s by bracket time, so we need
    # extra margin to handle price drift. At 5 ticks: worst-case combined = threshold + 0.05,
    # still < 1.0 and profitable. Tradeoff: slightly wider limits > presigned order failures.
    down_extra_ticks: int = 5

    # Parallel order submission (hybrid approach)
    # Enable parallel submission when order book depth is sufficient.
    # Parallel reduces latency from ~723ms → ~360ms but increases partial fill risk.
    # Only enabled when both sides have depth_threshold × shares available.
    parallel_submission_enabled: bool = True
    parallel_depth_threshold_multiplier: float = 1.5  # require 150% of shares on each side

STRATEGY = StrategyConfig()

# ─── Maker pre-positioning (DOWN GTC queue priority) ──────────────
@dataclass
class MakerPositioningConfig:
    # Enable resting GTC DOWN orders posted at near-bracket to gain queue priority
    enabled: bool = True

    # Resting DOWN order posted at this limit price:
    # limit = bracket_threshold - ask_up + maker_margin_pct
    # So if DOWN fills at this price + UP fills at current ask, combined ≈ threshold
    # Set to 0.0 for breakeven, or negative for slightly aggressive (e.g., -0.01)
    maker_margin_pct: float = 0.0

    # How long to wait (in seconds) after posting the DOWN GTC before cancelling if
    # bracket threshold hasn't fired. Prevents orphaned resting orders.
    down_gtc_timeout_s: int = 10

    # Only post maker GTC if current DOWN depth < this threshold (in multiples of our size).
    # If DOWN already has 3x our shares at good prices, we don't need to be a maker.
    min_down_depth_for_maker_x: float = 2.0

MAKER = MakerPositioningConfig()

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

# ─── Bonding mode ─────────────────────────────────────────────────
BOT_MODE = os.getenv("BOT_MODE", "ARBI")   # "ARBI" | "BOND"

BOND_LOG_FILE    = os.getenv("BOND_LOG_FILE",    "/app/logs/polybot_bond.log")
BOND_LEDGER_FILE = os.getenv("BOND_LEDGER_FILE", "/app/logs/bonding_positions.json")

# Entry thresholds
BOND_MIN_EV_CORE       = 0.02   # min expected value per share, core tier
BOND_MIN_EV_SECONDARY  = 0.01   # min EV, secondary tier
BOND_CONFIDENCE_FLOOR  = 0.20   # min forecast probability to enter CORE tier
                                 # (ensemble model: 30 members → max ~33% per 2°F bucket)
BOND_EDGE_FLOOR        = 0.15   # min gap between true probability and market ask

# Position sizing
BOND_SHARES_CORE             = 25    # shares for core bonds
BOND_SHARES_SECONDARY        = 15    # shares for secondary positions
BOND_SHARES_WING             = 20    # shares for wing bets (cheap, more shares)
BOND_MAX_CAPITAL_PER_CLUSTER = 4.00  # max $ across all buckets for one city/date

# Exit thresholds
BOND_EARLY_EXIT_PRICE     = 0.97  # sell core when price hits this
BOND_WING_EXIT_MULTIPLIER = 5.0   # sell wing if price >= cost × this
BOND_WING_MIN_ABS_GAIN    = 2.00  # AND absolute gain >= this value (USD)
BOND_GAS_FLOOR_HOURS      = 4     # don't exit within N hours of resolution

# Scanner settings
BOND_POLL_INTERVAL_SECS  = 360   # seconds between full market scans
BOND_MAX_MARKETS_PER_RUN = 150   # max orders placed per scan cycle

# City list with (lat, lon) — extend as forecast accuracy is validated
BOND_CITIES: dict[str, tuple[float, float]] = {
    "Tokyo":         (35.6762,  139.6503),
    "London":        (51.5074,   -0.1278),
    "New York":      (40.7128,  -74.0060),
    "Los Angeles":   (34.0522, -118.2437),
    "Chicago":       (41.8781,  -87.6298),
    "Sydney":       (-33.8688,  151.2093),
    "Munich":        (48.1351,   11.5820),
    "Paris":         (48.8566,    2.3522),
    "Dubai":         (25.2048,   55.2708),
    "Singapore":      (1.3521,  103.8198),
    "Seoul":         (37.5665,  126.9780),
    "Istanbul":      (41.0082,   28.9784),
    "Ankara":        (39.9334,   32.8597),
    "Chengdu":       (30.5728,  104.0668),
    "Busan":         (35.1796,  129.0756),
    "Seattle":       (47.6062, -122.3321),
    "Miami":         (25.7617,  -80.1918),
    "Toronto":       (43.6532,  -79.3832),
    "Berlin":        (52.5200,   13.4050),
    "Amsterdam":     (52.3676,    4.9041),
    # Extended — common in Polymarket weather markets
    "Jakarta":       (-6.2088,  106.8456),
    "Helsinki":      (60.1699,   24.9384),
    "Chongqing":     (29.5630,  106.5516),
    "Kuala Lumpur":   (3.1390,  101.6869),
    "Wellington":   (-41.2865,  174.7762),
    "Sao Paulo":    (-23.5505,  -46.6333),
    "Buenos Aires": (-34.6037,  -58.3816),
    "Mexico City":   (19.4326,  -99.1332),
    "Mumbai":        (19.0760,   72.8777),
    "Delhi":         (28.6139,   77.2090),
    "Shanghai":      (31.2304,  121.4737),
    "Beijing":       (39.9042,  116.4074),
    "Lagos":          (6.5244,    3.3792),
    "Cairo":         (30.0444,   31.2357),
    "Nairobi":       (-1.2921,   36.8219),
    "Johannesburg": (-26.2041,   28.0473),
    "Rio de Janeiro":(-22.9068,  -43.1729),
    "Bogota":         (4.7110,  -74.0721),
    "Lima":         (-12.0464,  -77.0428),
    "Bangkok":       (13.7563,  100.5018),
    "Ho Chi Minh":   (10.8231,  106.6297),
    "Manila":        (14.5995,  120.9842),
    "Osaka":         (34.6937,  135.5023),
    "Taipei":        (25.0330,  121.5654),
    "Hong Kong":     (22.3193,  114.1694),
    "Karachi":       (24.8607,   67.0011),
    "Lahore":        (31.5204,   74.3587),
    "Dhaka":         (23.8103,   90.4125),
    "Colombo":        (6.9271,   79.8612),
}

# City name aliases — maps Polymarket's naming variants to canonical names above
BOND_CITY_ALIASES: dict[str, str] = {
    # New York variants
    "NYC": "New York", "NY": "New York", "New_York": "New York",
    "New York City": "New York", "New York, NY": "New York",
    "New York, USA": "New York",
    # Los Angeles variants
    "LA": "Los Angeles", "Los_Angeles": "Los Angeles",
    "Los Angeles, CA": "Los Angeles",
    # Country-qualified names
    "Tokyo, Japan": "Tokyo",
    "London, UK": "London", "London, England": "London",
    "London, United Kingdom": "London",
    "Paris, France": "Paris",
    "Berlin, Germany": "Berlin",
    "Seoul, South Korea": "Seoul",
    "Seoul, Korea": "Seoul",
    "Sydney, Australia": "Sydney",
    "Toronto, Canada": "Toronto",
    "Dubai, UAE": "Dubai",
    "Singapore, Singapore": "Singapore",
    "Istanbul, Turkey": "Istanbul",
    # Sao Paulo — Polymarket omits the tilde
    "São Paulo": "Sao Paulo",
    "Sao Paulo, Brazil": "Sao Paulo",
    "São Paulo, Brazil": "Sao Paulo",
    # Other common variants
    "Kuala Lumpur, Malaysia": "Kuala Lumpur", "KL": "Kuala Lumpur",
    "Hong Kong, China": "Hong Kong", "Hong Kong SAR": "Hong Kong",
    "Ho Chi Minh City": "Ho Chi Minh", "HCMC": "Ho Chi Minh",
    "Bogotá": "Bogota",
    "México City": "Mexico City", "Mexico City, Mexico": "Mexico City",
    "Mumbai, India": "Mumbai", "Bombay": "Mumbai",
    "Delhi, India": "Delhi", "New Delhi": "Delhi",
    "Buenos Aires, Argentina": "Buenos Aires",
    "Rio de Janeiro, Brazil": "Rio de Janeiro", "Rio": "Rio de Janeiro",
    "Jakarta, Indonesia": "Jakarta",
    "Manila, Philippines": "Manila",
    "Bangkok, Thailand": "Bangkok",
    "Osaka, Japan": "Osaka",
    "Taipei, Taiwan": "Taipei",
    "Beijing, China": "Beijing", "Peking": "Beijing",
    "Shanghai, China": "Shanghai",
    "Johannesburg, South Africa": "Johannesburg", "Joburg": "Johannesburg",
    "Nairobi, Kenya": "Nairobi",
    "Lagos, Nigeria": "Lagos",
    "Cairo, Egypt": "Cairo",
    "Lima, Peru": "Lima",
    "Bogota, Colombia": "Bogota",
}

# ─── Dashboard ────────────────────────────────────────────────────
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 8080
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", "changeme")  # basic auth password

# ─── Logging ──────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE  = "/app/logs/polybot.log"
TRADE_LOG = "/app/logs/trades.jsonl"
