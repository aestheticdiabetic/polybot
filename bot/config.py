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
    polygon_gas_gwei: float = 130  # estimated gas price in gwei (Polygon, April 2026)
    # Estimated gas cost per on-chain sell: ~207k gas × 128 gwei × $0.086/POL ≈ $0.004.
    gas_fee_live_usdc: float = 0.004

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

BOND_LOG_FILE       = os.getenv("BOND_LOG_FILE",       "/app/logs/polybot_bond.log")
BOND_LEDGER_FILE    = os.getenv("BOND_LEDGER_FILE",    "/app/logs/bonding_positions.json")
BOND_EVENT_LOG_FILE = os.getenv("BOND_EVENT_LOG_FILE", "/app/logs/bond_events.jsonl")

# Entry thresholds
BOND_MIN_EDGE_CHEAP    = 0.07   # min edge (prob - ask) for CHEAP tier (2-8¢ tokens)
BOND_MIN_EDGE_CORE     = 0.15   # min edge (prob - ask) for CORE tier (8-30¢ tokens)

# Source quality gates — require multi-source agreement before entry.
# Filters out bets where forecasts are uncertain or sources disagree.
BOND_CHEAP_MIN_SOURCES:      int   = 2    # met sources needed (GFS, ECMWF, TIO)
BOND_CHEAP_MAX_SOURCE_SPREAD_C: float = 4.0  # max °C between source point forecasts
BOND_CORE_MIN_SOURCES:       int   = 2    # met sources needed for CORE tier
BOND_CORE_MAX_SOURCE_SPREAD_C:  float = 3.0  # tighter spread for higher-stake CORE bets

# Position sizing
# CHEAP: adaptive shares = ceil(1.00 / ask), capped at BOND_SHARES_CHEAP_MAX
# CORE:  max(BOND_SHARES_CORE, ceil(1.00 / ask)) — ensures >= $1 order at low end of range
BOND_SHARES_CHEAP_MAX        = 75    # cap on adaptive share count for CHEAP tier
BOND_SHARES_CORE             = 10    # base share count for CORE tier
BOND_MAX_CAPITAL_PER_CLUSTER = 4.00  # max $ across all buckets for one city/date
BOND_MIN_GTC_ORDER_USDC      = 1.00  # minimum capital for a GTC limit order (CLOB rejects below this)

# Exit thresholds
BOND_EARLY_EXIT_PRICE      = 0.97  # sell when price hits near-certainty (both tiers)
BOND_CHEAP_EXIT_MULTIPLIER = 8.0   # sell CHEAP if price >= cost × this
BOND_CHEAP_MIN_ABS_GAIN    = 1.00  # AND absolute gain >= this value (USD)
BOND_GAS_FLOOR_HOURS       = 4     # don't exit within N hours of resolution
BOND_STOP_LOSS_RATIO              = 0.40  # exit if bid falls to this fraction of entry price
BOND_STOP_LOSS_HOURS              = 3     # only trigger if >N hours remain until resolution
BOND_STOP_LOSS_MIN_FILL_FRACTION  = 0.50  # require ≥50% of shares fillable at stop price (depth guard)
BOND_STOP_LOSS_CONFIRM_POLLS      = 2     # require condition true in N consecutive 60s polls before firing

# Confidence-based early exit thresholds (same-day current-obs monitoring)
BOND_CONF_CERTAIN_DROP:        float = 0.20  # CERTAIN: exit if prob drops ≥ this from entry
BOND_CONF_CORE_DROP:           float = 0.25  # CORE: exit if prob drops ≥ this from entry
BOND_CONF_CERTAIN_ABS:         float = 0.55  # CERTAIN: and current prob < this absolute floor
BOND_CONF_CORE_ABS:            float = 0.40  # CORE: and current prob < this absolute floor
BOND_CONF_PROFIT_MULT:         float = 2.0   # profit-lock: current_price >= entry × this
BOND_CONF_PROFIT_DROP:         float = 0.20  # profit-lock: and prob dropped ≥ this
BOND_CONF_EXIT_MIN_PROCEEDS:   float = 0.50  # min sell proceeds (USD) to justify gas cost
BOND_CONF_MONITORING_START_HOUR: int = 10    # earliest local hour to begin current-obs checks

BOND_MIN_ENTRY_HOURS      = 10    # FALLBACK ONLY — superseded by dynamic peak-hour gate
                                  # (peak_hour_stats.py + historical_peak_seeder.py).
                                  # Retained as documentation of the old threshold.

# ── Live-toggle filters (empty set = all enabled) ─────────────────────────────
# BOND_DISABLED_TIERS:          skip entry for tiers in this set (e.g. {"CHEAP"})
# BOND_DISABLED_SIDES:          skip entry for sides in this set (e.g. {"NO"})
# BOND_DISABLED_ENTRY_BUCKETS:  skip entry when hours-to-resolution falls in bucket
#   valid bucket labels: "0-10h", "10-20h", "20-30h", "30-48h", "48h+"
BOND_DISABLED_TIERS:          set = set()
BOND_DISABLED_SIDES:          set = set()
# Disable entries made 12+ hours before resolution — data shows negative PnL outside 0-12h window.
BOND_DISABLED_ENTRY_BUCKETS:  set = {"10-20h", "20-30h", "30-48h", "48h+"}

# ── Targeted NO bet restrictions ───────────────────────────────────────────────
# Data analysis (2026-04-14) showed CHEAP NO bets have 6.1% WR (-$16.03 total).
# CORE NO bets below 15¢ have 0% WR (-$22.63 total). CORE NO at 15-20¢ however
# have 46.7% WR (+$41.94) and are worth keeping.
BOND_CHEAP_NO_ENABLED:   bool  = False   # disable all CHEAP tier NO bets
BOND_CORE_YES_ENABLED:   bool  = False   # disable all CORE tier YES bets (11% WR, -$38.61 total)
BOND_CORE_NO_MIN_ASK:    float = 0.15    # skip CORE NO bets priced below this ask

# Market-implied confidence cap
# When our model probability exceeds (ask × this ratio), the model is disagreeing
# with the market by more than N-fold. Cap model prob to ask × ratio before
# computing EV. Markets pricing a side at 0.001 are near-certain; a 5x cap
# means we will never claim more than 0.5% probability for that side.
BOND_MARKET_DISAGREEMENT_RATIO = 2.5

# ─── Cross-source weather ─────────────────────────────────────────────────────
TOMORROW_IO_API_KEY           = os.getenv("TOMORROW_IO_API_KEY", "")
TOMORROW_IO_CACHE_TTL_SECS    = 21_600   # 6 hours — 74 cities / 6h = ~12 calls/hr, under 20/hr cap
TOMORROW_IO_MAX_REQ_PER_HOUR  = 20       # headroom below 25/hr hard limit
ECMWF_ENSEMBLE_MODEL          = "ecmwf_ifs025"  # 50 members, 0.25° global, free via Open-Meteo
ECMWF_DISK_CACHE_PATH         = os.environ.get("ECMWF_CACHE_PATH", "/app/data/ecmwf_cache.json")

# ─── CERTAIN tier ────────────────────────────────────────────────────────────
CERTAIN_ASK_MIN                  = 0.65   # min YES ask — market sees it as likely
CERTAIN_ASK_MAX                  = 0.95   # max YES ask — still room for edge
CERTAIN_MIN_SOURCE_PROB          = 0.80   # each source must reach this individually
CERTAIN_MAX_TEMP_DELTA_C         = 2.0    # max °C between source point forecasts
CERTAIN_MAX_SPREAD_C             = 1.5    # max std dev of all combined ensemble members
CERTAIN_MIN_CONSENSUS_PROB       = 0.90   # averaged probability floor
CERTAIN_MIN_SOURCES              = 3      # all three sources must be present
CERTAIN_MIN_EDGE                 = 0.05   # consensus_prob − ask
CERTAIN_SHARES                   = 20     # conservative during validation
CERTAIN_MAX_CAPITAL_PER_CLUSTER  = 20.00  # separate from BOND_MAX_CAPITAL_PER_CLUSTER

# Per-city forecast bias corrections (°C).
# Applied to daily_max_c before generating synthetic ensemble members.
# Populate using bot/calibrate_forecasts.py and update as new data arrives.
BOND_CITY_BIAS_CORRECTIONS: dict[str, float] = {
    # Generated by calibrate_forecasts.py — ERA5 actual vs target range centre.
    # Positive = actual temps warmer than forecast; negative = colder.
    # Last updated: 2026-04-12
    "Ankara":       1.0,
    "Buenos Aires": 0.5,
    "Busan":        1.5,
    "Chengdu":     -0.6,
    "Chongqing":   -1.0,
    "Hong Kong":   -0.9,
    "Istanbul":     0.8,
    "Lagos":        1.4,
    "Los Angeles":  2.4,
    "Lucknow":     -1.0,
    "Madrid":       0.7,
    "Miami":       -0.9,
    "Milan":        1.0,
    "Moscow":       1.2,
    "Munich":       0.7,
    "San Francisco":-0.8,
    "Seoul":        0.6,
    "Taipei":      -0.8,
    "Tokyo":        1.4,
    "Toronto":      1.8,
    "Warsaw":       0.6,
    "Wellington":   0.7,
    "Wuhan":       -1.1,
}

# Per-city, per-month additive bias corrections (°C).
# Applied ON TOP of BOND_CITY_BIAS_CORRECTIONS for the specific target month.
# Use calibrate_forecasts.py --month-specific to generate these.
# Format: {city: {month_int: correction_c}}  e.g. {"Munich": {4: 1.5, 5: 2.0}}
BOND_CITY_MONTHLY_BIAS_CORRECTIONS: dict[str, dict[int, float]] = {}

# ─── Statistical forecast (ARIMA/Naïve 4th source) ───────────────────────────
# Weight applied to the statistical source in consensus_prob() relative to each
# meteorological source (GFS, ECMWF, TIO each have weight 1.0).
# 0.5 means the statistical signal contributes half as much as one met source.
BOND_STATISTICAL_WEIGHT: float = 0.5

# Disk path for per-city daily max temperature history (JSON).
# Seeded with 2 years of archive data at startup; updated daily thereafter.
BOND_STATISTICAL_CACHE_PATH: str = os.environ.get(
    "STATISTICAL_CACHE_PATH", "/app/data/statistical_temp_cache.json"
)

# Scanner settings
BOND_POLL_INTERVAL_SECS  = 60    # seconds between REST market discovery scans (WS handles real-time pricing)
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
    # US cities active on Polymarket
    "San Francisco": (37.7749, -122.4194),
    "Houston":       (29.7604,  -95.3698),
    "Dallas":        (32.7767,  -96.7970),
    "Austin":        (30.2672,  -97.7431),
    "Denver":        (39.7392, -104.9903),
    "Atlanta":       (33.7490,  -84.3880),
    # Europe
    "Madrid":        (40.4168,   -3.7038),
    "Milan":         (45.4642,    9.1900),
    "Warsaw":        (52.2297,   21.0122),
    "Moscow":        (55.7558,   37.6173),
    # Middle East
    "Tel Aviv":      (32.0853,   34.7818),
    # South Asia
    "Lucknow":       (26.8467,   80.9462),
    # China
    "Wuhan":         (30.5928,  114.3055),
    "Shenzhen":      (22.5431,  114.0579),
    # Central America
    "Panama City":    (8.9936,  -79.5197),
}

# IANA timezone for each city — used to compute hours until LOCAL day end (not UTC midnight)
# so the time gate fires correctly regardless of city timezone.
BOND_CITY_TIMEZONES: dict[str, str] = {
    "Tokyo":          "Asia/Tokyo",
    "London":         "Europe/London",
    "New York":       "America/New_York",
    "Los Angeles":    "America/Los_Angeles",
    "Chicago":        "America/Chicago",
    "Sydney":         "Australia/Sydney",
    "Munich":         "Europe/Berlin",
    "Paris":          "Europe/Paris",
    "Dubai":          "Asia/Dubai",
    "Singapore":      "Asia/Singapore",
    "Seoul":          "Asia/Seoul",
    "Istanbul":       "Europe/Istanbul",
    "Ankara":         "Europe/Istanbul",
    "Chengdu":        "Asia/Shanghai",
    "Busan":          "Asia/Seoul",
    "Seattle":        "America/Los_Angeles",
    "Miami":          "America/New_York",
    "Toronto":        "America/Toronto",
    "Berlin":         "Europe/Berlin",
    "Amsterdam":      "Europe/Amsterdam",
    "Jakarta":        "Asia/Jakarta",
    "Helsinki":       "Europe/Helsinki",
    "Chongqing":      "Asia/Shanghai",
    "Kuala Lumpur":   "Asia/Kuala_Lumpur",
    "Wellington":     "Pacific/Auckland",
    "Sao Paulo":      "America/Sao_Paulo",
    "Buenos Aires":   "America/Argentina/Buenos_Aires",
    "Mexico City":    "America/Mexico_City",
    "Mumbai":         "Asia/Kolkata",
    "Delhi":          "Asia/Kolkata",
    "Shanghai":       "Asia/Shanghai",
    "Beijing":        "Asia/Shanghai",
    "Lagos":          "Africa/Lagos",
    "Cairo":          "Africa/Cairo",
    "Nairobi":        "Africa/Nairobi",
    "Johannesburg":   "Africa/Johannesburg",
    "Rio de Janeiro": "America/Sao_Paulo",
    "Bogota":         "America/Bogota",
    "Lima":           "America/Lima",
    "Bangkok":        "Asia/Bangkok",
    "Ho Chi Minh":    "Asia/Ho_Chi_Minh",
    "Manila":         "Asia/Manila",
    "Osaka":          "Asia/Tokyo",
    "Taipei":         "Asia/Taipei",
    "Hong Kong":      "Asia/Hong_Kong",
    "Karachi":        "Asia/Karachi",
    "Lahore":         "Asia/Karachi",
    "Dhaka":          "Asia/Dhaka",
    "Colombo":        "Asia/Colombo",
    # US cities
    "San Francisco":  "America/Los_Angeles",
    "Houston":        "America/Chicago",
    "Dallas":         "America/Chicago",
    "Austin":         "America/Chicago",
    "Denver":         "America/Denver",
    "Atlanta":        "America/New_York",
    # Europe
    "Madrid":         "Europe/Madrid",
    "Milan":          "Europe/Rome",
    "Warsaw":         "Europe/Warsaw",
    "Moscow":         "Europe/Moscow",
    # Middle East
    "Tel Aviv":       "Asia/Jerusalem",
    # South Asia
    "Lucknow":        "Asia/Kolkata",
    # China
    "Wuhan":          "Asia/Shanghai",
    "Shenzhen":       "Asia/Shanghai",
    # Central America
    "Panama City":    "America/Panama",
    # Middle East
    "Jeddah":         "Asia/Riyadh",
    # Africa
    "Cape Town":      "Africa/Johannesburg",
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
