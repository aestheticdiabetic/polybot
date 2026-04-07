# Bonding Bot Implementation Guide
> Weather Market Bonding Mode for polybot — April 2026

---

## Overview

This guide covers adding a `BOND` mode to the existing polybot arbitrage infrastructure. The bonding strategy buys shares in mispriced Polymarket weather/temperature markets using real-time Open-Meteo forecast data, then exits when prices reach target thresholds.

- `BOT_MODE=ARBI` — existing bracket arbitrage (no changes)
- `BOT_MODE=BOND` — new weather bonding strategy

Both modes share the same wallet, CLOB client, Alchemy RPC, and logging infrastructure. They are **mutually exclusive** — never run both at the same time (shared wallet/USDC balance).

**Critical timing:** Polymarket is mid-infrastructure-upgrade (April 2026 — new order book + Polymarket USD collateral token replacing USDC.e). Do **not** run live orders (Phase 3) until the upgrade is confirmed stable, estimated late April 2026. Use the interim for Phases 1–2.

---

## New Credentials Required

No new credentials are needed beyond what polybot already uses. The bonding mode reuses:

- `PRIVATE_KEY` — Polygon wallet private key
- `FUNDER_ADDRESS` — Polymarket proxy wallet
- `POLY_API_KEY` / `POLY_API_SECRET` / `POLY_API_PASSPHRASE` — CLOB API
- `ALCHEMY_API_KEY` — Polygon RPC

**Open-Meteo is free and requires no API key.** Just HTTP requests to `https://api.open-meteo.com/v1/forecast`.

Add the following to your `.env` file (optional — these have sensible defaults):

```env
# ── Bonding mode ──────────────────────────────────────────────────
BOT_MODE=BOND              # Switch to bonding strategy (default: ARBI)
BOND_LOG_FILE=/app/logs/polybot_bond.log
BOND_LEDGER_FILE=/app/logs/bonding_positions.json
```

---

## New Files to Create

```
bot/
├── bonding/
│   ├── __init__.py          (empty)
│   ├── weather_client.py    (Open-Meteo forecast fetching)
│   ├── market_scanner.py    (Gamma API polling + question parsing)
│   ├── opportunity_scorer.py (EV calculation + tier assignment)
│   ├── exit_manager.py      (async background position monitor)
│   └── paper_sim.py         (paper trade simulation runner)
```

## Files to Modify

```
bot/config.py    — add BOT_MODE + all BOND_ parameters
bot/main.py      — add run_bonding_loop() + dispatch on BOT_MODE
```

---

## Step 1 — Add Parameters to `bot/config.py`

Append this block at the bottom of `config.py`:

```python
# ─── Bonding mode ─────────────────────────────────────────────────
import os

BOT_MODE = os.getenv("BOT_MODE", "ARBI")   # "ARBI" | "BOND"

BOND_LOG_FILE    = os.getenv("BOND_LOG_FILE",    "/app/logs/polybot_bond.log")
BOND_LEDGER_FILE = os.getenv("BOND_LEDGER_FILE", "/app/logs/bonding_positions.json")

# Entry thresholds
BOND_MIN_EV_CORE       = 0.02   # min expected value per share, core tier
BOND_MIN_EV_SECONDARY  = 0.01   # min EV, secondary tier
BOND_CONFIDENCE_FLOOR  = 0.70   # min forecast probability to enter any position
BOND_EDGE_FLOOR        = 0.15   # min gap between true probability and market ask

# Position sizing
BOND_SHARES_CORE               = 25    # shares for core bonds
BOND_SHARES_SECONDARY          = 15    # shares for secondary positions
BOND_SHARES_WING               = 20    # shares for wing bets (cheap, more shares)
BOND_MAX_CAPITAL_PER_CLUSTER   = 4.00  # max $ across all buckets for one city/date

# Exit thresholds
BOND_EARLY_EXIT_PRICE       = 0.97   # sell core when price hits this
BOND_WING_EXIT_MULTIPLIER   = 5.0    # sell wing if price >= cost × this
BOND_WING_MIN_ABS_GAIN      = 2.00   # AND absolute gain >= this value
BOND_GAS_FLOOR_HOURS        = 4      # don't exit within N hours of resolution

# Scanner settings
BOND_POLL_INTERVAL_SECS   = 360   # seconds between market scans
BOND_MAX_MARKETS_PER_RUN  = 150   # cap on orders placed per cycle

# City list with coordinates — extend as forecast accuracy is validated
BOND_CITIES = {
    "Tokyo":       (35.6762,  139.6503),
    "London":      (51.5074,   -0.1278),
    "New York":    (40.7128,  -74.0060),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago":     (41.8781,  -87.6298),
    "Sydney":     (-33.8688,  151.2093),
    "Munich":      (48.1351,   11.5820),
    "Paris":       (48.8566,    2.3522),
    "Dubai":       (25.2048,   55.2708),
    "Singapore":    (1.3521,  103.8198),
    "Seoul":       (37.5665,  126.9780),
    "Istanbul":    (41.0082,   28.9784),
    "Ankara":      (39.9334,   32.8597),
    "Chengdu":     (30.5728,  104.0668),
    "Busan":       (35.1796,  129.0756),
    "Seattle":     (47.6062, -122.3321),
    "Miami":       (25.7617,  -80.1918),
    "Toronto":     (43.6532,  -79.3832),
    "Berlin":      (52.5200,   13.4050),
    "Amsterdam":   (52.3676,    4.9041),
}

# City name aliases — maps Polymarket's various naming conventions to canonical names
BOND_CITY_ALIASES = {
    "NYC": "New York", "NY": "New York", "New_York": "New York",
    "LA": "Los Angeles", "Los_Angeles": "Los Angeles",
    "SF": "San Francisco",
    "DC": "Washington",
}
```

---

## Step 2 — Create `bot/bonding/__init__.py`

Empty file — just makes it a Python package.

---

## Step 3 — `bot/bonding/weather_client.py`

Full implementation spec:

```python
"""
weather_client.py — Fetch city temperature forecasts from Open-Meteo.
No API key required. Caches responses for 30 minutes.
"""
import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional
import math
import aiohttp

from config import BOND_CITIES, BOND_CITY_ALIASES

log = logging.getLogger("bond.weather")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CACHE_TTL_SECS = 1800  # 30 minutes

@dataclass
class ForecastResult:
    city: str
    target_date: date
    daily_max_c: float           # predicted daily high (°C)
    hourly_spread: list[float]   # hourly temps across the day
    confidence_interval_c: float # ±°C spread (std dev of hourly spread)

# Key functions to implement:

async def get_forecast(city: str, target_date: date) -> ForecastResult:
    """
    Returns daily high forecast + hourly spread for city on target_date.
    Uses canonical city name; raises UnknownCityError if not found.
    """

async def get_all_forecasts(city_date_pairs: list[tuple[str, date]]) -> dict[tuple[str, date], ForecastResult]:
    """Batch fetch all city/date pairs concurrently. De-dupes requests."""

def prob_in_range(forecast: ForecastResult, temp_min: float, temp_max: float) -> float:
    """
    Returns probability (0-1) that the daily high falls in [temp_min, temp_max].
    Uses Gaussian approximation over the hourly spread.
    Mean = forecast.daily_max_c, std = forecast.confidence_interval_c.
    """
    # scipy not available — implement using math.erf directly:
    # P(a < X < b) = 0.5 * (erf((b - mu) / (std * sqrt(2))) - erf((a - mu) / (std * sqrt(2))))

async def _fetch_open_meteo(lat: float, lon: float, target_date: date) -> dict:
    """
    Raw aiohttp GET to Open-Meteo. Params:
      latitude, longitude, daily=temperature_2m_max,
      hourly=temperature_2m, timezone=auto,
      start_date=YYYY-MM-DD, end_date=YYYY-MM-DD
    Cache key: (lat, lon, date). TTL: 30 min.
    """

def _resolve_city(city_name: str) -> tuple[str, float, float]:
    """
    Resolves potentially aliased city name to (canonical_name, lat, lon).
    Checks BOND_CITIES first, then BOND_CITY_ALIASES.
    Raises UnknownCityError if not found.
    """

def celsius_to_fahrenheit(c: float) -> float:
    return c * 9/5 + 32

def fahrenheit_to_celsius(f: float) -> float:
    return (f - 32) * 5/9
```

**Validation test to run:**
```bash
# From bot/ directory
python -c "
import asyncio
from bonding.weather_client import get_all_forecasts
from datetime import date, timedelta

async def test():
    tomorrow = date.today() + timedelta(days=1)
    cities = ['Tokyo', 'London', 'New York', 'Munich', 'Sydney']
    results = await get_all_forecasts([(c, tomorrow) for c in cities])
    for (city, d), f in results.items():
        print(f'{city}: {f.daily_max_c:.1f}°C ±{f.confidence_interval_c:.1f}°C')

asyncio.run(test())
"
```

Expected output: 5 lines with sensible temperatures. If any city throws `UnknownCityError`, fix the lookup table.

---

## Step 4 — `bot/bonding/market_scanner.py`

```python
"""
market_scanner.py — Poll Polymarket Gamma API for open weather markets.
Read-only. Parses natural language questions to extract structured data.
"""
import re
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional
import aiohttp

log = logging.getLogger("bond.scanner")

GAMMA_API = "https://gamma-api.polymarket.com"

@dataclass
class MarketCandidate:
    market_id: str
    token_id: str          # YES outcome token ID (for CLOB orders)
    question: str
    city: str              # canonical city name
    target_date: date
    temp_min: Optional[float]  # bucket lower bound (°C)
    temp_max: Optional[float]  # bucket upper bound (°C)
    unit: str              # "C" or "F"
    best_ask: float        # current ask price for YES
    resolution_time: datetime

async def scan_weather_markets() -> list[MarketCandidate]:
    """
    Query Gamma API: GET /markets?tag=weather&active=true&limit=500
    Parse each market question. Filter to BOND_CITIES.
    Return list of MarketCandidate objects.
    """

def parse_market_question(question: str) -> Optional[dict]:
    """
    Extract city, date, temp bucket, and unit from question text.
    
    Regex patterns to handle:
    - "Highest temperature in Tokyo on April 7?"       → single °C bucket
    - "Highest temperature in Munich on April 8?"      → single °C bucket  
    - "Daily high in London above 18°C on April 9?"    → threshold ≥18°C
    - "Will the highest temperature in Paris be 22°C?" → exact match
    - "80-81°F" in question                            → Fahrenheit range
    - "17°C or 18°C" / "17-18°C"                      → range bucket
    
    Returns dict with keys: city, date, temp_min, temp_max, unit
    Returns None if parsing fails (log a warning).
    """

async def get_market_orderbook(token_id: str) -> tuple[float, float]:
    """
    GET /book?token_id={token_id} from CLOB API.
    Returns (best_ask_yes, best_bid_yes).
    Returns (1.0, 0.0) on failure — scorer will reject it.
    """

def filter_by_cities(markets: list[MarketCandidate]) -> list[MarketCandidate]:
    """Drop markets where city wasn't resolved or not in BOND_CITIES."""
```

**Regex validation test:**

```bash
# Run scanner in read-only mode — prints what it finds, places no orders
python -c "
import asyncio
from bonding.market_scanner import scan_weather_markets

async def test():
    markets = await scan_weather_markets()
    print(f'Found {len(markets)} weather markets')
    for m in markets[:10]:
        print(f'  {m.city} {m.target_date}: {m.temp_min}-{m.temp_max}{m.unit} ask={m.best_ask:.3f}')

asyncio.run(test())
"
```

**48-hour read-only monitoring:**
Run the scanner on a loop and log everything to a file before touching the scorer:
```bash
BOT_MODE=BOND python -c "
import asyncio, json, time
from bonding.market_scanner import scan_weather_markets
from datetime import datetime

async def monitor():
    while True:
        markets = await scan_weather_markets()
        entry = {'ts': datetime.utcnow().isoformat(), 'count': len(markets),
                 'markets': [{'city': m.city, 'date': str(m.target_date), 
                               'ask': m.best_ask, 'tier_guess': 'CORE' if m.best_ask < 0.08 else 'OTHER'} 
                              for m in markets]}
        with open('/app/logs/scanner_monitor.jsonl', 'a') as f:
            f.write(json.dumps(entry) + '\n')
        print(f'[{entry[\"ts\"]}] {len(markets)} markets found')
        await asyncio.sleep(360)

asyncio.run(monitor())
"
```

---

## Step 5 — `bot/bonding/opportunity_scorer.py`

```python
"""
opportunity_scorer.py — Join forecast data with market data.
Compute EV, assign tiers, size positions.
"""
from dataclasses import dataclass
from typing import Optional
from config import (
    BOND_MIN_EV_CORE, BOND_MIN_EV_SECONDARY, BOND_CONFIDENCE_FLOOR,
    BOND_EDGE_FLOOR, BOND_SHARES_CORE, BOND_SHARES_SECONDARY, BOND_SHARES_WING,
    BOND_MAX_CAPITAL_PER_CLUSTER,
)

TIER_CORE      = "CORE"
TIER_SECONDARY = "SECONDARY"
TIER_WING      = "WING"

@dataclass
class ScoredOpportunity:
    market: MarketCandidate      # from market_scanner
    forecast: ForecastResult     # from weather_client
    prob: float                  # true probability from forecast
    ev: float                    # expected value per share = prob*1.0 - ask
    edge: float                  # prob - ask
    tier: str                    # CORE | SECONDARY | WING
    shares: int                  # position size
    capital: float               # shares * ask (cost basis)

def score_all(markets: list[MarketCandidate], 
              forecasts: dict) -> list[ScoredOpportunity]:
    """
    Full pipeline. Returns list sorted by EV descending.
    Applies cluster capital cap after sorting.
    """

def score_market(market: MarketCandidate, 
                 forecast: ForecastResult) -> Optional[ScoredOpportunity]:
    """
    EV formula:
        prob = prob_in_range(forecast, market.temp_min, market.temp_max)
        # convert F→C if market.unit == "F"
        ev   = (prob * 1.00) - market.best_ask
        edge = prob - market.best_ask
    
    Tier assignment:
        CORE:      ask 0.02–0.08, ev > BOND_MIN_EV_CORE, prob > BOND_CONFIDENCE_FLOOR
        SECONDARY: ask 0.009–0.019, ev > BOND_MIN_EV_SECONDARY
        WING:      ask 0.001–0.008, ev > 0 (positive EV sufficient)
    
    Returns None if no tier criteria met or edge < BOND_EDGE_FLOOR.
    """

def cluster_by_city_date(opps: list[ScoredOpportunity]) -> list[list[ScoredOpportunity]]:
    """
    Group by (city, date). Within each cluster apply BOND_MAX_CAPITAL_PER_CLUSTER.
    Sort cluster by EV desc, take until capital cap reached.
    """

def assign_tier(ask: float, ev: float, prob: float) -> Optional[str]:
    if 0.02 <= ask <= 0.08 and ev > BOND_MIN_EV_CORE and prob > BOND_CONFIDENCE_FLOOR:
        return TIER_CORE
    if 0.009 <= ask <= 0.019 and ev > BOND_MIN_EV_SECONDARY:
        return TIER_SECONDARY
    if 0.001 <= ask <= 0.008 and ev > 0:
        return TIER_WING
    return None
```

---

## Step 6 — `bot/bonding/paper_sim.py`

Run this for 5+ days before touching Phase 3:

```python
"""
paper_sim.py — Paper trade simulation. No orders placed.
Run standalone: python -m bonding.paper_sim
"""
import asyncio, json, logging
from datetime import datetime
from pathlib import Path
from bonding.weather_client import get_all_forecasts
from bonding.market_scanner import scan_weather_markets
from bonding.opportunity_scorer import score_all
from config import BOND_POLL_INTERVAL_SECS, BOND_MAX_MARKETS_PER_RUN

PAPER_LOG = Path("/app/logs/paper_trades.jsonl")
log = logging.getLogger("bond.paper")

async def run():
    log.info("Paper simulation started")
    while True:
        markets   = await scan_weather_markets()
        forecasts = await get_all_forecasts([(m.city, m.target_date) for m in markets])
        opps      = score_all(markets, forecasts)[:BOND_MAX_MARKETS_PER_RUN]
        
        for opp in opps:
            record = {
                "ts":         datetime.utcnow().isoformat(),
                "event":      "WOULD_BUY",
                "market_id":  opp.market.market_id,
                "question":   opp.market.question,
                "city":       opp.market.city,
                "date":       str(opp.market.target_date),
                "tier":       opp.tier,
                "shares":     opp.shares,
                "ask":        opp.market.best_ask,
                "prob":       opp.prob,
                "ev":         opp.ev,
                "edge":       opp.edge,
                "capital":    opp.capital,
            }
            PAPER_LOG.write_text(  # append mode
                PAPER_LOG.read_text() + json.dumps(record) + "\n"
                if PAPER_LOG.exists() else json.dumps(record) + "\n"
            )
            log.info(f"WOULD_BUY {opp.market.city} {opp.tier} ask={opp.market.best_ask:.3f} ev={opp.ev:.3f}")
        
        log.info(f"Paper cycle complete: {len(opps)} opportunities logged")
        await asyncio.sleep(BOND_POLL_INTERVAL_SECS)

if __name__ == "__main__":
    asyncio.run(run())
```

**Run paper sim:**
```bash
cd /app/bot
python -m bonding.paper_sim
```

**Analyse paper sim results after 5+ days:**
```bash
python -c "
import json
from collections import defaultdict

lines = open('/app/logs/paper_trades.jsonl').readlines()
records = [json.loads(l) for l in lines]

# After markets resolve, you need to manually check outcomes via Gamma API
# or add a resolve-checker script. Quick summary of what was 'bet':
by_tier = defaultdict(list)
for r in records:
    by_tier[r['tier']].append(r)

for tier, recs in by_tier.items():
    total_capital = sum(r['capital'] for r in recs)
    avg_ev = sum(r['ev'] for r in recs) / len(recs)
    print(f'{tier}: {len(recs)} bets, \${total_capital:.2f} capital, avg_ev={avg_ev:.3f}')
"
```

---

## Step 7 — `bot/bonding/exit_manager.py`

```python
"""
exit_manager.py — Async background task. Monitors open bonding positions.
Fires sell orders when exit criteria are met.
"""
import asyncio, json, logging, os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from config import (
    BOND_LEDGER_FILE, BOND_EARLY_EXIT_PRICE, BOND_WING_EXIT_MULTIPLIER,
    BOND_WING_MIN_ABS_GAIN, BOND_GAS_FLOOR_HOURS,
)

log = logging.getLogger("bond.exit")
GAMMA_API = "https://gamma-api.polymarket.com"

@dataclass
class BondPosition:
    market_id: str
    token_id: str
    question: str
    city: str
    outcome: str         # always "YES"
    tier: str            # CORE | SECONDARY | WING
    shares: int
    entry_price: float
    entry_time: str      # ISO8601
    resolution_time: str # ISO8601
    status: str          # OPEN | SOLD | RESOLVED

class ExitManager:
    def __init__(self, client: ClobClient):
        self._client = client
        self._ledger_path = Path(BOND_LEDGER_FILE)

    async def run(self):
        log.info("ExitManager started")
        while True:
            try:
                await self._check_exits()
            except Exception as e:
                log.error(f"ExitManager error: {e}", exc_info=True)
            await asyncio.sleep(60)

    async def _check_exits(self):
        positions = self._load_positions()
        open_pos  = [p for p in positions if p.status == "OPEN"]
        for pos in open_pos:
            current_price = await self._get_current_price(pos.token_id)
            hours_left    = await self._get_hours_to_resolution(pos.market_id)
            if self._should_exit(pos, current_price, hours_left):
                await self._execute_sell(pos, current_price)

    def _should_exit(self, pos: BondPosition, price: float, hours: float) -> bool:
        """
        Exit decision tree (from strategy document):
        1. Core: price >= BOND_EARLY_EXIT_PRICE (0.97) → SELL
        2. Any tier: price >= entry_price * 10 → SELL
        3. Wing/secondary: price >= entry_price * BOND_WING_EXIT_MULTIPLIER
                           AND gain >= BOND_WING_MIN_ABS_GAIN → SELL
        4. Sub-cent entry AND price < 0.50 → HOLD (gas floor)
        5. hours < BOND_GAS_FLOOR_HOURS → HOLD
        """
        if hours < BOND_GAS_FLOOR_HOURS:
            log.debug(f"HOLD {pos.market_id[:8]} — {hours:.1f}h to resolution < gas floor")
            return False
        if pos.tier == "CORE" and price >= BOND_EARLY_EXIT_PRICE:
            return True
        if price >= pos.entry_price * 10:
            return True
        if pos.tier in ("WING", "SECONDARY"):
            gain = (price - pos.entry_price) * pos.shares
            if price >= pos.entry_price * BOND_WING_EXIT_MULTIPLIER and gain >= BOND_WING_MIN_ABS_GAIN:
                return True
        if pos.entry_price < 0.01 and price < 0.50:
            return False  # gas cost > profit
        return False

    def _load_positions(self) -> list[BondPosition]:
        if not self._ledger_path.exists():
            return []
        data = json.loads(self._ledger_path.read_text())
        return [BondPosition(**p) for p in data.get("positions", [])]

    def _save_positions(self, positions: list[BondPosition]):
        """Atomic write — write to .tmp then rename."""
        tmp = Path(str(self._ledger_path) + ".tmp")
        tmp.write_text(json.dumps({"positions": [asdict(p) for p in positions]}, indent=2))
        tmp.replace(self._ledger_path)

    async def add_position(self, pos: BondPosition):
        """Called by main loop after a successful buy fill."""
        positions = self._load_positions()
        positions.append(pos)
        self._save_positions(positions)
        log.info(f"BOND_LEDGER_ADD {pos.city} {pos.tier} shares={pos.shares} entry={pos.entry_price:.4f}")

    async def _execute_sell(self, pos: BondPosition, current_price: float):
        """Place limit sell order via CLOB client."""
        # Sell at current_price - 1 tick to ensure fill
        limit = round(current_price - 0.01, 2)
        order_args = OrderArgs(token_id=pos.token_id, price=limit, size=pos.shares, side="SELL")
        signed = await asyncio.get_event_loop().run_in_executor(
            None, self._client.create_order, order_args
        )
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: self._client.post_order(signed, OrderType.GTC)
        )
        pnl = (current_price - pos.entry_price) * pos.shares
        log.info(f"BOND_EXIT_TRIGGERED market={pos.market_id[:8]} tier={pos.tier} "
                 f"price={current_price:.3f} pnl={pnl:+.2f}")
        # Update ledger
        positions = self._load_positions()
        for p in positions:
            if p.market_id == pos.market_id:
                p.status = "SOLD"
        self._save_positions(positions)

    async def _get_current_price(self, token_id: str) -> float:
        import aiohttp
        async with aiohttp.ClientSession() as s:
            r = await s.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=aiohttp.ClientTimeout(total=5))
            data = await r.json()
            asks = data.get("asks", [])
            return float(asks[0]["price"]) if asks else 0.0

    async def _get_hours_to_resolution(self, market_id: str) -> float:
        import aiohttp
        from datetime import datetime, timezone
        async with aiohttp.ClientSession() as s:
            r = await s.get(f"{GAMMA_API}/markets/{market_id}", timeout=aiohttp.ClientTimeout(total=5))
            data = await r.json()
            end_str = data.get("endDate") or data.get("end_date_iso")
            if not end_str:
                return 999.0
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return max(0, (end_dt - now).total_seconds() / 3600)
```

---

## Step 8 — Modify `bot/main.py`

Add bonding loop dispatch. Minimal change — existing `run_bot()` function becomes `run_arbi_loop()` internally, and we add `run_bonding_loop()`:

```python
# Add to imports at top of main.py:
from config import BOT_MODE, BOND_LOG_FILE, BOND_POLL_INTERVAL_SECS, BOND_MAX_MARKETS_PER_RUN
from config import CLOB_HOST, CHAIN_ID, PRIVATE_KEY, FUNDER_ADDRESS, API_KEY, API_SECRET, API_PASSPHRASE

# Add bond log handler after existing logging setup:
if BOT_MODE == "BOND":
    import os
    os.makedirs(os.path.dirname(BOND_LOG_FILE), exist_ok=True)
    logging.getLogger().addHandler(logging.FileHandler(BOND_LOG_FILE))

# Replace the existing run_bot() call dispatch in main():
# In the main() function, change:
#   bot_task = loop.create_task(run_bot(state))
# to:
if BOT_MODE == "BOND":
    bot_task = loop.create_task(run_bonding_loop(state))
else:
    bot_task = loop.create_task(run_bot(state))

# Add this new async function:
async def run_bonding_loop(state: StateManager):
    """BOND mode main loop — weather market bonding strategy."""
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from bonding.weather_client import get_all_forecasts
    from bonding.market_scanner import scan_weather_markets
    from bonding.opportunity_scorer import score_all
    from bonding.exit_manager import ExitManager, BondPosition
    import time

    # Create dedicated CLOB client for bonding mode
    creds = ApiCreds(api_key=API_KEY, api_secret=API_SECRET, api_passphrase=API_PASSPHRASE)
    bond_client = ClobClient(
        host=CLOB_HOST, chain_id=CHAIN_ID, key=PRIVATE_KEY,
        creds=creds, signature_type=2, funder=FUNDER_ADDRESS,
    )

    exit_mgr = ExitManager(bond_client)
    asyncio.get_event_loop().create_task(exit_mgr.run())

    state.set_running(True)
    log.info(f"PolyBot BOND mode starting — poll_interval={BOND_POLL_INTERVAL_SECS}s")

    while state.is_running():
        try:
            markets   = await scan_weather_markets()
            city_dates = list({(m.city, m.target_date) for m in markets})
            forecasts  = await get_all_forecasts(city_dates)
            opps       = score_all(markets, forecasts)

            log.info(f"BOND_SCAN_COMPLETE markets_found={len(markets)} qualifying={len(opps)}")

            for opp in opps[:BOND_MAX_MARKETS_PER_RUN]:
                await _place_bond_order(bond_client, exit_mgr, opp)

        except Exception as e:
            log.error(f"Bonding loop error: {e}", exc_info=True)

        await asyncio.sleep(BOND_POLL_INTERVAL_SECS)


async def _place_bond_order(client, exit_mgr, opp):
    """Place a single FOK buy order for a bonding opportunity."""
    from py_clob_client.clob_types import OrderArgs, OrderType
    from bonding.exit_manager import BondPosition
    from datetime import datetime, timezone

    order_args = OrderArgs(
        token_id=opp.market.token_id,
        price=opp.market.best_ask,
        size=opp.shares,
        side="BUY",
    )
    try:
        signed = await asyncio.get_event_loop().run_in_executor(
            None, client.create_order, order_args
        )
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: client.post_order(signed, OrderType.FOK)
        )
        log.info(f"BOND_ORDER_PLACED city={opp.market.city} date={opp.market.target_date} "
                 f"tier={opp.tier} shares={opp.shares} price={opp.market.best_ask:.4f} ev={opp.ev:.4f}")

        # Record in position ledger
        pos = BondPosition(
            market_id=opp.market.market_id,
            token_id=opp.market.token_id,
            question=opp.market.question,
            city=opp.market.city,
            outcome="YES",
            tier=opp.tier,
            shares=opp.shares,
            entry_price=opp.market.best_ask,
            entry_time=datetime.now(timezone.utc).isoformat(),
            resolution_time=opp.market.resolution_time.isoformat(),
            status="OPEN",
        )
        await exit_mgr.add_position(pos)

    except Exception as e:
        log.warning(f"BOND_ORDER_FAILED city={opp.market.city} error={e}")
```

---

## Running the Bot

### Switch to BOND mode (VPS)
```bash
systemctl stop polybot
# Edit /app/.env and set:
#   BOT_MODE=BOND
systemctl start polybot
```

### Or via environment variable directly:
```bash
BOT_MODE=BOND python bot/main.py
```

### Phase 3 initial run (reduced sizing):
```bash
# In .env:
BOT_MODE=BOND
# Start with reduced limits — edit config.py temporarily:
#   BOND_SHARES_CORE = 5
#   BOND_MAX_MARKETS_PER_RUN = 10
systemctl restart polybot
```

### Switch back to ARBI mode:
```bash
systemctl stop polybot
# Edit .env: BOT_MODE=ARBI
systemctl start polybot
```

### Safe BOND shutdown (let positions wind down):
```bash
# Set max markets to 0 — stops new positions, exit manager continues
# Edit config.py: BOND_MAX_MARKETS_PER_RUN = 0
# Restart, wait for exit_manager to close positions at target prices
# Then switch BOT_MODE=ARBI
```

---

## Phase-by-Phase Testing

### Phase 1 — Data Pipeline (Days 1–3)
**Goal:** Weather client returns accurate forecasts for all 20 cities.

```bash
# Test 1: All cities resolve with coordinates
python -c "
from bot.bonding.weather_client import _resolve_city
from bot.config import BOND_CITIES
for city in BOND_CITIES:
    name, lat, lon = _resolve_city(city)
    print(f'{name}: {lat}, {lon}')
"

# Test 2: Forecast returns sensible data
python -c "
import asyncio
from bot.bonding.weather_client import get_forecast
from datetime import date, timedelta

async def t():
    f = await get_forecast('Tokyo', date.today() + timedelta(days=1))
    print(f'Tokyo tomorrow: {f.daily_max_c:.1f}°C ±{f.confidence_interval_c:.1f}')
    assert 10 < f.daily_max_c < 45, 'Implausible temperature'
    print('PASS')
asyncio.run(t())
"

# Test 3: prob_in_range sanity check
python -c "
from bot.bonding.weather_client import prob_in_range, ForecastResult
from datetime import date
f = ForecastResult('Tokyo', date.today(), daily_max_c=20.0, 
                   hourly_spread=[18,19,20,21,20,19], confidence_interval_c=1.5)
p = prob_in_range(f, 19.0, 21.0)
print(f'P(19-21°C | mean=20, std=1.5) = {p:.3f}')
assert 0.6 < p < 0.9, f'Expected ~0.68, got {p}'
print('PASS')
"
```

**Milestone 1 check:** Compare `prob_in_range()` outputs against 2–4 weeks of historical Open-Meteo data + Polymarket resolution outcomes. Target: within ±5% of actual resolution rate per bucket.

### Phase 2 — Scanner + Scorer (Days 4–6)

```bash
# Test scanner parses questions correctly
python -c "
from bot.bonding.market_scanner import parse_market_question

tests = [
    ('Highest temperature in Tokyo on April 7?', 'Tokyo', 7),
    ('Will the highest temperature in Munich be 22°C on April 8?', 'Munich', 22),
    ('Daily high in London above 18°C on April 9?', 'London', 18),
    ('Highest temperature in Los Angeles on April 7?', 'Los Angeles', None),
]
for question, expected_city, expected_temp in tests:
    result = parse_market_question(question)
    status = 'PASS' if result and result['city'] == expected_city else 'FAIL'
    print(f'{status}: \"{question[:50]}...\" → city={result[\"city\"] if result else None}')
"

# Test scorer produces valid tiers
python -c "
from bot.bonding.opportunity_scorer import assign_tier
cases = [
    (0.035, 0.03, 0.75),   # expect CORE
    (0.015, 0.01, 0.65),   # expect SECONDARY
    (0.005, 0.002, 0.55),  # expect WING
    (0.50,  0.01, 0.60),   # expect None (too expensive)
]
expected = ['CORE', 'SECONDARY', 'WING', None]
for (ask, ev, prob), exp in zip(cases, expected):
    result = assign_tier(ask, ev, prob)
    status = 'PASS' if result == exp else 'FAIL'
    print(f'{status}: ask={ask} ev={ev} prob={prob} → {result} (expected {exp})')
"

# Run paper simulation (leave running 5+ days)
cd /app/bot && python -m bonding.paper_sim
```

**Milestone 2 check:**
```bash
# After 5+ days, analyse paper sim
python -c "
import json
from collections import defaultdict

records = [json.loads(l) for l in open('/app/logs/paper_trades.jsonl')]
print(f'Total paper bets: {len(records)}')
by_tier = defaultdict(list)
for r in records:
    by_tier[r['tier']].append(r)
for tier, recs in sorted(by_tier.items()):
    print(f'  {tier}: n={len(recs)}, avg_ev={sum(r[\"ev\"] for r in recs)/len(recs):.4f}, '
          f'total_capital=\${sum(r[\"capital\"] for r in recs):.2f}')
"
# You need to cross-reference with actual outcomes via Gamma API to compute win rate
```

### Phase 3 — Live Execution (Days 7–10)
**Only start after Polymarket upgrade is confirmed complete.**

```bash
# Verify you can connect and create orders (dry-run — don't POST)
python -c "
import os
os.environ.setdefault('BOT_MODE', 'BOND')
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from config import CLOB_HOST, CHAIN_ID, PRIVATE_KEY, FUNDER_ADDRESS
from config import API_KEY, API_SECRET, API_PASSPHRASE

creds = ApiCreds(api_key=API_KEY, api_secret=API_SECRET, api_passphrase=API_PASSPHRASE)
client = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=PRIVATE_KEY,
                    creds=creds, signature_type=2, funder=FUNDER_ADDRESS)
bal = client.get_balance_allowance()
print(f'USDC balance: \${bal}')
print('CLOB client connected OK')
"

# Start with minimal sizing (edit config.py):
#   BOND_SHARES_CORE = 5
#   BOND_MAX_MARKETS_PER_RUN = 10
BOT_MODE=BOND python bot/main.py

# Monitor ledger growth
watch -n 10 "python -c \"
import json
data = json.load(open('/app/logs/bonding_positions.json'))
positions = data['positions']
open_pos = [p for p in positions if p['status'] == 'OPEN']
print(f'Total: {len(positions)}, Open: {len(open_pos)}')
for p in open_pos[-5:]:
    print(f'  {p[\"city\"]} {p[\"tier\"]} {p[\"shares\"]}sh @ {p[\"entry_price\"]:.4f}')
\""

# Check logs for errors
tail -f /app/logs/polybot_bond.log | grep -E "ERROR|WARN|BOND_"
```

**Milestone 3 checks:**
- Ledger file exists and grows with each fill
- Exit manager log shows `BOND_EXIT_TRIGGERED` events at correct thresholds
- No Python exceptions in `polybot_bond.log` over 48 hours
- USDC balance decreases by expected amount per buy

### Phase 4 — Scale Up (Days 11–20)

```bash
# After 48h of stable Phase 3, increase sizing:
# Edit config.py: BOND_SHARES_CORE = 15, BOND_MAX_MARKETS_PER_RUN = 50
# Restart: systemctl restart polybot

# Daily win rate monitor
python -c "
import json
from datetime import datetime, timedelta, timezone

data = json.load(open('/app/logs/bonding_positions.json'))
week_ago = datetime.now(timezone.utc) - timedelta(days=7)

recent = [p for p in data['positions'] 
          if p['status'] in ('SOLD', 'RESOLVED')
          and datetime.fromisoformat(p['entry_time']) > week_ago]

by_tier = {}
for p in recent:
    t = p['tier']
    if t not in by_tier:
        by_tier[t] = {'wins': 0, 'losses': 0}
    # 'RESOLVED' with pnl > 0 = win; 'SOLD' before resolution = win
    if p['status'] == 'SOLD':
        by_tier[t]['wins'] += 1  # exited at profit (exit_mgr only sells at gain)
    # RESOLVED wins need to check if resolution was YES

for tier, stats in by_tier.items():
    total = stats['wins'] + stats['losses']
    rate = stats['wins'] / total if total else 0
    print(f'{tier}: {total} positions, win_rate={rate:.1%}')
"
```

**Scale milestones:**
- CORE=15, MAX=50 → stable for 3 days → add SECONDARY tier
- SECONDARY added → stable for 3 days → add WING tier
- WING added → stable for 7 days → CORE=25, MAX=150 (full scale)
- Full scale → 14 days → win rate ≥68% sustained

---

## Monitoring Checklist

| Metric | Alert threshold | Command |
|--------|----------------|---------|
| CORE win rate (7-day) | < 62% | See Phase 4 script above |
| Average CORE entry price | > $0.10 | grep BOND_ORDER_PLACED in log |
| Markets per scan cycle | < 20 | grep BOND_SCAN_COMPLETE in log |
| Open position count | > 500 | check ledger |
| Total deployed capital | > $500 | sum entry_price * shares in ledger |

**Auto-halt trigger:** If 7-day rolling win rate drops below 60%, set `BOND_MAX_MARKETS_PER_RUN=0` and investigate before resuming.

---

## Structured Log Events (Section 8.2 from strategy doc)

All bonding log lines are prefixed `BOND_` for easy grep:

```
BOND_SCAN_COMPLETE markets_found=87 qualifying=34
BOND_ORDER_PLACED city=Tokyo date=2026-04-07 tier=CORE shares=25 price=0.034 ev=0.031
BOND_ORDER_PLACED city=Munich date=2026-04-08 tier=WING shares=20 price=0.006 ev=0.044
BOND_ORDER_FAILED city=Seoul error=<msg>
BOND_LEDGER_ADD Tokyo CORE shares=25 entry=0.0340
BOND_EXIT_TRIGGERED market=0xabc tier=CORE current_price=0.971 pnl=+24.08
BOND_EXIT_SKIPPED market=0xdef tier=WING current_price=0.009 reason=GAS_FLOOR
```

---

## Dependencies

No new pip packages required. The bonding mode uses only what's already in `requirements.txt`:
- `aiohttp` — Open-Meteo + Gamma API calls
- `py-clob-client` — order placement
- `python-dotenv` — .env loading

Only standard library additions: `math` (for `erf`), `dataclasses`, `json`, `pathlib`.

---

## Important Notes

1. **Never run ARBI and BOND simultaneously** — shared wallet, risk of USDC overallocation
2. **Polymarket upgrade (April 2026)** — wait for confirmation before Phase 3 live orders
3. **City name parsing is the critical failure point** — validate regex against 200+ real market questions before going live
4. **Ledger is append-only** — `bonding_positions.json` should only be edited manually if there's a bug; the exit_manager writes atomically
5. **`BOND_MAX_CAPITAL_PER_CLUSTER = 4.00`** hard cap per city/date — prevents overconcentration on a single weather event
6. **Wing bets are intentional** — many small losses offset by occasional large payouts; track tier EV separately
7. **Redeemer.py still handles settlement** — when a YES position resolves, the existing redeemer webhook will collect the $1.00 automatically. No changes needed there.
