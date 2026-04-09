# Design: Cross-Source Weather Validation + CERTAIN Tier

**Date:** 2026-04-09  
**Status:** Approved  
**Branch:** master  

---

## Context

BOND mode currently derives all forecast probability from a single weather provider
(Open-Meteo GFS ensemble). There is no cross-check against independent data sources,
meaning a miscalibrated GFS run could cause confident but wrong bets.

This design adds two independent sources (ECMWF via Open-Meteo, tomorrow.io) and
uses their consensus to:

1. **Improve all standard BOND bets** — replace single-source GFS probability with
   an average across all three sources.
2. **Enable a new CERTAIN tier** — bet on high-confidence, high-ask outcomes where
   all three sources tightly agree AND the market still underprices the likely outcome.

---

## Architecture Overview

```
REST Scan (every 60s)
│
├─ get_consensus_forecasts()          ← enhanced from get_all_forecasts()
│  ├─ GFS ensemble (Open-Meteo)       existing — 2h cache
│  ├─ ECMWF ensemble (Open-Meteo)     new      — 2h cache
│  └─ tomorrow.io daily forecast      new      — 3h cache
│  └─ returns dict[tuple, SourceConsensus]
│
├─ score_all()                        existing scorer, now uses consensus_prob
│  └─ ScoredOpportunity.prob = mean(gfs_prob, ecmwf_prob, tomorrowio_prob)
│
└─ score_certain()                    new module: sure_thing_scorer.py
   └─ additional hard gates on top of consensus_prob
   └─ produces ScoredOpportunity(tier="CERTAIN")

WebSocket on_opportunity callback
└─ reads pre-cached SourceConsensus — zero hot-path latency added
```

---

## Feature 1: Cross-Source Validation (all BOND bets)

### New dataclass: `SourceConsensus`

Add to `bot/bonding/weather_client.py`:

```python
@dataclass
class SourceConsensus:
    city: str
    target_date: date
    gfs: ForecastResult           # existing GFS ensemble result
    ecmwf: Optional[ForecastResult]     # ECMWF ensemble via Open-Meteo (None if unavailable)
    tomorrowio: Optional[ForecastResult]  # tomorrow.io result (None if cache miss/unavailable)

    def consensus_prob(self, temp_min: float, temp_max: float) -> float:
        """Average P(YES) across all available sources."""
        probs = [prob_in_range(self.gfs, temp_min, temp_max)]
        if self.ecmwf:
            probs.append(prob_in_range(self.ecmwf, temp_min, temp_max))
        if self.tomorrowio:
            probs.append(prob_in_range(self.tomorrowio, temp_min, temp_max))
        return sum(probs) / len(probs)

    def available_sources(self) -> int:
        return sum(1 for s in [self.gfs, self.ecmwf, self.tomorrowio] if s is not None)
```

If ECMWF or tomorrow.io is unavailable (cache miss, API down), the consensus
degrades gracefully — it averages whatever sources are present. GFS is always
required (existing behaviour).

### New function: `get_ecmwf_forecast()`

Add to `bot/bonding/weather_client.py`.  
Same Open-Meteo ensemble endpoint, different model parameter:

```
ECMWF_ENSEMBLE_MODEL = "ecmwf_ifs04"   # 50 members, global, free via Open-Meteo
```

Cache key prefix: `ecmwf_` to avoid collision with GFS cache.  
TTL: 2 hours (same as GFS).  
Shares the existing rate limiter and disk cache infrastructure.

### New module: `bot/bonding/tomorrow_client.py`

Responsibilities:
- Fetch tomorrow.io `/timelines` endpoint for daily max temperature
- Convert point forecast to a synthetic ensemble (same Gaussian method as near-term OM)
  - Sigma: 1.5°C (tomorrow.io daily max RMSE is comparable to OM next-day)
  - Members: 100 synthetic
- Disk-persistent cache, TTL: 3 hours
- Rate limit guard: max 20 requests/hour (headroom below the 25/hour limit)
- Graceful degradation: on any API error, log a warning and return `None`
- API key read from env: `TOMORROW_IO_API_KEY`

Tomorrow.io endpoint used:
```
GET https://api.tomorrow.io/v4/timelines
  ?location={lat},{lon}
  &fields=temperatureMax
  &timesteps=1d
  &units=metric
  &apikey={TOMORROW_IO_API_KEY}
```

Returns a single `ForecastResult` with synthetic ensemble members, compatible
with existing `prob_in_range()`.

### Changes to `get_all_forecasts()` → `get_consensus_forecasts()`

Rename and enhance in `bot/bonding/weather_client.py`:
- Fetches GFS (existing), ECMWF (new), tomorrow.io (new) for each city/date
- Returns `dict[tuple[str, date], SourceConsensus]` instead of `dict[tuple, ForecastResult]`

### Changes to `opportunity_scorer.py`

- `score_all()` and `score_market()` accept `dict[tuple, SourceConsensus]` instead of
  `dict[tuple, ForecastResult]`
- `prob` in `ScoredOpportunity` is now `consensus.consensus_prob(temp_min, temp_max)`
  instead of the raw GFS `prob_in_range()`
- All other logic (tiers, EV, edge, sizing, capital caps) unchanged

### Changes to `bot/main.py`

Replace `get_all_forecasts()` call with `get_consensus_forecasts()`.  
Pass `SourceConsensus` dict to both `score_all()` and (new) `score_certain()`.

---

## Feature 2: CERTAIN Tier (sure-thing bets)

### New module: `bot/bonding/sure_thing_scorer.py`

Produces `ScoredOpportunity` objects with `tier="CERTAIN"`.  
Called from the REST scan alongside `score_all()`.

```python
def score_certain(
    markets: list[MarketCandidate],
    consensus: dict[tuple[str, date], SourceConsensus],
) -> list[ScoredOpportunity]:
    ...
```

### Market candidate filter

CERTAIN only considers YES tokens (not NO — the favour is on the high-probability side):
- `market.best_ask` (YES token) between `CERTAIN_ASK_MIN` (0.75) and `CERTAIN_ASK_MAX` (0.95)
- `market.target_date` passes the existing time gate (same gate logic as standard tiers)
- Market must not already have an open position in the ledger

### Hard gates (all must pass)

| Gate | Config constant | Value |
|------|----------------|-------|
| Min probability per individual source | `CERTAIN_MIN_SOURCE_PROB` | 0.88 |
| Max inter-source point-forecast delta | `CERTAIN_MAX_TEMP_DELTA_C` | 2.0°C |
| Max ensemble spread (std dev across all members) | `CERTAIN_MAX_SPREAD_C` | 1.5°C |
| Min consensus probability | `CERTAIN_MIN_CONSENSUS_PROB` | 0.90 |
| Min sources available | `CERTAIN_MIN_SOURCES` | 3 |
| Min edge (consensus_prob − ask) | `CERTAIN_MIN_EDGE` | 0.05 |

All three sources must be present (`available_sources() == 3`). If any source
is unavailable (cache miss, API error), the market is skipped for CERTAIN — no
degraded fallback here, since the whole point is three-way agreement.

### Spread calculation

```python
all_members = gfs.ensemble_members + ecmwf.ensemble_members + tomorrowio.ensemble_members
spread_std = statistics.stdev(all_members)
```

### Scoring output

```python
ScoredOpportunity(
    tier="CERTAIN",
    outcome="YES",
    prob=consensus_prob,       # averaged across all 3 sources
    ev=consensus_prob - ask,
    edge=consensus_prob - ask,
    shares=CERTAIN_SHARES,     # 20 (conservative during validation)
    ...
)
```

### Exit strategy

Hold to resolution. `exit_manager.py` must explicitly **skip** the CORE early-exit
check (price ≥ 0.97) for CERTAIN positions — that trigger is irrelevant at these
ask levels and would cause premature exits near resolution. CERTAIN positions are
flagged `status=OPEN` until market resolution only.

### Position sizing

`CERTAIN_SHARES = 20` — conservative during validation phase.  
CERTAIN bets use a **separate** cluster cap: `CERTAIN_MAX_CAPITAL_PER_CLUSTER = $20.00`
(20 shares × max ask 0.95 = $19.00, fits within cap).  
This cap is tracked independently from `BOND_MAX_CAPITAL_PER_CLUSTER` so CERTAIN bets
do not crowd out standard BOND positions in the same city/date cluster.

---

## New Config Constants (`bot/config.py`)

```python
# ─── Cross-source weather ─────────────────────────────────────────
TOMORROW_IO_API_KEY = os.getenv("TOMORROW_IO_API_KEY", "")
TOMORROW_IO_CACHE_TTL_SECS = 10800  # 3 hours
TOMORROW_IO_MAX_REQUESTS_PER_HOUR = 20  # headroom below 25/hr hard limit
ECMWF_ENSEMBLE_MODEL = "ecmwf_ifs04"

# ─── CERTAIN tier ────────────────────────────────────────────────
CERTAIN_ASK_MIN             = 0.75   # min YES ask for CERTAIN candidates
CERTAIN_ASK_MAX             = 0.95   # max YES ask for CERTAIN candidates
CERTAIN_MIN_SOURCE_PROB     = 0.88   # each source must reach this individually
CERTAIN_MAX_TEMP_DELTA_C    = 2.0    # max spread between source point forecasts
CERTAIN_MAX_SPREAD_C        = 1.5    # max std dev of combined ensemble members
CERTAIN_MIN_CONSENSUS_PROB  = 0.90   # averaged probability floor
CERTAIN_MIN_SOURCES         = 3      # all sources must be present
CERTAIN_MIN_EDGE            = 0.05   # consensus_prob - ask
CERTAIN_SHARES              = 20     # conservative position size during validation
CERTAIN_MAX_CAPITAL_PER_CLUSTER = 20.00  # separate cap from BOND_MAX_CAPITAL_PER_CLUSTER
```

---

## Files Modified

| File | Change |
|------|--------|
| `bot/bonding/weather_client.py` | Add `SourceConsensus` dataclass, `get_ecmwf_forecast()`, `get_consensus_forecasts()` |
| `bot/bonding/opportunity_scorer.py` | Accept `SourceConsensus`, use `consensus_prob` |
| `bot/bonding/exit_manager.py` | Handle `tier="CERTAIN"`: hold-to-resolution only, skip CORE 0.97 early-exit check |
| `bot/main.py` | Wire `get_consensus_forecasts()` and `score_certain()` into scan loop |
| `bot/config.py` | Add all new constants above |

## Files Created

| File | Purpose |
|------|---------|
| `bot/bonding/tomorrow_client.py` | tomorrow.io API client with cache and rate limiting |
| `bot/bonding/sure_thing_scorer.py` | CERTAIN tier scoring logic |

---

## Verification

1. **Unit tests** for `SourceConsensus.consensus_prob()` — verify averaging with 1, 2, and 3 sources
2. **Unit tests** for `sure_thing_scorer.score_certain()` — verify each hard gate independently blocks bets
3. **tomorrow_client tests** — mock HTTP, verify cache TTL, rate limit guard, graceful degradation
4. **Integration smoke test** — run paper mode locally, confirm CERTAIN bets appear in `paper_trades.jsonl` with `tier=CERTAIN`
5. **Regression check** — confirm existing CORE/SECONDARY/WING bets still fire with consensus_prob replacing single-source prob (EV should shift slightly but not dramatically)
6. **Rate limit log check** — after a full 24h paper run, confirm tomorrow.io call count stays under 500/day via log grep
