# Bond Paper Trade Analysis — 2026-04-14

## Overview

Analysis of all 1,166 WOULD_BUY entries in `paper_trades.jsonl` as of 2026-04-14.
827 resolved (SOLD or YES/NO outcome), 339 still open.

---

## Overall Statistics

| Metric | Value |
|--------|-------|
| Resolved trades | 827 |
| Open trades | 339 |
| Capital deployed | $1,119.87 |
| Total P&L | +$219.56 |
| ROI | 19.6% |
| Overall win rate | 13.9% (115W / 712L) |

---

## Critical Finding: Three Completely Different Strategies

| Outcome Type | N | Win Rate | P&L | ROI |
|---|---|---|---|---|
| **SOLD (early exit)** | 176 | **55.7%** | **+$859.57** | **+367%** |
| Resolved YES | 125 | 9.6% | -$63.60 | -25% |
| Resolved NO | 526 | 1.0% | -$576.42 | **-91%** |

**The profit engine is entirely the SOLD early-exit strategy.** Resolution bets are net-negative by $640. The bot is effectively an intraday price-movement trader, not a weather resolution predictor.

---

## By Tier

| Tier | N | Win Rate | P&L | ROI |
|---|---|---|---|---|
| CHEAP | 614 | 11.7% | +$199.69 | +28.4% |
| CORE | 212 | 19.8% | +$17.46 | +4.4% |
| CERTAIN | 1 | 100% | +$2.40 | +13.6% |

CHEAP dominates volume and profit. CORE has mediocre ROI because losses (-$2-3 at 20-30¢) overwhelm wins.

---

## By Side

| Side | N | Win Rate | P&L | ROI |
|---|---|---|---|---|
| YES | 687 | 13.4% | +$242.14 | **+28.6%** |
| NO | 140 | 16.4% | **-$22.58** | **-8.3%** |

NO bets are net-negative despite similar headline win rate because capital loss per NO-bet-loss is larger.

---

## By Price Bucket

| Price | N | Win Rate | P&L | ROI |
|---|---|---|---|---|
| 02-05¢ | 309 | 11.3% | +$104.02 | +35.0% |
| 05-10¢ | 324 | 12.3% | +$109.63 | +25.7% |
| 10-15¢ | 50 | 10.0% | -$11.86 | -19.7% |
| 15-20¢ | 55 | 21.8% | +$27.22 | +29.0% |
| 20-30¢ | 74 | 27.0% | +$10.55 | +5.8% |
| 30-50¢ | 14 | 14.3% | -$22.40 | -53.3% |
| 50-95¢ | 1 | 100% | +$2.40 | +13.6% |

Sweet spot: 2-10¢ and 15-20¢. The 10-15¢ and 30-50¢ buckets are drains.

---

## By City (Top 15 by Volume)

| City | N | Win Rate | P&L |
|---|---|---|---|
| **Taipei** | 23 | **34.8%** | **+$69.88** |
| **Chongqing** | 26 | 19.2% | +$29.16 |
| **London** | 25 | 24.0% | +$21.97 |
| Tel Aviv | 24 | 16.7% | +$11.89 |
| Milan | 21 | 14.3% | +$10.59 |
| Toronto | 33 | 18.2% | +$7.96 |
| Singapore | 22 | 9.1% | +$0.25 |
| Sao Paulo | 32 | 15.6% | +$0.54 |
| Warsaw | 25 | 16.0% | +$1.87 |
| Madrid | 22 | 13.6% | +$5.20 |
| Wellington | 21 | 14.3% | +$6.92 |
| Seoul | 26 | 15.4% | -$5.53 |
| Moscow | 21 | 9.5% | -$6.97 |
| Lucknow | 25 | 8.0% | -$12.36 |
| Buenos Aires | 26 | 11.5% | -$17.77 |

---

## Top 15 Individual Wins

All top 15 are CHEAP YES bets. Average SOLD multiple: 8.7x. Best: 37x (Jeddah).

| Rank | Tier | Side | Ask | P&L | Exit | City |
|---|---|---|---|---|---|---|
| 1 | CHEAP | YES | 2.7¢ | +$36.90 | SOLD | Jeddah |
| 2 | CHEAP | YES | 3.1¢ | +$31.98 | YES | Los Angeles |
| 3 | CHEAP | YES | 4.5¢ | +$23.83 | SOLD | Chengdu |
| 4 | CHEAP | YES | 7.0¢ | +$23.25 | YES | Taipei |
| 5 | CHEAP | YES | 6.5¢ | +$23.15 | SOLD | Wuhan |
| 6 | CHEAP | YES | 8.0¢ | +$23.00 | YES | Istanbul |
| 7 | CHEAP | YES | 4.2¢ | +$22.97 | SOLD | Lagos |
| 8 | CHEAP | YES | 8.0¢ | +$22.88 | SOLD | Istanbul |
| 9 | CHEAP | YES | 7.4¢ | +$21.21 | SOLD | London |
| 10 | CHEAP | NO | 8.0¢ | +$19.25 | SOLD | Miami |

---

## SOLD Exit Analysis

| Exit Reason | N | Win Rate | P&L |
|---|---|---|---|
| UNKNOWN (old log format) | 82 | 100% | +$762.90 |
| PROFIT_EXIT | 16 | 100% | +$190.04 |

SOLD exits via WOULD_SELL records are **always profitable**. The 78 "SOLD" outcomes
that appear as losses are WOULD_BUY records patched by `_patch_would_buy()` — these
are handled correctly by the dashboard's recompute-from-WOULD_SELL logic.

Best SOLD multiples (entry → exit):
- 37.0x: Jeddah, 2.7¢ → 99.8¢
- 23.8x: Lagos, 4.2¢ → 99.9¢
- 23.0x: Shanghai, 2.0¢ → 46¢
- 22.2x: Chengdu, 4.5¢ → 99.8¢
- 18.4x: Chongqing, 4.3¢ → 79¢

---

## Root Cause: Model is Severely Miscalibrated

### YES bets: Model prob vs actual outcome

| Model says | Actual win rate | N |
|---|---|---|
| ~10% | 1.1% | 267 |
| ~20% | 0.0% | 133 |
| ~30% | 4.8% | 83 |
| ~40% | 10.5% | 38 |

Average model prob: **18.3%**. Average actual win rate (resolved only): **2.3%**.
Market-implied rate at 3-8¢: **3-8%**. **The market is better calibrated than our model.**

### NO bets: Model prob vs actual outcome

Average model P(NO): **55.1%**. Actual NO win rate: **4.2%**.
The temperature reaches the threshold ~95.8% of the time we bet NO.

### Interpretation

The model overestimates YES probability and overestimates NO probability simultaneously.
This is because weather reach/miss thresholds are highly non-linear and the ensemble's
raw output is not calibrated against historical Polymarket resolution data.

The strategy's true edge is **intraday price momentum**: buy low when the market sees
a small chance, then sell when temperature readings during the day make the market move
up temporarily. This doesn't require accurate resolution probability — only accurate
intraday direction.

---

## CORE Tier Detailed Bucket Analysis

### Overall (YES + NO combined)

| Bucket | N | WR | P&L | ROI | Sold P&L | Res WR | Res P&L |
|---|---|---|---|---|---|---|---|
| 08-10¢ | 19 | 15.8% | +$13.95 | +70.5% | +$29.62 | 0% | -$15.67 |
| 10-12¢ | 20 | 5.0% | -$11.56 | -54.1% | +$3.36 | 0% | -$14.93 |
| 12-15¢ | 30 | 13.3% | -$0.30 | -0.8% | +$32.04 | 0% | -$32.34 |
| 15-20¢ | 55 | 21.8% | +$27.22 | +29.0% | +$47.23 | 12.2% | -$20.01 |
| 20-25¢ | 34 | 26.5% | +$10.75 | +14.4% | +$52.24 | 7.7% | -$41.49 |
| 25-30¢ | 40 | 27.5% | -$0.20 | -0.2% | +$48.68 | 10.3% | -$48.88 |

Pattern: **SOLD exits carry all profit in every bucket.** Resolution wins are near-zero.
The 10-12¢ bucket is the worst: few SOLD exits, all resolutions lose.

### CORE YES by bucket

| Bucket | N | WR | P&L | ROI |
|---|---|---|---|---|
| 08-10¢ | 18 | 16.7% | +$15.03 | +80.3% |
| 10-12¢ | 16 | 6.2% | -$7.26 | -42.6% |
| 12-15¢ | 17 | 23.5% | +$16.95 | +78.7% |
| 15-20¢ | 40 | 12.5% | -$14.72 | -21.8% |
| 20-25¢ | 18 | 33.3% | +$17.44 | +45.6% |
| 25-30¢ | 11 | 27.3% | -$0.43 | -1.4% |

### CORE NO by bucket

| Bucket | N | WR | P&L | ROI |
|---|---|---|---|---|
| 08-10¢ | 1 | 0% | -$1.08 | -100% |
| 10-12¢ | 4 | 0% | -$4.30 | -100% |
| 12-15¢ | 13 | 0% | -$17.25 | -100% |
| **15-20¢** | **15** | **46.7%** | **+$41.94** | **+158.7%** |
| 20-25¢ | 16 | 18.8% | -$6.69 | -18.5% |
| 25-30¢ | 29 | 27.6% | +$0.23 | +0.3% |

The 15-20¢ CORE NO bucket is a genuine edge and should be preserved.
The 10-15¢ CORE NO buckets are total losses (0% WR, -$22.63).

---

## Changes Implemented (2026-04-14)

### 1. Targeted NO Bet Restrictions

**Problem**: CHEAP NO bets (6.1% WR, -$16.03) and CORE NO <15¢ (0% WR, -$22.63) are structural losses.
**Preserved**: CORE NO 15-20¢ (46.7% WR, +$41.94) — this is a genuine edge.

**Config changes** (`bot/config.py`):
```python
BOND_CHEAP_NO_ENABLED:   bool  = False   # disable all CHEAP tier NO bets
BOND_CORE_NO_MIN_ASK:    float = 0.15    # skip CORE NO bets below this ask
```

**Enforcement** (`bot/bonding/opportunity_scorer.py`):
Added check in `_score_side()` after tier assignment:
- CHEAP + outcome=="NO" → skip if `BOND_CHEAP_NO_ENABLED` is False
- CORE + outcome=="NO" + ask < `BOND_CORE_NO_MIN_ASK` → skip

**Estimated P&L gain**: +$38.41 per equivalent trading period
(avoids -$16.03 CHEAP NO + -$22.63 low-CORE NO, retains +$41.94 high-CORE NO)

---

## Deferred Improvements

### Probability Calibration (Platt Scaling)
Fit `sigmoid(a * raw_prob + b)` on historical (model_prob, actual_outcome) pairs.
Would reduce overconfidence (18% model → 2.3% actual) and cut volume to higher-edge bets.
Risk: drastically reduces trade count. Needs paper mode validation.

### CORE 10-12¢ Ask Range Review
This bucket has -54.1% ROI with very few SOLD exits. Could add a dead zone exclusion
`BOND_CORE_DEAD_ZONE = (0.10, 0.12)` in `assign_tier()` to skip this range.
