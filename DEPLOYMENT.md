# Deployment Guide: Latency Optimization (ac56f3b)

## Quick Start

```bash
# Pull latest changes (commit ac56f3b)
git fetch && git pull

# Rebuild container
docker-compose up -d --build

# Monitor logs for the first 10 minutes
docker-compose logs -f polybot | grep -E "BRACKET|PRESIGN|metadata"
```

## What Changed

Commit **ac56f3b** implements two latency reduction options:

### Option 1: Metadata Caching (60s → 5min HTTP calls)
- Market metadata is now cached for 5 minutes
- Polling runs every 10s (cache hits are instant)
- Reduces HTTP roundtrips from 1/60s → 1/300s

### Option 2: Parallel HTTP & WebSocket (Decoupled)
- New `_market_discovery_loop()` runs HTTP in background
- WebSocket processing no longer blocked by HTTP latency
- 3 concurrent tasks ensure no starvation

## Pre-Deployment Checklist

**Verify test suite passes:**
```bash
python3 test_latency.py
# Expected output: ✓ ALL TESTS PASSED
```

**Check current metrics (for before/after comparison):**
```bash
docker-compose exec polybot curl -s http://localhost:8080/api/stats | jq '.scanner'
# Record: brackets_detected, near_brackets_detected, markets_tracked
```

## Deployment Steps

1. **Build new image:**
   ```bash
   docker-compose build polybot
   ```

2. **Stop current container gracefully:**
   ```bash
   docker-compose stop polybot
   # Wait ~5s for cleanup
   ```

3. **Start new container:**
   ```bash
   docker-compose up -d polybot
   ```

4. **Verify health:**
   ```bash
   sleep 10
   docker-compose ps
   # Status should be 'healthy' after 15s
   ```

## Monitoring: Key Metrics to Watch

### Logs (first 5 minutes)

**Good signs:**
```
[PRESIGN] SOL 5M ready | metadata_age=45ms
BRACKET SOL 5M | ... metadata_age=186ms
```

**Bad signs:**
```
[PRESIGN] SOL 5M ready | metadata_age=12000ms  # Cache miss?
Pre-signed stale/invalid — signing fresh      # Prices moved too much?
```

### Dashboard Stats (http://localhost:8080)

**Expected numbers after 1 hour:**

| Metric | Before | After | Interpretation |
|--------|--------|-------|---|
| `brackets_detected` | Same | Same or +5% | More opportunities caught |
| `near_brackets_detected` | Same | Same or +10% | Faster near-bracket detection |
| Cache hit rate | 0% | >95% | Caching working |
| HTTP calls/min | ~1 | ~0.2 | Reduced HTTP load |

**To see stats:**
```bash
curl -s http://localhost:8080/api/stats | jq '.scanner | {
  brackets_detected,
  near_brackets_detected,
  markets_tracked,
  metadata_fetches_http,
  metadata_fetches_cache,
  ws_reconnects
}'
```

### Trade Quality Indicators

**Expected improvements (measured over 1 hour):**

| Metric | Target |
|--------|--------|
| Partial fills (DOWN cancelled) | ↓ 30-50% |
| Pre-signed order usage | ↑ 50-100% (more age < 1s) |
| Emergency exit attempts | ↓ 30-50% |
| Net profit per trade | ↑ ~2-5% (fewer slips) |

**To monitor:**
```bash
# Watch logs for 10 minutes and count:
docker-compose logs -f polybot | grep -c "Both legs filled"
docker-compose logs -f polybot | grep -c "Partial fill"
docker-compose logs -f polybot | grep -c "Emergency exit"
```

## Expected Log Changes

### Before (Old: 60s polling)
```
00:01:46,456 [trader] INFO: [PRESIGN] SOL 5M ready | lim_up=0.800 lim_dn=0.200
00:01:58,779 [scanner] INFO: BRACKET SOL 5M | ... metadata_age=12326ms
00:01:58,783 [trader] WARNING: Pre-signed stale/invalid (age=12326ms)
```
**Problem:** 12.3s gap, prices drifted 5 ticks, presigned invalid

### After (New: 5min HTTP, 10s poll)
```
00:01:46,456 [trader] INFO: [PRESIGN] SOL 5M ready | metadata_age=45ms
00:01:46,587 [scanner] INFO: BRACKET SOL 5M | ... metadata_age=186ms
00:01:46,590 [trader] INFO: Using pre-signed orders (age=134ms)
```
**Expected:** ~150ms gap, prices stable, presigned reused

## Troubleshooting

### Issue: More `Pre-signed stale/invalid` messages?

**Cause:** Cache is too stale (TTL was 300s, now fixed to 30s)

**Solution:** 
1. Verify cache TTL is 30 seconds: `self._metadata_cache_ttl = 30.0` (line 84 in scanner.py)
2. If still seeing metadata_age > 5000ms, reduce further to 15-20s:
   ```python
   self._metadata_cache_ttl = 15.0  # Line 84 in scanner.py
   ```
3. Rebuild: `docker-compose up -d --build`

**Note**: The original 300s TTL was too aggressive — metadata aged 300+ seconds before refresh, defeating the optimization. 30s is the correct value.

### Issue: `markets_tracked` decreased?

**Cause:** Market expiry or inactivity (normal)

**Check:**
```bash
docker-compose logs polybot | grep "Markets: pruned"
# Should show: pruned X expired, Y inactive, Z remaining
```

**If > 20% pruned:** Possible WS connectivity issue
```bash
docker-compose logs polybot | grep "WS stale\|WebSocket error"
```

### Issue: No reduction in metadata_age_ms?

**Cause:** Cache not being used (HTTP calls not happening)

**Check stats:**
```bash
curl -s http://localhost:8080/api/stats | jq '.scanner | {http: .metadata_fetches_http, cache: .metadata_fetches_cache}'
```

**If cache = 0:** 
1. Cache not initialized properly
2. Try full restart: `docker-compose down && docker-compose up -d`

### Issue: Performance worse than before?

**Cause:** Possible parallel task contention

**Rollback:**
```bash
git reset --hard e0a27cc  # Previous commit
docker-compose up -d --build
```

**Then escalate:** Implement Option 3 (direct WS detection)

## Performance Benchmarks

### Expected Latency Improvements

| Step | Before | After | Reduction |
|------|--------|-------|-----------|
| HTTP poll blocking | 60s | 0s | 100% |
| Market refresh delay | 60s | 10s | 83% |
| Metadata staleness | 12-60s | <100ms | 99% |
| Near-bracket → Bracket gap | 12.3s | <500ms | 96% |
| Presign validity at execution | 12s old | <200ms old | 98% |

### Estimated Impact on Profitability

**Scenario:** 10 brackets/hour, 10% currently partial-fill rate

- **Before:** 1 partial fill/hour → +2% emergency exit losses
- **After:** 0.5 partial fills/hour → -1% emergency exit losses
- **Net:** +3% improvement in net profit/hour

**For $5/leg position:** ~$0.15 saved per partial fill avoided

## Success Criteria

✅ **Deployment successful if:**

1. **Logs show:**
   - `metadata_age_ms < 1000ms` in BRACKET messages (95%+ of the time)
   - `Using pre-signed orders` messages appear frequently
   - Few/no `Pre-signed stale/invalid` messages

2. **Stats show:**
   - `metadata_fetches_cache >> metadata_fetches_http` (ratio >50:1)
   - `brackets_detected` ≥ previous (no regression)
   - `markets_tracked` stable

3. **Trades improve:**
   - Partial fill rate ↓ 30-50%
   - Emergency exits ↓ 30-50%
   - Net profit per trade ↑ 2-5%

## Post-Deployment Tasks

1. **Monitor for 1 hour:**
   ```bash
   watch -n 10 'docker-compose logs polybot | tail -20'
   ```

2. **Capture baseline metrics:**
   ```bash
   curl -s http://localhost:8080/api/stats > stats_after_ac56f3b.json
   ```

3. **Compare before/after:**
   ```bash
   jq '.scanner | {brackets: .brackets_detected, near_brackets: .near_brackets_detected}' stats_after_ac56f3b.json
   ```

4. **If all green:** No further action needed
5. **If issues:** Follow troubleshooting section above

## Rollback Procedure

If latency WORSENS after deployment:

```bash
# Revert to e0a27cc (previous known-good commit)
git reset --hard e0a27cc

# Rebuild and deploy
docker-compose up -d --build

# Monitor: should return to previous behavior
docker-compose logs -f polybot | grep metadata_age
```

## Next Steps

### If latency still > 1 second:
Implement **Option 3** (direct WS detection)
- Track token ID → market mapping in memory
- Detect brackets from WS alone (no HTTP dependency)
- Would reduce latency further to < 200ms

### If latency < 500ms and stable:
✅ **Optimization complete!** Monitor for 1 week, then consider:
- Wider DOWN limit (already done)
- DOWN-first batch order (already done)
- Depth-proportional sizing (already done)

These are already implemented in earlier commits.
