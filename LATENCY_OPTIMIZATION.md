# Latency Optimization: Options 1-2

## Changes Implemented

### Option 1: Metadata Caching + Reduced Polling
- **Before**: HTTP fetch every 60s, 100% fresh API calls
- **After**: HTTP fetch every 5 min (cached), poll every 10s (mostly cache hits)
- **Impact**: Eliminates ~12s wait for HTTP refresh when detecting new markets

### Option 2: Parallel HTTP & WebSocket
- **Before**: `asyncio.gather(_market_refresh_loop(), _ws_loop())`
- **After**: `asyncio.gather(_market_discovery_loop(), _ws_loop(), _market_refresh_loop())`
- **Impact**: HTTP latency no longer blocks real-time price updates

## Expected Log Changes

### Before (Old Behavior)
```
2026-04-03 00:01:46,456 [trader] INFO: [PRESIGN] SOL 5M ready | lim_up=0.800 lim_dn=0.200
2026-04-03 00:01:58,779 [scanner] INFO: BRACKET SOL 5M | ... metadata_age=12326ms
2026-04-03 00:01:58,783 [trader] INFO: [56c37cb1] Pre-signed stale/invalid — signing fresh
```
**Gap: 12.3 seconds between near-bracket and bracket (prices drifted 5 ticks)**

### After (New Behavior)
Expected logs should show:
1. `metadata_age_ms` in bracket logs should be **< 100ms** (cache hits) instead of 0-12,000ms
2. If you see HTTP latency (metadata_fetches_http in stats), it will be ~5 min apart, not 1 min
3. Fewer pre-signed invalidations because metadata is fresher

### Example After
```
2026-04-03 00:01:46,456 [trader] INFO: [PRESIGN] SOL 5M ready | metadata_age=45ms
2026-04-03 00:01:46,587 [scanner] INFO: BRACKET SOL 5M | ... metadata_age=186ms
2026-04-03 00:01:46,590 [trader] INFO: [56c37cb1] Using pre-signed orders (age=134ms)
```
**Gap: ~134ms between near-bracket and bracket (price stable)**

## Monitoring Checklist

**Deploy and watch logs for:**

1. ✅ `metadata_age_ms` in BRACKET logs:
   - `< 1000ms` = cache working (good)
   - `> 5000ms` = cache miss (rare, but OK if HTTP succeeds)

2. ✅ Stats should show majority cache hits:
   - `metadata_fetches_cache >> metadata_fetches_http`
   - E.g., 1000 cache hits / 2 HTTP fetches (ratio ~500:1)

3. ✅ Pre-signed order usage:
   - Log should show `Using pre-signed orders` more often
   - Fewer `Pre-signed stale/invalid` messages
   - If stale still happens, it should be < 500ms old, not 12s

4. ✅ Bracket detection latency:
   - Time from [PRESIGN] to [BRACKET] should be < 500ms
   - Previously was 12+ seconds

5. ✅ Fewer emergency exits:
   - Partial fills should decrease (DOWN leg should fill more often)
   - Emergency exit failures should trend toward 0

## Performance Metrics

### Latency Targets

| Metric | Before | Target | How |
|--------|--------|--------|-----|
| HTTP → Cache | 12s gap | <100ms | TTL caching |
| WS blocked by HTTP | ~100ms | 0ms | Parallel tasks |
| Presign age at bracket | 12s | <500ms | Fresh metadata |
| Total detection gap | 12.3s | <500ms | All above |

### Stats to Compare

**Before deployment** (commit e0a27cc):
```python
scanner.stats["metadata_fetches_http"]    # ~1 per 60s
scanner.stats["metadata_fetches_cache"]   # 0 (no cache)
```

**After deployment** (commit ac56f3b):
```python
scanner.stats["metadata_fetches_http"]    # ~1 per 5 min (12x less!)
scanner.stats["metadata_fetches_cache"]   # ~30 per 5 min (300x more!)
```

## Regression Tests

**Verify no breakage:**

1. ✅ Markets are still detected correctly
   - `markets_tracked` count should be same or higher
   - `markets_active` should stabilize (may dip if WS stalls)

2. ✅ Brackets are still found
   - `brackets_detected` rate should be stable or higher
   - `brackets_throttled` should be similar

3. ✅ Near-bracket pre-signing works
   - `near_brackets_detected` should be high
   - [PRESIGN] messages should appear frequently

4. ✅ Order placement succeeds
   - `brackets_opened` rate stable
   - "Both legs filled" messages should be common

## Rollback Plan

If latency WORSENS after deployment:

1. Revert commit ac56f3b: `git reset --hard e0a27cc`
2. Investigate: Check if cache TTL too long or polling too slow
3. Consider Option 3 if cache approach isn't sufficient

## Next Steps (Option 3)

If metadata_age_ms still > 1000ms frequently, implement **Option 3**:
- Track all token IDs from WS
- Derive market info from token pairs without waiting for HTTP
- Direct WS-based detection (no metadata dependency)
- More complex but near-zero latency
