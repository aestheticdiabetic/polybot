#!/usr/bin/env python3
"""
Quick test script to verify latency optimization changes don't break scanner.
Run: python3 test_latency.py
"""
import asyncio
import time
from bot.scanner import Scanner, MarketInfo

async def test_metadata_cache():
    """Test metadata caching and TTL."""
    print("Testing metadata caching...")

    scanner = Scanner(
        on_bracket=lambda x: None,
        on_near_bracket=lambda x: None,
    )

    # Verify cache fields exist
    assert hasattr(scanner, '_metadata_cache')
    assert hasattr(scanner, '_metadata_cache_time')
    assert hasattr(scanner, '_metadata_cache_ttl')
    assert scanner._metadata_cache_ttl == 300.0, "TTL should be 300s (5 min)"
    print("✓ Cache fields initialized correctly")

    # Verify stats
    assert 'metadata_fetches_http' in scanner.stats
    assert 'metadata_fetches_cache' in scanner.stats
    assert scanner.stats['metadata_fetches_http'] == 0
    assert scanner.stats['metadata_fetches_cache'] == 0
    print("✓ Cache stats initialized")

async def test_discovery_loop():
    """Test that _market_discovery_loop method exists and is callable."""
    print("\nTesting discovery loop...")

    scanner = Scanner(
        on_bracket=lambda x: None,
        on_near_bracket=lambda x: None,
    )

    # Verify method exists
    assert hasattr(scanner, '_market_discovery_loop')
    assert callable(getattr(scanner, '_market_discovery_loop'))
    print("✓ _market_discovery_loop exists and is callable")

async def test_bracket_metadata_age():
    """Test that BracketOpportunity has metadata_age_ms field."""
    print("\nTesting BracketOpportunity metadata_age_ms...")

    from bot.scanner import BracketOpportunity

    market = MarketInfo(
        token_id_up="123",
        token_id_down="456",
        condition_id="cond_1",
        title="ETH UP/DOWN",
        window="5M",
        asset="ETH",
        end_time=time.time() + 3600,
    )

    opp = BracketOpportunity(
        market=market,
        ask_up=0.5,
        ask_down=0.5,
        combined_ask=1.0,
        spread=0.0,
        gross_profit_usdc=0.0,
        net_profit_usdc=0.0,
        detected_at=time.time(),
        metadata_age_ms=42.0,
    )

    assert opp.metadata_age_ms == 42.0
    print(f"✓ BracketOpportunity.metadata_age_ms works: {opp.metadata_age_ms}ms")

async def main():
    """Run all tests."""
    print("=" * 60)
    print("LATENCY OPTIMIZATION TEST SUITE")
    print("=" * 60)

    await test_metadata_cache()
    await test_discovery_loop()
    await test_bracket_metadata_age()

    print("\n" + "=" * 60)
    print("✓ ALL TESTS PASSED")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Deploy with: docker-compose up -d")
    print("2. Monitor logs for metadata_age_ms in BRACKET messages")
    print("3. Expected: metadata_age_ms < 1000ms (cache hits)")
    print("4. Check stats: metadata_fetches_cache >> metadata_fetches_http")

if __name__ == "__main__":
    asyncio.run(main())
