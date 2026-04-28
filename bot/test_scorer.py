"""
test_scorer.py — Unit tests for opportunity_scorer share-sizing and fill logic.

Run from the bot/ directory:
    python -m pytest test_scorer.py -v
"""
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from bonding.opportunity_scorer import (
    _compute_fill_split,
    _shares_for_tier,
    TIER_CHEAP,
    TIER_CORE,
)


# ── _compute_fill_split ───────────────────────────────────────────────────────

class TestComputeFillSplit:
    def test_empty_book_returns_all_shares_immediate(self):
        """No depth info → FOK all shares (old behaviour, caller decides entry)."""
        immediate, limit = _compute_fill_split([], 16, 0.073)
        assert immediate == 16
        assert limit == 0

    def test_single_level_fully_covered(self):
        book = [(0.063, 20.0)]
        immediate, limit = _compute_fill_split(book, 16, 0.073)
        assert immediate == 16
        assert limit == 0

    def test_single_level_partially_covered(self):
        """Only 3 shares at profitable price — immediate capped at depth."""
        book = [(0.063, 3.0)]
        immediate, limit = _compute_fill_split(book, 16, 0.073)
        assert immediate == 3
        assert limit == 13

    def test_sweeps_multiple_price_levels(self):
        """3 @ 0.063 + 40 @ 0.065 — both below max_profitable 0.073, total 43."""
        book = [(0.063, 3.0), (0.065, 40.0)]
        immediate, limit = _compute_fill_split(book, 16, 0.073)
        assert immediate == 16
        assert limit == 0

    def test_excludes_levels_above_max_profitable(self):
        """Levels above max_profitable_price are not counted."""
        book = [(0.063, 3.0), (0.075, 40.0)]  # 0.075 > 0.073
        immediate, limit = _compute_fill_split(book, 16, 0.073)
        assert immediate == 3
        assert limit == 13

    def test_no_profitable_depth(self):
        """All levels above max_profitable → 0 immediate, all as limit."""
        book = [(0.080, 50.0)]  # 0.08 > 0.073
        immediate, limit = _compute_fill_split(book, 16, 0.073)
        assert immediate == 0
        assert limit == 16

    def test_fractional_size_truncated(self):
        """Size 3.9 truncated to int(3) shares."""
        book = [(0.063, 3.9)]
        immediate, limit = _compute_fill_split(book, 16, 0.073)
        assert immediate == 3


# ── _shares_for_tier ─────────────────────────────────────────────────────────

class TestSharesForTier:
    def test_cheap_at_0063(self):
        """ceil(1.00 / 0.063) = 16 shares for CHEAP tier."""
        assert _shares_for_tier(TIER_CHEAP, 0.063) == 16

    def test_cheap_capped_at_max(self):
        """Very low ask — capped at BOND_SHARES_CHEAP_MAX (75)."""
        shares = _shares_for_tier(TIER_CHEAP, 0.010)
        assert shares == 75   # ceil(1.00 / 0.01) = 100, capped at 75

    def test_core_base_count(self):
        """CORE at 0.25 → max(10, ceil(1/0.25)) = max(10, 4) = 10."""
        import config
        assert _shares_for_tier(TIER_CORE, 0.25) == config.BOND_SHARES_CORE

    def test_core_floor_at_low_ask(self):
        """CORE at 0.09 → max(10, ceil(1/0.09)) = max(10, 12) = 12."""
        shares = _shares_for_tier(TIER_CORE, 0.09)
        assert shares >= 12


# ── minimum capital guard (integration via _score_side) ──────────────────────

class TestMinimumCapitalGuard:
    """
    Test that _score_side rejects entries where fillable capital < $1.

    We construct a minimal MarketCandidate + SourceConsensus so we can call
    score_market() directly and verify the DEPTH_MISS path triggers.
    """

    def _make_market(self, ask: float, ask_book: list):
        from bonding.market_scanner import MarketCandidate
        return MarketCandidate(
            market_id="test-market-id",
            token_id="test-token-yes",
            question="Will the highest temperature in Austin be 95°F on April 28?",
            city="Austin",
            target_date=date(2026, 4, 28),
            temp_min=34.5,   # 94°F = 34.4°C
            temp_max=35.5,   # 96°F = 35.6°C
            unit="C",
            best_ask=ask,
            resolution_time=datetime(2026, 4, 28, 23, 0, 0, tzinfo=timezone.utc),
            ask_book=ask_book,
            no_token_id="test-token-no",
            no_best_ask=1.0 - ask,
            no_ask_book=[],
        )

    def _make_forecast(self, prob_yes: float = 0.143):
        """Build a minimal SourceConsensus that returns a fixed probability."""
        from unittest.mock import MagicMock
        consensus = MagicMock()
        consensus.consensus_prob.return_value = prob_yes
        consensus.available_sources.return_value = 3
        consensus.point_forecasts.return_value = [34.8, 35.1, 35.0]
        gfs = MagicMock()
        gfs.forecast_peak_hour = 14
        gfs.daily_max_c = 35.0
        consensus.gfs = gfs
        return consensus

    def test_thin_depth_becomes_depth_miss(self):
        """
        Austin scenario: only 3 shares at 0.063 = $0.189 < $1 minimum.
        _score_side should set shares_immediate=0, causing a DEPTH_MISS.
        """
        from bonding.opportunity_scorer import _score_side
        market = self._make_market(ask=0.063, ask_book=[(0.063, 3.0)])
        forecast = self._make_forecast(prob_yes=0.143)

        with patch("config.BOND_CITY_TIMEZONES", {"Austin": "America/Chicago"}):
            opp = _score_side(
                market=market,
                forecast=forecast,
                prob=0.143,
                ask=0.063,
                ask_book=[(0.063, 3.0)],
                token_id="test-token-yes",
                outcome="YES",
            )

        assert opp is not None, "Should return opportunity (for DEPTH_MISS logging)"
        assert opp.shares_immediate == 0, (
            f"Expected 0 shares_immediate (depth miss), got {opp.shares_immediate} "
            f"(capital would be ${opp.shares_immediate * 0.063:.4f})"
        )

    def test_adequate_depth_with_sweep_enters(self):
        """
        3 shares @ 0.063 + 40 shares @ 0.065 → 16 shares fillable = $1.008.
        Should enter normally (shares_immediate = 16).
        """
        from bonding.opportunity_scorer import _score_side
        market = self._make_market(
            ask=0.063,
            ask_book=[(0.063, 3.0), (0.065, 40.0)],
        )
        forecast = self._make_forecast(prob_yes=0.143)

        with patch("config.BOND_CITY_TIMEZONES", {"Austin": "America/Chicago"}):
            opp = _score_side(
                market=market,
                forecast=forecast,
                prob=0.143,
                ask=0.063,
                ask_book=[(0.063, 3.0), (0.065, 40.0)],
                token_id="test-token-yes",
                outcome="YES",
            )

        assert opp is not None
        assert opp.shares_immediate == 16, (
            f"Expected 16 shares_immediate (sweeping both levels), got {opp.shares_immediate}"
        )
        assert opp.shares_immediate * opp.side_ask >= 1.00

    def test_exact_threshold_boundary(self):
        """shares_immediate * ask == exactly $1.00 → should enter."""
        from bonding.opportunity_scorer import _score_side
        # ceil(1/0.05) = 20 shares; 20 * 0.05 = $1.00 exactly
        ask = 0.05
        shares_wanted = 20
        book = [(ask, float(shares_wanted))]

        with patch("config.BOND_CITY_TIMEZONES", {"Austin": "America/Chicago"}):
            opp = _score_side(
                market=self._make_market(ask=ask, ask_book=book),
                forecast=self._make_forecast(prob_yes=0.15),
                prob=0.15,
                ask=ask,
                ask_book=book,
                token_id="test-token-yes",
                outcome="YES",
            )

        # $1.00 == $1.00 minimum → should enter (not a depth miss)
        assert opp is not None
        assert opp.shares_immediate == shares_wanted

    def test_empty_book_still_enters(self):
        """
        Gamma-sourced price (no depth info) → ask_book=[], all shares immediate.
        Old behaviour preserved: we attempt FOK with full share count.
        """
        from bonding.opportunity_scorer import _score_side
        # ceil(1/0.063) = 16 shares; 16 * 0.063 = $1.008 ≥ $1 → enters
        ask = 0.063
        with patch("config.BOND_CITY_TIMEZONES", {"Austin": "America/Chicago"}):
            opp = _score_side(
                market=self._make_market(ask=ask, ask_book=[]),
                forecast=self._make_forecast(prob_yes=0.143),
                prob=0.143,
                ask=ask,
                ask_book=[],
                token_id="test-token-yes",
                outcome="YES",
            )

        assert opp is not None
        assert opp.shares_immediate == 16
