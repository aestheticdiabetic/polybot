"""Tests for era5_sigma_calibration.py"""
import math
import statistics
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "bot"))

from bonding.era5_sigma_calibration import (
    _gaussian_cdf_diff,
    brier_score,
    optimal_sigma,
    compute_sigma_table,
    build_sigma_config,
)


# ── _gaussian_cdf_diff ────────────────────────────────────────────────────────

def test_gaussian_cdf_diff_centre():
    """Symmetric 1°C bucket centred on mean should capture ~38% for sigma=1."""
    p = _gaussian_cdf_diff(20.0, 1.0, 19.5, 20.5)
    assert 0.35 < p < 0.42, f"Expected ~0.38, got {p:.4f}"


def test_gaussian_cdf_diff_zero_sigma_inside():
    p = _gaussian_cdf_diff(20.0, 0.0, 19.5, 20.5)
    assert p == 1.0


def test_gaussian_cdf_diff_zero_sigma_outside():
    p = _gaussian_cdf_diff(25.0, 0.0, 19.5, 20.5)
    assert p == 0.0


def test_gaussian_cdf_diff_large_sigma_approaches_zero():
    """Very wide sigma → bucket gets tiny probability."""
    p = _gaussian_cdf_diff(20.0, 50.0, 19.5, 20.5)
    assert p < 0.02


# ── brier_score ───────────────────────────────────────────────────────────────

def test_brier_score_perfect_sigma():
    """With a stable constant series the Brier score should be finite and reasonable."""
    data = [20.0] * 50
    score = brier_score(data, sigma=0.5, window=14)
    assert 0.0 < score < 1.0


def test_brier_score_too_few_points():
    score = brier_score([20.0] * 5, sigma=1.0, window=14)
    assert score == float("inf")


def test_brier_score_decreases_with_better_sigma():
    """For normally distributed data, score should worsen as sigma deviates from true."""
    import random
    random.seed(42)
    true_sigma = 2.0
    data = [20.0 + random.gauss(0, true_sigma) for _ in range(200)]
    score_good = brier_score(data, sigma=2.0)
    score_bad  = brier_score(data, sigma=0.1)
    assert score_good < score_bad


# ── optimal_sigma ─────────────────────────────────────────────────────────────

def test_optimal_sigma_close_to_empirical():
    """Returned sigma is the empirical stdev, which converges to true sigma for large N."""
    import random, statistics
    random.seed(0)
    true_sigma = 3.0
    data = [20.0 + random.gauss(0, true_sigma) for _ in range(500)]
    emp_stdev = statistics.stdev(data)
    sigma, brier = optimal_sigma(data)
    # Sigma IS the empirical stdev (rounded to 2dp, minimum 0.5)
    assert abs(sigma - emp_stdev) < 0.01, f"sigma={sigma:.3f} emp={emp_stdev:.3f}"
    # Brier score should be a valid float between 0 and 1
    assert 0.0 < brier < 1.0, f"Unexpected brier={brier}"


def test_optimal_sigma_minimum_enforced():
    """Even for a flat series, sigma should not drop below 0.5."""
    data = [20.0] * 100
    sigma, _ = optimal_sigma(data)
    assert sigma >= 0.5


# ── compute_sigma_table ───────────────────────────────────────────────────────

def test_compute_sigma_table_basic():
    import random
    random.seed(1)
    city_data = {
        "TestCity": {
            f"2024-04-{d:02d}": 20.0 + random.gauss(0, 2.0)
            for d in range(1, 30)
        }
    }
    table = compute_sigma_table(city_data, min_obs=15)
    assert "TestCity" in table
    assert 4 in table["TestCity"]
    entry = table["TestCity"][4]
    assert "sigma" in entry
    assert "brier_score" in entry
    assert entry["n"] == 29
    assert entry["sigma"] >= 0.5


def test_compute_sigma_table_skips_sparse_months():
    city_data = {
        "SparseCity": {f"2024-04-{d:02d}": 20.0 for d in range(1, 5)}  # only 4 obs
    }
    table = compute_sigma_table(city_data, min_obs=15)
    assert "SparseCity" not in table


# ── build_sigma_config ────────────────────────────────────────────────────────

def test_build_sigma_config():
    sigma_table = {
        "Munich":    {4: {"sigma": 4.5, "brier_score": 0.75, "n": 30}},
        "Singapore": {4: {"sigma": 1.0, "brier_score": 0.60, "n": 28}},
    }
    cfg = build_sigma_config(sigma_table)
    assert cfg["Munich"][4] == 4.5
    assert cfg["Singapore"][4] == 1.0
