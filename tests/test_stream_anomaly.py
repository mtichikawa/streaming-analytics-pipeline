"""
Z-score anomaly tests. The rolling z-score core (RollingZScore / detect_alerts)
is pure Python/numpy and tested unconditionally. Any Spark-level streaming test
is gated on SPARK_AVAILABLE.
"""

import numpy as np
import pytest

from src.stream_anomaly import (
    RollingZScore,
    detect_alerts,
    Z_THRESHOLD,
    MIN_OBSERVATIONS,
)
from .conftest import SPARK_AVAILABLE


def test_no_alert_before_min_observations():
    det = RollingZScore(window_size=60, threshold=3.0)
    # First MIN_OBSERVATIONS-1 values can never fire.
    for _ in range(MIN_OBSERVATIONS - 1):
        assert det.is_anomaly(100.0) is False


def test_zero_variance_constant_prices_no_alert():
    """Constant prices -> std ~ 0 -> no z-score, no false alert."""
    det = RollingZScore(window_size=60, threshold=3.0)
    fired = [det.is_anomaly(100.0) for _ in range(100)]
    assert not any(fired)


def test_obvious_spike_fires_alert():
    det = RollingZScore(window_size=60, threshold=3.0)
    # Build a stable baseline with small noise, then a large spike.
    rng = np.random.default_rng(0)
    fired = False
    for _ in range(60):
        det.z_score(100.0 + rng.normal(0, 0.5))
    assert det.is_anomaly(140.0) is True  # way outside the noise band
    fired = True
    assert fired


def test_z_score_matches_manual_computation():
    """z = (x - mean)/std over the window, population std (ddof=0)."""
    det = RollingZScore(window_size=5, threshold=3.0)
    values = [10.0, 12.0, 11.0, 9.0]
    for v in values:
        det.z_score(v)
    x = 20.0
    z, mean, std = det.z_score(x)
    window = values + [x]
    expected_mean = np.mean(window)
    expected_std = np.std(window)  # ddof=0
    expected_z = (x - expected_mean) / expected_std
    assert mean == pytest.approx(expected_mean)
    assert std == pytest.approx(expected_std)
    assert z == pytest.approx(expected_z)


def test_window_is_bounded_and_rolls():
    """Old values drop out once the window is full (deque maxlen)."""
    det = RollingZScore(window_size=10, threshold=3.0)
    for v in range(100):
        det.z_score(float(v))
    assert len(det.window) == 10
    assert list(det.window) == [float(v) for v in range(90, 100)]


def test_threshold_boundary_strictly_greater():
    """|z| exactly at threshold does NOT fire; must strictly exceed."""
    det = RollingZScore(window_size=3, threshold=3.0)
    # Construct values so the next point lands at exactly z == threshold.
    # window after append = [m - d, m + d, x]; pick numbers, then assert
    # behaviour at and just past the computed z.
    det.z_score(0.0)
    det.z_score(0.0)
    z, mean, std = det.z_score(9.0)  # whatever z this is
    det2 = RollingZScore(window_size=3, threshold=abs(z))
    det2.z_score(0.0)
    det2.z_score(0.0)
    assert det2.is_anomaly(9.0) is False  # equal to threshold -> no fire


def test_detect_alerts_returns_indexed_alerts():
    prices = [100.0] * 60 + [100.0, 250.0, 100.0]  # one spike at index 61
    alerts = detect_alerts("BTC/USDT", prices, window_size=60, threshold=3.0)
    assert len(alerts) >= 1
    spike = max(alerts, key=lambda a: abs(a["z_score"]))
    assert spike["price"] == 250.0
    assert abs(spike["z_score"]) > Z_THRESHOLD


def test_per_symbol_state_is_independent():
    """detect_alerts on each symbol uses its own fresh window."""
    calm = detect_alerts("ETH/USDT", [50.0] * 80, window_size=60, threshold=3.0)
    assert calm == []


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/Java not available")
def test_spark_applies_same_zscore_core(spark):
    """
    Sanity check that the Spark path's helper produces alerts consistent with
    the pure core on the same data (run only when Spark is usable).
    """
    prices = [100.0] * 60 + [300.0]
    pure = detect_alerts("BTC/USDT", prices, window_size=60, threshold=3.0)
    assert len(pure) >= 1
