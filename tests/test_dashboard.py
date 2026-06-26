"""
Dashboard data-layer tests. The synthetic generators and the Delta/synthetic
loaders are pure pandas/numpy, so they run anywhere (no Spark, no deltalake, no
Streamlit). These lock down the column contracts the dashboard depends on and
the fallback behaviour when no live Delta tables exist.
"""

from datetime import datetime, timezone

import pandas as pd

from dashboards.data_access import (
    synthetic_ohlcv,
    synthetic_alerts,
    load_ohlcv,
    load_alerts,
    latest_prices,
    OHLCV_COLUMNS,
    ALERT_COLUMNS,
    SYMBOLS,
)


def test_synthetic_ohlcv_columns_and_shape():
    df = synthetic_ohlcv(n_windows=10)
    assert list(df.columns) == OHLCV_COLUMNS
    assert len(df) == 10 * len(SYMBOLS)
    assert set(df["symbol"].unique()) == set(SYMBOLS)


def test_synthetic_ohlcv_is_internally_consistent():
    """low <= open/close <= high for every bar; volume positive."""
    df = synthetic_ohlcv(n_windows=20)
    assert (df["low"] <= df["open"] + 1e-9).all()
    assert (df["low"] <= df["close"] + 1e-9).all()
    assert (df["high"] >= df["open"] - 1e-9).all()
    assert (df["high"] >= df["close"] - 1e-9).all()
    assert (df["volume"] > 0).all()


def test_synthetic_ohlcv_is_deterministic():
    anchor = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    a = synthetic_ohlcv(n_windows=8, anchor=anchor, seed=7)
    b = synthetic_ohlcv(n_windows=8, anchor=anchor, seed=7)
    pd.testing.assert_frame_equal(a, b)


def test_synthetic_ohlcv_windows_are_five_minutes():
    df = synthetic_ohlcv(n_windows=4)
    spans = (df["window_end"] - df["window_start"]).dt.total_seconds().unique()
    assert list(spans) == [300.0]


def test_synthetic_alerts_columns_and_threshold():
    alerts = synthetic_alerts()
    assert list(alerts.columns) == ALERT_COLUMNS
    assert not alerts.empty
    # every synthetic alert is a genuine |z| > 3 spike
    assert (alerts["z_score"].abs() > 3.0).all()


def test_load_ohlcv_falls_back_to_synthetic(tmp_path):
    """No Delta table at the given root -> synthetic frame, flagged not-real."""
    frame = load_ohlcv(delta_root=tmp_path)
    assert frame.is_real is False
    assert list(frame.df.columns) == OHLCV_COLUMNS
    assert "synthetic" in frame.source_label


def test_load_alerts_falls_back_to_synthetic(tmp_path):
    frame = load_alerts(delta_root=tmp_path)
    assert frame.is_real is False
    assert list(frame.df.columns) == ALERT_COLUMNS


def test_load_ohlcv_no_synthetic_returns_empty(tmp_path):
    """allow_synthetic=False yields an empty real-shaped frame, no fabrication."""
    frame = load_ohlcv(delta_root=tmp_path, allow_synthetic=False)
    assert frame.df.empty
    assert list(frame.df.columns) == OHLCV_COLUMNS


def test_latest_prices_shape_and_change():
    ohlcv = synthetic_ohlcv(n_windows=6)
    prices = latest_prices(ohlcv)
    assert set(prices.columns) == {"symbol", "close", "change_pct"}
    assert len(prices) == len(SYMBOLS)


def test_latest_prices_empty_input():
    empty = pd.DataFrame(columns=OHLCV_COLUMNS)
    assert latest_prices(empty).empty
