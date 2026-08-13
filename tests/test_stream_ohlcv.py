"""
OHLCV aggregation + watermark tests.

These require a real SparkSession (Java + PySpark), so the whole module is
skip-gated on SPARK_AVAILABLE. They run `aggregate_ohlcv` against a BATCH
DataFrame — the same transform the streaming query uses — which is the
stream-batch unification point and lets us assert OHLCV correctness
deterministically without standing up Kafka.

When Spark/Java is unavailable (as in the build/CI environment here) every
test below reports as skipped, not failed.
"""

from datetime import datetime

import pytest

from .conftest import SPARK_AVAILABLE

pytestmark = pytest.mark.skipif(
    not SPARK_AVAILABLE, reason="PySpark/Java not available"
)


def _ticks_df(spark, rows):
    """rows: list of (symbol, datetime, price, volume)."""
    from src.schemas import tick_schema
    return spark.createDataFrame(rows, schema=tick_schema())


def test_ohlcv_open_high_low_close_volume(spark):
    from src.stream_ohlcv import aggregate_ohlcv
    base = datetime(2026, 6, 1, 12, 0, 0)
    rows = [
        ("BTC/USDT", base.replace(second=10), 100.0, 1.0),  # open
        ("BTC/USDT", base.replace(second=20), 120.0, 1.0),  # high
        ("BTC/USDT", base.replace(second=30), 90.0, 1.0),   # low
        ("BTC/USDT", base.replace(second=40), 110.0, 1.0),  # close
    ]
    out = aggregate_ohlcv(_ticks_df(spark, rows)).collect()
    assert len(out) == 1
    r = out[0]
    assert r["open"] == 100.0
    assert r["high"] == 120.0
    assert r["low"] == 90.0
    assert r["close"] == 110.0
    assert r["volume"] == 4.0


def test_open_close_respect_event_time_order(spark):
    """Out-of-order arrival still yields correct open/close by event time."""
    from src.stream_ohlcv import aggregate_ohlcv
    base = datetime(2026, 6, 1, 12, 0, 0)
    rows = [
        ("ETH/USDT", base.replace(second=40), 110.0, 1.0),  # latest arrives first
        ("ETH/USDT", base.replace(second=10), 100.0, 1.0),  # earliest arrives later
        ("ETH/USDT", base.replace(second=25), 105.0, 1.0),
    ]
    r = aggregate_ohlcv(_ticks_df(spark, rows)).collect()[0]
    assert r["open"] == 100.0
    assert r["close"] == 110.0


def test_multi_symbol_independence(spark):
    from src.stream_ohlcv import aggregate_ohlcv
    base = datetime(2026, 6, 1, 12, 0, 0)
    rows = [
        ("BTC/USDT", base.replace(second=10), 100.0, 1.0),
        ("BTC/USDT", base.replace(second=20), 200.0, 1.0),
        ("SOL/USDT", base.replace(second=15), 5.0, 10.0),
        ("SOL/USDT", base.replace(second=25), 7.0, 10.0),
    ]
    out = {r["symbol"]: r for r in aggregate_ohlcv(_ticks_df(spark, rows)).collect()}
    assert out["BTC/USDT"]["high"] == 200.0
    assert out["SOL/USDT"]["volume"] == 20.0


def test_separate_windows_for_distinct_5m_buckets(spark):
    from src.stream_ohlcv import aggregate_ohlcv
    base = datetime(2026, 6, 1, 12, 0, 0)
    rows = [
        ("BTC/USDT", base.replace(minute=0, second=30), 100.0, 1.0),   # 12:00-12:05
        ("BTC/USDT", base.replace(minute=6, second=30), 200.0, 1.0),   # 12:05-12:10
    ]
    out = aggregate_ohlcv(_ticks_df(spark, rows)).collect()
    assert len(out) == 2  # two distinct tumbling windows


def test_window_bounds_are_5_minutes(spark):
    from src.stream_ohlcv import aggregate_ohlcv
    base = datetime(2026, 6, 1, 12, 2, 0)
    rows = [("BTC/USDT", base, 100.0, 1.0)]
    r = aggregate_ohlcv(_ticks_df(spark, rows)).collect()[0]
    delta = r["window_end"] - r["window_start"]
    assert delta.total_seconds() == 300


def test_watermark_present_in_streaming_plan(spark):
    """
    aggregate_ohlcv must apply a watermark so streaming state stays bounded.

    The watermark node only exists in a STREAMING plan — on a batch DataFrame
    Spark treats withWatermark as a no-op and the node never appears, which is
    why this has to build a real streaming source rather than a list of rows.
    """
    from src.stream_ohlcv import aggregate_ohlcv

    ticks = (
        spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        .selectExpr(
            "'BTC/USDT' as symbol",
            "timestamp as timestamp",
            "cast(value as double) as price",
            "cast(1 as double) as volume",
        )
    )
    assert ticks.isStreaming, "test setup failed: source is not streaming"

    plan = aggregate_ohlcv(ticks)._jdf.queryExecution().analyzed().toString()
    assert "EventTimeWatermark" in plan
