"""
src/schemas.py — Single source of truth for all stream payload schemas.

Every PySpark StructType used anywhere in the pipeline is defined here once.
Do NOT redefine these inline in producer.py, stream_ohlcv.py, or
stream_anomaly.py — import them from this module instead. The producer also
uses TICK_FIELDS / validate_tick() to build and check JSON payloads without
pulling in PySpark, so the same field contract holds on both sides of the
broker.

The tick payload is the contract published to the `crypto.ticks` topic:

    {"symbol": "BTC/USDT", "timestamp": "2026-06-01T12:00:00.000Z",
     "price": 67000.5, "volume": 1.23}
"""

# ---------------------------------------------------------------------------
# Lightweight (PySpark-free) field contract — usable by the producer + tests.
# ---------------------------------------------------------------------------

# Ordered (field_name, python_type) pairs for the tick payload. price/volume
# accept int too; the producer always emits floats.
TICK_FIELDS = (
    ("symbol", str),
    ("timestamp", str),
    ("price", (int, float)),
    ("volume", (int, float)),
)

TICK_FIELD_NAMES = tuple(name for name, _ in TICK_FIELDS)


def validate_tick(payload: dict) -> bool:
    """
    Validate a tick dict against the TICK_FIELDS contract without PySpark.

    Returns True only if every required field is present with the right type
    and no extra fields are included. Raises nothing — callers branch on the
    boolean. Booleans are rejected for numeric fields (bool is a subclass of
    int in Python and we don't want True/False slipping through as price).
    """
    if not isinstance(payload, dict):
        return False
    if set(payload.keys()) != set(TICK_FIELD_NAMES):
        return False
    for name, expected_type in TICK_FIELDS:
        value = payload.get(name)
        if isinstance(value, bool):
            return False
        if not isinstance(value, expected_type):
            return False
    return True


# ---------------------------------------------------------------------------
# PySpark schemas. Imported lazily so that producer.py and the pure-Python
# tests do not require a PySpark install just to build/validate payloads.
# ---------------------------------------------------------------------------

def tick_schema():
    """PySpark StructType for an incoming tick on `crypto.ticks`."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType,
    )
    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=False),
    ])


def ohlcv_schema():
    """
    PySpark StructType for a 5-minute OHLCV aggregate row written to
    delta/ohlcv_5m/. window_start / window_end bound the tumbling window.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType,
    )
    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=False),
        StructField("window_end", TimestampType(), nullable=False),
        StructField("open", DoubleType(), nullable=False),
        StructField("high", DoubleType(), nullable=False),
        StructField("low", DoubleType(), nullable=False),
        StructField("close", DoubleType(), nullable=False),
        StructField("volume", DoubleType(), nullable=False),
    ])


def alert_schema():
    """
    PySpark StructType for a z-score anomaly alert written to delta/alerts/.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, TimestampType,
    )
    return StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("rolling_mean", DoubleType(), nullable=False),
        StructField("rolling_std", DoubleType(), nullable=False),
        StructField("z_score", DoubleType(), nullable=False),
    ])
