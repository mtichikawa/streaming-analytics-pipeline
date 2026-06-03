"""
Schema-contract tests. The PySpark-free TICK_FIELDS contract is tested
unconditionally; the PySpark StructType builders are gated on SPARK_AVAILABLE.
"""

import pytest

from src.schemas import TICK_FIELD_NAMES, validate_tick
from .conftest import SPARK_AVAILABLE


def _good_tick():
    return {
        "symbol": "BTC/USDT",
        "timestamp": "2026-06-01T12:00:00.000Z",
        "price": 67000.5,
        "volume": 1.23,
    }


def test_field_names_are_the_contract():
    assert TICK_FIELD_NAMES == ("symbol", "timestamp", "price", "volume")


def test_valid_tick_passes():
    assert validate_tick(_good_tick()) is True


def test_integer_price_and_volume_allowed():
    t = _good_tick()
    t["price"] = 67000
    t["volume"] = 2
    assert validate_tick(t) is True


def test_missing_field_fails():
    t = _good_tick()
    del t["price"]
    assert validate_tick(t) is False


def test_extra_field_fails():
    t = _good_tick()
    t["exchange"] = "kraken"
    assert validate_tick(t) is False


def test_wrong_type_fails():
    t = _good_tick()
    t["price"] = "expensive"
    assert validate_tick(t) is False


def test_bool_rejected_for_numeric():
    # bool is a subclass of int; it must not slip through as a price.
    t = _good_tick()
    t["price"] = True
    assert validate_tick(t) is False


def test_non_dict_fails():
    assert validate_tick(["not", "a", "dict"]) is False


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/Java not available")
def test_tick_schema_structure():
    from src.schemas import tick_schema
    schema = tick_schema()
    assert [f.name for f in schema.fields] == list(TICK_FIELD_NAMES)


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/Java not available")
def test_ohlcv_and_alert_schema_fields():
    from src.schemas import ohlcv_schema, alert_schema
    ohlcv_fields = [f.name for f in ohlcv_schema().fields]
    assert ohlcv_fields == [
        "symbol", "window_start", "window_end",
        "open", "high", "low", "close", "volume",
    ]
    alert_fields = [f.name for f in alert_schema().fields]
    assert alert_fields == [
        "symbol", "timestamp", "price",
        "rolling_mean", "rolling_std", "z_score",
    ]
