"""
Producer unit tests with a mocked ccxt exchange and a mocked Kafka producer.
No live network, no live broker. Covers payload schema, multi-symbol rounds,
connection-drop handling, and the run loop's bounded execution.
"""

from unittest.mock import MagicMock

import ccxt
import pytest

from src.producer import TickProducer
from src.schemas import validate_tick


class FakeKafkaProducer:
    """Records produced messages; mimics confluent_kafka.Producer surface."""

    def __init__(self):
        self.messages = []
        self.flushed = False

    def produce(self, topic, key=None, value=None):
        self.messages.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        return 0


def make_exchange(price=100.0, volume=5.0):
    ex = MagicMock()
    ex.fetch_ticker.return_value = {"last": price, "baseVolume": volume}
    return ex


def test_fetch_tick_builds_valid_payload():
    p = TickProducer(symbols=["BTC/USDT"], exchange=make_exchange(67000.0, 1.5))
    tick = p.fetch_tick("BTC/USDT")
    assert tick["symbol"] == "BTC/USDT"
    assert tick["price"] == 67000.0
    assert tick["volume"] == 1.5
    assert validate_tick(tick) is True


def test_fetch_tick_falls_back_to_close_when_last_missing():
    ex = MagicMock()
    ex.fetch_ticker.return_value = {"last": None, "close": 42.0, "baseVolume": None}
    p = TickProducer(symbols=["SOL/USDT"], exchange=ex)
    tick = p.fetch_tick("SOL/USDT")
    assert tick["price"] == 42.0
    assert tick["volume"] == 0.0


def test_publish_sends_json_keyed_by_symbol():
    import json
    kafka = FakeKafkaProducer()
    p = TickProducer(symbols=["ETH/USDT"], exchange=make_exchange(),
                     kafka_producer=kafka, topic="crypto.ticks")
    p.publish({"symbol": "ETH/USDT", "timestamp": "2026-06-01T00:00:00.000Z",
               "price": 3000.0, "volume": 2.0})
    assert len(kafka.messages) == 1
    msg = kafka.messages[0]
    assert msg["topic"] == "crypto.ticks"
    assert msg["key"] == b"ETH/USDT"
    assert json.loads(msg["value"])["price"] == 3000.0


def test_publish_rejects_malformed_tick():
    p = TickProducer(exchange=make_exchange(), kafka_producer=FakeKafkaProducer())
    with pytest.raises(ValueError):
        p.publish({"symbol": "BTC/USDT", "price": 1.0})  # missing fields


def test_round_publishes_one_tick_per_symbol():
    kafka = FakeKafkaProducer()
    p = TickProducer(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"],
                     exchange=make_exchange(), kafka_producer=kafka)
    n = p.fetch_and_publish_round()
    assert n == 3
    assert len(kafka.messages) == 3


def test_connection_drop_skips_symbol_not_round():
    """A NetworkError on one symbol is logged + skipped; others still publish."""
    ex = MagicMock()
    ex.fetch_ticker.side_effect = [
        {"last": 100.0, "baseVolume": 1.0},      # BTC ok
        ccxt.NetworkError("connection reset"),    # ETH drops
        {"last": 50.0, "baseVolume": 1.0},        # SOL ok
    ]
    kafka = FakeKafkaProducer()
    p = TickProducer(symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"],
                     exchange=ex, kafka_producer=kafka)
    n = p.fetch_and_publish_round()
    assert n == 2
    assert len(kafka.messages) == 2


def test_run_is_bounded_by_max_rounds_and_flushes():
    kafka = FakeKafkaProducer()
    p = TickProducer(symbols=["BTC/USDT"], exchange=make_exchange(),
                     kafka_producer=kafka, tick_rate_hz=1000.0)
    p.run(max_rounds=3)
    assert len(kafka.messages) == 3
    assert kafka.flushed is True
