"""
src/producer.py — Publish live crypto ticks to the Redpanda `crypto.ticks` topic.

Reuses the T1 crypto-data-pipeline ccxt pattern (an `enableRateLimit` Kraken
client, `fetch_ticker` instead of `fetch_ohlcv`) to pull a current price per
symbol, wraps it in the tick payload defined once in schemas.py, and produces
JSON to Redpanda via confluent-kafka.

The ccxt fetch is isolated in `TickProducer.fetch_tick()` so tests can mock the
exchange and the Kafka producer independently — no live network or broker
needed. Broker address and tick rate are configurable via constructor args or
env vars (KAFKA_BROKER, TICK_RATE_HZ, TOPIC).

Run:
    python -m src.producer                      # defaults: localhost:9092, 1 Hz
    KAFKA_BROKER=localhost:19092 python -m src.producer
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import ccxt

# Schemas are defined once in schemas.py; the producer uses the PySpark-free
# field contract to validate every payload before it hits the wire.
try:
    from src.schemas import TICK_FIELD_NAMES, validate_tick
except ImportError:  # allow `python producer.py` from within src/
    from schemas import TICK_FIELD_NAMES, validate_tick

log = logging.getLogger(__name__)

DEFAULT_SYMBOLS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
DEFAULT_TOPIC = "crypto.ticks"
DEFAULT_BROKER = "localhost:19092"


class TickProducer:
    """
    Fetches a current ticker per symbol via ccxt and publishes JSON ticks to
    a Kafka-compatible broker (Redpanda).
    """

    def __init__(
        self,
        symbols=None,
        broker=None,
        topic=None,
        tick_rate_hz=None,
        exchange=None,
        kafka_producer=None,
    ):
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.broker = broker or os.getenv("KAFKA_BROKER", DEFAULT_BROKER)
        self.topic = topic or os.getenv("TOPIC", DEFAULT_TOPIC)
        self.tick_rate_hz = float(
            tick_rate_hz if tick_rate_hz is not None else os.getenv("TICK_RATE_HZ", "1.0")
        )

        # Injectable for tests; built lazily otherwise so importing this module
        # (e.g. in the pure-Python test suite) never touches ccxt or Kafka.
        self._exchange = exchange
        self._producer = kafka_producer

    # -- ccxt side (reuses T1 pattern: rate-limited client) -----------------

    @property
    def exchange(self):
        if self._exchange is None:
            self._exchange = ccxt.kraken({"enableRateLimit": True})
        return self._exchange

    def fetch_tick(self, symbol: str) -> dict:
        """
        Fetch the latest price/volume for one symbol and build a tick payload.

        Mirrors T1's fetch_candles structure but uses fetch_ticker for a single
        live point. Isolated here so tests mock `self._exchange.fetch_ticker`.

        Returns a dict matching the schemas.py TICK_FIELDS contract.
        """
        ticker = self.exchange.fetch_ticker(symbol)
        price = ticker.get("last") or ticker.get("close")
        volume = ticker.get("baseVolume") or 0.0
        return {
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "price": float(price),
            "volume": float(volume),
        }

    # -- Kafka side ---------------------------------------------------------

    @property
    def producer(self):
        if self._producer is None:
            from confluent_kafka import Producer
            self._producer = Producer({"bootstrap.servers": self.broker})
        return self._producer

    def publish(self, tick: dict):
        """
        Validate then publish a single tick as JSON, keyed by symbol so all
        ticks for one symbol land on the same partition (preserves order).

        Raises ValueError if the payload violates the schema contract — we never
        want a malformed tick on the topic.
        """
        if not validate_tick(tick):
            raise ValueError(f"tick failed schema validation: {tick}")
        payload = json.dumps(tick).encode("utf-8")
        self.producer.produce(self.topic, key=tick["symbol"].encode("utf-8"), value=payload)
        self.producer.poll(0)

    def fetch_and_publish_round(self) -> int:
        """
        Fetch + publish one tick per symbol. Connection drops on an individual
        symbol are logged and skipped so one bad symbol doesn't stall the loop.

        Returns the number of ticks successfully published this round.
        """
        published = 0
        for symbol in self.symbols:
            try:
                tick = self.fetch_tick(symbol)
            except ccxt.NetworkError as exc:
                log.warning("network error fetching %s, skipping: %s", symbol, exc)
                continue
            except ccxt.ExchangeError as exc:
                log.warning("exchange error fetching %s, skipping: %s", symbol, exc)
                continue
            self.publish(tick)
            published += 1
        return published

    def run(self, max_rounds=None):
        """
        Main loop: every 1/tick_rate_hz seconds, fetch + publish a round.

        max_rounds bounds the loop for demos/tests; None runs forever until
        interrupted. Flushes the Kafka producer on exit.
        """
        interval = 1.0 / self.tick_rate_hz if self.tick_rate_hz > 0 else 0.0
        rounds = 0
        log.info(
            "producing %s -> %s on %s at %.2f Hz",
            self.symbols, self.topic, self.broker, self.tick_rate_hz,
        )
        try:
            while max_rounds is None or rounds < max_rounds:
                n = self.fetch_and_publish_round()
                log.info("round %d: published %d ticks", rounds, n)
                rounds += 1
                if interval and (max_rounds is None or rounds < max_rounds):
                    time.sleep(interval)
        except KeyboardInterrupt:
            log.info("interrupted, flushing producer")
        finally:
            try:
                self.producer.flush(5)
            except Exception:  # producer may never have been built in tests
                pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    TickProducer().run()
