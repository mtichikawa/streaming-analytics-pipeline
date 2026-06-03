"""
src/stream_anomaly.py — Spark Structured Streaming z-score anomaly job.

Second consumer on the Redpanda `crypto.ticks` topic. Maintains a rolling
mean + std over the last 60 ticks PER SYMBOL and emits an alert whenever a new
tick's |z-score| exceeds 3. Alerts are written to a Delta table at
delta/alerts/.

The z-score math is adapted from the anomaly-detection project's
StatisticalDetector (rolling window of recent values, z = (x - mean) / std,
flag when |z| > threshold, skip when variance is ~0). It is pulled out into the
pure-Python `RollingZScore` class below so it is unit-testable WITHOUT Spark or
a broker. The Spark job then applies that exact class to per-symbol tick groups
via applyInPandasWithState, so the streaming path and the tested path share one
implementation.

Run (needs a live broker + Java + Spark):
    python -m src.stream_anomaly
    python -m src.stream_anomaly --broker localhost:19092
"""

import argparse
import logging
from collections import deque

import numpy as np

log = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:19092"
TOPIC = "crypto.ticks"
WINDOW_SIZE = 60      # ticks
Z_THRESHOLD = 3.0     # |z| above this fires an alert
MIN_OBSERVATIONS = 3  # need a few points before a z-score is meaningful
ZERO_VAR_EPS = 1e-10  # std below this is treated as zero variance (no alert)


class RollingZScore:
    """
    Rolling z-score detector over a fixed-size window of recent values.

    Adapted from anomaly-detection's StatisticalDetector: keeps the last
    `window_size` values, and for each new value computes the z-score against
    the window's mean/std. `update()` appends the value FIRST (so the point is
    part of its own window, matching the source detector) then evaluates.

    Pure Python / numpy — no Spark, no Kafka. This is the unit-tested core.
    """

    def __init__(self, window_size: int = WINDOW_SIZE, threshold: float = Z_THRESHOLD):
        self.window_size = window_size
        self.threshold = threshold
        self.window = deque(maxlen=window_size)

    def z_score(self, value: float):
        """
        Append `value` to the window and return (z_score, mean, std).

        Returns z_score = 0.0 while there are too few observations or when the
        window has ~zero variance (constant prices), so neither case fires a
        false alert. Otherwise z = (value - mean) / std using the population
        std (ddof=0), matching the source detector's np.std default.
        """
        self.window.append(value)
        if len(self.window) < MIN_OBSERVATIONS:
            return 0.0, float(value), 0.0

        mean = float(np.mean(self.window))
        std = float(np.std(self.window))  # population std, ddof=0
        if std < ZERO_VAR_EPS:
            return 0.0, mean, std

        return (value - mean) / std, mean, std

    def is_anomaly(self, value: float) -> bool:
        """Convenience: True when |z-score(value)| exceeds the threshold."""
        z, _, _ = self.z_score(value)
        return abs(z) > self.threshold


def detect_alerts(symbol: str, prices, window_size: int = WINDOW_SIZE,
                  threshold: float = Z_THRESHOLD):
    """
    Run RollingZScore over an ordered iterable of prices for one symbol and
    return the list of alert dicts (index, price, mean, std, z_score) where
    |z| > threshold.

    Used by the Spark job's per-group state function and directly by tests so
    streaming and tested behavior can't drift.
    """
    detector = RollingZScore(window_size=window_size, threshold=threshold)
    alerts = []
    for i, price in enumerate(prices):
        z, mean, std = detector.z_score(price)
        if abs(z) > threshold:
            alerts.append({
                "index": i,
                "price": float(price),
                "rolling_mean": mean,
                "rolling_std": std,
                "z_score": z,
            })
    return alerts


# ---------------------------------------------------------------------------
# Spark wiring (imported lazily inside run() so the pure-Python core + tests
# don't require PySpark to be installed).
# ---------------------------------------------------------------------------

def run(broker: str = DEFAULT_BROKER):
    """
    Stateful streaming job: per-symbol rolling z-score, alerts -> Delta.

    Uses applyInPandasWithState so the 60-tick rolling window survives across
    microbatches per symbol. Each emitted alert row matches alert_schema().
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout

    try:
        from src.schemas import tick_schema, alert_schema
        from src import checkpoint_config as ckpt
        from src.stream_ohlcv import parse_ticks
    except ImportError:
        from schemas import tick_schema, alert_schema
        import checkpoint_config as ckpt
        from stream_ohlcv import parse_ticks

    spark = (
        SparkSession.builder.appName("stream-anomaly")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    ticks = parse_ticks(raw)

    out_schema = alert_schema()

    def flag_group(key, pdf_iter, state):
        """
        applyInPandasWithState function. `key` = (symbol,); state holds the
        rolling window of recent prices (a list, capped at WINDOW_SIZE).
        Reuses RollingZScore so the math matches the tested core exactly.
        """
        import pandas as pd

        (symbol,) = key
        window = list(state.get[0]) if state.exists else []

        detector = RollingZScore(window_size=WINDOW_SIZE, threshold=Z_THRESHOLD)
        detector.window.extend(window)

        rows = []
        for pdf in pdf_iter:
            pdf = pdf.sort_values("timestamp")
            for _, r in pdf.iterrows():
                z, mean, std = detector.z_score(float(r["price"]))
                if abs(z) > Z_THRESHOLD:
                    rows.append((symbol, r["timestamp"], float(r["price"]),
                                 mean, std, z))

        state.update((list(detector.window),))
        if rows:
            yield pd.DataFrame(
                rows,
                columns=["symbol", "timestamp", "price",
                         "rolling_mean", "rolling_std", "z_score"],
            )

    alerts = ticks.groupBy("symbol").applyInPandasWithState(
        flag_group,
        outputStructType=out_schema,
        stateStructType="window array<double>",
        outputMode="append",
        timeoutConf=GroupStateTimeout.NoTimeout,
    )

    query = (
        alerts.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", ckpt.as_str(ckpt.ALERTS_CHECKPOINT))
        .trigger(processingTime=ckpt.TRIGGER_INTERVAL)
        .start(ckpt.as_str(ckpt.ALERTS_TABLE))
    )
    log.info("anomaly stream started -> %s", ckpt.ALERTS_TABLE)
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default=DEFAULT_BROKER)
    args = ap.parse_args()
    run(args.broker)
