"""
src/stream_ohlcv.py — Spark Structured Streaming OHLCV aggregator.

Reads JSON ticks from the Redpanda `crypto.ticks` topic, parses them against
the tick schema (defined once in schemas.py), and aggregates each symbol into
5-minute tumbling-window OHLCV bars:

    open   = first price in the window (earliest by event time)
    high   = max price
    low    = min price
    close  = last price in the window (latest by event time)
    volume = sum of tick volumes

A 30-second watermark on the event-time column bounds state: a tick that
arrives late but still falls within 30s of the current watermark updates the
correct (already-open) bucket; anything later is dropped so state stays
bounded. Output is appended to a Delta table at delta/ohlcv_5m/ with a
checkpoint for exactly-once recovery.

The transformation lives in `aggregate_ohlcv(df)` and operates on a DataFrame
with columns (symbol, timestamp, price, volume). That's the stream-batch
unification point: the same function runs against a streaming read of Redpanda
or a batch read of a historical Delta table with identical code.

Run (needs a live broker + Java + Spark):
    python -m src.stream_ohlcv
    python -m src.stream_ohlcv --broker localhost:19092
"""

import argparse
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

try:
    from src.schemas import tick_schema
    from src import checkpoint_config as ckpt
except ImportError:  # `python stream_ohlcv.py` from within src/
    from schemas import tick_schema
    import checkpoint_config as ckpt

log = logging.getLogger(__name__)

DEFAULT_BROKER = "localhost:19092"
TOPIC = "crypto.ticks"


def build_spark(app_name: str = "stream-ohlcv") -> SparkSession:
    """
    Build a Delta-enabled local SparkSession. Pulls the Kafka + Delta packages
    via spark.jars.packages so no manual jar wrangling is needed.
    """
    return (
        SparkSession.builder.appName(app_name)
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


def parse_ticks(raw: DataFrame) -> DataFrame:
    """
    Decode the Kafka `value` bytes as JSON into the tick schema columns.

    Input: a DataFrame with a binary `value` column (from the Kafka source).
    Output: columns (symbol, timestamp, price, volume), timestamp as a Spark
    TimestampType ready for event-time windowing.
    """
    return (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", tick_schema()).alias("t"))
        .select("t.*")
    )


def aggregate_ohlcv(
    ticks: DataFrame,
    window_duration: str = ckpt.WINDOW_DURATION,
    watermark_delay: str = ckpt.WATERMARK_DELAY,
) -> DataFrame:
    """
    Aggregate parsed ticks into 5-minute tumbling-window OHLCV bars.

    Pure DataFrame transform — works on a streaming OR a batch DataFrame
    (stream-batch unification). open/close use first/last ordered by event
    time within the window; the watermark bounds late-data state.

    Returns columns: symbol, window_start, window_end, open, high, low, close,
    volume.
    """
    windowed = (
        ticks.withWatermark("timestamp", watermark_delay)
        .groupBy(
            F.col("symbol"),
            F.window(F.col("timestamp"), window_duration).alias("w"),
        )
        .agg(
            # first/last ordered by event time -> true open/close even if ticks
            # arrive out of order within the window.
            F.first("price", ignorenulls=True).alias("open_unordered"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price", ignorenulls=True).alias("close_unordered"),
            F.sum("volume").alias("volume"),
            F.min_by("price", "timestamp").alias("open"),
            F.max_by("price", "timestamp").alias("close"),
        )
        .select(
            "symbol",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
        )
    )
    return windowed


def run(broker: str = DEFAULT_BROKER):
    """Wire the streaming read -> aggregate -> Delta sink and block."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    ohlcv = aggregate_ohlcv(parse_ticks(raw))

    query = (
        ohlcv.writeStream.format("delta")
        .outputMode("append")  # append fires on watermark close of each window
        .option("checkpointLocation", ckpt.as_str(ckpt.OHLCV_CHECKPOINT))
        .trigger(processingTime=ckpt.TRIGGER_INTERVAL)
        .start(ckpt.as_str(ckpt.OHLCV_TABLE))
    )
    log.info("OHLCV stream started -> %s", ckpt.OHLCV_TABLE)
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", default=DEFAULT_BROKER)
    args = ap.parse_args()
    run(args.broker)
