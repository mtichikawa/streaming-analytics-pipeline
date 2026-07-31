"""
Checkpoint-recovery tests — the "exactly-once on restart" claim the README
makes, turned into an actual test.

Two layers:
- Pure config tests (always run): the two jobs must use distinct checkpoint and
  Delta paths so a restart of one can't clobber the other's state.
- A Spark-gated end-to-end recovery test: run a checkpointed streaming query
  over a file source, stop it, add MORE input, restart the SAME query from the
  SAME checkpoint, and assert the already-committed input is NOT reprocessed.
  That's the offset-recovery guarantee that makes the pipeline exactly-once on
  the output. It uses a file source + parquet sink so it needs no Kafka/Delta
  jars — just a local SparkSession, same as the other Spark-gated tests.
"""

from typing import Any
import json
from datetime import datetime, timedelta

import pytest

from src import checkpoint_config as ckpt
from .conftest import SPARK_AVAILABLE


# --- pure config tests (no Spark) ------------------------------------------

def test_checkpoint_paths_are_distinct():
    """OHLCV and alerts must not share a checkpoint dir, or restart corrupts state."""
    assert ckpt.OHLCV_CHECKPOINT != ckpt.ALERTS_CHECKPOINT
    assert ckpt.OHLCV_TABLE != ckpt.ALERTS_TABLE


def test_checkpoint_under_repo_root():
    """Paths stay local + predictable so state is easy to wipe (README contract)."""
    assert str(ckpt.OHLCV_CHECKPOINT).startswith(str(ckpt.CHECKPOINT_ROOT))
    assert str(ckpt.ALERTS_TABLE).startswith(str(ckpt.DELTA_ROOT))


def test_as_str_creates_directory(tmp_path: Any):
    target = tmp_path / "checkpoints" / "ohlcv_5m"
    assert not target.exists()
    out = ckpt.as_str(target)
    assert isinstance(out, str)
    assert target.exists()


def test_settings_are_coherent():
    """Watermark/window/trigger must be parseable, non-empty strings."""
    for setting in (ckpt.WATERMARK_DELAY, ckpt.WINDOW_DURATION, ckpt.TRIGGER_INTERVAL):
        assert isinstance(setting, str) and setting.strip()


# --- Spark-gated end-to-end recovery test ----------------------------------

def _write_tick_file(path, rows):
    """rows: list of (symbol, datetime, price, volume) -> one JSON-lines file."""
    with open(path, "w") as fh:
        for symbol, ts, price, volume in rows:
            fh.write(json.dumps({
                "symbol": symbol,
                "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "price": price,
                "volume": volume,
            }) + "\n")


@pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark/Java not available")
def test_restart_does_not_reprocess_committed_input(spark, tmp_path):
    """
    Kill-and-restart with the same checkpoint must resume from committed offsets,
    not reprocess from the start. We run two `availableNow` passes over a growing
    file-source directory and assert the output row count is batch1 + batch2,
    NOT batch1 + batch1 + batch2 (which is what a checkpoint-less restart gives).
    """
    from src.schemas import tick_schema

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "out"
    checkpoint_dir = tmp_path / "ckpt"
    input_dir.mkdir()

    base = datetime(2026, 6, 1, 12, 0, 0)
    batch1 = [("BTC/USDT", base + timedelta(seconds=i), 100.0 + i, 1.0) for i in range(5)]
    batch2 = [("BTC/USDT", base + timedelta(seconds=10 + i), 200.0 + i, 1.0) for i in range(3)]

    def run_once():
        q = (
            spark.readStream.schema(tick_schema()).json(str(input_dir))
            .writeStream.format("parquet")
            .option("path", str(output_dir))
            .option("checkpointLocation", str(checkpoint_dir))
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        q.awaitTermination()

    # Pass 1: only batch1 is present.
    _write_tick_file(input_dir / "batch1.json", batch1)
    run_once()
    after_first = spark.read.parquet(str(output_dir)).count()
    assert after_first == len(batch1)

    # Pass 2: batch2 arrives; restart the SAME query/checkpoint.
    _write_tick_file(input_dir / "batch2.json", batch2)
    run_once()
    after_second = spark.read.parquet(str(output_dir)).count()

    # Exactly-once: batch1 committed in pass 1 is not re-read in pass 2.
    assert after_second == len(batch1) + len(batch2)
