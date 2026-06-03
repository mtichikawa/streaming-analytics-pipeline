"""
src/checkpoint_config.py — Checkpoint paths + streaming recovery settings.

Spark Structured Streaming uses a checkpoint directory to persist offsets and
aggregation state between microbatches. On restart the query reads the last
committed offsets from here, so a killed-and-restarted job resumes exactly
where it left off — no reprocessing, no gaps. Combined with the Delta sink's
transactional commits this gives effective exactly-once on the output.

To start a stream fresh, delete the relevant checkpoint directory (and the
target Delta table) before launching. Paths are kept local and predictable on
purpose so state is easy to wipe.

    rm -rf checkpoints/ohlcv_5m delta/ohlcv_5m     # reset the OHLCV job
    rm -rf checkpoints/alerts   delta/alerts       # reset the anomaly job
"""

from pathlib import Path

# Repo root = two levels up from this file (src/checkpoint_config.py).
REPO_ROOT = Path(__file__).resolve().parent.parent

CHECKPOINT_ROOT = REPO_ROOT / "checkpoints"
DELTA_ROOT = REPO_ROOT / "delta"

# Per-job checkpoint directories.
OHLCV_CHECKPOINT = CHECKPOINT_ROOT / "ohlcv_5m"
ALERTS_CHECKPOINT = CHECKPOINT_ROOT / "alerts"

# Per-job Delta table output paths.
OHLCV_TABLE = DELTA_ROOT / "ohlcv_5m"
ALERTS_TABLE = DELTA_ROOT / "alerts"

# Watermark + trigger settings, documented in one place so both jobs and the
# README agree.
WATERMARK_DELAY = "30 seconds"   # late ticks within this bound still update their bucket
WINDOW_DURATION = "5 minutes"    # OHLCV tumbling-window size
TRIGGER_INTERVAL = "10 seconds"  # microbatch cadence


def as_str(path) -> str:
    """Spark wants string paths, not Path objects. Ensures parent exists."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return str(p)
