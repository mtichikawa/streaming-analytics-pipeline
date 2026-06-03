import sys
from pathlib import Path

import pytest

# Add the repo root to sys.path so tests can `import src.*` the same way the
# modules import each other (e.g. `from src.schemas import ...`).
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Detect PySpark + a usable SparkSession once. Spark needs a Java runtime, so
# `import pyspark` succeeding is not enough — we try to actually build a tiny
# local session. Tests that require Spark are skip-gated on SPARK_AVAILABLE.
try:
    from pyspark.sql import SparkSession  # noqa: F401
    _PYSPARK_IMPORTABLE = True
except Exception:
    _PYSPARK_IMPORTABLE = False


def _spark_usable() -> bool:
    if not _PYSPARK_IMPORTABLE:
        return False
    try:
        from pyspark.sql import SparkSession
        s = (
            SparkSession.builder.appName("probe")
            .master("local[1]")
            .getOrCreate()
        )
        s.stop()
        return True
    except Exception:
        return False


SPARK_AVAILABLE = _spark_usable()


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession; only used by Spark-gated tests."""
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder.appName("streaming-analytics-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
