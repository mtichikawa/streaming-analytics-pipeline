"""
examples/quick_demo.py — Run the pipeline's pure cores with no broker, no Spark.

The full pipeline needs Redpanda + Spark + Java. This demo exercises the parts
that don't — the tick contract, the rolling z-score anomaly core, and the
dashboard's data layer — so you can see the moving pieces in a couple of seconds
on a bare checkout:

    python examples/quick_demo.py

It prints: a validated tick, a synthetic price stream with an injected spike,
the z-score alert that fires on it, and the synthetic OHLCV/alerts frames the
dashboard shows when no live Delta tables exist yet.
"""

import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.schemas import validate_tick                      # noqa: E402
from src.stream_anomaly import RollingZScore, detect_alerts  # noqa: E402
from dashboards.data_access import synthetic_ohlcv, synthetic_alerts  # noqa: E402


def section(title: str) -> None:
    print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")


def main() -> None:
    section("1. Tick contract (schemas.validate_tick)")
    good = {"symbol": "BTC/USDT", "timestamp": "2026-06-01T12:00:00.000Z",
            "price": 67000.5, "volume": 1.23}
    bad = {"symbol": "BTC/USDT", "price": 67000.5}  # missing fields
    print(f"  valid tick   -> validate_tick = {validate_tick(good)}")
    print(f"  broken tick  -> validate_tick = {validate_tick(bad)}")

    section("2. Rolling z-score anomaly core (src.stream_anomaly)")
    rng = np.random.default_rng(0)
    # Stable baseline around $67k, then a single obvious spike.
    prices = list(67000 + rng.normal(0, 20, size=60))
    prices += [67010.0, 68500.0, 67005.0]  # spike at index 61
    alerts = detect_alerts("BTC/USDT", prices, window_size=60, threshold=3.0)
    print(f"  {len(prices)} prices streamed, {len(alerts)} alert(s) fired")
    if alerts:
        spike = max(alerts, key=lambda a: abs(a["z_score"]))
        print(f"  biggest: price=${spike['price']:,.2f}  "
              f"z={spike['z_score']:+.2f}  (mean=${spike['rolling_mean']:,.2f})")

    # The same class, used point-by-point the way the Spark job does.
    det = RollingZScore(window_size=60, threshold=3.0)
    for p in prices[:-1]:
        det.z_score(p)
    print(f"  is_anomaly(${prices[-1]:,.2f}) = {det.is_anomaly(prices[-1])}")

    section("3. Dashboard data (synthetic fallback)")
    ohlcv = synthetic_ohlcv(n_windows=12)
    alerts_df = synthetic_alerts(ohlcv=ohlcv)
    print(f"  OHLCV frame:  {len(ohlcv)} rows, "
          f"{ohlcv['symbol'].nunique()} symbols, columns={list(ohlcv.columns)}")
    print(ohlcv.tail(3).to_string(index=False))
    print(f"\n  Alerts frame: {len(alerts_df)} rows")
    print(alerts_df.head(3).to_string(index=False))

    print("\nDone. Start the full stack (see README) for live Delta-backed data.")


if __name__ == "__main__":
    main()
