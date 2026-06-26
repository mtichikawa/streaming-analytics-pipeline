"""
dashboards/data_access.py — Delta-table reads for the Streamlit dashboard,
with a synthetic fallback so the showcase runs with no live stack.

The two Spark jobs write Delta tables (delta/ohlcv_5m/, delta/alerts/). The
dashboard reads them on the host side with `deltalake` (delta-rs) — a pure
Rust/Python reader, so no Java or SparkSession is needed just to display data.

Everything here is import-safe without `deltalake` or `streamlit` installed:
those are imported lazily inside the functions that need them, so this module
(and its synthetic generators) can be unit-tested in a bare environment. When a
real Delta table is missing or unreadable, the loaders fall back to deterministic
synthetic data and report which one you got, matching the portfolio convention
that every demo runs with zero setup.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Repo root = one level up from dashboards/. Mirrors checkpoint_config.DELTA_ROOT
# without importing it (that module pulls in Path-mkdir side effects on import).
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DELTA_ROOT = REPO_ROOT / "delta"

SYMBOLS = ("BTC/USDT", "ETH/USDT", "SOL/USDT")

# Column contracts mirror schemas.ohlcv_schema() / alert_schema() exactly so the
# synthetic frames are shape-compatible with the real Delta reads.
OHLCV_COLUMNS = [
    "symbol", "window_start", "window_end",
    "open", "high", "low", "close", "volume",
]
ALERT_COLUMNS = [
    "symbol", "timestamp", "price",
    "rolling_mean", "rolling_std", "z_score",
]

# Rough per-symbol base prices for synthetic data — order-of-magnitude realistic
# so the demo charts look like the real feed, nothing more.
_BASE_PRICE = {"BTC/USDT": 67000.0, "ETH/USDT": 3500.0, "SOL/USDT": 150.0}


@dataclass
class Frame:
    """A loaded table plus where it came from (so the UI can flag demo data)."""
    df: pd.DataFrame
    is_real: bool

    @property
    def source_label(self) -> str:
        return "live Delta table" if self.is_real else "synthetic demo data"


# ---------------------------------------------------------------------------
# Delta reads (lazy deltalake import; None on any failure -> caller falls back)
# ---------------------------------------------------------------------------

def _read_delta(path: Path) -> pd.DataFrame | None:
    """
    Read a Delta table to pandas via delta-rs, or return None if it can't be
    read (deltalake not installed, path missing, empty/uninitialised table).
    Never raises — the dashboard must stay up even with no data yet.
    """
    try:
        from deltalake import DeltaTable
    except ImportError:
        return None
    try:
        if not Path(path).exists():
            return None
        return DeltaTable(str(path)).to_pandas()
    except Exception:
        # Table dir exists but isn't a committed Delta table yet, etc.
        return None


# ---------------------------------------------------------------------------
# Synthetic generators (deterministic given a seed + anchor) — used for the
# zero-setup demo and unit tests.
# ---------------------------------------------------------------------------

def synthetic_ohlcv(
    n_windows: int = 48,
    symbols=SYMBOLS,
    anchor: datetime | None = None,
    seed: int = 7,
) -> pd.DataFrame:
    """
    Build a believable OHLCV frame: `n_windows` consecutive 5-minute bars per
    symbol ending at `anchor` (default: now, UTC). Close follows a gentle random
    walk; open/high/low/volume are derived to be internally consistent
    (low <= open,close <= high). Deterministic for a fixed (seed, anchor).
    """
    rng = np.random.default_rng(seed)
    anchor = anchor or datetime.now(timezone.utc)
    # Snap anchor to a 5-minute boundary so windows look like real tumbling bars.
    anchor = anchor.replace(second=0, microsecond=0)
    anchor -= timedelta(minutes=anchor.minute % 5)

    rows = []
    for symbol in symbols:
        price = _BASE_PRICE.get(symbol, 100.0)
        for i in range(n_windows):
            start = anchor - timedelta(minutes=5 * (n_windows - i))
            end = start + timedelta(minutes=5)
            ret = rng.normal(0, 0.004)            # ~0.4% per-bar volatility
            close = price * (1 + ret)
            open_ = price
            high = max(open_, close) * (1 + abs(rng.normal(0, 0.0015)))
            low = min(open_, close) * (1 - abs(rng.normal(0, 0.0015)))
            volume = abs(rng.normal(50, 15))
            rows.append((symbol, start, end, open_, high, low, close, volume))
            price = close
    return pd.DataFrame(rows, columns=OHLCV_COLUMNS)


def synthetic_alerts(
    ohlcv: pd.DataFrame | None = None,
    seed: int = 7,
) -> pd.DataFrame:
    """
    Build a small alerts frame consistent with an OHLCV frame: a handful of
    z-score spikes scattered across symbols. If `ohlcv` is given, alerts borrow
    its symbols and a recent timestamp so the two tables line up in the UI.
    """
    rng = np.random.default_rng(seed + 1)
    if ohlcv is None or ohlcv.empty:
        ohlcv = synthetic_ohlcv(seed=seed)

    rows = []
    for symbol in ohlcv["symbol"].unique():
        sym_bars = ohlcv[ohlcv["symbol"] == symbol]
        # ~2 alerts per symbol, anchored on random recent windows.
        for _ in range(2):
            bar = sym_bars.sample(1, random_state=int(rng.integers(0, 1_000_000))).iloc[0]
            mean = float(bar["close"])
            std = max(mean * 0.002, 1e-6)
            z = float(rng.choice([-1, 1]) * rng.uniform(3.1, 5.5))
            price = mean + z * std
            rows.append((symbol, bar["window_end"], price, mean, std, z))
    return pd.DataFrame(rows, columns=ALERT_COLUMNS).sort_values("timestamp")


# ---------------------------------------------------------------------------
# Public loaders — try real Delta, fall back to synthetic.
# ---------------------------------------------------------------------------

def load_ohlcv(delta_root: Path | None = None, allow_synthetic: bool = True) -> Frame:
    """Load the OHLCV table, or synthetic data if it isn't available yet."""
    root = Path(delta_root) if delta_root else DEFAULT_DELTA_ROOT
    df = _read_delta(root / "ohlcv_5m")
    if df is not None and not df.empty:
        return Frame(df=df.sort_values(["symbol", "window_start"]), is_real=True)
    if not allow_synthetic:
        return Frame(df=pd.DataFrame(columns=OHLCV_COLUMNS), is_real=True)
    return Frame(df=synthetic_ohlcv(), is_real=False)


def load_alerts(delta_root: Path | None = None, allow_synthetic: bool = True,
                ohlcv: pd.DataFrame | None = None) -> Frame:
    """Load the alerts table, or synthetic alerts if it isn't available yet."""
    root = Path(delta_root) if delta_root else DEFAULT_DELTA_ROOT
    df = _read_delta(root / "alerts")
    if df is not None and not df.empty:
        return Frame(df=df.sort_values("timestamp"), is_real=True)
    if not allow_synthetic:
        return Frame(df=pd.DataFrame(columns=ALERT_COLUMNS), is_real=True)
    return Frame(df=synthetic_alerts(ohlcv=ohlcv), is_real=False)


def latest_prices(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Most recent close per symbol with its 5-minute change — drives the dashboard
    metric row. Returns columns: symbol, close, change_pct.
    """
    if ohlcv.empty:
        return pd.DataFrame(columns=["symbol", "close", "change_pct"])
    out = []
    for symbol, g in ohlcv.sort_values("window_start").groupby("symbol"):
        last = g.iloc[-1]
        prev_close = g.iloc[-2]["close"] if len(g) > 1 else last["open"]
        change = (last["close"] - prev_close) / prev_close * 100 if prev_close else 0.0
        out.append((symbol, float(last["close"]), float(change)))
    return pd.DataFrame(out, columns=["symbol", "close", "change_pct"])
