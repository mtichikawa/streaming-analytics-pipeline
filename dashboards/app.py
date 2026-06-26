"""
dashboards/app.py — Streamlit live dashboard (Day 4).

Reads the two Delta tables the Spark jobs write (delta/ohlcv_5m/, delta/alerts/)
and renders them: a per-symbol price metric row, candlestick-ish OHLC charts,
and the rolling-z-score anomaly feed. Auto-refreshes on an interval so a running
stack shows up live. When the Delta tables aren't there yet (fresh checkout, no
broker), it falls back to synthetic demo data and says so in a banner — the
showcase runs with zero setup.

Run:
    streamlit run dashboards/app.py
    # or via the stack:  docker-compose --profile dashboard up dashboard
"""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from dashboards.data_access import load_ohlcv, load_alerts, latest_prices, SYMBOLS
except ImportError:  # `streamlit run dashboards/app.py` from repo root
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from dashboards.data_access import load_ohlcv, load_alerts, latest_prices, SYMBOLS

REFRESH_SECONDS = 10  # match the Spark trigger cadence


def _render(placeholder) -> None:
    ohlcv = load_ohlcv()
    alerts = load_alerts(ohlcv=ohlcv.df)

    with placeholder.container():
        if not ohlcv.is_real:
            st.warning(
                "Showing **synthetic demo data** — no live Delta tables found. "
                "Start the stack (`docker-compose up -d redpanda`, the producer, "
                "and the two Spark jobs) to see live ticks.",
                icon="🧪",
            )
        else:
            st.success(f"Live — reading {ohlcv.source_label}.", icon="🟢")

        # --- metric row: latest close + 5m change per symbol ---------------
        prices = latest_prices(ohlcv.df)
        cols = st.columns(len(SYMBOLS))
        price_by_symbol = {r["symbol"]: r for _, r in prices.iterrows()}
        for col, symbol in zip(cols, SYMBOLS):
            row = price_by_symbol.get(symbol)
            if row is None:
                col.metric(symbol, "—")
                continue
            col.metric(
                symbol,
                f"${row['close']:,.2f}",
                f"{row['change_pct']:+.2f}% (5m)",
            )

        st.divider()

        # --- per-symbol OHLC close line + high/low band --------------------
        st.subheader("5-minute OHLCV")
        tabs = st.tabs(list(SYMBOLS))
        for tab, symbol in zip(tabs, SYMBOLS):
            with tab:
                g = ohlcv.df[ohlcv.df["symbol"] == symbol].copy()
                if g.empty:
                    st.info(f"No bars for {symbol} yet.")
                    continue
                g = g.sort_values("window_start").set_index("window_start")
                st.line_chart(g[["low", "close", "high"]], height=280)
                st.caption(
                    f"{len(g)} bars · last close ${g['close'].iloc[-1]:,.2f} · "
                    f"window high ${g['high'].max():,.2f} / low ${g['low'].min():,.2f}"
                )

        st.divider()

        # --- anomaly feed --------------------------------------------------
        st.subheader("Anomaly alerts (|z| > 3)")
        if alerts.df.empty:
            st.info("No anomaly alerts in the current window.")
        else:
            feed = alerts.df.copy().sort_values("timestamp", ascending=False)
            feed["z_score"] = feed["z_score"].round(2)
            feed["price"] = feed["price"].round(2)
            st.dataframe(
                feed[["timestamp", "symbol", "price", "z_score",
                      "rolling_mean", "rolling_std"]],
                hide_index=True,
                use_container_width=True,
            )
            st.caption(f"{len(feed)} alerts · {alerts.source_label}")


def main() -> None:
    st.set_page_config(page_title="Streaming Analytics", page_icon="📈", layout="wide")
    st.title("📈 Streaming Crypto Analytics")
    st.caption(
        "Redpanda → Spark Structured Streaming → Delta Lake. "
        "5-minute OHLCV + rolling z-score anomaly detection, read live from Delta."
    )

    auto = st.sidebar.toggle("Auto-refresh", value=True)
    st.sidebar.caption(f"Refreshes every {REFRESH_SECONDS}s when on.")

    placeholder = st.empty()
    _render(placeholder)

    # Cheap auto-refresh loop: re-render then rerun. Streamlit reruns top-to-
    # bottom, so the sleep+rerun keeps the single placeholder updating in place.
    if auto:
        time.sleep(REFRESH_SECONDS)
        st.rerun()


if __name__ == "__main__":
    main()
