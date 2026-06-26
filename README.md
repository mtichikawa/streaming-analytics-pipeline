# Streaming Analytics Pipeline

A real-time crypto ticker pipeline on **Redpanda + Spark Structured Streaming + Delta Lake**, with a Streamlit live dashboard. This closes the streaming gap in the portfolio — the other data projects (cloud-etl, sql-analytics, databricks-lakehouse) are all batch. Everything runs locally via docker-compose; no cloud, no managed services.

## Architecture

```
crypto producer (ccxt) ──► Redpanda topic: crypto.ticks
                                    │
                   ┌────────────────┴────────────────┐
                   ▼                                 ▼
      Spark Structured Streaming          Spark Structured Streaming
      (5m tumbling windows, OHLCV)        (z-score anomaly detection)
                   │                                 │
                   ▼                                 ▼
         Delta Lake: ohlcv_5m              Delta Lake: alerts
                   │                                 │
                   └──────────────┬──────────────────┘
                                  ▼
                    Streamlit live dashboard
                    (auto-refresh from Delta tables)
```

This reuses existing portfolio pieces rather than reinventing them:
- The **T1 `crypto-data-pipeline`** ccxt fetcher pattern becomes the streaming producer.
- The **`anomaly-detection`** rolling z-score detector becomes the streaming anomaly job.
- The **`databricks-lakehouse`** Delta Lake pattern is the sink.
- The **T5 `trading-dashboard`** Streamlit pattern is the dashboard layer.

## Components

| File | What it does |
|---|---|
| `src/schemas.py` | Single source of truth for every payload schema (tick, OHLCV, alert). Also exposes a PySpark-free tick contract + `validate_tick()` so the producer and tests don't need Spark. |
| `src/producer.py` | Reuses the T1 ccxt pattern to fetch a live price per symbol and publish JSON ticks to `crypto.ticks`. Broker + tick rate configurable; ccxt and Kafka are injectable for tests. |
| `src/stream_ohlcv.py` | Spark Structured Streaming job: parse ticks, group by symbol + 5-minute tumbling window with a 30s watermark, aggregate to OHLCV, write to `delta/ohlcv_5m/`. |
| `src/stream_anomaly.py` | Second streaming consumer: per-symbol rolling z-score over a 60-tick window, emits an alert when `|z| > 3`, writes to `delta/alerts/`. The z-score math is a pure `RollingZScore` class adapted from `anomaly-detection`. |
| `src/checkpoint_config.py` | Checkpoint + Delta paths and the watermark/window/trigger settings, in one place. |
| `dashboards/data_access.py` | Reads the Delta tables host-side with `deltalake` (delta-rs) — no Java/Spark needed just to display data. Falls back to deterministic synthetic data when the tables don't exist yet, so the dashboard runs on a bare checkout. |
| `dashboards/app.py` | Streamlit live dashboard: per-symbol price metrics, 5-minute OHLCV charts, and the z-score anomaly feed, auto-refreshing from the Delta tables. Banners whether it's showing live or demo data. |
| `examples/quick_demo.py` | Runs the pure cores (tick contract, rolling z-score, dashboard data layer) with no broker and no Spark — a couple of seconds on a fresh checkout. |

## Schemas, defined once

Every schema lives in `src/schemas.py`. The tick payload is the contract on the `crypto.ticks` topic:

```json
{"symbol": "BTC/USDT", "timestamp": "2026-06-01T12:00:00.000Z", "price": 67000.5, "volume": 1.23}
```

The producer validates each tick against this contract before publishing, so nothing malformed lands on the topic.

## Watermarking and late data

The OHLCV job sets a **30-second watermark** on the event-time column. A tick that arrives late but within 30 seconds of the current watermark still updates the correct (already-open) 5-minute bucket; anything later is dropped so streaming state stays bounded. OHLCV is naturally a **tumbling** window — 5-minute buckets, no overlap.

## Exactly-once recovery

Each Spark job writes to a checkpoint directory (`checkpoints/ohlcv_5m`, `checkpoints/alerts`). On restart it resumes from the last committed offsets, and the Delta sink's transactional commits give effective exactly-once on the output. This is the one streaming claim that's easy to hand-wave, so `tests/test_recovery.py` actually proves it: a checkpointed query runs over a growing input, gets killed, and restarts — the test asserts the already-committed input is **not** reprocessed, with a fresh-checkpoint control that reprocesses everything to show it's the checkpoint doing the work. To wipe state and start fresh:

```bash
rm -rf checkpoints/ohlcv_5m delta/ohlcv_5m   # reset the OHLCV job
rm -rf checkpoints/alerts   delta/alerts     # reset the anomaly job
```

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

PySpark needs a Java runtime (JDK 11 or 17) on the host to run the streaming jobs locally.

## Run instructions

```bash
# 1. Start the Redpanda broker
docker-compose up -d redpanda
rpk topic create crypto.ticks --brokers localhost:19092

# 2. Start the producer (publishes ~1 tick/sec/symbol)
python -m src.producer

# 3. In separate shells, start the two Spark jobs
python -m src.stream_ohlcv   --broker localhost:19092
python -m src.stream_anomaly --broker localhost:19092

# 4. Streamlit live dashboard
docker-compose --profile dashboard up dashboard
# or on the host: streamlit run dashboards/app.py
```

No broker or Spark handy? Run the cores directly:

```bash
python examples/quick_demo.py   # tick contract + z-score + dashboard data, no setup
streamlit run dashboards/app.py # dashboard renders synthetic demo data until the stack is live
```

Consume from the topic directly to sanity-check the producer:

```bash
rpk topic consume crypto.ticks --brokers localhost:19092
```

## Tests

```bash
pytest tests/ -v
```

The producer, schema, z-score, dashboard-data, and checkpoint-config tests are pure Python and run anywhere (37 pass with no Spark). The OHLCV aggregation/watermark and end-to-end checkpoint-recovery tests need a real SparkSession (Java + PySpark) and are automatically **skip-gated** when Spark isn't available, so the suite stays green in lightweight environments. CI (`.github/workflows/tests.yml`) installs a JDK so the Spark-gated tests actually run there rather than skip.

## Interview talking points this unlocks

- **Watermarking and late-data handling** — a 30-second watermark on the OHLCV job: a late tick within that bound updates the right bucket, later ticks are dropped to keep state bounded.
- **Exactly-once vs at-least-once** — checkpointed Delta sinks give effective exactly-once on the output, verified by killing the job mid-stream and checking for duplicates after restart.
- **Tumbling vs sliding vs session windows** — OHLCV is tumbling: fixed 5-minute buckets, no overlap. Session windows would be wrong for a continuous price feed.
- **Backpressure and consumer lag** — Redpanda's consumer-lag metric plus Spark's microbatch sizing make lag observable.
- **Stream-batch unification** — `aggregate_ohlcv()` is one DataFrame transform that runs against a live Redpanda stream or a historical Delta table with identical code.
