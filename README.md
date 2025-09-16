# MACO Signals App

This repository contains ingestion jobs, MACO feature/signal compute, and deployment scaffolding for the MACO dashboard described in `maco_requirement_log.md`.

Note: Do NOT run tests/builds yet. First complete the GUI setup below.

## Prerequisites
- GCP project with billing enabled
- BigQuery and Cloud Storage APIs enabled
- Finnhub API key

## GUI Setup (GCS, BigQuery, Looker Studio)

### 1) Google Cloud Storage (GCS)
1. Cloud Console → Storage → Buckets → Create
2. Name: `maco-data-<unique>`; Location: match BigQuery (e.g., US); Class: Standard
3. Uniform access, not public, Google-managed encryption
4. Create folders:
   - `raw/yahoo_finance/`
   - `raw/finnhub/`
5. Optional lifecycle: transition ≥180d to Nearline; delete after 365–730d
6. IAM: grant your future Cloud Run job service account Storage Object Admin on this bucket

### 2) BigQuery
1. Create datasets (same location):
   - `prices_raw`, `prices_1d`, `maco_features`, `signals_maco`, `pred_next_day`
2. Create tables:
   - `prices_1d.prices_1d`
     - Schema: trade_date DATE (partition), open FLOAT, high FLOAT, low FLOAT, close FLOAT, adj_close FLOAT, volume INT64, symbol STRING, provider STRING, ingest_ts TIMESTAMP
     - Partition: trade_date (Daily); Cluster: symbol
   - Optionally external raw tables pointing to GCS `raw/...`
3. IAM: grant your ingestion service account BigQuery Data Editor on these datasets; reporting identity BigQuery Data Viewer
4. Create views (paste and replace placeholders):
   - `sql/views/v_candles_with_signals.sql`
   - `sql/views/v_latest_next_day_cards.sql`
   Replace: `${PROJECT_ID}`, `${ANALYTICS_DATASET}`, `${PRICES_1D_DATASET}`, `${MACO_FEATURES_DATASET}`, `${SIGNALS_DATASET}`, `${PRED_NEXT_DAY_DATASET}`

### 3) Looker Studio
1. Create → Data source → BigQuery → select the views you created
2. Build report:
   - Controls: date range (default last 1 year), symbol filter (AAPL, TSLA, BTC-USD)
   - Candlestick: dimension trade_date; metrics open, high, low, close; overlay SMA fields
   - Markers: BUY/SELL from signals
   - Scorecard: next-day predicted signal, direction, close, generated_at
3. Share access: ensure the report viewers have BigQuery dataset access or use owner’s credentials

## Config
Create environment variables (in CI/Cloud Run or your shell):

- GCP_PROJECT_ID, GCS_BUCKET, BQ_LOCATION
- BQ_DATASET_PRICES_RAW, BQ_DATASET_PRICES_1D, BQ_DATASET_MACO_FEATURES, BQ_DATASET_SIGNALS_MACO, BQ_DATASET_PRED_NEXT_DAY
- SYMBOLS (e.g., `AAPL,TSLA,BTC-USD`)
- FINNHUB_API_KEY

## Jobs
- Backfill: `python -m app.jobs.backfill` (do not run until storage/BQ ready)
- Daily snapshot: `python -m app.jobs.daily_snapshot`
- Compute MACO: `python -m app.jobs.compute_maco`

## Deployment
- Dockerfile provided for Cloud Run Jobs
- `.github/workflows/ci.yml` contains a basic CI skeleton without tests
