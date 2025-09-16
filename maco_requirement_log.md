# Requirement Log — MACO Signals App (Yahoo + Finnhub)

## 1. Overview
We want to build a Moving Average Crossover (MACO) trading signal app that:
- **Backfills 2 years of OHLCV data** for Apple (AAPL), Tesla (TSLA), and BTC-USD from **Yahoo Finance**.
- **Appends daily snapshots** of the latest trading day from the **Finnhub API** (stocks + crypto).
- **Generates Buy/Sell/Hold signals** using a MACO strategy (configurable SMA windows).
- **Predicts next-day signals and price direction** based on current MACO state.
- **Stores data in GCS + BigQuery** and visualizes in a **Looker Studio dashboard**.
- Uses **GitHub Actions** for CI/CD, with version control in GitHub and container deployment to Cloud Run.

---

## 2. Goals
- Historical data ingestion from Yahoo Finance (2 years).
- Daily snapshot ingestion from Finnhub (latest day only).
- Compute MACO indicators, signals, and next-day predictions.
- Expose dashboard with candlesticks, range toggles, ticker switch, and next-day card.
- Automate deployment with CI/CD (GitHub Actions).
- Ensure robustness with monitoring, testing, and idempotent ingestion.

**Non-Goals**
- Multi-day history from Finnhub (restricted to snapshots).
- High-frequency/intraday trading.
- Live brokerage execution.

---

## 3. Data Sources
**Yahoo Finance** (via `yfinance`)  
- Used for initial 2-year backfill.  
- Provides OHLCV + adjusted close.

**Finnhub API**  
- Used for **daily one-day snapshot** only.  
- Symbol mapping configurable (e.g., BTC-USD → COINBASE:BTC-USD).

**Common Schema**
- `trade_date`, `open`, `high`, `low`, `close`, `adj_close`, `volume`, `symbol`, `provider`, `ingest_ts`.

---

## 4. Functional Requirements
1. **Backfill Job**
   - Fetch 2 years of daily candles from Yahoo Finance.
   - Write raw Parquet → GCS.
   - Load into `prices_raw` staging, normalize into `prices_1d`.

2. **Daily Snapshot Job**
   - Run at 01:00 UTC.
   - Fetch **previous day** from Finnhub (stock/crypto candle).
   - If snapshot unavailable, fallback to Yahoo for that day.
   - Deduplicate/upsert into `prices_1d`.

3. **MACO Strategy**
   - Compute SMA_fast (10) and SMA_slow (20) on close.
   - Generate signals (BUY, SELL, HOLD).
   - Predict next-day signal + close direction.

4. **Dashboard**
   - Candlestick with range toggle (1M/1Y).
   - Overlay Buy/Sell markers + predicted close.
   - Next-day signal card (Buy/Sell/Hold, direction, price, timestamp).
   - Symbol toggle: AAPL, TSLA, BTC-USD.

5. **CI/CD**
   - Linting, testing, container build (CI).
   - Deploy to Cloud Run, run migrations (CD).
   - GCP auth via Workload Identity Federation.

---

## 5. Architecture
- **Ingestion**: Python jobs (`yfinance`, `requests`) → Cloud Run (Jobs).
- **Orchestration**: Cloud Scheduler → Pub/Sub → Cloud Run.
- **Storage**:  
  - GCS raw zone:  
    - `raw/yahoo_finance/<symbol>/date=<YYYY-MM-DD>/candles.parquet`  
    - `raw/finnhub/<symbol>/date=<YYYY-MM-DD>/candles.json`  
  - BigQuery datasets: `prices_raw`, `prices_1d`, `maco_features`, `signals_maco`, `pred_next_day`.
- **Dashboard**: Looker Studio → BigQuery views.
- **CI/CD**: GitHub Actions → Artifact Registry → Cloud Run.

---

## 6. Data Model (BigQuery)
- `prices_raw`: provider JSON/raw schema.
- `prices_1d`: normalized daily OHLCV (partitioned by `trade_date`, clustered by `symbol`).
- `maco_features`: SMA_fast, SMA_slow.
- `signals_maco`: Buy/Sell/Hold + crossover info.
- `pred_next_day`: next-day signal, predicted direction, predicted close.

**Data Quality**
- Range validation (`low ≤ open,close ≤ high`).
- Uniqueness (`symbol + trade_date`).
- Drift detection (returns > ±25% flagged).
- Idempotent upserts.

---

## 7. Pseudocode

**Yahoo Backfill**
```python
import yfinance as yf
from datetime import date, timedelta

def fetch_yahoo(sym, days=730):
    end = date.today()
    start = end - timedelta(days=days)
    df = yf.download(sym, start=start, end=end, interval="1d")
    df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close",
                            "Adj Close":"adj_close","Volume":"volume"})
    df.reset_index(inplace=True)
    df["symbol"]=sym; df["provider"]="yahoo"
    return df
```

**Finnhub Snapshot**
```python
import requests, os, time
from datetime import datetime, timedelta, timezone

KEY = os.environ["FINNHUB_API_KEY"]
MAP = {"AAPL":"AAPL","TSLA":"TSLA","BTC-USD":"COINBASE:BTC-USD"}

def prev_day_range():
    now = datetime.now(timezone.utc)
    prev = (now - timedelta(days=1)).date()
    start = datetime(prev.year, prev.month, prev.day, tzinfo=timezone.utc)
    return int(start.timestamp()), int((start+timedelta(days=1)).timestamp()), start

def fetch_snapshot(sym):
    f, t, dt = prev_day_range()
    vendor = MAP[sym]
    url = "https://finnhub.io/api/v1/stock/candle" if sym!="BTC-USD" else "https://finnhub.io/api/v1/crypto/candle"
    r = requests.get(url, params={"symbol":vendor,"resolution":"D","from":f,"to":t,"token":KEY})
    if r.status_code==429: time.sleep(2); r = requests.get(url, params={"symbol":vendor,"resolution":"D","from":f,"to":t,"token":KEY})
    r.raise_for_status()
    data = r.json()
    if data.get("s")!="ok" or not data.get("t"): return None
    return {"trade_date": dt.date(), "open": data["o"][-1], "high": data["h"][-1],
            "low": data["l"][-1], "close": data["c"][-1], "volume": data["v"][-1],
            "symbol": sym, "provider":"finnhub"}
```

**BigQuery Merge**
```sql
MERGE `project.dataset.prices_1d` T
USING `project.dataset._incoming` S
ON T.symbol=S.symbol AND T.trade_date=S.trade_date
WHEN MATCHED THEN UPDATE SET
  open=S.open, high=S.high, low=S.low, close=S.close,
  volume=S.volume, provider=S.provider, ingest_ts=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (symbol,trade_date,open,high,low,close,volume,provider,ingest_ts)
  VALUES (S.symbol,S.trade_date,S.open,S.high,S.low,S.close,S.volume,S.provider,CURRENT_TIMESTAMP());
```

---

## 8. Security
- **Finnhub key** in Secret Manager → injected to Cloud Run.
- No API key for Yahoo.
- BQ IAM: Looker SA = read-only, ingestion jobs = write.
- Audit logging enabled.

---

## 9. Monitoring
- Freshness checks: latest `trade_date` per symbol.
- Job failures, retries, DQ rule alerts.
- Alert if lag >36h.

---

## 10. Testing
- Unit: SMA calculations, signal rules, snapshot ingestion.
- Integration: backfill + daily jobs populate BQ correctly, no duplicates.
- UAT: dashboard shows expected toggles, signals, and next-day card.

---

## 11. Open Items
- Confirm crypto vendor (COINBASE vs BINANCE).
- Decide SMA window defaults (10/20 vs 20/50).
- Confirm daily job run time relative to Finnhub data availability.

---
