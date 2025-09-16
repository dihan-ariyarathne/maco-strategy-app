
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

from app.config import settings
from app.utils.gcs import build_partition_path, upload_bytes
from app.utils.bq import get_bq_client, load_dataframe
from google.cloud import bigquery

from app.ingestion.yahoo_backfill import fetch_yahoo_prices


SYMBOL_MAP = {"AAPL": "AAPL", "TSLA": "TSLA", "BTC-USD": "COINBASE:BTC-USD"}


def prev_day_range() -> tuple[int, int, datetime]:
    now = datetime.now(timezone.utc)
    prev = (now - timedelta(days=1)).date()
    start = datetime(prev.year, prev.month, prev.day, tzinfo=timezone.utc)
    return int(start.timestamp()), int((start + timedelta(days=1)).timestamp()), start


def fetch_snapshot(symbol: str) -> Optional[dict]:
    start, end, start_dt = prev_day_range()
    vendor = SYMBOL_MAP.get(symbol, symbol)
    is_crypto = symbol == "BTC-USD"
    url = "https://finnhub.io/api/v1/crypto/candle" if is_crypto else "https://finnhub.io/api/v1/stock/candle"
    r = requests.get(
        url,
        params={"symbol": vendor, "resolution": "D", "from": start, "to": end, "token": settings.finnhub_api_key},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("s") != "ok" or not data.get("t"):
        return None
    return {
        "trade_date": start_dt.date(),
        "open": data["o"][-1],
        "high": data["h"][-1],
        "low": data["l"][-1],
        "close": data["c"][-1],
        "volume": data["v"][-1],
        "symbol": symbol,
        "provider": "finnhub",
    }


def fetch_yahoo_fallback(symbol: str) -> Optional[dict]:
    # Pull a small window and pick the latest trading day.
    df = fetch_yahoo_prices(symbol, days=5)
    if df.empty:
        return None
    row = df.sort_values("trade_date").iloc[-1]
    return {
        "trade_date": row["trade_date"],
        "open": float(row["open"]) if row["open"] is not None else None,
        "high": float(row["high"]) if row["high"] is not None else None,
        "low": float(row["low"]) if row["low"] is not None else None,
        "close": float(row["close"]) if row["close"] is not None else None,
        "adj_close": float(row["adj_close"]) if row.get("adj_close") is not None else None,
        "volume": int(row["volume"]) if row.get("volume") is not None else None,
        "symbol": symbol,
        "provider": "yahoo",
    }


def to_dataframe(record: dict) -> pd.DataFrame:
    base = {
        "trade_date": record.get("trade_date"),
        "open": record.get("open"),
        "high": record.get("high"),
        "low": record.get("low"),
        "close": record.get("close"),
        "volume": record.get("volume"),
        "symbol": record.get("symbol"),
        "provider": record.get("provider"),
    }
    if "adj_close" in record:
        base["adj_close"] = record.get("adj_close")
    df = pd.DataFrame([base])
    columns = ["trade_date", "open", "high", "low", "close", "adj_close", "volume", "symbol", "provider"]
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[columns]


def write_raw_json(symbol: str, payload: dict, provider: str) -> str:
    trade_date = pd.to_datetime(payload["trade_date"]).date()
    base_prefix = "raw/finnhub" if provider == "finnhub" else "raw/yahoo_finance"
    object_name = build_partition_path(base_prefix, symbol, trade_date, "candles.json")
    return upload_bytes(
        settings.gcs_bucket,
        json.dumps(payload, default=str).encode("utf-8"),
        object_name,
        "application/json",
    )


def load_to_prices_1d(df: pd.DataFrame) -> None:
    client = get_bq_client(settings.bq_location)
    table = f"{settings.gcp_project_id}.{settings.dataset_prices_1d}.prices_1d"
    schema = [
        bigquery.SchemaField("trade_date", "DATE"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("adj_close", "FLOAT"),
        bigquery.SchemaField("volume", "INT64"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("provider", "STRING"),
        bigquery.SchemaField("ingest_ts", "TIMESTAMP"),
    ]
    df = df.copy()
    if "adj_close" not in df.columns:
        df["adj_close"] = pd.NA
    df["ingest_ts"] = pd.Timestamp.utcnow()
    load_dataframe(
        client,
        df,
        table,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="trade_date"),
        clustering_fields=["symbol"],
    )


def run(symbols: Optional[list[str]] = None) -> None:
    target_symbols = symbols or settings.symbols
    frames: list[pd.DataFrame] = []
    for sym in target_symbols:
        record: Optional[dict] = None
        if settings.finnhub_api_key:
            try:
                record = fetch_snapshot(sym)
            except requests.HTTPError as http_err:
                print(f"Finnhub HTTP error for {sym}: {http_err}")
            except Exception as exc:
                print(f"Unexpected Finnhub error for {sym}: {exc}")
        else:
            print("FINNHUB_API_KEY not set; skipping Finnhub fetch and falling back to Yahoo.")

        if not record:
            fallback = fetch_yahoo_fallback(sym)
            if fallback:
                write_raw_json(sym, fallback, provider="yahoo")
                frames.append(to_dataframe(fallback))
                print(f"Used Yahoo fallback for {sym}.")
            else:
                print(f"No data available for {sym} from Finnhub or Yahoo fallback.")
            continue

        write_raw_json(sym, record, provider="finnhub")
        frames.append(to_dataframe(record))

    if frames:
        load_to_prices_1d(pd.concat(frames, ignore_index=True))


if __name__ == "__main__":
    run()
