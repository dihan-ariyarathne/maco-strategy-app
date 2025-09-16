from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

from app.config import settings
from app.utils.gcs import build_partition_path, upload_bytes
from app.utils.bq import get_bq_client, load_dataframe
from google.cloud import bigquery


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


def to_dataframe(record: dict) -> pd.DataFrame:
    return pd.DataFrame([record])[
        ["trade_date", "open", "high", "low", "close", "volume", "symbol", "provider"]
    ]


def write_raw_json(symbol: str, payload: dict) -> str:
    trade_date = pd.to_datetime(payload["trade_date"]).date()
    object_name = build_partition_path("raw/finnhub", symbol, trade_date, "candles.json")
    return upload_bytes(settings.gcs_bucket, pd.Series(payload).to_json().encode("utf-8"), object_name, "application/json")


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
        snap = fetch_snapshot(sym)
        if snap is None:
            # Fallback handled by separate Yahoo fetch in backfill job; skip here to avoid duplicate logic
            continue
        write_raw_json(sym, snap)
        frames.append(to_dataframe(snap))
    if frames:
        load_to_prices_1d(pd.concat(frames, ignore_index=True))


if __name__ == "__main__":
    run()
