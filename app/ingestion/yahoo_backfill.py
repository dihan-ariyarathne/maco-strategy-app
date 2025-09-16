from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

import pandas as pd
import yfinance as yf

from app.config import settings
from app.utils.gcs import build_partition_path, upload_dataframe_as_parquet
from app.utils.bq import get_bq_client, load_dataframe
from google.cloud import bigquery


def fetch_yahoo_prices(symbol: str, days: int = 730) -> pd.DataFrame:
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    df = yf.download(symbol, start=start_date, end=end_date, interval="1d")
    df = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    df.reset_index(inplace=True)
    df.rename(columns={"Date": "trade_date"}, inplace=True)
    df["symbol"] = symbol
    df["provider"] = "yahoo"
    return df[["trade_date", "open", "high", "low", "close", "adj_close", "volume", "symbol", "provider"]]


def write_raw_to_gcs(df: pd.DataFrame, symbol: str) -> list[str]:
    uris: list[str] = []
    for _, row in df.iterrows():
        trade_date: date = pd.to_datetime(row["trade_date"]).date()
        object_name = build_partition_path("raw/yahoo_finance", symbol, trade_date, "candles.parquet")
        uris.append(
            upload_dataframe_as_parquet(
                settings.gcs_bucket, pd.DataFrame([row]), object_name
            )
        )
    return uris


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


def run(symbols: Iterable[str] | None = None, days: int = 730) -> None:
    target_symbols = list(symbols) if symbols else settings.symbols
    all_frames: list[pd.DataFrame] = []
    for sym in target_symbols:
        df = fetch_yahoo_prices(sym, days=days)
        write_raw_to_gcs(df, sym)
        all_frames.append(df)
    if all_frames:
        load_to_prices_1d(pd.concat(all_frames, ignore_index=True))


if __name__ == "__main__":
    run()
