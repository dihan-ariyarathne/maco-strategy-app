from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable

import pandas as pd


from app.config import settings
from app.utils.gcs import build_partition_path, upload_dataframe_as_parquet
from app.utils.bq import get_bq_client, load_dataframe
from google.cloud import bigquery


# In app/ingestion/yahoo_backfill.py, update fetch_yahoo_prices function:
def fetch_yahoo_prices(symbol: str, days: int = 730) -> pd.DataFrame:
    import random
    import time
    from datetime import datetime, time as dtime, timezone

    import requests

    max_retries = 6
    base_sleep = 10
    max_sleep = 120  # Cap backoff to 2 minutes
    session = requests.Session()
    session.headers.update(
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
    )

    for attempt in range(max_retries):
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days)
            start_dt = datetime.combine(start_date, dtime.min, tzinfo=timezone.utc)
            end_dt = datetime.combine(end_date + timedelta(days=1), dtime.min, tzinfo=timezone.utc)
            params = {
                'period1': int(start_dt.timestamp()),
                'period2': int(end_dt.timestamp()),
                'interval': '1d',
                'includePrePost': 'false',
                'events': 'div,splits',
            }
            response = session.get(
                f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}',
                params=params,
                timeout=30,
            )
            if response.status_code == 429:
                raise requests.HTTPError('429 Too Many Requests', response=response)
            response.raise_for_status()
            payload = response.json().get('chart', {})
            results = payload.get('result') or []
            if not results:
                raise ValueError(f'Yahoo chart API returned no result for {symbol}')

            chart = results[0]
            timestamps = chart.get('timestamp') or []
            quote = (chart.get('indicators') or {}).get('quote') or [{}]
            adj = (chart.get('indicators') or {}).get('adjclose') or [{}]
            quote = quote[0] if isinstance(quote, list) else quote
            adj = adj[0] if isinstance(adj, list) else adj

            opens = quote.get('open') or []
            highs = quote.get('high') or []
            lows = quote.get('low') or []
            closes = quote.get('close') or []
            volumes = quote.get('volume') or []
            adj_close = adj.get('adjclose') or []

            if not timestamps or not closes:
                raise ValueError(f'Yahoo chart API returned empty candles for {symbol}')

            records = []
            for idx, ts in enumerate(timestamps):
                records.append(
                    {
                        'trade_date': datetime.utcfromtimestamp(ts).date(),
                        'open': opens[idx] if idx < len(opens) else None,
                        'high': highs[idx] if idx < len(highs) else None,
                        'low': lows[idx] if idx < len(lows) else None,
                        'close': closes[idx] if idx < len(closes) else None,
                        'adj_close': adj_close[idx] if idx < len(adj_close) else None,
                        'volume': volumes[idx] if idx < len(volumes) else None,
                    }
                )

            df = pd.DataFrame(records)
            if df.empty:
                raise ValueError(f'Parsed Yahoo data frame is empty for {symbol}')

            df['symbol'] = symbol
            df['provider'] = 'yahoo'
            ordered_cols = [
                'trade_date',
                'open',
                'high',
                'low',
                'close',
                'adj_close',
                'volume',
                'symbol',
                'provider',
            ]
            df = df[ordered_cols]
            return df

        except Exception as exc:
            print(f'Error fetching {symbol}, attempt {attempt + 1}: {exc}')
            import traceback

            traceback.print_exc()
            should_retry = True
            if isinstance(exc, requests.HTTPError):
                status_code = getattr(exc.response, 'status_code', None)
                if status_code and status_code >= 500:
                    should_retry = True
                elif status_code == 404:
                    should_retry = False
                elif status_code != 429:
                    should_retry = attempt < max_retries - 1

            if attempt < max_retries - 1 and should_retry:
                sleep_time = min(base_sleep * (2 ** attempt), max_sleep)
                jitter = random.uniform(0, base_sleep)
                total_sleep = min(sleep_time + jitter, max_sleep)
                print(f'Sleeping for {total_sleep:.1f} seconds before retrying...')
                time.sleep(total_sleep)
                continue
            print(f'All attempts failed for {symbol}. Returning empty DataFrame.')
            return pd.DataFrame()

    return pd.DataFrame()


def write_raw_to_gcs(df: pd.DataFrame, symbol: str) -> list[str]:
    uris: list[str] = []
    for _, row in df.iterrows():
        trade_date = row["trade_date"]
        # Handle possible types for trade_date
        if isinstance(trade_date, pd.Series):
            trade_date = trade_date.iloc[0]
        if isinstance(trade_date, pd.Timestamp):
            trade_date = trade_date.date()
        elif isinstance(trade_date, str):
            trade_date = pd.to_datetime(trade_date).date()
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
        if df.empty:
            print(f"No data fetched for {sym}. Skipping downstream processing for this symbol.")
            continue
        write_raw_to_gcs(df, sym)
        all_frames.append(df)
    if all_frames:
        load_to_prices_1d(pd.concat(all_frames, ignore_index=True))
    else:
        print("No valid data frames to load to prices_1d.")


if __name__ == "__main__":
    run()
