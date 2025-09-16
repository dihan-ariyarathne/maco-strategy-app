from __future__ import annotations

import pandas as pd
from google.cloud import bigquery

from app.config import settings
from app.transforms.maco import compute_maco_features, generate_signals
from app.utils.bq import get_bq_client, run_query, load_dataframe
from app.utils.tables import PRICES_1D, MACO_FEATURES, SIGNALS_MACO, PRED_NEXT_DAY


def fetch_prices(client: bigquery.Client) -> pd.DataFrame:
    sql = f"""
    SELECT trade_date, symbol, open, high, low, close
    FROM `{PRICES_1D()}`
    WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 800 DAY)
    """
    return client.query(sql).to_dataframe()


def write_features(client: bigquery.Client, df: pd.DataFrame) -> None:
    df_out = df[["trade_date", "symbol", "sma_fast", "sma_slow"]].copy()
    df_out["ingest_ts"] = pd.Timestamp.utcnow()
    load_dataframe(
        client,
        df_out,
        MACO_FEATURES(),
        schema=[
            bigquery.SchemaField("trade_date", "DATE"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("sma_fast", "FLOAT"),
            bigquery.SchemaField("sma_slow", "FLOAT"),
            bigquery.SchemaField("ingest_ts", "TIMESTAMP"),
        ],
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="trade_date"),
        clustering_fields=["symbol"],
    )


def write_signals(client: bigquery.Client, df: pd.DataFrame) -> None:
    df_out = df[["trade_date", "symbol", "signal", "crossover"]].copy()
    df_out["ingest_ts"] = pd.Timestamp.utcnow()
    load_dataframe(
        client,
        df_out,
        SIGNALS_MACO(),
        schema=[
            bigquery.SchemaField("trade_date", "DATE"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("signal", "STRING"),
            bigquery.SchemaField("crossover", "STRING"),
            bigquery.SchemaField("ingest_ts", "TIMESTAMP"),
        ],
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="trade_date"),
        clustering_fields=["symbol"],
    )


def write_next_day_prediction(client: bigquery.Client, df: pd.DataFrame) -> None:
    latest = df.sort_values(["symbol", "trade_date"]).groupby("symbol").tail(1).copy()
    latest["predicted_signal"] = latest["signal"]
    latest["predicted_direction"] = latest.apply(
        lambda r: "UP" if r["sma_fast"] >= r["sma_slow"] else "DOWN", axis=1
    )
    latest["predicted_close"] = latest["close"]
    latest["generated_at"] = pd.Timestamp.utcnow()

    out = latest[[
        "trade_date",
        "symbol",
        "predicted_signal",
        "predicted_direction",
        "predicted_close",
        "generated_at",
    ]]

    load_dataframe(
        client,
        out,
        PRED_NEXT_DAY(),
        schema=[
            bigquery.SchemaField("trade_date", "DATE"),
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("predicted_signal", "STRING"),
            bigquery.SchemaField("predicted_direction", "STRING"),
            bigquery.SchemaField("predicted_close", "FLOAT"),
            bigquery.SchemaField("generated_at", "TIMESTAMP"),
        ],
        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="trade_date"),
        clustering_fields=["symbol"],
    )


def main() -> None:
    client = get_bq_client(settings.bq_location)
    prices = fetch_prices(client)
    if prices.empty:
        return
    features = compute_maco_features(prices)
    signals = generate_signals(features)
    write_features(client, signals)
    write_signals(client, signals)
    write_next_day_prediction(client, signals)


if __name__ == "__main__":
    main()
