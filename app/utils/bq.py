from __future__ import annotations

from typing import Optional

import pandas as pd
from google.cloud import bigquery


def get_bq_client(location: Optional[str] = None) -> bigquery.Client:
    return bigquery.Client(location=location)


def load_dataframe(
    client: bigquery.Client,
    df: pd.DataFrame,
    full_table_id: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    schema: Optional[list[bigquery.SchemaField]] = None,
    time_partitioning: Optional[bigquery.TimePartitioning] = None,
    clustering_fields: Optional[list[str]] = None,
) -> bigquery.LoadJob:
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=(schema is None),
        schema=schema,
        time_partitioning=time_partitioning,
        clustering_fields=clustering_fields,
    )
    load_job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    return load_job.result()


def run_query(client: bigquery.Client, sql: str, job_config: Optional[bigquery.QueryJobConfig] = None) -> bigquery.QueryJob:
    job = client.query(sql, job_config=job_config)
    return job.result()
