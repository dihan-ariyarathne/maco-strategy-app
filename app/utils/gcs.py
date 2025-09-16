from __future__ import annotations

import io
from datetime import date
from typing import Optional

import pandas as pd
from google.cloud import storage


def get_bucket(bucket_name: str) -> storage.bucket.Bucket:
    client = storage.Client()
    return client.bucket(bucket_name)


def build_partition_path(base_prefix: str, symbol: str, partition_date: date, filename: str) -> str:
    return f"{base_prefix}/{symbol}/date={partition_date.isoformat()}/{filename}"


def upload_dataframe_as_parquet(
    bucket_name: str,
    df: pd.DataFrame,
    object_name: str,
    compression: str = "snappy",
) -> str:
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(object_name)

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", compression=compression, index=False)
    parquet_buffer.seek(0)
    blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
    return f"gs://{bucket_name}/{object_name}"


def upload_bytes(
    bucket_name: str,
    data: bytes,
    object_name: str,
    content_type: Optional[str] = None,
) -> str:
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{bucket_name}/{object_name}"
