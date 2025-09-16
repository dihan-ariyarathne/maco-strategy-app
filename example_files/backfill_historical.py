# Backfills ~2 years of daily OHLCV from Yahoo Finance to GCS.
# Normalizes to columns: time, o, h, l, c, v

import os, io
import pandas as pd
import yfinance as yf
from google.cloud import storage
from pipeline.config import SYMBOLS  # <- your tickers list

BUCKET = os.getenv("GCS_BUCKET")
DATA_PREFIX = os.getenv("DATA_PREFIX", "data/raw")
if not BUCKET:
    raise RuntimeError("GCS_BUCKET env var is required")

_storage = storage.Client()

def _flatten_cols(cols):
    """Make sure columns are simple strings (handles MultiIndex tuples)."""
    flat = []
    for c in cols:
        if isinstance(c, tuple):
            # take first non-empty part (e.g., ('Open', '') -> 'Open', ('Date',) -> 'Date')
            flat.append(next((str(p) for p in c if p), str(c)))
        else:
            flat.append(str(c))
    return flat

def fetch_yahoo_2y(symbol: str) -> pd.DataFrame:
    # 1) Download daily candles for ~2 years
    raw = yf.download(symbol, period="2y", interval="1d",
                      auto_adjust=False, actions=False, progress=False)
    if raw is None or raw.empty:
        raise RuntimeError(f"Yahoo returned no data for {symbol}")

    df = raw.reset_index()

    # 2) Flatten any MultiIndex columns and normalise names (case-insensitive)
    df.columns = _flatten_cols(df.columns)
    lower_map = {c.lower(): c for c in df.columns}

    def pick(name: str) -> str:
        # find a column by name, ignoring case and common variants
        candidates = [name, name.lower(), name.title(), name.capitalize(), name.upper()]
        for cand in candidates:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        raise KeyError(f"Expected column '{name}' not found in {list(df.columns)}")

    # 3) Build a clean frame with our exact schema
    out = pd.DataFrame({
        "time": pd.to_datetime(df[pick("Date")], errors="coerce").dt.tz_localize(None)
                 if ("date" in lower_map or "datetime" in lower_map) else pd.to_datetime(df.iloc[:,0]).dt.tz_localize(None),
        "o": pd.to_numeric(df[pick("Open")],  errors="coerce"),
        "h": pd.to_numeric(df[pick("High")],  errors="coerce"),
        "l": pd.to_numeric(df[pick("Low")],   errors="coerce"),
        "c": pd.to_numeric(df[pick("Close")], errors="coerce"),
        "v": pd.to_numeric(df[pick("Volume")],errors="coerce"),
    })

    # 4) Clean up
    out = (out.dropna(subset=["time", "c"])
               .drop_duplicates(subset=["time"])
               .sort_values("time"))
    return out

def _write_csv_to_gcs(df: pd.DataFrame, path: str) -> None:
    bucket = _storage.bucket(BUCKET)
    blob = bucket.blob(path)
    b = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_file(io.BytesIO(b), content_type="text/csv")

def backfill_symbol(symbol: str) -> str:
    df = fetch_yahoo_2y(symbol)
    dest = f"{DATA_PREFIX}/{symbol}.csv"
    _write_csv_to_gcs(df, dest)
    return dest

if __name__ == "__main__":
    for s in SYMBOLS:
        out = backfill_symbol(s)
        print(f"Wrote: gs://{BUCKET}/{out}")
