import os
from pydantic import BaseModel


class Settings(BaseModel):
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcs_bucket: str = os.getenv("GCS_BUCKET", "")
    bq_location: str = os.getenv("BQ_LOCATION", "US")

    dataset_prices_raw: str = os.getenv("BQ_DATASET_PRICES_RAW", "prices_raw")
    dataset_prices_1d: str = os.getenv("BQ_DATASET_PRICES_1D", "prices_1d")
    dataset_maco_features: str = os.getenv("BQ_DATASET_MACO_FEATURES", "maco_features")
    dataset_signals_maco: str = os.getenv("BQ_DATASET_SIGNALS_MACO", "signals_maco")
    dataset_pred_next_day: str = os.getenv("BQ_DATASET_PRED_NEXT_DAY", "pred_next_day")

    symbols: list[str] = os.getenv("SYMBOLS", "AAPL,TSLA,BTC-USD").split(",")

    finnhub_api_key: str = os.getenv("FINNHUB_API_KEY", "")


settings = Settings()
