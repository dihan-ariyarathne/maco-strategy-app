from __future__ import annotations

from app.config import settings


def fq(dataset: str, table: str) -> str:
    return f"{settings.gcp_project_id}.{dataset}.{table}"


PRICES_1D = lambda: fq(settings.dataset_prices_1d, "prices_1d")
MACO_FEATURES = lambda: fq(settings.dataset_maco_features, "maco_features")
SIGNALS_MACO = lambda: fq(settings.dataset_signals_maco, "signals_maco")
PRED_NEXT_DAY = lambda: fq(settings.dataset_pred_next_day, "pred_next_day")
