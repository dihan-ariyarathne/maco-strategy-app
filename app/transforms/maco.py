from __future__ import annotations

import pandas as pd


def compute_sma(df: pd.DataFrame, window: int, value_col: str = "close", output_col: str | None = None) -> pd.Series:
    out_col = output_col or f"sma_{window}"
    return df[value_col].rolling(window=window, min_periods=1).mean().rename(out_col)


def compute_maco_features(df: pd.DataFrame, sma_fast: int = 10, sma_slow: int = 20) -> pd.DataFrame:
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    df["sma_fast"] = df.groupby("symbol", group_keys=False).apply(lambda g: compute_sma(g, sma_fast))
    df["sma_slow"] = df.groupby("symbol", group_keys=False).apply(lambda g: compute_sma(g, sma_slow))
    return df


def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    df["prev_fast"] = df.groupby("symbol")["sma_fast"].shift(1)
    df["prev_slow"] = df.groupby("symbol")["sma_slow"].shift(1)

    def classify(row) -> tuple[str, str]:
        if pd.isna(row["prev_fast"]) or pd.isna(row["prev_slow"]):
            return "HOLD", "NONE"
        crossed_up = row["prev_fast"] <= row["prev_slow"] and row["sma_fast"] > row["sma_slow"]
        crossed_down = row["prev_fast"] >= row["prev_slow"] and row["sma_fast"] < row["sma_slow"]
        if crossed_up:
            return "BUY", "GOLDEN"
        if crossed_down:
            return "SELL", "DEAD"
        return "HOLD", "NONE"

    sigs = df.apply(lambda r: classify(r), axis=1, result_type="expand")
    df["signal"] = sigs[0]
    df["crossover"] = sigs[1]
    return df.drop(columns=["prev_fast", "prev_slow"])
