import pandas as pd
import numpy as np
from typing import Optional

class PriceTracker:
    """Tracks daily price trajectory and volatility metrics (Section 33 in plan.md).
    Configurable column names, defensive against missing timestamps or malformed data.
    """

    @staticmethod
    def product_price_trajectory(
        df_prices: pd.DataFrame,
        product_id: str,
        product_id_col: str = "product_id",
        price_col: str = "price",
        timestamp_col: str = "timestamp"
    ) -> pd.DataFrame:
        req_cols = [product_id_col, price_col, timestamp_col]
        if df_prices.empty or any(c not in df_prices.columns for c in req_cols):
            return pd.DataFrame()

        filtered = df_prices[df_prices[product_id_col] == product_id].copy()
        if filtered.empty:
            return pd.DataFrame()

        filtered["date"] = pd.to_datetime(
            filtered[timestamp_col], errors="coerce", utc=True
        ).dt.date
        filtered[price_col] = pd.to_numeric(filtered[price_col], errors="coerce")
        filtered = filtered[
            filtered["date"].notna() & filtered[price_col].gt(0) & np.isfinite(filtered[price_col])
        ].copy()
        if filtered.empty:
            return pd.DataFrame()

        daily = filtered.groupby("date").agg(
            price=(price_col, "mean"),
            min_price=(price_col, "min"),
            max_price=(price_col, "max")
        ).reset_index().sort_values("date")

        return daily

    @staticmethod
    def calculate_price_volatility(
        df_prices: pd.DataFrame,
        product_id_col: str = "product_id",
        price_col: str = "price"
    ) -> pd.DataFrame:
        if df_prices.empty or product_id_col not in df_prices.columns or price_col not in df_prices.columns:
            return pd.DataFrame()

        clean = df_prices.copy()
        clean[price_col] = pd.to_numeric(clean[price_col], errors="coerce")
        clean = clean[clean[price_col].gt(0) & np.isfinite(clean[price_col])].dropna(subset=[price_col])
        if clean.empty:
            return pd.DataFrame()

        stats = clean.groupby(product_id_col).agg(
            mean_price=(price_col, "mean"),
            std_price=(price_col, "std"),
            min_price=(price_col, "min"),
            max_price=(price_col, "max"),
            data_points=(price_col, "count")
        ).reset_index()

        # Avoid division by zero
        stats["volatility_ratio"] = (
            stats["std_price"] / stats["mean_price"].replace(0, float("nan"))
        ).round(4)
        stats["volatility_status"] = np.where(stats["data_points"] >= 2, "available", "insufficient_data")
        return stats.sort_values(by="volatility_ratio", ascending=False)

