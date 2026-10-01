import pandas as pd
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

        try:
            filtered["date"] = pd.to_datetime(filtered[timestamp_col]).dt.date
        except Exception:
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

        stats = df_prices.groupby(product_id_col).agg(
            mean_price=(price_col, "mean"),
            std_price=(price_col, "std"),
            min_price=(price_col, "min"),
            max_price=(price_col, "max"),
            data_points=(price_col, "count")
        ).reset_index()

        stats["std_price"] = stats["std_price"].fillna(0.0)
        # Avoid division by zero
        stats["volatility_ratio"] = (
            stats["std_price"] / stats["mean_price"].replace(0, float("nan"))
        ).fillna(0.0).round(4)
        return stats.sort_values(by="volatility_ratio", ascending=False)

