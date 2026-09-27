import pandas as pd

class PriceTracker:
    """Tracks daily price trajectory and volatility metrics (Section 33 in plan.md)."""

    @staticmethod
    def product_price_trajectory(df_prices: pd.DataFrame, product_id: str) -> pd.DataFrame:
        if df_prices.empty:
            return pd.DataFrame()

        filtered = df_prices[df_prices["product_id"] == product_id].copy()
        if filtered.empty:
            return pd.DataFrame()

        filtered["date"] = pd.to_datetime(filtered["timestamp"]).dt.date
        daily = filtered.groupby("date").agg(
            price=("price", "mean"),
            min_price=("price", "min"),
            max_price=("price", "max")
        ).reset_index().sort_values("date")

        return daily

    @staticmethod
    def calculate_price_volatility(df_prices: pd.DataFrame) -> pd.DataFrame:
        if df_prices.empty:
            return pd.DataFrame()

        stats = df_prices.groupby("product_id").agg(
            mean_price=("price", "mean"),
            std_price=("price", "std"),
            min_price=("price", "min"),
            max_price=("price", "max"),
            data_points=("price", "count")
        ).reset_index()

        stats["std_price"] = stats["std_price"].fillna(0.0)
        stats["volatility_ratio"] = (stats["std_price"] / stats["mean_price"]).round(4)
        return stats.sort_values(by="volatility_ratio", ascending=False)
