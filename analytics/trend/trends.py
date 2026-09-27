import pandas as pd

class TrendAnalyzer:
    """Calculates temporal metrics, moving averages, and trajectory trends (Section 31 & 32)."""

    @staticmethod
    def daily_review_trends(df_reviews: pd.DataFrame) -> pd.DataFrame:
        if df_reviews.empty or "review_date" not in df_reviews.columns:
            return pd.DataFrame()

        df = df_reviews.copy()
        df["date"] = pd.to_datetime(df["review_date"]).dt.date

        daily = df.groupby("date").agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            negative_count=("rating", lambda x: (x <= 2.0).sum()),
            positive_count=("rating", lambda x: (x >= 4.0).sum())
        ).reset_index()

        daily = daily.sort_values("date")
        daily["avg_rating"] = daily["avg_rating"].round(2)
        # 7-day rolling moving averages
        daily["rolling_avg_count"] = daily["review_count"].rolling(window=7, min_periods=1).mean().round(1)
        daily["rolling_avg_rating"] = daily["avg_rating"].rolling(window=7, min_periods=1).mean().round(2)

        return daily
