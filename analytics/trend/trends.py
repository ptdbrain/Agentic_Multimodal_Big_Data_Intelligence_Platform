import pandas as pd
from typing import Optional
from config.settings import settings

class TrendAnalyzer:
    """Calculates temporal metrics, moving averages, and trajectory trends (Section 31 & 32).
    Fully parameter-driven with customizable moving windows and sentiment cutoffs.
    """

    @staticmethod
    def daily_review_trends(
        df_reviews: pd.DataFrame,
        rolling_window: Optional[int] = None,
        positive_threshold: Optional[float] = None,
        negative_threshold: Optional[float] = None,
        review_date_col: str = "review_date",
        rating_col: str = "rating",
        review_id_col: str = "review_id"
    ) -> pd.DataFrame:
        if df_reviews.empty or review_date_col not in df_reviews.columns or rating_col not in df_reviews.columns:
            return pd.DataFrame()

        w_size = rolling_window if rolling_window is not None else settings.analytics.trend_rolling_window
        pos_thresh = positive_threshold if positive_threshold is not None else settings.analytics.sentiment_positive_threshold
        neg_thresh = negative_threshold if negative_threshold is not None else settings.analytics.sentiment_negative_threshold

        df = df_reviews.copy()
        try:
            df["date"] = pd.to_datetime(df[review_date_col]).dt.date
        except Exception:
            return pd.DataFrame()

        count_col = review_id_col if review_id_col in df.columns else rating_col

        daily = df.groupby("date").agg(
            review_count=(count_col, "count"),
            avg_rating=(rating_col, "mean"),
            negative_count=(rating_col, lambda x: (x <= neg_thresh).sum()),
            positive_count=(rating_col, lambda x: (x >= pos_thresh).sum())
        ).reset_index()

        daily = daily.sort_values("date")
        daily["avg_rating"] = daily["avg_rating"].round(2)

        # Dynamic rolling moving averages
        daily["rolling_avg_count"] = daily["review_count"].rolling(window=w_size, min_periods=1).mean().round(1)
        daily["rolling_avg_rating"] = daily["avg_rating"].rolling(window=w_size, min_periods=1).mean().round(2)

        return daily

