import pandas as pd
from typing import Optional
from config.settings import settings

class TrendAnalyzer:
    """Calculates temporal metrics, moving averages, and trajectory trends (Section 31 & 32).
    Fully parameter-driven with customizable moving windows and sentiment cutoffs.
    """

    @staticmethod
    def daily_volume_trends(
        daily_stats: pd.DataFrame, rolling_window: int = 7, min_observed_days: int = 2,
    ) -> pd.DataFrame:
        """Aggregate daily mart counts; average only observed days in a calendar window."""
        if rolling_window <= 0 or min_observed_days <= 0:
            raise ValueError("Window and minimum observed days must be positive")
        if daily_stats.empty or not {'date', 'review_count'}.issubset(daily_stats.columns):
            return pd.DataFrame()
        data = daily_stats[['date', 'review_count']].copy()
        data['date'] = pd.to_datetime(data['date'], errors='coerce', utc=True, format='mixed').dt.floor('D')
        data['review_count'] = pd.to_numeric(data['review_count'], errors='coerce')
        data = data[data['date'].notna() & data['review_count'].ge(0)
                    & data['review_count'].lt(float('inf'))]
        if data.empty:
            return pd.DataFrame()
        daily = data.groupby('date')['review_count'].sum().to_frame().sort_index()
        window = daily['review_count'].rolling(f'{rolling_window}D', closed='right')
        daily['rolling_avg_count'] = window.mean().round(1)
        daily['observed_days'] = window.count().astype(int)
        daily['missing_days'] = rolling_window - daily['observed_days']
        daily['baseline_start'] = (daily.index - pd.Timedelta(days=rolling_window - 1)).date
        daily['baseline_end'] = daily.index.date
        daily['trend_status'] = 'insufficient_data'
        daily.loc[daily['observed_days'] >= min_observed_days, 'trend_status'] = 'sufficient_data'
        daily = daily.reset_index()
        daily['date'] = daily['date'].dt.date
        return daily

    @staticmethod
    def daily_review_trends(
        df_reviews: pd.DataFrame,
        rolling_window: Optional[int] = None,
        positive_threshold: Optional[float] = None,
        negative_threshold: Optional[float] = None,
        review_date_col: str = "review_date",
        rating_col: str = "rating",
        review_id_col: str = "review_id",
        min_observed_days: int = 2,
        min_reviews: int = 2,
    ) -> pd.DataFrame:
        if df_reviews.empty or review_date_col not in df_reviews.columns or rating_col not in df_reviews.columns:
            return pd.DataFrame()

        w_size = rolling_window if rolling_window is not None else settings.analytics.trend_rolling_window
        pos_thresh = positive_threshold if positive_threshold is not None else settings.analytics.sentiment_positive_threshold
        neg_thresh = negative_threshold if negative_threshold is not None else settings.analytics.sentiment_negative_threshold

        if w_size <= 0 or min_observed_days <= 0 or min_reviews <= 0:
            raise ValueError("Window and minimum sample sizes must be positive")
        df = df_reviews.copy()
        df["date"] = pd.to_datetime(df[review_date_col], errors="coerce", utc=True, format="mixed").dt.date
        df[rating_col] = pd.to_numeric(df[rating_col], errors="coerce")
        df = df[
            df["date"].notna()
            & df[rating_col].between(1.0, 5.0, inclusive="both")
        ].copy()
        if df.empty:
            return pd.DataFrame()

        if review_id_col in df.columns:
            df = df[df[review_id_col].notna() & df[review_id_col].astype(str).str.strip().ne('')].copy()

        count_col = review_id_col if review_id_col in df.columns else rating_col

        daily = df.groupby("date").agg(
            review_count=(count_col, "count"),
            avg_rating=(rating_col, "mean"),
            rating_sum=(rating_col, "sum"),
            rating_sample_size=(rating_col, "count"),
            negative_count=(rating_col, lambda x: (x <= neg_thresh).sum()),
            positive_count=(rating_col, lambda x: (x >= pos_thresh).sum())
        ).reset_index()

        daily = daily.sort_values("date")
        daily["avg_rating"] = daily["avg_rating"].round(2)

        # Calendar windows include the displayed day and preceding w_size-1
        # days. Missing days stay unobserved, never fabricated as review rows.
        daily = daily.set_index(pd.DatetimeIndex(daily["date"]))
        window = f"{w_size}D"
        sample = daily["rating_sample_size"].rolling(window, closed="right").sum()
        rating_sum = daily["rating_sum"].rolling(window, closed="right").sum()
        daily["rolling_avg_count"] = daily["review_count"].rolling(window, closed="right").mean().round(1)
        daily["rolling_avg_rating"] = (rating_sum / sample).round(2)
        daily["rolling_rating_sample_size"] = sample.astype(int)
        daily["observed_days"] = daily["review_count"].rolling(window, closed="right").count().astype(int)
        daily["missing_days"] = w_size - daily["observed_days"]
        daily["baseline_start"] = (daily.index - pd.Timedelta(days=w_size-1)).date
        daily["baseline_end"] = daily.index.date
        daily["trend_status"] = "insufficient_data"
        sufficient = (daily["observed_days"] >= min_observed_days) & (sample >= min_reviews)
        daily.loc[sufficient, "trend_status"] = "sufficient_data"
        daily["data_quality_status"] = "valid"
        return daily.drop(columns=["rating_sum"]).reset_index(drop=True)


