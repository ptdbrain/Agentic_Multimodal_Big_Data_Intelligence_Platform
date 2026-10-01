import pandas as pd
from typing import List, Dict, Any, Optional
from config.settings import settings

class RatingAnomalyDetector:
    """Detects sudden precipitous drops in product rating (Section 36 in plan.md).
    Configurable rolling windows, drop deltas, and minimum sample thresholds.
    """

    @staticmethod
    def detect_rating_drops(
        df_reviews: pd.DataFrame,
        drop_threshold: Optional[float] = None,
        window_days: Optional[int] = None,
        min_daily_reviews: Optional[int] = None,
        min_periods: int = 2,
        min_history_days: int = 2,
        product_id_col: str = "product_id",
        review_date_col: str = "review_date",
        rating_col: str = "rating",
        review_id_col: str = "review_id"
    ) -> List[Dict[str, Any]]:
        anomalies = []
        req_cols = [product_id_col, review_date_col, rating_col]
        if df_reviews.empty or any(c not in df_reviews.columns for c in req_cols):
            return anomalies

        threshold = drop_threshold if drop_threshold is not None else settings.analytics.rating_drop_threshold
        w_days = window_days if window_days is not None else settings.analytics.rating_drop_window_days
        min_revs = min_daily_reviews if min_daily_reviews is not None else settings.analytics.rating_drop_min_reviews

        df = df_reviews.copy()
        try:
            df["date"] = pd.to_datetime(df[review_date_col]).dt.date
        except Exception:
            return anomalies

        count_col = review_id_col if review_id_col in df.columns else rating_col

        daily = df.groupby([product_id_col, "date"]).agg(
            avg_rating=(rating_col, "mean"),
            count=(count_col, "count")
        ).reset_index().sort_values([product_id_col, "date"])

        for prod_id, group in daily.groupby(product_id_col):
            if len(group) < min_history_days:
                continue

            group_sorted = group.sort_values("date").copy()
            group_sorted["baseline_rating"] = (
                group_sorted["avg_rating"].rolling(window=w_days, min_periods=min_periods).mean().shift(1)
            )
            group_sorted["rating_delta"] = group_sorted["baseline_rating"] - group_sorted["avg_rating"]

            drops = group_sorted[(group_sorted["rating_delta"] >= threshold) & (group_sorted["count"] >= min_revs)]
            for _, row in drops.iterrows():
                base_r = float(row["baseline_rating"])
                curr_r = float(row["avg_rating"])
                delta_r = float(row["rating_delta"])
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": str(prod_id),
                    "anomaly_type": "RATING_DROP",
                    "score": round(delta_r, 2),
                    "timestamp": str(row["date"]),
                    "description": (
                        f"Rating drop from {w_days}-day baseline {base_r:.2f} "
                        f"to {curr_r:.2f} (delta: -{delta_r:.2f}, {int(row['count'])} reviews)"
                    )
                })

        return anomalies

