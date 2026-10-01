import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from config.settings import settings

class ReviewBurstDetector:
    """Detects sudden bursts, velocity spikes, and review bombing (Section 35 & 37).
    Fully parameter-driven, removing arbitrary hardcoded thresholds.
    """

    @staticmethod
    def detect_bursts(
        df_reviews: pd.DataFrame,
        window_hours: Optional[int] = None,
        threshold_multiplier: Optional[float] = None,
        min_burst_reviews: Optional[int] = None,
        bombing_rating_threshold: Optional[float] = None,
        min_history_points: int = 2,
        zero_std_multiplier: float = 2.0,
        product_id_col: str = "product_id",
        review_date_col: str = "review_date",
        rating_col: str = "rating",
        review_id_col: str = "review_id"
    ) -> List[Dict[str, Any]]:
        anomalies = []
        req_cols = [product_id_col, review_date_col, rating_col]
        if df_reviews.empty or any(c not in df_reviews.columns for c in req_cols):
            return anomalies

        w_hours = window_hours if window_hours is not None else settings.analytics.burst_window_hours
        mult = threshold_multiplier if threshold_multiplier is not None else settings.analytics.burst_threshold_multiplier
        min_burst = min_burst_reviews if min_burst_reviews is not None else settings.analytics.burst_min_reviews
        bomb_thresh = bombing_rating_threshold if bombing_rating_threshold is not None else settings.analytics.bombing_rating_threshold

        df = df_reviews.copy()
        try:
            df["hour_dt"] = pd.to_datetime(df[review_date_col]).dt.floor(f"{w_hours}h")
        except Exception:
            return anomalies

        # Count column can be review_id if present, else synthesize
        count_col = review_id_col if review_id_col in df.columns else rating_col

        # Hourly counts per product
        hourly_counts = df.groupby([product_id_col, "hour_dt"]).agg(
            review_count=(count_col, "count"),
            avg_rating=(rating_col, "mean")
        ).reset_index()

        for prod_id, group in hourly_counts.groupby(product_id_col):
            counts = group["review_count"]
            if len(counts) < min_history_points:
                continue

            mean_vol = float(counts.mean())
            std_vol = float(counts.std())
            if std_vol == 0 or np.isnan(std_vol):
                threshold = mean_vol * zero_std_multiplier
            else:
                threshold = mean_vol + (mult * std_vol)

            cutoff = max(threshold, float(min_burst))
            spikes = group[group["review_count"] >= cutoff]
            for _, row in spikes.iterrows():
                score = round(row["review_count"] / max(mean_vol, 1.0), 2)
                is_bombing = row["avg_rating"] <= bomb_thresh
                anom_type = "REVIEW_BOMBING" if is_bombing else "REVIEW_BURST"
                desc = (
                    f"Traffic surge: {row['review_count']} reviews in {w_hours}h "
                    f"(baseline: {mean_vol:.1f}/h, Avg rating: {row['avg_rating']:.1f})"
                )
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": str(prod_id),
                    "anomaly_type": anom_type,
                    "score": score,
                    "timestamp": str(row["hour_dt"]),
                    "description": desc
                })

        return anomalies

