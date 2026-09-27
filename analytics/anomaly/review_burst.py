import pandas as pd
import numpy as np
from typing import List, Dict, Any

class ReviewBurstDetector:
    """Detects sudden bursts, velocity spikes, and review bombing (Section 35 & 37)."""

    @staticmethod
    def detect_bursts(df_reviews: pd.DataFrame, window_hours: int = 1, threshold_multiplier: float = 3.0) -> List[Dict[str, Any]]:
        anomalies = []
        if df_reviews.empty:
            return anomalies

        df = df_reviews.copy()
        df["hour_dt"] = pd.to_datetime(df["review_date"]).dt.floor(f"{window_hours}h")

        # Hourly counts per product
        hourly_counts = df.groupby(["product_id", "hour_dt"]).agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean")
        ).reset_index()

        for prod_id, group in hourly_counts.groupby("product_id"):
            counts = group["review_count"]
            if len(counts) < 3:
                continue

            mean_vol = counts.mean()
            std_vol = counts.std()
            if std_vol == 0 or np.isnan(std_vol):
                threshold = mean_vol * 2.5
            else:
                threshold = mean_vol + (threshold_multiplier * std_vol)

            spikes = group[group["review_count"] >= max(threshold, 15)] # at least 15 reviews
            for _, row in spikes.iterrows():
                score = round(row["review_count"] / max(mean_vol, 1.0), 2)
                is_bombing = row["avg_rating"] <= 2.5
                anom_type = "REVIEW_BOMBING" if is_bombing else "REVIEW_BURST"
                desc = (
                    f"Traffic surge: {row['review_count']} reviews in {window_hours}h "
                    f"(baseline: {mean_vol:.1f}/h, Avg rating: {row['avg_rating']:.1f})"
                )
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": prod_id,
                    "anomaly_type": anom_type,
                    "score": score,
                    "timestamp": str(row["hour_dt"]),
                    "description": desc
                })

        return anomalies
