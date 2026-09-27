import pandas as pd
from typing import List, Dict, Any

class RatingAnomalyDetector:
    """Detects sudden precipitous drops in product rating (Section 36 in plan.md)."""

    @staticmethod
    def detect_rating_drops(df_reviews: pd.DataFrame, drop_threshold: float = 0.5) -> List[Dict[str, Any]]:
        anomalies = []
        if df_reviews.empty:
            return anomalies

        df = df_reviews.copy()
        df["date"] = pd.to_datetime(df["review_date"]).dt.date

        daily = df.groupby(["product_id", "date"]).agg(
            avg_rating=("rating", "mean"),
            count=("review_id", "count")
        ).reset_index().sort_values(["product_id", "date"])

        for prod_id, group in daily.groupby("product_id"):
            if len(group) < 3:
                continue

            group["baseline_rating"] = group["avg_rating"].rolling(window=7, min_periods=2).mean().shift(1)
            group["rating_delta"] = group["baseline_rating"] - group["avg_rating"]

            drops = group[(group["rating_delta"] >= drop_threshold) & (group["count"] >= 5)]
            for _, row in drops.iterrows():
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": prod_id,
                    "anomaly_type": "RATING_DROP",
                    "score": round(row["rating_delta"], 2),
                    "timestamp": str(row["date"]),
                    "description": (
                        f"Significant rating drop from 7-day baseline {row['baseline_rating']:.2f} "
                        f"to {row['avg_rating']:.2f} (drop: -{row['rating_delta']:.2f})"
                    )
                })

        return anomalies
