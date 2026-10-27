import hashlib
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
        review_id_col: str = "review_id",
        run_id: Optional[str] = None,
        data_quality_status: str = "unverified",
    ) -> List[Dict[str, Any]]:
        anomalies = []
        # Offline exploratory analysis is explicitly unverified; failed/stale DQ
        # must never produce an incident presented as a successful evaluation.
        if data_quality_status not in {"valid", "unverified"}:
            return anomalies
        req_cols = [product_id_col, review_date_col, rating_col]
        if df_reviews.empty or any(c not in df_reviews.columns for c in req_cols):
            return anomalies

        threshold = drop_threshold if drop_threshold is not None else settings.analytics.rating_drop_threshold
        w_days = window_days if window_days is not None else settings.analytics.rating_drop_window_days
        min_revs = min_daily_reviews if min_daily_reviews is not None else settings.analytics.rating_drop_min_reviews

        df = df_reviews.copy()
        if w_days <= 0:
            raise ValueError("window_days must be positive")
        df["date"] = pd.to_datetime(
            df[review_date_col], utc=True, format="mixed", errors="coerce"
        ).dt.floor("D")
        df[rating_col] = pd.to_numeric(df[rating_col], errors="coerce")
        df = df[
            df["date"].notna()
            & df[rating_col].between(1.0, 5.0, inclusive="both")
        ].copy()
        if df.empty:
            return anomalies

        if review_id_col in df.columns:
            df = df[df[review_id_col].notna() & df[review_id_col].astype(str).str.strip().ne('')].copy()

        count_col = review_id_col if review_id_col in df.columns else rating_col

        daily = df.groupby([product_id_col, "date"]).agg(
            avg_rating=(rating_col, "mean"),
            count=(count_col, "count")
        ).reset_index().sort_values([product_id_col, "date"])

        for prod_id, group in daily.groupby(product_id_col):
            if len(group) < min_history_days:
                continue

            group_sorted = group.sort_values("date").copy()
            group_sorted = group_sorted.set_index("date")
            window = f"{w_days}D"
            minimum = max(min_periods, min_history_days)
            rating_sum = (group_sorted["avg_rating"] * group_sorted["count"]).rolling(window, closed="left", min_periods=minimum).sum()
            review_count = group_sorted["count"].rolling(window, closed="left", min_periods=minimum).sum()
            group_sorted["baseline_rating"] = rating_sum / review_count
            group_sorted["baseline_sample_size"] = review_count
            group_sorted["baseline_observed_days"] = group_sorted["count"].rolling(
                window, closed="left", min_periods=minimum
            ).count()
            group_sorted = group_sorted.reset_index()
            group_sorted["rating_delta"] = group_sorted["baseline_rating"] - group_sorted["avg_rating"]

            drops = group_sorted[(group_sorted["rating_delta"] >= threshold) & (group_sorted["count"] >= min_revs)]
            for _, row in drops.iterrows():
                base_r = float(row["baseline_rating"])
                curr_r = float(row["avg_rating"])
                delta_r = float(row["rating_delta"])
                ts_str = str(row["date"])
                raw_key = f"PRODUCT|{prod_id}|RATING_DROP|{ts_str}"
                event_id = f"anom_{hashlib.sha256(raw_key.encode('utf-8')).hexdigest()[:20]}"
                anomalies.append({
                    "event_id": event_id,
                    "entity_type": "PRODUCT",
                    "entity_id": str(prod_id),
                    "anomaly_type": "RATING_DROP",
                    "score": round(delta_r, 2),
                    "baseline_rating": round(base_r, 4),
                    "current_rating": round(curr_r, 4),
                    "timestamp": ts_str,
                    "run_id": run_id,
                    "data_quality_status": data_quality_status,
                    "baseline_window_days": int(w_days),
                    "baseline_sample_size": int(row["baseline_sample_size"]),
                    "baseline_observed_days": int(row["baseline_observed_days"]),
                    "baseline_sample_unit": "reviews",
                    "baseline_method": "past_calendar_window",
                    "baseline_start": (row["date"] - pd.Timedelta(days=w_days)).isoformat(),
                    "baseline_end": row["date"].isoformat(),
                    "current_sample_size": int(row["count"]),
                    "description": (
                        f"Rating drop from {w_days}-day baseline {base_r:.2f} "
                        f"to {curr_r:.2f} (delta: -{delta_r:.2f}, {int(row['count'])} reviews)"
                    )
                })

        return anomalies

