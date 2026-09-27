import pandas as pd
import numpy as np
from typing import List, Dict, Any

class PriceAnomalyDetector:
    """Statistical Price Outlier & Anomaly Detector using Z-Score and IQR (Section 34)."""

    @staticmethod
    def detect_zscore_anomalies(df_prices: pd.DataFrame, threshold: float = 3.0) -> List[Dict[str, Any]]:
        anomalies = []
        if df_prices.empty:
            return anomalies

        for prod_id, group in df_prices.groupby("product_id"):
            prices = group["price"]
            if len(prices) < 5:
                continue

            mean = prices.mean()
            std = prices.std()
            if std == 0 or np.isnan(std):
                continue

            z_scores = (prices - mean) / std
            outliers = group[np.abs(z_scores) >= threshold]

            for _, row in outliers.iterrows():
                z_val = round(float((row["price"] - mean) / std), 2)
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": prod_id,
                    "anomaly_type": "PRICE_ANOMALY",
                    "score": abs(z_val),
                    "timestamp": str(row["timestamp"]),
                    "description": f"Price outlier detected: {row['price']} VND (baseline mean: {mean:.0f}, Z-Score: {z_val})"
                })

        return anomalies

    @staticmethod
    def detect_iqr_anomalies(df_prices: pd.DataFrame, k: float = 1.5) -> List[Dict[str, Any]]:
        anomalies = []
        if df_prices.empty:
            return anomalies

        for prod_id, group in df_prices.groupby("product_id"):
            prices = group["price"]
            if len(prices) < 5:
                continue

            q25 = prices.quantile(0.25)
            q75 = prices.quantile(0.75)
            iqr = q75 - q25
            if iqr == 0:
                continue

            lower_bound = q25 - (k * iqr)
            upper_bound = q75 + (k * iqr)

            outliers = group[(prices < lower_bound) | (prices > upper_bound)]
            for _, row in outliers.iterrows():
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": prod_id,
                    "anomaly_type": "PRICE_IQR_OUTLIER",
                    "score": round(abs(row["price"] - prices.median()) / iqr, 2),
                    "timestamp": str(row["timestamp"]),
                    "description": f"Price outside IQR bounds [{lower_bound:.0f}, {upper_bound:.0f}]: {row['price']} VND"
                })

        return anomalies
