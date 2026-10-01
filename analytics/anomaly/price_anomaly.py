import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from config.settings import settings

class PriceAnomalyDetector:
    """Statistical Price Outlier & Anomaly Detector using Z-Score and IQR (Section 34).
    Dynamically configurable with settings fallback, resilient to missing columns and corrupt data.
    """

    @staticmethod
    def detect_zscore_anomalies(
        df_prices: pd.DataFrame,
        threshold: Optional[float] = None,
        min_points: Optional[int] = None,
        product_id_col: str = "product_id",
        price_col: str = "price",
        timestamp_col: str = "timestamp",
        currency_symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        anomalies = []
        if df_prices.empty or product_id_col not in df_prices.columns or price_col not in df_prices.columns:
            return anomalies

        thresh = threshold if threshold is not None else settings.analytics.price_zscore_threshold
        min_pts = min_points if min_points is not None else settings.analytics.price_min_data_points
        curr = currency_symbol or settings.analytics.default_currency

        # Clean prices to numeric
        df = df_prices.copy()
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
        df = df.dropna(subset=[price_col])

        for prod_id, group in df.groupby(product_id_col):
            prices = group[price_col]
            if len(prices) < min_pts:
                continue

            mean = prices.mean()
            std = prices.std()
            if std == 0 or np.isnan(std):
                continue

            z_scores = (prices - mean) / std
            outliers = group[np.abs(z_scores) >= thresh]

            for idx, row in outliers.iterrows():
                z_val = round(float((row[price_col] - mean) / std), 2)
                ts_str = str(row[timestamp_col]) if timestamp_col in row else ""
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": str(prod_id),
                    "anomaly_type": "PRICE_ANOMALY",
                    "score": abs(z_val),
                    "timestamp": ts_str,
                    "description": f"Price outlier: {row[price_col]:,.0f} {curr} (baseline: {mean:,.0f}, Z-Score: {z_val})"
                })

        return anomalies

    @staticmethod
    def detect_iqr_anomalies(
        df_prices: pd.DataFrame,
        k: Optional[float] = None,
        min_points: Optional[int] = None,
        product_id_col: str = "product_id",
        price_col: str = "price",
        timestamp_col: str = "timestamp",
        currency_symbol: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        anomalies = []
        if df_prices.empty or product_id_col not in df_prices.columns or price_col not in df_prices.columns:
            return anomalies

        factor = k if k is not None else settings.analytics.price_iqr_k
        min_pts = min_points if min_points is not None else settings.analytics.price_min_data_points
        curr = currency_symbol or settings.analytics.default_currency

        df = df_prices.copy()
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
        df = df.dropna(subset=[price_col])

        for prod_id, group in df.groupby(product_id_col):
            prices = group[price_col]
            if len(prices) < min_pts:
                continue

            q25 = prices.quantile(0.25)
            q75 = prices.quantile(0.75)
            iqr = q75 - q25
            if iqr == 0 or np.isnan(iqr):
                continue

            lower_bound = q25 - (factor * iqr)
            upper_bound = q75 + (factor * iqr)

            outliers = group[(prices < lower_bound) | (prices > upper_bound)]
            for idx, row in outliers.iterrows():
                ts_str = str(row[timestamp_col]) if timestamp_col in row else ""
                anomalies.append({
                    "entity_type": "PRODUCT",
                    "entity_id": str(prod_id),
                    "anomaly_type": "PRICE_IQR_OUTLIER",
                    "score": round(abs(row[price_col] - prices.median()) / iqr, 2),
                    "timestamp": ts_str,
                    "description": f"Price outside IQR bounds [{lower_bound:,.0f}, {upper_bound:,.0f}]: {row[price_col]:,.0f} {curr}"
                })

        return anomalies

