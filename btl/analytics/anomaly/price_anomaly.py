import hashlib
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
        currency_symbol: Optional[str] = None,
        run_id: Optional[str] = None,
        data_quality_status: str = "unverified"
    ) -> List[Dict[str, Any]]:
        anomalies = []
        # Offline exploratory analysis is explicitly unverified; failed/stale DQ
        # must never produce an incident presented as a successful evaluation.
        if data_quality_status not in {"valid", "unverified"}:
            return anomalies
        if df_prices.empty or product_id_col not in df_prices.columns or price_col not in df_prices.columns:
            return anomalies

        thresh = threshold if threshold is not None else settings.analytics.price_zscore_threshold
        min_pts = min_points if min_points is not None else settings.analytics.price_min_data_points
        curr = currency_symbol or settings.analytics.default_currency

        # Clean prices to numeric
        df = df_prices.copy()
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
        df = df[df[price_col].gt(0) & np.isfinite(df[price_col])].dropna(subset=[price_col])

        for prod_id, group in df.groupby(product_id_col):
            group = group.reset_index(drop=True)
            prices = group[price_col]
            times = (pd.to_datetime(group[timestamp_col], utc=True, format="mixed", errors="coerce")
                     if timestamp_col in group else pd.Series(pd.NaT, index=group.index))
            if len(prices) < min_pts:
                continue

            for idx, row in group.iterrows():
                # Baseline calculation strictly excludes the current candidate point being evaluated
                baseline_prices = prices.drop(idx)
                if len(baseline_prices) < 2:
                    continue
                baseline_times = times.drop(idx).dropna()
                baseline_start = baseline_times.min().isoformat() if not baseline_times.empty else None
                baseline_end = baseline_times.max().isoformat() if not baseline_times.empty else None

                base_mean = float(baseline_prices.mean())
                base_std = float(baseline_prices.std())
                candidate_price = float(row[price_col])

                if base_std == 0 or np.isnan(base_std):
                    # When all baseline prices are identical, any significant deviation is an outlier
                    if base_mean > 0 and abs(candidate_price - base_mean) / base_mean >= 0.10:
                        z_raw = (candidate_price - base_mean) / max(base_mean * 0.01, 1e-9)
                    else:
                        continue
                else:
                    z_raw = (candidate_price - base_mean) / base_std

                if abs(z_raw) >= thresh:
                    z_val = round(float(z_raw), 2)
                    ts_str = str(row[timestamp_col]) if timestamp_col in row else ""
                    raw_key = f"PRODUCT|{prod_id}|PRICE_ANOMALY|{ts_str}|{row[price_col]}"
                    event_id = f"anom_{hashlib.sha256(raw_key.encode('utf-8')).hexdigest()[:20]}"
                    anomalies.append({
                        "event_id": event_id,
                        "entity_type": "PRODUCT",
                        "entity_id": str(prod_id),
                        "anomaly_type": "PRICE_ANOMALY",
                        "score": abs(z_val),
                        "baseline_mean": round(base_mean, 2),
                        "baseline_std": round(0.0 if np.isnan(base_std) else base_std, 2),
                        "observed_price": round(candidate_price, 2),
                        "timestamp": ts_str,
                        "run_id": run_id,
                        "data_quality_status": data_quality_status,
                        "baseline_sample_size": int(len(baseline_prices)),
                        "baseline_sample_unit": "price_observations",
                        "baseline_method": "leave_one_out",
                        "baseline_start": baseline_start,
                        "baseline_end": baseline_end,
                        "current_sample_size": 1,
                        "description": f"Price outlier: {row[price_col]:,.0f} {curr} (baseline: {base_mean:,.0f}, Z-Score: {z_val})"
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
        currency_symbol: Optional[str] = None,
        run_id: Optional[str] = None,
        data_quality_status: str = "unverified"
    ) -> List[Dict[str, Any]]:
        anomalies = []
        # Offline exploratory analysis is explicitly unverified; failed/stale DQ
        # must never produce an incident presented as a successful evaluation.
        if data_quality_status not in {"valid", "unverified"}:
            return anomalies
        if df_prices.empty or product_id_col not in df_prices.columns or price_col not in df_prices.columns:
            return anomalies

        factor = k if k is not None else settings.analytics.price_iqr_k
        min_pts = min_points if min_points is not None else settings.analytics.price_min_data_points
        curr = currency_symbol or settings.analytics.default_currency

        df = df_prices.copy()
        df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
        df = df[df[price_col].gt(0) & np.isfinite(df[price_col])].dropna(subset=[price_col])

        for prod_id, group in df.groupby(product_id_col):
            group = group.reset_index(drop=True)
            prices = group[price_col]
            times = (pd.to_datetime(group[timestamp_col], utc=True, format="mixed", errors="coerce")
                     if timestamp_col in group else pd.Series(pd.NaT, index=group.index))
            if len(prices) < min_pts:
                continue

            for idx, row in group.iterrows():
                # Baseline calculation strictly excludes the current candidate point being evaluated
                baseline_prices = prices.drop(idx)
                if len(baseline_prices) < 2:
                    continue
                baseline_times = times.drop(idx).dropna()
                baseline_start = baseline_times.min().isoformat() if not baseline_times.empty else None
                baseline_end = baseline_times.max().isoformat() if not baseline_times.empty else None

                q25 = float(baseline_prices.quantile(0.25))
                q75 = float(baseline_prices.quantile(0.75))
                iqr = q75 - q25
                base_median = float(baseline_prices.median())
                candidate_price = float(row[price_col])

                if iqr == 0 or np.isnan(iqr):
                    continue

                lower_bound = q25 - (factor * iqr)
                upper_bound = q75 + (factor * iqr)

                if candidate_price < lower_bound or candidate_price > upper_bound:
                    ts_str = str(row[timestamp_col]) if timestamp_col in row else ""
                    raw_key = f"PRODUCT|{prod_id}|PRICE_IQR_OUTLIER|{ts_str}|{row[price_col]}"
                    event_id = f"anom_{hashlib.sha256(raw_key.encode('utf-8')).hexdigest()[:20]}"
                    anomalies.append({
                        "event_id": event_id,
                        "entity_type": "PRODUCT",
                        "entity_id": str(prod_id),
                        "anomaly_type": "PRICE_IQR_OUTLIER",
                        "score": round(abs(candidate_price - base_median) / iqr, 2),
                        "baseline_median": round(base_median, 2),
                        "lower_bound": round(lower_bound, 2),
                        "upper_bound": round(upper_bound, 2),
                        "observed_price": round(candidate_price, 2),
                        "timestamp": ts_str,
                        "run_id": run_id,
                        "data_quality_status": data_quality_status,
                        "baseline_sample_size": int(len(baseline_prices)),
                        "baseline_sample_unit": "price_observations",
                        "baseline_method": "leave_one_out",
                        "baseline_start": baseline_start,
                        "baseline_end": baseline_end,
                        "current_sample_size": 1,
                        "description": f"Price outside IQR bounds [{lower_bound:,.0f}, {upper_bound:,.0f}]: {row[price_col]:,.0f} {curr}"
                    })

        return anomalies


