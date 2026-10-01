import pandas as pd
from typing import Dict, Any, Optional
from config.settings import settings

class DescriptiveStats:
    """Calculates baseline descriptive statistics for products and reviews (Section 27 & 28).
    Resilient to empty tables, missing columns, and parameter-driven.
    """

    @staticmethod
    def compute_summary(df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> Dict[str, Any]:
        total_products = len(df_products) if df_products is not None else 0
        total_reviews = len(df_reviews) if df_reviews is not None else 0
        total_brands = df_products["brand"].nunique() if (df_products is not None and "brand" in df_products.columns) else 0
        total_categories = df_products["category"].nunique() if (df_products is not None and "category" in df_products.columns) else 0

        avg_rating = 0.0
        median_rating = 0.0
        star_dist = {}
        if df_reviews is not None and not df_reviews.empty and "rating" in df_reviews.columns:
            r_series = pd.to_numeric(df_reviews["rating"], errors="coerce").dropna()
            if not r_series.empty:
                avg_rating = round(float(r_series.mean()), 2)
                median_rating = round(float(r_series.median()), 2)
                star_counts = r_series.round().value_counts().to_dict()
                star_dist = {f"{int(k)}_star_ratio": round(v / len(r_series), 4) for k, v in star_counts.items()}

        price_stats = {}
        if df_products is not None and not df_products.empty and "price" in df_products.columns:
            p_series = pd.to_numeric(df_products["price"], errors="coerce").dropna()
            if not p_series.empty:
                price_stats = {
                    "min_price": float(p_series.min()),
                    "max_price": float(p_series.max()),
                    "mean_price": round(float(p_series.mean()), 2),
                    "median_price": float(p_series.median())
                }

        return {
            "total_products": total_products,
            "total_reviews": total_reviews,
            "total_brands": total_brands,
            "total_categories": total_categories,
            "avg_rating": avg_rating,
            "median_rating": median_rating,
            "star_distribution": star_dist,
            "price_stats": price_stats
        }

    @staticmethod
    def compute_product_metrics(
        df_reviews: pd.DataFrame,
        df_products: pd.DataFrame,
        positive_threshold: Optional[float] = None,
        negative_threshold: Optional[float] = None,
        product_id_col: str = "product_id",
        rating_col: str = "rating",
        review_id_col: str = "review_id"
    ) -> pd.DataFrame:
        """Calculates granular metrics per product.
        If reviews are empty, returns products with zero review_count and 0.0 rating instead of crashing.
        """
        pos_thresh = positive_threshold if positive_threshold is not None else settings.analytics.sentiment_positive_threshold
        neg_thresh = negative_threshold if negative_threshold is not None else settings.analytics.sentiment_negative_threshold

        has_prods = df_products is not None and not df_products.empty
        has_revs = df_reviews is not None and not df_reviews.empty and product_id_col in df_reviews.columns and rating_col in df_reviews.columns

        # Case 1: No reviews at all
        if not has_revs:
            if has_prods:
                res = df_products.copy()
                res["review_count"] = 0
                res["avg_rating"] = 0.0
                res["rating_std"] = 0.0
                res["positive_ratio"] = 0.0
                res["negative_ratio"] = 0.0
                return res
            return pd.DataFrame()

        # Case 2: Process reviews
        count_col = review_id_col if review_id_col in df_reviews.columns else rating_col
        grouped = df_reviews.groupby(product_id_col).agg(
            review_count=(count_col, "count"),
            avg_rating=(rating_col, "mean"),
            rating_std=(rating_col, "std"),
            positive_count=(rating_col, lambda x: (x >= pos_thresh).sum()),
            negative_count=(rating_col, lambda x: (x <= neg_thresh).sum())
        ).reset_index()

        grouped["rating_std"] = grouped["rating_std"].fillna(0.0).round(2)
        grouped["avg_rating"] = grouped["avg_rating"].round(2)
        grouped["positive_ratio"] = (grouped["positive_count"] / grouped["review_count"]).round(4)
        grouped["negative_ratio"] = (grouped["negative_count"] / grouped["review_count"]).round(4)

        if has_prods and product_id_col in df_products.columns:
            merged = pd.merge(df_products, grouped, on=product_id_col, how="left")
            merged["review_count"] = merged["review_count"].fillna(0).astype(int)
            merged["avg_rating"] = merged["avg_rating"].fillna(0.0)
            merged["rating_std"] = merged["rating_std"].fillna(0.0)
            merged["positive_ratio"] = merged["positive_ratio"].fillna(0.0)
            merged["negative_ratio"] = merged["negative_ratio"].fillna(0.0)
            return merged

        return grouped

