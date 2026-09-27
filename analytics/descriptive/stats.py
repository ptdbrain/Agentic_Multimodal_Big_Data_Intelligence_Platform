import pandas as pd
from typing import Dict, Any

class DescriptiveStats:
    """Calculates baseline descriptive statistics for products and reviews (Section 27 & 28)."""

    @staticmethod
    def compute_summary(df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> Dict[str, Any]:
        total_products = len(df_products)
        total_reviews = len(df_reviews)
        total_brands = df_products["brand"].nunique() if "brand" in df_products.columns else 0
        total_categories = df_products["category"].nunique() if "category" in df_products.columns else 0

        avg_rating = round(df_reviews["rating"].mean(), 2) if not df_reviews.empty else 0.0
        median_rating = round(df_reviews["rating"].median(), 2) if not df_reviews.empty else 0.0

        # Star distributions
        if not df_reviews.empty:
            star_counts = df_reviews["rating"].value_counts().to_dict()
            star_dist = {f"{int(k)}_star_ratio": round(v / total_reviews, 4) for k, v in star_counts.items()}
        else:
            star_dist = {}

        # Price metrics
        if not df_products.empty and "price" in df_products.columns:
            prices = df_products["price"]
            price_stats = {
                "min_price": float(prices.min()),
                "max_price": float(prices.max()),
                "mean_price": round(float(prices.mean()), 2),
                "median_price": float(prices.median())
            }
        else:
            price_stats = {}

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
    def compute_product_metrics(df_reviews: pd.DataFrame, df_products: pd.DataFrame) -> pd.DataFrame:
        """Calculates granular metrics per product."""
        if df_reviews.empty:
            return pd.DataFrame()

        grouped = df_reviews.groupby("product_id").agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            rating_std=("rating", "std"),
            positive_count=("rating", lambda x: (x >= 4.0).sum()),
            negative_count=("rating", lambda x: (x <= 2.0).sum())
        ).reset_index()

        grouped["rating_std"] = grouped["rating_std"].fillna(0.0).round(2)
        grouped["avg_rating"] = grouped["avg_rating"].round(2)
        grouped["positive_ratio"] = (grouped["positive_count"] / grouped["review_count"]).round(4)
        grouped["negative_ratio"] = (grouped["negative_count"] / grouped["review_count"]).round(4)

        if not df_products.empty:
            merged = pd.merge(df_products, grouped, on="product_id", how="left")
            merged["review_count"] = merged["review_count"].fillna(0).astype(int)
            merged["avg_rating"] = merged["avg_rating"].fillna(0.0)
            return merged

        return grouped
