import pandas as pd
from typing import Dict, Any
from storage.storage_manager import storage

class GoldAggregator:
    """Generates Gold analytical data marts from Silver layer Parquet (Section 38)."""

    @staticmethod
    def build_gold_marts() -> Dict[str, pd.DataFrame]:
        df_products = storage.read_silver_parquet("products")
        df_reviews = storage.read_silver_parquet("reviews")

        if df_products.empty or df_reviews.empty:
            return {}

        df_reviews["date"] = pd.to_datetime(df_reviews["review_date"]).dt.date

        # 1. Product Daily Stats
        prod_daily = df_reviews.groupby(["date", "product_id"]).agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            rating_std=("rating", "std"),
            positive_count=("rating", lambda x: (x >= 4.0).sum()),
            negative_count=("rating", lambda x: (x <= 2.0).sum())
        ).reset_index()

        prod_daily["rating_std"] = prod_daily["rating_std"].fillna(0.0).round(2)
        prod_daily["avg_rating"] = prod_daily["avg_rating"].round(2)
        prod_daily["positive_ratio"] = (prod_daily["positive_count"] / prod_daily["review_count"]).round(4)
        prod_daily["negative_ratio"] = (prod_daily["negative_count"] / prod_daily["review_count"]).round(4)

        # Merge price from products
        merged_p = pd.merge(prod_daily, df_products[["product_id", "price"]], on="product_id", how="left")
        merged_p["min_price"] = merged_p["price"]
        merged_p["max_price"] = merged_p["price"]
        merged_p["avg_price"] = merged_p["price"]
        merged_p.drop(columns=["price", "positive_count", "negative_count"], inplace=True)

        storage.write_gold_parquet("product_daily_stats", merged_p)

        # 2. Brand Daily Stats
        rev_with_brand = pd.merge(df_reviews, df_products[["product_id", "brand", "price"]], on="product_id", how="inner")
        brand_daily = rev_with_brand.groupby(["date", "brand"]).agg(
            product_count=("product_id", "nunique"),
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            avg_price=("price", "mean")
        ).reset_index()
        brand_daily["avg_rating"] = brand_daily["avg_rating"].round(2)
        brand_daily["avg_price"] = brand_daily["avg_price"].round(2)
        storage.write_gold_parquet("brand_daily_stats", brand_daily)

        # 3. Category Daily Stats
        rev_with_cat = pd.merge(df_reviews, df_products[["product_id", "category", "price"]], on="product_id", how="inner")
        cat_daily = rev_with_cat.groupby(["date", "category"]).agg(
            product_count=("product_id", "nunique"),
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            avg_price=("price", "mean")
        ).reset_index()
        cat_daily["avg_rating"] = cat_daily["avg_rating"].round(2)
        cat_daily["avg_price"] = cat_daily["avg_price"].round(2)
        storage.write_gold_parquet("category_daily_stats", cat_daily)

        print("[✓] Gold Data Marts successfully generated and stored in Data Lake.")
        return {
            "product_daily_stats": merged_p,
            "brand_daily_stats": brand_daily,
            "category_daily_stats": cat_daily
        }
