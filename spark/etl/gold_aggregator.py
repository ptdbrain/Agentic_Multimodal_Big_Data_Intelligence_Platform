import pandas as pd
from typing import Dict, Any, Optional
from storage.storage_manager import storage, StorageManager
from config.settings import settings

class GoldAggregator:
    """Generates Gold analytical data marts from Silver layer Parquet (Section 38).
    Dynamically configurable with flexible inputs and defensive against schema variations.
    """

    @staticmethod
    def build_gold_marts(
        df_products: Optional[pd.DataFrame] = None,
        df_reviews: Optional[pd.DataFrame] = None,
        storage_mgr: Optional[StorageManager] = None,
        positive_threshold: Optional[float] = None,
        negative_threshold: Optional[float] = None,
        save_to_storage: bool = True
    ) -> Dict[str, pd.DataFrame]:
        mgr = storage_mgr or storage
        pos_thresh = positive_threshold if positive_threshold is not None else settings.analytics.sentiment_positive_threshold
        neg_thresh = negative_threshold if negative_threshold is not None else settings.analytics.sentiment_negative_threshold

        prods = df_products if df_products is not None else mgr.read_silver_parquet("products")
        revs = df_reviews if df_reviews is not None else mgr.read_silver_parquet("reviews")

        if prods.empty or revs.empty or "review_date" not in revs.columns:
            return {}

        revs_df = revs.copy()
        revs_df["date"] = pd.to_datetime(revs_df["review_date"]).dt.date

        # 1. Product Daily Stats
        prod_daily = revs_df.groupby(["date", "product_id"]).agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            rating_std=("rating", "std"),
            positive_count=("rating", lambda x: (x >= pos_thresh).sum()),
            negative_count=("rating", lambda x: (x <= neg_thresh).sum())
        ).reset_index()

        prod_daily["rating_std"] = prod_daily["rating_std"].fillna(0.0).round(2)
        prod_daily["avg_rating"] = prod_daily["avg_rating"].round(2)
        prod_daily["positive_ratio"] = (prod_daily["positive_count"] / prod_daily["review_count"]).round(4)
        prod_daily["negative_ratio"] = (prod_daily["negative_count"] / prod_daily["review_count"]).round(4)

        # Merge price from products defensively
        avail_prod_cols = [c for c in ["product_id", "price"] if c in prods.columns]
        if "price" in avail_prod_cols:
            merged_p = pd.merge(prod_daily, prods[avail_prod_cols], on="product_id", how="left")
            merged_p["min_price"] = merged_p["price"]
            merged_p["max_price"] = merged_p["price"]
            merged_p["avg_price"] = merged_p["price"]
            merged_p.drop(columns=["price", "positive_count", "negative_count"], inplace=True)
        else:
            merged_p = prod_daily.drop(columns=["positive_count", "negative_count"])

        if save_to_storage:
            mgr.write_gold_parquet("product_daily_stats", merged_p)

        # 2. Brand Daily Stats
        brand_cols = [c for c in ["product_id", "brand", "price"] if c in prods.columns]
        if "brand" in prods.columns:
            rev_with_brand = pd.merge(revs_df, prods[brand_cols], on="product_id", how="inner")
            agg_dict = {
                "product_count": ("product_id", "nunique"),
                "review_count": ("review_id", "count"),
                "avg_rating": ("rating", "mean")
            }
            if "price" in prods.columns:
                agg_dict["avg_price"] = ("price", "mean")
            brand_daily = rev_with_brand.groupby(["date", "brand"]).agg(**agg_dict).reset_index()
            brand_daily["avg_rating"] = brand_daily["avg_rating"].round(2)
            if "avg_price" in brand_daily.columns:
                brand_daily["avg_price"] = brand_daily["avg_price"].round(2)
            if save_to_storage:
                mgr.write_gold_parquet("brand_daily_stats", brand_daily)
        else:
            brand_daily = pd.DataFrame()

        # 3. Category Daily Stats
        if "category" in prods.columns:
            cat_cols = [c for c in ["product_id", "category", "price"] if c in prods.columns]
            rev_with_cat = pd.merge(revs_df, prods[cat_cols], on="product_id", how="inner")
            agg_dict_cat = {
                "product_count": ("product_id", "nunique"),
                "review_count": ("review_id", "count"),
                "avg_rating": ("rating", "mean")
            }
            if "price" in prods.columns:
                agg_dict_cat["avg_price"] = ("price", "mean")
            cat_daily = rev_with_cat.groupby(["date", "category"]).agg(**agg_dict_cat).reset_index()
            cat_daily["avg_rating"] = cat_daily["avg_rating"].round(2)
            if "avg_price" in cat_daily.columns:
                cat_daily["avg_price"] = cat_daily["avg_price"].round(2)
            if save_to_storage:
                mgr.write_gold_parquet("category_daily_stats", cat_daily)
        else:
            cat_daily = pd.DataFrame()

        print("[OK] Gold Data Marts successfully generated.")
        return {
            "product_daily_stats": merged_p,
            "brand_daily_stats": brand_daily,
            "category_daily_stats": cat_daily
        }

