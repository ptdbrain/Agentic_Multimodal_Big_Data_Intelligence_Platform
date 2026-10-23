import pandas as pd
from typing import Optional, List

class ComparativeAnalytics:
    """Comparative analytics for Brands, Categories, and arbitrary dimensions (Section 29 & 30).
    Generic dimension comparator that defensively handles missing columns and empty datasets.
    """

    @staticmethod
    def compare_by_dimension(
        df_products: pd.DataFrame,
        df_reviews: pd.DataFrame,
        dimension: str = "brand",
        product_id_col: str = "product_id",
        price_col: str = "price",
        rating_col: str = "rating",
        review_id_col: str = "review_id"
    ) -> pd.DataFrame:
        if df_products is None or df_products.empty or dimension not in df_products.columns:
            return pd.DataFrame()

        # Step 1: Count products per dimension
        prod_counts = (
            df_products.groupby(dimension)[product_id_col]
            .nunique()
            .reset_index()
            .rename(columns={product_id_col: "product_count"})
        )

        # Step 2: Handle empty reviews
        has_revs = (
            df_reviews is not None
            and not df_reviews.empty
            and product_id_col in df_reviews.columns
            and rating_col in df_reviews.columns
        )
        if not has_revs:
            prod_counts["review_count"] = 0
            prod_counts["avg_rating"] = 0.0
            if price_col in df_products.columns:
                p_stats = df_products.groupby(dimension)[price_col].mean().reset_index().rename(columns={price_col: "avg_price"})
                prod_counts = pd.merge(prod_counts, p_stats, on=dimension, how="left")
            return prod_counts.sort_values(by="product_count", ascending=False)

        # Step 3: Merge reviews and products for dimension aggregation
        prod_merge_cols = [c for c in [product_id_col, dimension, price_col] if c in df_products.columns]
        merged = pd.merge(df_reviews, df_products[prod_merge_cols], on=product_id_col, how="inner")
        if merged.empty:
            prod_counts["review_count"] = 0
            prod_counts["avg_rating"] = 0.0
            return prod_counts

        count_col = review_id_col if review_id_col in merged.columns else rating_col
        agg_rules = {
            "review_count": (count_col, "count"),
            "avg_rating": (rating_col, "mean")
        }
        if price_col in merged.columns:
            agg_rules["avg_price"] = (price_col, "mean")
            agg_rules["min_price"] = (price_col, "min")
            agg_rules["max_price"] = (price_col, "max")

        dim_stats = merged.groupby(dimension).agg(**agg_rules).reset_index()

        res = pd.merge(prod_counts, dim_stats, on=dimension, how="left")
        res["avg_rating"] = res["avg_rating"].fillna(0.0).round(2)
        res["review_count"] = res["review_count"].fillna(0).astype(int)
        if "avg_price" in res.columns:
            res["avg_price"] = res["avg_price"].fillna(0.0).round(2)

        return res.sort_values(by="review_count", ascending=False)

    @classmethod
    def brand_comparison(cls, df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> pd.DataFrame:
        return cls.compare_by_dimension(df_products, df_reviews, dimension="brand")

    @classmethod
    def category_comparison(cls, df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> pd.DataFrame:
        return cls.compare_by_dimension(df_products, df_reviews, dimension="category")

