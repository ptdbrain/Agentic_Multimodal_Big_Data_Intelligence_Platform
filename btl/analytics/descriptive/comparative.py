import numpy as np
import pandas as pd


class ComparativeAnalytics:
    """Compare catalog dimensions with explicit rating and price sample sizes."""

    @staticmethod
    def compare_by_dimension(
        df_products: pd.DataFrame,
        df_reviews: pd.DataFrame,
        dimension: str = "brand",
        product_id_col: str = "product_id",
        price_col: str = "price",
        rating_col: str = "rating",
        review_id_col: str = "review_id",
    ) -> pd.DataFrame:
        if df_products is None or df_products.empty or dimension not in df_products.columns:
            return pd.DataFrame()
        products = df_products.copy()
        result = products.groupby(dimension)[product_id_col].nunique().rename("product_count").reset_index()
        # Catalog price is a per-product statistic, independent of review volume.
        if price_col in products:
            products[price_col] = pd.to_numeric(products[price_col], errors="coerce")
            products[price_col] = products[price_col].where(
                products[price_col].gt(0) & np.isfinite(products[price_col])
            )
            prices = products.groupby(dimension).agg(
                avg_price=(price_col, "mean"), min_price=(price_col, "min"),
                max_price=(price_col, "max"), price_sample_size=(price_col, "count"),
            ).reset_index()
            result = result.merge(prices, on=dimension, how="left")
        has_reviews = (df_reviews is not None and not df_reviews.empty
                       and product_id_col in df_reviews and rating_col in df_reviews)
        if has_reviews:
            reviews = df_reviews.copy()
            reviews[rating_col] = pd.to_numeric(reviews[rating_col], errors="coerce")
            reviews = reviews[reviews[rating_col].between(1., 5.)].copy()
            if review_id_col in reviews:
                reviews = reviews[reviews[review_id_col].notna() & reviews[review_id_col].astype(str).str.strip().ne('')]
            joined = reviews[[product_id_col, rating_col]].merge(
                products[[product_id_col, dimension]], on=product_id_col, how="inner",
                validate="many_to_one",
            )
            ratings = joined.groupby(dimension).agg(
                rating_sample_size=(rating_col, "count"), avg_rating=(rating_col, "mean")
            ).reset_index()
            result = result.merge(ratings, on=dimension, how="left")
        else:
            result["rating_sample_size"] = 0
            result["avg_rating"] = float("nan")
        result["rating_sample_size"] = result["rating_sample_size"].fillna(0).astype(int)
        result["review_count"] = result["rating_sample_size"]
        result["avg_rating"] = result["avg_rating"].round(2)
        result["weighted_avg_rating"] = result["avg_rating"]
        result["rating_availability_status"] = np.where(result["rating_sample_size"].gt(0), "available", "unavailable")
        for col in ("avg_price", "min_price", "max_price"):
            if col in result:
                result[col] = result[col].round(2)
        return result.sort_values("review_count", ascending=False)

    @classmethod
    def brand_comparison(cls, df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> pd.DataFrame:
        return cls.compare_by_dimension(df_products, df_reviews, dimension="brand")

    @classmethod
    def category_comparison(cls, df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> pd.DataFrame:
        return cls.compare_by_dimension(df_products, df_reviews, dimension="category")
