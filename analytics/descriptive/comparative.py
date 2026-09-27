import pandas as pd

class ComparativeAnalytics:
    """Comparative analytics for Brands and Categories (Section 29 & 30 in plan.md)."""

    @staticmethod
    def brand_comparison(df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> pd.DataFrame:
        if df_products.empty or df_reviews.empty:
            return pd.DataFrame()

        merged = pd.merge(df_reviews, df_products[["product_id", "brand", "price"]], on="product_id", how="inner")
        
        brand_stats = merged.groupby("brand").agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            avg_price=("price", "mean"),
            min_price=("price", "min"),
            max_price=("price", "max")
        ).reset_index()

        # Count products per brand
        prod_counts = df_products.groupby("brand")["product_id"].nunique().reset_index().rename(columns={"product_id": "product_count"})
        res = pd.merge(prod_counts, brand_stats, on="brand", how="left")
        res["avg_rating"] = res["avg_rating"].fillna(0.0).round(2)
        res["avg_price"] = res["avg_price"].fillna(0.0).round(2)
        res["review_count"] = res["review_count"].fillna(0).astype(int)
        return res.sort_values(by="review_count", ascending=False)

    @staticmethod
    def category_comparison(df_products: pd.DataFrame, df_reviews: pd.DataFrame) -> pd.DataFrame:
        if df_products.empty or df_reviews.empty:
            return pd.DataFrame()

        merged = pd.merge(df_reviews, df_products[["product_id", "category", "price"]], on="product_id", how="inner")
        cat_stats = merged.groupby("category").agg(
            review_count=("review_id", "count"),
            avg_rating=("rating", "mean"),
            avg_price=("price", "mean")
        ).reset_index()

        prod_counts = df_products.groupby("category")["product_id"].nunique().reset_index().rename(columns={"product_id": "product_count"})
        res = pd.merge(prod_counts, cat_stats, on="category", how="left")
        res["avg_rating"] = res["avg_rating"].fillna(0.0).round(2)
        res["avg_price"] = res["avg_price"].fillna(0.0).round(2)
        res["review_count"] = res["review_count"].fillna(0).astype(int)
        return res.sort_values(by="review_count", ascending=False)
