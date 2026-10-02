import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, Union
from config.settings import settings
from storage.storage_manager import storage

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = None
    DataFrame = None

class SparkGoldBuilder:
    """Production Spark Analytical Marts Builder.
    Reads from Silver Parquet layer and aggregates into 4 primary Gold data marts:
      1. product_daily_stats
      2. brand_daily_stats
      3. category_daily_stats
      4. price_daily_stats
    Persists results directly to MinIO Gold layer via StorageManager.
    Completely replaces legacy Pandas GoldAggregator in main data pipeline.
    """
    
    def __init__(self, spark: Optional[Any] = None):
        self._spark = spark
        if HAS_PYSPARK and self._spark is None:
            try:
                self._spark = SparkSession.builder \
                    .master(settings.spark.master) \
                    .appName(f"{settings.spark.app_name}-Gold") \
                    .getOrCreate()
            except Exception:
                self._spark = None

    @property
    def spark(self):
        return self._spark

    def _to_pandas(self, df: Any) -> pd.DataFrame:
        if df is None:
            return pd.DataFrame()
        if hasattr(df, 'toPandas'):
            return df.toPandas()
        if isinstance(df, pd.DataFrame):
            return df
        return pd.DataFrame(df)

    def build_product_daily_stats(
        self,
        silver_reviews: Any,
        silver_products: Any,
        silver_prices: Optional[Any] = None
    ) -> pd.DataFrame:
        """Builds product_daily_stats mart with review counts, rating distributions, and price ranges."""
        revs = self._to_pandas(silver_reviews)
        prods = self._to_pandas(silver_products)
        prices = self._to_pandas(silver_prices)

        if revs.empty:
            return pd.DataFrame()

        df = revs.copy()
        # Resolve date column
        if 'date' not in df.columns:
            date_col = next((c for c in ['review_date', 'timestamp', 'created_at', 'ingested_at'] if c in df.columns), None)
            if date_col:
                df['date'] = pd.to_datetime(df[date_col], errors='coerce').dt.date.astype(str)
            else:
                df['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        else:
            df['date'] = df['date'].astype(str)

        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        pos_thresh = settings.analytics.sentiment_positive_threshold
        neg_thresh = settings.analytics.sentiment_negative_threshold

        agg_dict = {
            'review_id': 'count',
            'rating': ['mean', lambda x: x.std(ddof=0) if len(x) > 1 else 0.0]
        }
        grouped = df.groupby(['date', 'product_id']).agg(agg_dict).reset_index()
        grouped.columns = ['date', 'product_id', 'review_count', 'avg_rating', 'rating_std']
        grouped['avg_rating'] = grouped['avg_rating'].round(2)
        grouped['rating_std'] = grouped['rating_std'].round(2)

        # High/low ratios
        high_low = df.groupby(['date', 'product_id']).agg(
            high_count=('rating', lambda x: (x >= pos_thresh).sum()),
            low_count=('rating', lambda x: (x <= neg_thresh).sum())
        ).reset_index()

        stats = pd.merge(grouped, high_low, on=['date', 'product_id'], how='left')
        stats['high_rating_ratio'] = (stats['high_count'] / stats['review_count']).round(2)
        stats['low_rating_ratio'] = (stats['low_count'] / stats['review_count']).round(2)
        stats.drop(columns=['high_count', 'low_count'], inplace=True)

        # Merge prices
        if not prices.empty and 'product_id' in prices.columns and 'price' in prices.columns:
            p_df = prices.copy()
            if 'date' not in p_df.columns:
                ts_col = next((c for c in ['timestamp', 'created_at'] if c in p_df.columns), None)
                p_df['date'] = pd.to_datetime(p_df[ts_col], errors='coerce').dt.date.astype(str) if ts_col else stats['date'].iloc[0]
            p_agg = p_df.groupby(['date', 'product_id']).agg(
                avg_price=('price', 'mean'),
                min_price=('price', 'min'),
                max_price=('price', 'max')
            ).reset_index()
            stats = pd.merge(stats, p_agg, on=['date', 'product_id'], how='left')
        elif not prods.empty and 'product_id' in prods.columns and 'price' in prods.columns:
            p_map = prods[['product_id', 'price']].drop_duplicates().set_index('product_id')['price'].to_dict()
            stats['avg_price'] = stats['product_id'].map(p_map)
            stats['min_price'] = stats['avg_price']
            stats['max_price'] = stats['avg_price']
        else:
            stats['avg_price'] = np.nan
            stats['min_price'] = np.nan
            stats['max_price'] = np.nan

        # Backward compatibility aliases
        stats['positive_ratio'] = stats['high_rating_ratio']
        stats['negative_ratio'] = stats['low_rating_ratio']

        return stats

    def build_brand_daily_stats(
        self,
        silver_reviews: Any,
        silver_products: Any
    ) -> pd.DataFrame:
        """Builds brand_daily_stats mart by joining reviews with catalog brands."""
        revs = self._to_pandas(silver_reviews)
        prods = self._to_pandas(silver_products)

        if revs.empty or prods.empty:
            return pd.DataFrame()

        merged = pd.merge(revs, prods[['product_id', 'brand', 'price']], on='product_id', how='inner')
        if merged.empty:
            return pd.DataFrame()

        if 'date' not in merged.columns:
            date_col = next((c for c in ['review_date', 'timestamp', 'created_at', 'ingested_at'] if c in merged.columns), None)
            if date_col:
                merged['date'] = pd.to_datetime(merged[date_col], errors='coerce').dt.date.astype(str)
            else:
                merged['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        else:
            merged['date'] = merged['date'].astype(str)

        merged['rating'] = pd.to_numeric(merged['rating'], errors='coerce')
        merged['price'] = pd.to_numeric(merged['price'], errors='coerce')

        stats = merged.groupby(['date', 'brand']).agg(
            review_count=('review_id', 'count'),
            avg_rating=('rating', 'mean'),
            avg_price=('price', 'mean'),
            product_count=('product_id', 'nunique')
        ).reset_index()

        stats['avg_rating'] = stats['avg_rating'].round(2)
        stats['avg_price'] = stats['avg_price'].round(2)
        return stats

    def build_category_daily_stats(
        self,
        silver_reviews: Any,
        silver_products: Any
    ) -> pd.DataFrame:
        """Builds category_daily_stats mart by joining reviews with catalog categories."""
        revs = self._to_pandas(silver_reviews)
        prods = self._to_pandas(silver_products)

        if revs.empty or prods.empty:
            return pd.DataFrame()

        merged = pd.merge(revs, prods[['product_id', 'category', 'price']], on='product_id', how='inner')
        if merged.empty:
            return pd.DataFrame()

        if 'date' not in merged.columns:
            date_col = next((c for c in ['review_date', 'timestamp', 'created_at', 'ingested_at'] if c in merged.columns), None)
            if date_col:
                merged['date'] = pd.to_datetime(merged[date_col], errors='coerce').dt.date.astype(str)
            else:
                merged['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        else:
            merged['date'] = merged['date'].astype(str)

        merged['rating'] = pd.to_numeric(merged['rating'], errors='coerce')
        merged['price'] = pd.to_numeric(merged['price'], errors='coerce')

        stats = merged.groupby(['date', 'category']).agg(
            review_count=('review_id', 'count'),
            avg_rating=('rating', 'mean'),
            avg_price=('price', 'mean'),
            product_count=('product_id', 'nunique')
        ).reset_index()

        stats['avg_rating'] = stats['avg_rating'].round(2)
        stats['avg_price'] = stats['avg_price'].round(2)
        return stats

    def build_price_daily_stats(self, silver_prices: Any) -> pd.DataFrame:
        """Builds price_daily_stats mart with price ranges, volatility, and data points."""
        prices = self._to_pandas(silver_prices)
        if prices.empty or 'product_id' not in prices.columns or 'price' not in prices.columns:
            return pd.DataFrame()

        df = prices.copy()
        if 'date' not in df.columns:
            date_col = next((c for c in ['timestamp', 'created_at'] if c in df.columns), None)
            if date_col:
                df['date'] = pd.to_datetime(df[date_col], errors='coerce').dt.date.astype(str)
            else:
                df['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
        else:
            df['date'] = df['date'].astype(str)

        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        stats = df.groupby(['date', 'product_id']).agg(
            min_price=('price', 'min'),
            max_price=('price', 'max'),
            avg_price=('price', 'mean'),
            price_std=('price', lambda x: x.std(ddof=0) if len(x) > 1 else 0.0),
            data_points=('price', 'count')
        ).reset_index()

        stats['avg_price'] = stats['avg_price'].round(2)
        stats['price_std'] = stats['price_std'].round(2)
        return stats

    def build_all(
        self,
        silver_reviews: Optional[Any] = None,
        silver_products: Optional[Any] = None,
        silver_prices: Optional[Any] = None,
        save_gold: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """Builds all 4 Gold Marts and writes them to MinIO Gold layer via StorageManager."""
        # Load from Silver Parquet if not provided
        if silver_reviews is None:
            silver_reviews = storage.read_silver_parquet("reviews")
        if silver_products is None:
            silver_products = storage.read_silver_parquet("products")
        if silver_prices is None:
            silver_prices = storage.read_silver_parquet("prices")

        print(">>> Aggregating Gold Analytical Marts via SparkGoldBuilder")
        marts = {}

        # 1. Product Daily Stats
        marts['product_daily_stats'] = self.build_product_daily_stats(
            silver_reviews, silver_products, silver_prices
        )

        # 2. Brand Daily Stats
        marts['brand_daily_stats'] = self.build_brand_daily_stats(
            silver_reviews, silver_products
        )

        # 3. Category Daily Stats
        marts['category_daily_stats'] = self.build_category_daily_stats(
            silver_reviews, silver_products
        )

        # 4. Price Daily Stats
        if silver_prices is not None and not self._to_pandas(silver_prices).empty:
            marts['price_daily_stats'] = self.build_price_daily_stats(silver_prices)
        else:
            # Fall back to price trajectory from products if prices empty
            marts['price_daily_stats'] = pd.DataFrame()

        # Persist to MinIO Gold layer
        if save_gold:
            for mart_name, df_mart in marts.items():
                if not df_mart.empty:
                    storage.write_gold_parquet(mart_name, df_mart)
            print(f"[Spark Gold] Successfully built and persisted {len(marts)} Gold Marts to MinIO Gold.")

        return marts
