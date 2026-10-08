"""Spark-Native Analytical Marts Builder.
100% Spark DataFrame distributed aggregations for 4 primary Gold data marts:
  1. product_daily_stats (Grain: date, product_id; PK: date, product_id)
  2. brand_daily_stats   (Grain: date, brand; PK: date, brand)
  3. category_daily_stats(Grain: date, category; PK: date, category)
  4. price_daily_stats   (Grain: date, product_id; PK: date, product_id)

Unified with schema.sql specifications and persisted to MinIO S3A / local Gold layer.
"""
import logging
from typing import Optional, Dict, Any, Union
import pandas as pd

from config.settings import settings
from storage.storage_manager import storage
from spark.spark_session import SparkSessionFactory

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StringType, IntegerType, DoubleType, DecimalType, DateType, StructType
    )
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = None
    DataFrame = None

logger = logging.getLogger(__name__)


class SparkGoldBuilder:
    """Production Spark Analytical Marts Builder.
    Reads from Silver Parquet layer and aggregates into 4 primary Gold data marts
    using purely distributed PySpark DataFrame operations (zero toPandas in aggregation).
    """

    def __init__(self, spark: Optional[Any] = None):
        self._spark = spark
        if self._spark is None and HAS_PYSPARK:
            self._spark = SparkSessionFactory.get_spark_session(
                app_name=f"{settings.spark.app_name}-Gold"
            )

    @property
    def spark(self) -> Optional[Any]:
        return self._spark

    @staticmethod
    def _sanitize_pandas_for_spark(pdf: pd.DataFrame) -> pd.DataFrame:
        import json
        out = pdf.copy()
        for col in out.columns:
            if out[col].isnull().all():
                out[col] = out[col].fillna("").astype(str)
            elif out[col].dtype == object:
                if out[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    out[col] = out[col].apply(
                        lambda x: json.dumps(x, ensure_ascii=False)
                        if isinstance(x, (dict, list))
                        else ("" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
                    )
        return out

    def _ensure_spark_df(self, data: Any, dataset_name: str) -> Optional[Any]:
        """Converts diverse inputs (DataFrame, pandas, path, None) into a PySpark DataFrame."""
        if not HAS_PYSPARK or self._spark is None:
            raise RuntimeError("PySpark is required to run SparkGoldBuilder.")

        if isinstance(data, DataFrame):
            return data

        if isinstance(data, pd.DataFrame):
            if data.empty:
                return None
            pdf = self._sanitize_pandas_for_spark(data)
            return self._spark.createDataFrame(pdf)

        if isinstance(data, str) and data:
            return self._spark.read.parquet(data)

        # Attempt to load from storage manager / S3A
        try:
            path = SparkSessionFactory.resolve_storage_path(dataset_name, layer="silver")
            return self._spark.read.parquet(path)
        except Exception:
            # Fall back to storage.read_silver_parquet
            pdf = storage.read_silver_parquet(dataset_name)
            if not pdf.empty:
                pdf = self._sanitize_pandas_for_spark(pdf)
                return self._spark.createDataFrame(pdf)
            return None


    def build_product_daily_stats(
        self,
        silver_reviews: Any,
        silver_products: Optional[Any] = None,
        silver_prices: Optional[Any] = None
    ) -> Optional[Any]:
        """Builds product_daily_stats mart using distributed Spark aggregations.
        Grain: (date, product_id)
        Primary Key: (date, product_id)
        Columns: date, product_id, review_count, avg_rating, min_price, max_price,
                 avg_price, rating_std, positive_ratio, negative_ratio
        """
        df_revs = self._ensure_spark_df(silver_reviews, "reviews")
        if df_revs is None or df_revs.rdd.isEmpty():
            return None

        # 1. Resolve date column from reviews
        rev_cols = df_revs.columns
        if "date" in rev_cols:
            revs = df_revs.withColumn("date", F.to_date(F.col("date")))
        else:
            date_col = next((c for c in ['review_date', 'timestamp', 'created_at', 'ingested_at'] if c in rev_cols), None)
            if date_col:
                revs = df_revs.withColumn("date", F.to_date(F.col(date_col)))
            else:
                revs = df_revs.withColumn("date", F.current_date())

        revs = revs.withColumn("product_id", F.col("product_id").cast(StringType())) \
                   .withColumn("rating", F.col("rating").cast(DoubleType()))

        pos_thresh = float(settings.analytics.sentiment_positive_threshold)
        neg_thresh = float(settings.analytics.sentiment_negative_threshold)

        # 2. Distributed aggregation on reviews
        r_agg = revs.groupBy("date", "product_id").agg(
            F.count("review_id").cast(IntegerType()).alias("review_count"),
            F.round(F.avg("rating"), 2).cast(DecimalType(3, 2)).alias("avg_rating"),
            F.coalesce(F.round(F.stddev("rating"), 2), F.lit(0.0)).cast(DecimalType(4, 2)).alias("rating_std"),
            F.coalesce(
                F.round(F.sum(F.when(F.col("rating") >= pos_thresh, 1).otherwise(0)) / F.count("review_id"), 4),
                F.lit(0.0)
            ).cast(DecimalType(5, 4)).alias("positive_ratio"),
            F.coalesce(
                F.round(F.sum(F.when(F.col("rating") <= neg_thresh, 1).otherwise(0)) / F.count("review_id"), 4),
                F.lit(0.0)
            ).cast(DecimalType(5, 4)).alias("negative_ratio")
        )

        # Backward compatibility aliases
        r_agg = r_agg.withColumn("high_rating_ratio", F.col("positive_ratio")) \
                     .withColumn("low_rating_ratio", F.col("negative_ratio"))

        # 3. Integrate Price Metrics (from silver_prices or catalog silver_products)
        df_prices = None
        if silver_prices is not None:
            try:
                df_prices = self._ensure_spark_df(silver_prices, "prices")
            except Exception:
                df_prices = None

        df_prods = None
        if silver_products is not None:
            try:
                df_prods = self._ensure_spark_df(silver_products, "products")
            except Exception:
                df_prods = None

        if df_prices is not None and not df_prices.rdd.isEmpty():
            p_cols = df_prices.columns
            if "date" in p_cols:
                pr = df_prices.withColumn("date", F.to_date(F.col("date")))
            else:
                p_date = next((c for c in ['timestamp', 'created_at', 'date'] if c in p_cols), None)
                pr = df_prices.withColumn("date", F.to_date(F.col(p_date)) if p_date else F.current_date())

            pr = pr.withColumn("product_id", F.col("product_id").cast(StringType())) \
                   .withColumn("price", F.col("price").cast(DoubleType()))

            p_agg = pr.groupBy("date", "product_id").agg(
                F.round(F.min("price"), 2).cast(DecimalType(15, 2)).alias("p_min_price"),
                F.round(F.max("price"), 2).cast(DecimalType(15, 2)).alias("p_max_price"),
                F.round(F.avg("price"), 2).cast(DecimalType(15, 2)).alias("p_avg_price")
            )
            stats = r_agg.join(p_agg, on=["date", "product_id"], how="left")
        else:
            stats = r_agg.withColumn("p_min_price", F.lit(None).cast(DecimalType(15, 2))) \
                         .withColumn("p_max_price", F.lit(None).cast(DecimalType(15, 2))) \
                         .withColumn("p_avg_price", F.lit(None).cast(DecimalType(15, 2)))

        # Fallback to catalog product prices if prices not recorded on date
        if df_prods is not None and not df_prods.rdd.isEmpty():
            catalog = df_prods.select(
                F.col("product_id").cast(StringType()),
                F.round(F.col("price").cast(DoubleType()), 2).cast(DecimalType(15, 2)).alias("catalog_price")
            ).dropDuplicates(["product_id"])

            stats = stats.join(catalog, on="product_id", how="left")
            stats = stats.withColumn("min_price", F.coalesce(F.col("p_min_price"), F.col("catalog_price"))) \
                         .withColumn("max_price", F.coalesce(F.col("p_max_price"), F.col("catalog_price"))) \
                         .withColumn("avg_price", F.coalesce(F.col("p_avg_price"), F.col("catalog_price"))) \
                         .drop("p_min_price", "p_max_price", "p_avg_price", "catalog_price")
        else:
            stats = stats.withColumn("min_price", F.col("p_min_price")) \
                         .withColumn("max_price", F.col("p_max_price")) \
                         .withColumn("avg_price", F.col("p_avg_price")) \
                         .drop("p_min_price", "p_max_price", "p_avg_price")

        return stats

    def build_brand_daily_stats(
        self,
        silver_reviews: Any,
        silver_products: Any
    ) -> Optional[Any]:
        """Builds brand_daily_stats mart using distributed Spark aggregations.
        Grain: (date, brand)
        Primary Key: (date, brand)
        Columns: date, brand, product_count, review_count, avg_rating, avg_price
        """
        df_revs = self._ensure_spark_df(silver_reviews, "reviews")
        df_prods = self._ensure_spark_df(silver_products, "products")

        if df_revs is None or df_prods is None or df_revs.rdd.isEmpty() or df_prods.rdd.isEmpty():
            return None

        rev_cols = df_revs.columns
        if "date" in rev_cols:
            revs = df_revs.withColumn("date", F.to_date(F.col("date")))
        else:
            date_col = next((c for c in ['review_date', 'timestamp', 'created_at', 'ingested_at'] if c in rev_cols), None)
            revs = df_revs.withColumn("date", F.to_date(F.col(date_col)) if date_col else F.current_date())

        revs = revs.withColumn("product_id", F.col("product_id").cast(StringType())) \
                   .withColumn("rating", F.col("rating").cast(DoubleType()))

        prods = df_prods.select(
            F.col("product_id").cast(StringType()),
            F.col("brand").cast(StringType()),
            F.col("price").cast(DoubleType()).alias("prod_price")
        )

        joined = revs.join(prods, on="product_id", how="inner")

        stats = joined.groupBy("date", "brand").agg(
            F.countDistinct("product_id").cast(IntegerType()).alias("product_count"),
            F.count("review_id").cast(IntegerType()).alias("review_count"),
            F.round(F.avg("rating"), 2).cast(DecimalType(3, 2)).alias("avg_rating"),
            F.round(F.avg("prod_price"), 2).cast(DecimalType(15, 2)).alias("avg_price")
        ).select("date", "brand", "product_count", "review_count", "avg_rating", "avg_price")

        return stats

    def build_category_daily_stats(
        self,
        silver_reviews: Any,
        silver_products: Any
    ) -> Optional[Any]:
        """Builds category_daily_stats mart using distributed Spark aggregations.
        Grain: (date, category)
        Primary Key: (date, category)
        Columns: date, category, product_count, review_count, avg_rating, avg_price
        """
        df_revs = self._ensure_spark_df(silver_reviews, "reviews")
        df_prods = self._ensure_spark_df(silver_products, "products")

        if df_revs is None or df_prods is None or df_revs.rdd.isEmpty() or df_prods.rdd.isEmpty():
            return None

        rev_cols = df_revs.columns
        if "date" in rev_cols:
            revs = df_revs.withColumn("date", F.to_date(F.col("date")))
        else:
            date_col = next((c for c in ['review_date', 'timestamp', 'created_at', 'ingested_at'] if c in rev_cols), None)
            revs = df_revs.withColumn("date", F.to_date(F.col(date_col)) if date_col else F.current_date())

        revs = revs.withColumn("product_id", F.col("product_id").cast(StringType())) \
                   .withColumn("rating", F.col("rating").cast(DoubleType()))

        prods = df_prods.select(
            F.col("product_id").cast(StringType()),
            F.col("category").cast(StringType()),
            F.col("price").cast(DoubleType()).alias("prod_price")
        )

        joined = revs.join(prods, on="product_id", how="inner")

        stats = joined.groupBy("date", "category").agg(
            F.countDistinct("product_id").cast(IntegerType()).alias("product_count"),
            F.count("review_id").cast(IntegerType()).alias("review_count"),
            F.round(F.avg("rating"), 2).cast(DecimalType(3, 2)).alias("avg_rating"),
            F.round(F.avg("prod_price"), 2).cast(DecimalType(15, 2)).alias("avg_price")
        ).select("date", "category", "product_count", "review_count", "avg_rating", "avg_price")

        return stats

    def build_price_daily_stats(
        self,
        silver_prices: Any,
        silver_products: Optional[Any] = None
    ) -> Optional[Any]:
        """Builds price_daily_stats mart with price ranges, volatility, and data points.
        Grain: (date, product_id)
        Primary Key: (date, product_id)
        Columns: date, product_id, min_price, max_price, avg_price, price_std, data_points
        """
        df_prices = None
        try:
            df_prices = self._ensure_spark_df(silver_prices, "prices")
        except Exception:
            df_prices = None

        if df_prices is None or df_prices.rdd.isEmpty():
            if silver_products is not None:
                try:
                    df_prods = self._ensure_spark_df(silver_products, "products")
                    if df_prods is not None and not df_prods.rdd.isEmpty():
                        # Create initial price point from catalog
                        return df_prods.select(
                            F.current_date().alias("date"),
                            F.col("product_id").cast(StringType()),
                            F.round(F.col("price").cast(DoubleType()), 2).cast(DecimalType(15, 2)).alias("min_price"),
                            F.round(F.col("price").cast(DoubleType()), 2).cast(DecimalType(15, 2)).alias("max_price"),
                            F.round(F.col("price").cast(DoubleType()), 2).cast(DecimalType(15, 2)).alias("avg_price"),
                            F.lit(0.0).cast(DecimalType(15, 2)).alias("price_std"),
                            F.lit(1).cast(IntegerType()).alias("data_points")
                        )
                except Exception:
                    pass
            return None

        p_cols = df_prices.columns
        if "date" in p_cols:
            pr = df_prices.withColumn("date", F.to_date(F.col("date")))
        else:
            p_date = next((c for c in ['timestamp', 'created_at', 'date'] if c in p_cols), None)
            pr = df_prices.withColumn("date", F.to_date(F.col(p_date)) if p_date else F.current_date())

        pr = pr.withColumn("product_id", F.col("product_id").cast(StringType())) \
               .withColumn("price", F.col("price").cast(DoubleType()))

        stats = pr.groupBy("date", "product_id").agg(
            F.round(F.min("price"), 2).cast(DecimalType(15, 2)).alias("min_price"),
            F.round(F.max("price"), 2).cast(DecimalType(15, 2)).alias("max_price"),
            F.round(F.avg("price"), 2).cast(DecimalType(15, 2)).alias("avg_price"),
            F.coalesce(F.round(F.stddev("price"), 2), F.lit(0.0)).cast(DecimalType(15, 2)).alias("price_std"),
            F.count("price").cast(IntegerType()).alias("data_points")
        ).select("date", "product_id", "min_price", "max_price", "avg_price", "price_std", "data_points")

        return stats

    def build_all(
        self,
        silver_reviews: Optional[Any] = None,
        silver_products: Optional[Any] = None,
        silver_prices: Optional[Any] = None,
        save_gold: bool = True,
        dq_metrics: Optional[Any] = None,
        enforce_dq_gate: bool = False,
        min_dq_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Builds all 4 Gold Marts and writes them to MinIO Gold layer via Spark Parquet writer.
        When enforce_dq_gate=True or min_dq_score is provided, validates DQ metrics first and
        raises DataQualityGateError to block Gold mart construction if thresholds are violated.
        """
        if enforce_dq_gate or min_dq_score is not None or dq_metrics is not None:
            from spark.quality.spark_dq import SparkDataQualityEvaluator
            effective_dq = dq_metrics
            if effective_dq is None:
                try:
                    dq_df = storage.read_gold_parquet("data_quality_metrics")
                    if not dq_df.empty:
                        effective_dq = dq_df.to_dict(orient="records")
                except Exception:
                    effective_dq = []
            if effective_dq:
                SparkDataQualityEvaluator.check_dq_gate(
                    effective_dq,
                    min_dq_score=min_dq_score,
                    raise_on_failure=True,
                )

        print(">>> Aggregating Gold Analytical Marts via SparkGoldBuilder (Distributed Spark)")
        marts: Dict[str, Any] = {}

        # 1. Product Daily Stats
        df_p = self.build_product_daily_stats(silver_reviews, silver_products, silver_prices)
        if df_p is not None:
            marts['product_daily_stats'] = df_p

        # 2. Brand Daily Stats
        df_b = self.build_brand_daily_stats(silver_reviews, silver_products)
        if df_b is not None:
            marts['brand_daily_stats'] = df_b

        # 3. Category Daily Stats
        df_c = self.build_category_daily_stats(silver_reviews, silver_products)
        if df_c is not None:
            marts['category_daily_stats'] = df_c

        # 4. Price Daily Stats
        df_pr = self.build_price_daily_stats(silver_prices, silver_products)
        if df_pr is not None:
            marts['price_daily_stats'] = df_pr

        # Persist to Gold layer using Spark Distributed Parquet Writer / StorageManager
        if save_gold:
            for mart_name, df_mart in marts.items():
                if df_mart is not None:
                    gold_dest = SparkSessionFactory.resolve_storage_path(mart_name, layer="gold")
                    try:
                        df_mart.write.mode("overwrite").parquet(gold_dest)
                        print(f"[Spark Gold] Distributed write completed for '{mart_name}' to {gold_dest}")
                    except Exception as e:
                        logger.warning(f"Direct S3A write to {gold_dest} fallback to StorageManager: {e}")
                        # If S3A direct write had an issue, sync via StorageManager
                        storage.write_gold_parquet(mart_name, df_mart)

            print(f"[Spark Gold] Successfully built and persisted {len(marts)} Gold Marts to Gold Layer.")

        return marts

