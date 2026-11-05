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
    from pyspark.sql.window import Window
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

    def __init__(self, spark: Optional[Any] = None, allow_local_fallback: Optional[bool] = None):
        if allow_local_fallback is None:
            allow_local_fallback = settings.analytics.allow_local_analytics_fallback
        if allow_local_fallback and not settings.is_local_mode:
            raise ValueError("Analytics fallback is available only in local/test mode")
        self.allow_local_fallback = allow_local_fallback
        self._spark = spark
        if self._spark is None and HAS_PYSPARK:
            self._spark = SparkSessionFactory.get_spark_session(
                app_name=f"{settings.spark.app_name}-Gold"
            )

    @property
    def spark(self) -> Optional[Any]:
        return self._spark

    def _empty_mart(self, name: str) -> Any:
        dimension = {'product_daily_stats':'product_id', 'price_daily_stats':'product_id',
                     'brand_daily_stats':'brand', 'category_daily_stats':'category'}[name]
        fields = f'date date, {dimension} string'
        if name == 'price_daily_stats':
            fields += ', min_price double, max_price double, avg_price double, price_std double, data_points int'
        else:
            fields += ', review_count int, rating_sample_size int, avg_rating double, weighted_avg_rating double'
            fields += ', rating_std double, positive_ratio double, negative_ratio double, avg_price double'
            if name == 'product_daily_stats':
                fields += ', min_price double, max_price double, price_source string'
            else:
                fields += ', product_count int'
        return self.spark.createDataFrame([], fields)

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

        if data is None:
            return None

        if isinstance(data, list) and not data:
            return None

        if isinstance(data, DataFrame):
            return data

        if isinstance(data, pd.DataFrame):
            if data.empty:
                return None
            pdf = self._sanitize_pandas_for_spark(data)
            return self._spark.createDataFrame(pdf)

        if isinstance(data, str) and data:
            return self._spark.read.parquet(data)

        # Attempt to load from storage manager / S3A. In integration/production,
        # Spark and storage errors must remain visible; a pandas fallback is only
        # valid for explicitly local/test execution.
        path = SparkSessionFactory.resolve_storage_path(dataset_name, layer="silver")
        try:
            return self._spark.read.parquet(path)
        except Exception as exc:
            error_class = exc.getErrorClass() if hasattr(exc, "getErrorClass") else None
            missing_path = isinstance(exc, FileNotFoundError) or error_class == "PATH_NOT_FOUND"
            if not (settings.is_local_mode and self.allow_local_fallback and missing_path):
                logger.exception("Gold input read failed: dataset=%s path=%s", dataset_name, path)
                raise
            logger.warning("Explicit local Gold fallback: dataset=%s path=%s", dataset_name, path)
            pdf = storage.read_silver_parquet(dataset_name)
            if not pdf.empty:
                pdf = self._sanitize_pandas_for_spark(pdf)
                return self._spark.createDataFrame(pdf)
            return None

    @staticmethod
    def _with_observation_date(df: Any, candidates: tuple[str, ...]) -> Any:
        """Add a nullable observation date without inventing a processing date."""
        # The Spark factory sets spark.sql.session.timeZone=UTC. Parse offsets
        # before extracting dates, and coalesce per record, not per schema.
        cols = set(df.columns)
        names = list(dict.fromkeys(("date",) + candidates))
        dates = [F.to_date(F.try_to_timestamp(F.col(c).cast(StringType())))
                 for c in names if c in cols]
        observed_date = F.coalesce(*dates) if dates else F.lit(None).cast(DateType())
        return df.withColumn("date", observed_date)



    @staticmethod
    def _positive_price(column: Any) -> Any:
        value = column.cast(DoubleType())
        return F.when(value.isNotNull() & ~F.isnan(value) & (value > 0) & (value < float("inf")), value)

    @classmethod
    def _review_observations(cls, df: Any) -> Any:
        dated = cls._with_observation_date(df, ("review_date", "timestamp", "created_at"))
        dated = dated.withColumn("time_available", F.col("date").isNotNull())
        if not settings.analytics.strict_analytics_time and "ingested_at" in dated.columns:
            dated = dated.withColumn("date", F.coalesce(
                F.col("date"), F.to_date(F.try_to_timestamp(F.col("ingested_at").cast(StringType())))
            ))
        return (dated.withColumn("product_id", F.col("product_id").cast(StringType()))
                     .withColumn("rating", F.col("rating").cast(DoubleType()))
                     .filter(F.col("date").isNotNull() & F.col("product_id").isNotNull()
                             & F.col("review_id").isNotNull()
                             & (F.length(F.trim(F.col('review_id').cast(StringType()))) > 0)
                             & F.col("rating").between(1, 5)))

    def _catalog_frame(self, data: Any) -> Any:
        frame = self._ensure_spark_df(data, 'products')
        if frame is None:
            return None
        if 'product_id' not in frame.columns:
            raise ValueError('Catalog requires product_id')
        from analytics.currency import require_single_currency
        require_single_currency(frame)
        frame = frame.withColumn('product_id', F.col('product_id').cast(StringType()))
        invalid = frame.groupBy('product_id').count().filter(
            (F.col('count') > 1) | F.col('product_id').isNull() | (F.length(F.trim('product_id')) == 0)
        )
        if invalid.limit(1).count():
            raise ValueError('Catalog product_id must be present and unique')
        if 'price' not in frame.columns:
            frame = frame.withColumn('price', F.lit(None).cast(DoubleType()))
        return frame

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
        revs = self._review_observations(df_revs)

        pos_thresh = float(settings.analytics.sentiment_positive_threshold)
        neg_thresh = float(settings.analytics.sentiment_negative_threshold)

        # 2. Distributed aggregation on reviews
        r_agg = revs.groupBy("date", "product_id").agg(
            F.min("time_available").alias("time_available"),
            F.count("review_id").cast(IntegerType()).alias("review_count"),
            F.count("rating").cast(IntegerType()).alias("rating_sample_size"),
            F.round(F.avg("rating"), 2).cast(DecimalType(3, 2)).alias("avg_rating"),
            F.round(F.stddev("rating"), 2).cast(DecimalType(4, 2)).alias("rating_std"),
            F.coalesce(
                F.round(F.sum(F.when(F.col("rating") >= pos_thresh, 1).otherwise(0)) / F.count("review_id"), 4),
                F.lit(0.0)
            ).cast(DecimalType(5, 4)).alias("positive_ratio"),
            F.coalesce(
                F.round(F.sum(F.when(F.col("rating") <= neg_thresh, 1).otherwise(0)) / F.count("review_id"), 4),
                F.lit(0.0)
            ).cast(DecimalType(5, 4)).alias("negative_ratio")
        )

        r_agg = r_agg.withColumn("weighted_avg_rating", F.col("avg_rating"))
        # Backward compatibility aliases
        r_agg = r_agg.withColumn("high_rating_ratio", F.col("positive_ratio")) \
                     .withColumn("low_rating_ratio", F.col("negative_ratio"))

        # 3. Integrate Price Metrics (from silver_prices or catalog silver_products)
        df_prices = None
        if silver_prices is not None:
            df_prices = self._ensure_spark_df(silver_prices, "prices")

        df_prods = None
        if silver_products is not None:
            df_prods = self._catalog_frame(silver_products)

        from analytics.currency import require_single_currency
        require_single_currency(df_prods, df_prices)
        if df_prices is not None and not df_prices.rdd.isEmpty():
            pr = self._with_observation_date(
                df_prices, ("timestamp", "created_at")
            ).filter(F.col("date").isNotNull())

            pr = pr.withColumn("product_id", F.col("product_id").cast(StringType())) \
                   .withColumn("price", self._positive_price(F.col("price")))

            pr = pr.filter(F.col("price").isNotNull() & F.col("product_id").isNotNull())
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
                F.round(self._positive_price(F.col("price")), 2).cast(DecimalType(15, 2)).alias("catalog_price")
            )

            stats = stats.join(catalog, on="product_id", how="left")
            stats = stats.withColumn(
                         "price_source",
                         F.when(F.col("p_avg_price").isNotNull(), F.lit("observed"))
                          .when(F.col("catalog_price").isNotNull(), F.lit("catalog_fallback"))
                          .otherwise(F.lit("unavailable"))
                     ).withColumn("min_price", F.coalesce(F.col("p_min_price"), F.col("catalog_price"))) \
                         .withColumn("max_price", F.coalesce(F.col("p_max_price"), F.col("catalog_price"))) \
                         .withColumn("avg_price", F.coalesce(F.col("p_avg_price"), F.col("catalog_price"))) \
                         .drop("p_min_price", "p_max_price", "p_avg_price", "catalog_price")
        else:
            stats = stats.withColumn("price_source", F.when(F.col("p_avg_price").isNotNull(), "observed").otherwise("unavailable")) \
                         .withColumn("min_price", F.col("p_min_price")) \
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
        df_prods = self._catalog_frame(silver_products)

        if df_revs is None or df_prods is None or df_revs.rdd.isEmpty() or df_prods.rdd.isEmpty():
            return None

        revs = self._review_observations(df_revs)

        prods = df_prods.select(
            F.col("product_id").cast(StringType()),
            F.col("brand").cast(StringType()),
            self._positive_price(F.col("price")).alias("prod_price")
        )

        joined = revs.join(prods, on="product_id", how="inner")

        stats = joined.groupBy("date", "brand").agg(
            F.min("time_available").alias("time_available"),
            F.countDistinct("product_id").cast(IntegerType()).alias("product_count"),
            F.count("review_id").cast(IntegerType()).alias("review_count"),
            F.count("rating").cast(IntegerType()).alias("rating_sample_size"),
            F.round(F.avg("rating"), 2).cast(DecimalType(3, 2)).alias("avg_rating")
        ).withColumn("weighted_avg_rating", F.col("avg_rating"))

        catalog_prices = (joined.select("date", "brand", "product_id", "prod_price")
                          .dropDuplicates(["date", "brand", "product_id"])
                          .groupBy("date", "brand").agg(
                              F.round(F.avg("prod_price"), 2).cast(DecimalType(15, 2)).alias("avg_price")))
        stats = stats.join(catalog_prices, on=["date", "brand"], how="left")

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
        df_prods = self._catalog_frame(silver_products)

        if df_revs is None or df_prods is None or df_revs.rdd.isEmpty() or df_prods.rdd.isEmpty():
            return None

        revs = self._review_observations(df_revs)

        prods = df_prods.select(
            F.col("product_id").cast(StringType()),
            F.col("category").cast(StringType()),
            self._positive_price(F.col("price")).alias("prod_price")
        )

        joined = revs.join(prods, on="product_id", how="inner")

        stats = joined.groupBy("date", "category").agg(
            F.min("time_available").alias("time_available"),
            F.countDistinct("product_id").cast(IntegerType()).alias("product_count"),
            F.count("review_id").cast(IntegerType()).alias("review_count"),
            F.count("rating").cast(IntegerType()).alias("rating_sample_size"),
            F.round(F.avg("rating"), 2).cast(DecimalType(3, 2)).alias("avg_rating")
        ).withColumn("weighted_avg_rating", F.col("avg_rating"))

        catalog_prices = (joined.select("date", "category", "product_id", "prod_price")
                          .dropDuplicates(["date", "category", "product_id"])
                          .groupBy("date", "category").agg(
                              F.round(F.avg("prod_price"), 2).cast(DecimalType(15, 2)).alias("avg_price")))
        stats = stats.join(catalog_prices, on=["date", "category"], how="left")

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
        df_prices = self._ensure_spark_df(silver_prices, "prices")
        if df_prices is None or df_prices.rdd.isEmpty():
            return None

        from analytics.currency import require_single_currency
        require_single_currency(df_prices)
        pr = self._with_observation_date(
            df_prices, ("timestamp", "created_at")
        ).filter(F.col("date").isNotNull())

        pr = pr.withColumn("product_id", F.col("product_id").cast(StringType())) \
               .withColumn("price", self._positive_price(F.col("price")))

        pr = pr.filter(F.col("price").isNotNull() & F.col("product_id").isNotNull())
        stats = pr.groupBy("date", "product_id").agg(
            F.round(F.min("price"), 2).cast(DecimalType(15, 2)).alias("min_price"),
            F.round(F.max("price"), 2).cast(DecimalType(15, 2)).alias("max_price"),
            F.round(F.avg("price"), 2).cast(DecimalType(15, 2)).alias("avg_price"),
            F.round(F.stddev("price"), 2).cast(DecimalType(15, 2)).alias("price_std"),
            F.count("price").cast(IntegerType()).alias("data_points")
        ).select("date", "product_id", "min_price", "max_price", "avg_price", "price_std", "data_points")

        return stats

    @staticmethod
    def _add_lineage(df: Any, run_id: str, source_run_id: Optional[str], source_group: str = "unverified",
                     currency: Optional[str] = None) -> Any:
        if "date" in df.columns:
            available = F.col("time_available") if "time_available" in df.columns else F.col("date").isNotNull()
            data_as_of = F.max(F.when(available, F.col("date"))).over(Window.partitionBy())
        else:
            data_as_of = F.lit(None).cast(DateType())
            available = F.lit(False)
        return (df.withColumn("currency", F.lit(currency).cast(StringType()))
                  .withColumn("source_group", F.lit(source_group))
                  .withColumn("run_id", F.lit(run_id))
                  .withColumn("source_run_id", F.lit(source_run_id).cast(StringType()))
                  .withColumn("data_as_of", data_as_of)
                  .withColumn("time_available", available))

    @staticmethod
    def _validate_mart(df: Any, mart_name: str) -> None:
        grains = {
            "product_daily_stats": ("date", "product_id"),
            "brand_daily_stats": ("date", "brand"),
            "category_daily_stats": ("date", "category"),
            "price_daily_stats": ("date", "product_id"),
        }
        grain = list(grains[mart_name])
        missing = set(grain) - set(df.columns)
        if missing:
            raise ValueError(f"{mart_name} missing grain columns: {sorted(missing)}")
        if grain and df.filter(" OR ".join(f"`{c}` IS NULL" for c in grain)).limit(1).count():
            raise ValueError(f"{mart_name} contains null grain key")
        if grain and df.groupBy(*grain).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"{mart_name} contains duplicate grain")
        numeric = [c for c in df.columns if c in {
            "avg_rating", "rating_std", "positive_ratio", "negative_ratio",
            "high_rating_ratio", "low_rating_ratio", "min_price", "max_price",
            "avg_price", "price_std"
        }]
        for col_name in numeric:
            bad = df.filter(
                F.col(col_name).isNotNull()
                & (F.isnan(F.col(col_name)) | (F.abs(F.col(col_name)) == float("inf")))
            ).limit(1).count()
            if bad:
                raise ValueError(f"{mart_name} contains non-finite metric {col_name}")
        for col_name in ("positive_ratio", "negative_ratio", "high_rating_ratio", "low_rating_ratio"):
            if col_name in df.columns and df.filter(
                F.col(col_name).isNotNull() & ((F.col(col_name) < 0) | (F.col(col_name) > 1))
            ).limit(1).count():
                raise ValueError(f"{mart_name} contains out-of-range metric {col_name}")
        if "avg_rating" in df.columns and df.filter(
            F.col("avg_rating").isNotNull() & ((F.col("avg_rating") < 1) | (F.col("avg_rating") > 5))
        ).limit(1).count():
            raise ValueError(f"{mart_name} contains out-of-range metric avg_rating")
        for col_name in ("review_count", "data_points"):
            if col_name in df.columns and df.filter(
                F.col(col_name).isNull() | (F.col(col_name) <= 0)
            ).limit(1).count():
                raise ValueError(f"{mart_name} contains non-positive {col_name}")
        for col_name in ("min_price", "max_price", "avg_price"):
            if col_name in df.columns and df.filter(
                F.col(col_name).isNotNull() & (F.col(col_name) <= 0)
            ).limit(1).count():
                raise ValueError(f"{mart_name} contains non-positive price metric {col_name}")

    def build_all(
        self,
        silver_reviews: Optional[Any] = None,
        silver_products: Optional[Any] = None,
        silver_prices: Optional[Any] = None,
        save_gold: bool = True,
        dq_metrics: Optional[Any] = None,
        enforce_dq_gate: bool = False,
        min_dq_score: Optional[float] = None,
        run_id: Optional[str] = None,
        source_run_id: Optional[str] = None,
        required_datasets: Optional[list] = None,
    ) -> Dict[str, Any]:
        """Builds all 4 Gold Marts and writes them to MinIO Gold layer via Spark Parquet writer.
        When enforce_dq_gate=True or min_dq_score is provided, validates DQ metrics first and
        raises DataQualityGateError to block Gold mart construction if thresholds are violated.
        """
        if not settings.is_local_mode or enforce_dq_gate or min_dq_score is not None or dq_metrics is not None:
            from spark.quality.spark_dq import SparkDataQualityEvaluator, DataQualityGateError
            if not settings.is_local_mode and (not run_id or not str(run_id).strip()):
                raise DataQualityGateError("Current run_id is required for Gold in integration/production")
            effective_dq = dq_metrics
            if effective_dq is None:
                try:
                    dq_df = storage.read_gold_parquet("data_quality_metrics")
                    if not dq_df.empty:
                        effective_dq = dq_df.to_dict(orient="records")
                except Exception:
                    effective_dq = []
            items = [effective_dq] if isinstance(effective_dq, dict) else (effective_dq or [])
            if not run_id and any(m.get("run_id") for m in items if isinstance(m, dict)):
                raise DataQualityGateError(
                    "Gold DQ metrics carry a run_id; current run_id is required to prevent stale metrics"
                )
            SparkDataQualityEvaluator.check_dq_gate(
                effective_dq or [],
                min_dq_score=min_dq_score,
                raise_on_failure=True,
                required_datasets=required_datasets or ["products", "reviews"],
                run_id=run_id,
            )

        import uuid
        effective_run_id = run_id or f"gold-{uuid.uuid4().hex}"
        effective_source_run_id = source_run_id
        print(">>> Aggregating Gold Analytical Marts via SparkGoldBuilder (Distributed Spark)")
        marts: Dict[str, Any] = {}

        # `None` at build_all means "load the canonical Silver input". Individual
        # mart methods use None to mean that an optional input is absent, so resolve
        # required inputs here explicitly.
        can_load_implicit = hasattr(self._spark, "read")
        resolved_reviews = self._ensure_spark_df(object(), "reviews") if silver_reviews is None and can_load_implicit else silver_reviews
        resolved_products = self._ensure_spark_df(object(), "products") if silver_products is None and can_load_implicit else silver_products
        if silver_prices is None and can_load_implicit:
            try:
                resolved_prices = self._ensure_spark_df(object(), "prices")
            except Exception as exc:
                error_class = exc.getErrorClass() if hasattr(exc, "getErrorClass") else None
                if error_class == "PATH_NOT_FOUND" or isinstance(exc, FileNotFoundError):
                    resolved_prices = None
                else:
                    raise
        else:
            resolved_prices = silver_prices

        from ingestion.provenance import classify_source_group
        source_group = classify_source_group(*[
            self._ensure_spark_df(data, name) if isinstance(data, str) else data
            for name, data in [('products', resolved_products), ('reviews', resolved_reviews), ('prices', resolved_prices)]
        ])

        from analytics.currency import require_single_currency
        currency = require_single_currency(*[
            self._ensure_spark_df(data, name) if isinstance(data, str) else data
            for name, data in [('products', resolved_products), ('prices', resolved_prices)]
        ])

        gated = not settings.is_local_mode or enforce_dq_gate or min_dq_score is not None or dq_metrics is not None
        if gated:
            consumed = set(required_datasets or ['products', 'reviews'])
            canonical_sources = {}
            for name, data in [('reviews',resolved_reviews),('products',resolved_products),('prices',resolved_prices)]:
                frame = self._ensure_spark_df(data, name)
                if frame is not None and not frame.rdd.isEmpty():
                    consumed.add(name)
                    supplied = {'reviews':silver_reviews,'products':silver_products,'prices':silver_prices}[name]
                    canonical_path = SparkSessionFactory.resolve_storage_path(name, layer='silver')
                    if supplied is None or (isinstance(supplied,str) and supplied == canonical_path):
                        try:
                            snapshot = storage.read_json(f'silver/_snapshot_{name}.json')
                        except FileNotFoundError:
                            raise DataQualityGateError(f'[{name}] Missing authoritative Silver snapshot provenance')
                        reports = [effective_dq] if isinstance(effective_dq,dict) else effective_dq
                        matching = [m for m in reports if m.get('dataset_name') == name
                                    and m.get('source_run_id',m.get('run_id')) == snapshot['run_id']]
                        if not matching or frame.count() != snapshot['rows']:
                            raise DataQualityGateError(f'[{name}] DQ does not describe the consumed Silver snapshot')
                        canonical_sources[name] = snapshot['run_id']
                elif name in consumed:
                    raise DataQualityGateError(f'[{name}] Required consumed input is empty despite the supplied DQ report')
            SparkDataQualityEvaluator.check_dq_gate(effective_dq or [], min_dq_score=min_dq_score,
                required_datasets=sorted(consumed),run_id=run_id,raise_on_failure=True)
            if canonical_sources:
                identities = set(canonical_sources.values())
                if source_run_id is not None and identities != {source_run_id}:
                    raise DataQualityGateError('Requested source_run_id contradicts Silver snapshot provenance')
                if len(identities) == 1 and len(canonical_sources) == len(consumed):
                    effective_source_run_id = next(iter(identities))

        # 1. Product Daily Stats
        df_p = self.build_product_daily_stats(resolved_reviews, resolved_products, resolved_prices)
        if df_p is not None:
            marts['product_daily_stats'] = self._add_lineage(df_p, effective_run_id, effective_source_run_id, source_group, currency)

        # 2. Brand Daily Stats
        df_b = self.build_brand_daily_stats(resolved_reviews, resolved_products)
        if df_b is not None:
            marts['brand_daily_stats'] = self._add_lineage(df_b, effective_run_id, effective_source_run_id, source_group, currency)

        # 3. Category Daily Stats
        df_c = self.build_category_daily_stats(resolved_reviews, resolved_products)
        if df_c is not None:
            marts['category_daily_stats'] = self._add_lineage(df_c, effective_run_id, effective_source_run_id, source_group, currency)

        # 4. Price Daily Stats
        df_pr = self.build_price_daily_stats(resolved_prices, resolved_products)
        if df_pr is not None:
            marts['price_daily_stats'] = self._add_lineage(df_pr, effective_run_id, effective_source_run_id, source_group, currency)

        # Persist to Gold layer using Spark Distributed Parquet Writer / StorageManager
        if save_gold:
            for name in ('product_daily_stats','brand_daily_stats','category_daily_stats','price_daily_stats'):
                if name not in marts:
                    marts[name] = self._add_lineage(self._empty_mart(name), effective_run_id, effective_source_run_id, source_group, currency)
            # Reject the complete group before publishing any individual mart.
            for mart_name, df_mart in marts.items():
                self._validate_mart(df_mart, mart_name)
            from storage.write_lock import write_lock
            with write_lock(SparkSessionFactory.resolve_storage_path('', layer='gold')):
                snapshot = {'run_id':effective_run_id,'source_run_id':effective_source_run_id,
                            'status':'publishing','source_group':source_group,'rows':{name:frame.count() for name,frame in marts.items()}}
                storage.write_json('gold/_snapshot.json', snapshot)
                for mart_name, df_mart in marts.items():
                    gold_dest = SparkSessionFactory.resolve_storage_path(mart_name, layer="gold")
                    from spark.storage_safe import SafeParquetIO
                    SafeParquetIO.publish_parquet_atomic(
                        self.spark, df_mart, gold_dest, expected_count=df_mart.count()
                    )
                    print(f"[Spark Gold] Validated publication completed for '{mart_name}'")
                snapshot['status'] = 'complete'
                storage.write_json('gold/_snapshot.json', snapshot)

            print(f"[Spark Gold] Successfully built and persisted {len(marts)} Gold Marts to Gold Layer.")

        return marts

