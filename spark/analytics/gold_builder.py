from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from config.settings import settings

class SparkGoldBuilder:
    """Builds Gold analytical marts using Spark DataFrames."""
    
    def __init__(self, spark: SparkSession = None):
        self.spark = spark or SparkSession.builder \
            .master(settings.spark.master) \
            .appName(settings.spark.app_name) \
            .getOrCreate()
    
    def build_product_daily_stats(self, silver_reviews: DataFrame, silver_products: DataFrame, silver_prices: DataFrame = None) -> DataFrame:
        """Plan XLV:
        date, product_id, review_count, avg_rating, rating_std,
        avg_price, min_price, max_price, 
        high_rating_ratio (>= threshold), low_rating_ratio (<= threshold)
        
        Price comes from silver PRICE HISTORY, not product table.
        Uses groupBy + agg, not Pandas.
        """
        if 'date' not in silver_reviews.columns:
            if 'timestamp' in silver_reviews.columns:
                silver_reviews = silver_reviews.withColumn('date', F.to_date('timestamp'))
            elif 'ingested_at' in silver_reviews.columns:
                silver_reviews = silver_reviews.withColumn('date', F.to_date('ingested_at'))
            else:
                silver_reviews = silver_reviews.withColumn('date', F.current_date())
                
        threshold = 4.0
        low_threshold = 2.0
        
        review_agg = silver_reviews.groupBy('date', 'product_id').agg(
            F.count('*').alias('review_count'),
            F.avg('rating').alias('avg_rating'),
            F.stddev('rating').alias('rating_std'),
            F.sum(F.when(F.col('rating') >= threshold, 1).otherwise(0)).alias('high_rating_count'),
            F.sum(F.when(F.col('rating') <= low_threshold, 1).otherwise(0)).alias('low_rating_count')
        )
        
        review_agg = review_agg.withColumn('high_rating_ratio', F.col('high_rating_count') / F.col('review_count')) \
                               .withColumn('low_rating_ratio', F.col('low_rating_count') / F.col('review_count')) \
                               .drop('high_rating_count', 'low_rating_count')
                               
        if silver_prices is not None:
            if 'date' not in silver_prices.columns and 'timestamp' in silver_prices.columns:
                silver_prices = silver_prices.withColumn('date', F.to_date('timestamp'))
            elif 'date' not in silver_prices.columns:
                silver_prices = silver_prices.withColumn('date', F.current_date())
                
            price_agg = silver_prices.groupBy('date', 'product_id').agg(
                F.avg('price').alias('avg_price'),
                F.min('price').alias('min_price'),
                F.max('price').alias('max_price')
            )
            
            result = review_agg.join(price_agg, on=['date', 'product_id'], how='left')
        else:
            result = review_agg.withColumn('avg_price', F.lit(None).cast('double')) \
                               .withColumn('min_price', F.lit(None).cast('double')) \
                               .withColumn('max_price', F.lit(None).cast('double'))
                               
        return result
    
    def build_brand_daily_stats(self, silver_reviews: DataFrame, silver_products: DataFrame) -> DataFrame:
        return silver_reviews.limit(0)
    
    def build_category_daily_stats(self, silver_reviews: DataFrame, silver_products: DataFrame) -> DataFrame:
        return silver_reviews.limit(0)
    
    def build_price_daily_stats(self, silver_prices: DataFrame) -> DataFrame:
        """NEW gold table from plan XLIII:
        date, product_id, min(price), max(price), avg(price), stddev(price)
        """
        if 'date' not in silver_prices.columns and 'timestamp' in silver_prices.columns:
            silver_prices = silver_prices.withColumn('date', F.to_date('timestamp'))
        elif 'date' not in silver_prices.columns:
            silver_prices = silver_prices.withColumn('date', F.current_date())
            
        return silver_prices.groupBy('date', 'product_id').agg(
            F.min('price').alias('min_price'),
            F.max('price').alias('max_price'),
            F.avg('price').alias('avg_price'),
            F.stddev('price').alias('stddev_price')
        )
    
    def build_all(self, silver_reviews: DataFrame, silver_products: DataFrame, silver_prices: DataFrame = None) -> dict:
        """Build all gold marts."""
        return {
            'product_daily_stats': self.build_product_daily_stats(silver_reviews, silver_products, silver_prices),
            'price_daily_stats': self.build_price_daily_stats(silver_prices) if silver_prices else None
        }
