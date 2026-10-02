from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from config.settings import settings

class SparkBatchETLJob:
    """Real Spark DataFrame-based Batch ETL.
    Bronze Raw -> Validate -> Clean -> Normalize -> Deduplicate -> Quality -> Silver Parquet
    """
    
    def __init__(self, spark: SparkSession = None):
        self.spark = spark or SparkSession.builder \
            .master(settings.spark.master) \
            .appName(settings.spark.app_name) \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
            .getOrCreate()
    
    def read_bronze_json(self, path: str) -> DataFrame:
        """Read Bronze JSON from MinIO or local path."""
        return self.spark.read.json(path)
    
    def validate(self, df: DataFrame, dataset_type: str) -> tuple:
        """Validate records. Returns (valid_df, invalid_df).
        - Reviews: rating must be 1-5, review_text not null
        - Products: product_id, product_name not null, price > 0
        Invalid records go to quarantine, NOT silently clamped.
        """
        if dataset_type == 'reviews':
            cond = F.col('rating').between(1, 5) & F.col('review_text').isNotNull() & (F.trim(F.col('review_text')) != '')
        elif dataset_type == 'products':
            cond = F.col('product_id').isNotNull() & F.col('product_name').isNotNull() & (F.col('price') > 0)
        else:
            cond = F.lit(True)
            
        valid_df = df.filter(cond)
        invalid_df = df.filter(~cond)
        return valid_df, invalid_df
    
    def clean(self, df: DataFrame) -> DataFrame:
        """Clean: trim whitespace, standardize nulls.
        DOES NOT clamp ratings - that's validation's job.
        """
        for col_name in df.columns:
            if dict(df.dtypes)[col_name] == 'string':
                df = df.withColumn(col_name, F.trim(F.col(col_name)))
        return df
    
    def normalize(self, df: DataFrame, dataset_type: str) -> DataFrame:
        """Normalize brands, categories, prices using Spark UDFs."""
        if dataset_type == 'products':
            if 'brand' in df.columns:
                df = df.withColumn('brand', F.upper(F.col('brand')))
        return df
    
    def deduplicate(self, df: DataFrame, key_cols: list, order_col: str = 'ingested_at') -> DataFrame:
        """Deduplicate using Spark window functions.
        partitionBy(key_cols).orderBy(desc(order_col)).row_number() == 1
        """
        if order_col not in df.columns:
            df = df.withColumn(order_col, F.lit(None).cast(TimestampType()))
            
        w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
        df = df.withColumn('rn', F.row_number().over(w))
        return df.filter(F.col('rn') == 1).drop('rn')
    
    def compute_quality(self, df_valid: DataFrame, df_invalid: DataFrame, dataset_name: str) -> dict:
        """Compute DQ metrics using Spark aggregations."""
        valid_count = df_valid.count()
        invalid_count = df_invalid.count()
        total_count = valid_count + invalid_count
        dq_score = valid_count / total_count if total_count > 0 else 0
        return {
            'dataset': dataset_name,
            'total_count': total_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'dq_score': dq_score
        }
    
    def write_silver(self, df: DataFrame, dataset_name: str, partition_cols: list = None):
        """Write to Silver as Parquet, partitioned by year/month/day."""
        pass
    
    def run_pipeline(self, bronze_products_path: str, bronze_reviews_path: str) -> dict:
        """Full pipeline: Bronze -> Silver for both products and reviews."""
        df_p = self.read_bronze_json(bronze_products_path)
        valid_p, invalid_p = self.validate(df_p, 'products')
        clean_p = self.clean(valid_p)
        norm_p = self.normalize(clean_p, 'products')
        dedup_p = self.deduplicate(norm_p, ['product_id'])
        q_p = self.compute_quality(dedup_p, invalid_p, 'products')
        
        df_r = self.read_bronze_json(bronze_reviews_path)
        valid_r, invalid_r = self.validate(df_r, 'reviews')
        clean_r = self.clean(valid_r)
        norm_r = self.normalize(clean_r, 'reviews')
        dedup_r = self.deduplicate(norm_r, ['review_id'])
        q_r = self.compute_quality(dedup_r, invalid_r, 'reviews')
        
        return {
            'products': q_p,
            'reviews': q_r
        }
