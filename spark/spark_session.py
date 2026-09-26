import os
from pathlib import Path
from config.settings import settings

class SparkSessionFactory:
    """Initializes PySpark Session with S3A / MinIO connector and local fallback."""
    
    @staticmethod
    def get_spark_session(app_name: str = "SentinelAI-Spark"):
        try:
            from pyspark.sql import SparkSession
            builder = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.driver.memory", "2g")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{settings.storage.endpoint}")
                .config("spark.hadoop.fs.s3a.access.key", settings.storage.access_key)
                .config("spark.hadoop.fs.s3a.secret.key", settings.storage.secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            )
            return builder.getOrCreate()
        except Exception:
            return None

    @staticmethod
    def is_spark_available() -> bool:
        try:
            import pyspark
            return True
        except ImportError:
            return False
