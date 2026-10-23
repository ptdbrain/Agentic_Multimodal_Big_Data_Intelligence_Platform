"""Centralized SparkSession Factory with Unified S3A & MinIO Configuration.
Ensures all batch, streaming, and analytics jobs share identical configuration
with automatic endpoint resolution for host vs Docker network environments.
"""
import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from config.settings import settings

REPO_ROOT = Path(__file__).resolve().parent.parent

# 1. Bootstrap Hadoop and Python environment on Windows
hadoop_dir = REPO_ROOT / "hadoop"
if hadoop_dir.exists() and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = str(hadoop_dir)
    hadoop_bin = str(hadoop_dir / "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class SparkSessionFactory:
    """Production SparkSession provider with unified S3A MinIO connector and local fallback."""

    _instance = None

    @classmethod
    def get_s3a_config_map(cls) -> Dict[str, str]:
        """Returns canonical S3A Hadoop configuration map resolved for current environment (host vs Docker)."""
        s3_endpoint = settings.storage.get_s3_endpoint()
        s3_protocol = "https" if settings.storage.secure else "http"
        endpoint_url = f"{s3_protocol}://{s3_endpoint}"
        return {
            "fs.s3a.endpoint": endpoint_url,
            "fs.s3a.access.key": settings.storage.access_key,
            "fs.s3a.secret.key": settings.storage.secret_key,
            "fs.s3a.path.style.access": "true",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.connection.ssl.enabled": "true" if settings.storage.secure else "false",
            "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "fs.s3a.change.detection.mode": "none",
            "fs.s3a.change.detection.version.required": "false",
            "fs.s3a.fast.upload": "true",
            "fs.s3a.multiobjectdelete.enable": "false",
        }

    @classmethod
    def configure_s3a(cls, spark: Any, extra_configs: Optional[Dict[str, str]] = None) -> None:
        """Applies S3A and streaming settings onto an existing SparkSession's Hadoop & SQL conf."""
        if spark is None:
            return
        try:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            spark.conf.set(
                "spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager",
            )
            hconf = spark.sparkContext._jsc.hadoopConfiguration()
            for k, v in cls.get_s3a_config_map().items():
                hconf.set(k, v)
            if extra_configs:
                for k, v in extra_configs.items():
                    spark.conf.set(k, v)
                    if k.startswith("spark.hadoop."):
                        hconf.set(k[len("spark.hadoop."):], v)
        except Exception:
            pass

    @classmethod
    def get_spark_session(
        cls,
        app_name: Optional[str] = None,
        master: Optional[str] = None,
        extra_configs: Optional[Dict[str, str]] = None
    ):
        """Constructs or retrieves the shared SparkSession configured for MinIO S3A."""
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            return None

        # Reuse active session if available, ensuring S3A configs are synced
        active = SparkSession.getActiveSession()
        if active is not None:
            cls.configure_s3a(active, extra_configs=extra_configs)
            return active

        app = app_name or settings.spark.app_name
        spark_master = master or settings.spark.master
        s3a_map = cls.get_s3a_config_map()

        driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
        builder = (
            SparkSession.builder
            .appName(app)
            .master(spark_master)
            .config("spark.driver.memory", driver_mem)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dcom.amazonaws.services.s3.disablePutObjectMD5Validation=true -Dcom.amazonaws.services.s3.disableGetObjectMD5Validation=true"
            )
            # --- Parquet & Streaming Behavior ---
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .config(
                "spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager",
            )
        )

        for k, v in s3a_map.items():
            builder = builder.config(f"spark.hadoop.{k}", v)

        if extra_configs:
            for k, v in extra_configs.items():
                builder = builder.config(k, v)

        try:
            spark = builder.getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            cls.configure_s3a(spark, extra_configs=extra_configs)
            cls._instance = spark
            return spark
        except Exception as e:
            print(f"[SparkSessionFactory] Failed to create SparkSession: {e}")
            return None

    @classmethod
    def get_s3a_url(cls, path: str, bucket: Optional[str] = None) -> str:
        """Converts an object path or prefix into a canonical s3a:// URI."""
        b = bucket or settings.storage.data_lake_bucket
        clean_path = str(path).replace("\\", "/").lstrip("/")
        return f"s3a://{b}/{clean_path}"

    @classmethod
    def resolve_storage_path(cls, dataset: str, layer: str = "bronze") -> str:
        """Resolves storage path: returns s3a:// URI in integration/minio mode,
        or local filesystem path when in local mode.
        """
        if str(dataset).startswith("s3a://") or str(dataset).startswith("file://"):
            return str(dataset)

        p = Path(str(dataset))
        if p.is_absolute():
            return str(p)

        is_local = settings.runtime_mode in ("local", "test")

        clean_name = str(dataset).replace("\\", "/").strip("/")
        if clean_name.startswith(f"{layer}/"):
            clean_name = clean_name[len(layer) + 1:]

        if is_local:
            local_path = settings.storage.local_data_dir / layer / clean_name
            return str(local_path)
        return cls.get_s3a_url(f"{layer}/{clean_name}")

    @classmethod
    def resolve_checkpoint_path(cls, checkpoint_name: str = "streaming") -> str:
        """Resolves streaming checkpoint directory path for active runtime mode (S3A vs local)."""
        if str(checkpoint_name).startswith("s3a://") or str(checkpoint_name).startswith("file://"):
            return str(checkpoint_name)
        is_local = settings.runtime_mode in ("local", "test")
        p = Path(str(checkpoint_name))
        if p.is_absolute():
            return str(p)
        clean_name = str(checkpoint_name).replace("\\", "/").strip("/")
        if clean_name.startswith("checkpoints/"):
            clean_name = clean_name[len("checkpoints/"):]
        if is_local:
            return str(settings.storage.local_data_dir / "checkpoints" / clean_name)
        return cls.get_s3a_url(f"checkpoints/{clean_name}")

    @staticmethod
    def is_spark_available() -> bool:
        try:
            import pyspark
            return True
        except ImportError:
            return False
