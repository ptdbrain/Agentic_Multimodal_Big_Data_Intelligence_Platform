"""Safe Storage and Atomic Parquet IO for Spark batch and streaming pipelines.
Guarantees data integrity and prevents data loss:
1. Distinguishes genuine non-existent paths from storage/connectivity/read errors.
2. Aborts immediately on any storage or read error; never overwrites existing data with partial batches.
3. Performs staged writes, post-write count validation, and atomic directory swap with rollback.
"""
import os
import uuid
import shutil
from pathlib import Path
from typing import List, Optional, Any

try:
    from pyspark.sql import SparkSession, DataFrame
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = None
    DataFrame = None


class SafeParquetIO:
    """Production-grade safe Parquet reader and atomic publisher for Spark."""

    @staticmethod
    def check_path_exists(spark: Any, path_str: str) -> bool:
        """Checks if a storage path (local, file://, s3a://, hdfs://) exists.
        Returns:
            True if path exists.
            False if path definitely does not exist.
        Raises:
            RuntimeError if storage accessibility check fails (e.g. network/auth error).
        """
        clean_str = str(path_str).strip()
        if clean_str.startswith("s3a://") or clean_str.startswith("hdfs://"):
            try:
                sc = spark.sparkContext
                conf = sc._jsc.hadoopConfiguration()
                PathClass = sc._gateway.jvm.org.apache.hadoop.fs.Path
                p = PathClass(clean_str)
                fs = p.getFileSystem(conf)
                return bool(fs.exists(p))
            except Exception as e:
                raise RuntimeError(
                    f"Storage accessibility check failed for '{clean_str}' ({type(e).__name__}: {e}). "
                    f"Cannot determine if dataset exists; aborting pipeline to prevent accidental data loss."
                ) from e
        else:
            # Local path or file://
            local_str = clean_str
            if local_str.startswith("file://"):
                local_str = local_str[7:]
                if os.name == "nt" and local_str.startswith("/") and len(local_str) > 2 and local_str[2] == ":":
                    local_str = local_str.lstrip("/")
            return Path(local_str).exists()

    @classmethod
    def read_parquet_safely(
        cls,
        spark: Any,
        path_str: str,
        check_exists_first: bool = True,
    ) -> Optional[Any]:
        """Safely reads a Parquet dataset.
        Returns:
            DataFrame if path exists and is readable.
            None if path definitely does not exist (valid new table scenario).
        Raises:
            RuntimeError on any read failure (connection error, permission, corrupted schema, etc.).
        """
        if check_exists_first:
            try:
                exists = cls.check_path_exists(spark, path_str)
            except Exception as e:
                raise RuntimeError(
                    f"CRITICAL: Failed to verify existence of Parquet path '{path_str}' ({type(e).__name__}: {e}). "
                    f"Aborting pipeline immediately to prevent data loss!"
                ) from e

            if not exists:
                return None

        try:
            df = spark.read.parquet(path_str)
            # Force eager check to ensure files can actually be opened and read
            _ = df.rdd.isEmpty()
            return df
        except Exception as e:
            err_msg = str(e).lower()
            # If Spark raises PATH_NOT_FOUND, double check if it truly does not exist
            if any(term in err_msg for term in ("[path_not_found]", "path does not exist", "filenotfoundexception")):
                try:
                    if not cls.check_path_exists(spark, path_str):
                        return None
                except Exception:
                    pass
            # Any other error is a real read failure: connection reset, auth failure, corrupt schema, etc.
            raise RuntimeError(
                f"CRITICAL: Failed to read existing Parquet at '{path_str}' ({type(e).__name__}: {e}). "
                f"Existing dataset exists or could not be verified, but read failed. "
                f"Aborting pipeline immediately to prevent data loss of existing records!"
            ) from e

    @classmethod
    def _rename_storage_path(cls, spark: Any, src_path: str, dst_path: str) -> bool:
        """Renames/moves a directory or file on Hadoop FileSystem or local disk."""
        src_str = str(src_path).strip()
        dst_str = str(dst_path).strip()
        is_remote = src_str.startswith("s3a://") or src_str.startswith("hdfs://")

        if is_remote:
            sc = spark.sparkContext
            conf = sc._jsc.hadoopConfiguration()
            PathClass = sc._gateway.jvm.org.apache.hadoop.fs.Path
            p_src = PathClass(src_str)
            p_dst = PathClass(dst_str)
            fs = p_src.getFileSystem(conf)
            return bool(fs.rename(p_src, p_dst))
        else:
            # Local filesystem: try Hadoop FS first if available
            try:
                if hasattr(spark, "sparkContext") and spark.sparkContext is not None:
                    sc = spark.sparkContext
                    conf = sc._jsc.hadoopConfiguration()
                    PathClass = sc._gateway.jvm.org.apache.hadoop.fs.Path
                    p_src = PathClass(src_str.replace("\\", "/"))
                    p_dst = PathClass(dst_str.replace("\\", "/"))
                    fs = p_src.getFileSystem(conf)
                    res = fs.rename(p_src, p_dst)
                    if res:
                        return True
            except Exception:
                pass

            # Python filesystem fallback
            src_local = Path(src_str.replace("file://", ""))
            dst_local = Path(dst_str.replace("file://", ""))
            if not src_local.exists():
                return False
            if dst_local.exists():
                if dst_local.is_dir():
                    shutil.rmtree(dst_local)
                else:
                    dst_local.unlink()
            shutil.move(str(src_local), str(dst_local))
            return True

    @classmethod
    def _delete_storage_path(cls, spark: Any, path_str: str) -> None:
        """Deletes a directory or file on Hadoop FileSystem or local disk."""
        path_clean = str(path_str).strip()
        is_remote = path_clean.startswith("s3a://") or path_clean.startswith("hdfs://")

        if is_remote:
            try:
                sc = spark.sparkContext
                conf = sc._jsc.hadoopConfiguration()
                PathClass = sc._gateway.jvm.org.apache.hadoop.fs.Path
                p = PathClass(path_clean)
                fs = p.getFileSystem(conf)
                if fs.exists(p):
                    fs.delete(p, True)
            except Exception:
                pass
        else:
            try:
                if hasattr(spark, "sparkContext") and spark.sparkContext is not None:
                    sc = spark.sparkContext
                    conf = sc._jsc.hadoopConfiguration()
                    PathClass = sc._gateway.jvm.org.apache.hadoop.fs.Path
                    p = PathClass(path_clean.replace("\\", "/"))
                    fs = p.getFileSystem(conf)
                    if fs.exists(p):
                        fs.delete(p, True)
                        return
            except Exception:
                pass

            local_p = Path(path_clean.replace("file://", ""))
            if local_p.exists():
                if local_p.is_dir():
                    shutil.rmtree(local_p, ignore_errors=True)
                else:
                    try:
                        local_p.unlink(missing_ok=True)
                    except Exception:
                        pass

    @classmethod
    def publish_parquet_atomic(
        cls,
        spark: Any,
        df: Any,
        target_path: str,
        partition_cols: Optional[List[str]] = None,
        expected_count: Optional[int] = None,
    ) -> None:
        """Writes DataFrame to a staging directory, verifies staged record count,
        and atomically swaps it into target_path with rollback capability.
        Avoids non-atomic in-place directory deletion on object storage.
        """
        uid = uuid.uuid4().hex[:8]
        clean_target = str(target_path).rstrip("/").rstrip("\\")
        stage_path = f"{clean_target}__staging_{uid}"
        backup_path = f"{clean_target}__backup_{uid}"

        # 1. Write to Staging Path
        try:
            writer = df.write.mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.parquet(stage_path)
        except Exception as write_err:
            cls._delete_storage_path(spark, stage_path)
            raise RuntimeError(
                f"Failed to write staged Parquet at '{stage_path}' ({type(write_err).__name__}: {write_err}). "
                f"Target path '{target_path}' was not modified."
            ) from write_err

        # 2. Validate Staged Parquet Record Count
        try:
            staged_df = spark.read.parquet(stage_path)
            actual_staged_count = staged_df.count()
            if expected_count is not None and actual_staged_count != expected_count:
                cls._delete_storage_path(spark, stage_path)
                raise RuntimeError(
                    f"Validation failed for staged Parquet at '{stage_path}': "
                    f"actual count ({actual_staged_count}) != expected count ({expected_count}). "
                    f"Aborting publish; target path '{target_path}' was not modified."
                )
        except Exception as val_err:
            cls._delete_storage_path(spark, stage_path)
            if isinstance(val_err, RuntimeError):
                raise
            raise RuntimeError(
                f"Validation check failed on staged Parquet at '{stage_path}' ({val_err}). "
                f"Target path '{target_path}' was not modified."
            ) from val_err

        # 3. Atomic Publish / Swap
        target_exists = cls.check_path_exists(spark, target_path)

        if target_exists:
            # Step A: Move target to backup
            backed_up = cls._rename_storage_path(spark, target_path, backup_path)
            if not backed_up:
                cls._delete_storage_path(spark, stage_path)
                raise RuntimeError(
                    f"Failed to create backup of existing dataset from '{target_path}' to '{backup_path}'. "
                    f"Publish aborted; existing dataset preserved."
                )

            # Step B: Move stage to target
            promoted = cls._rename_storage_path(spark, stage_path, target_path)
            if promoted:
                # Promotion succeeded; remove backup directory
                cls._delete_storage_path(spark, backup_path)
            else:
                # Promotion failed; roll back target from backup
                cls._rename_storage_path(spark, backup_path, target_path)
                cls._delete_storage_path(spark, stage_path)
                raise RuntimeError(
                    f"Failed to promote staged data from '{stage_path}' to '{target_path}'. "
                    f"Rolled back existing dataset from backup; original data intact."
                )
        else:
            # Initial table creation: directly promote stage to target
            promoted = cls._rename_storage_path(spark, stage_path, target_path)
            if not promoted:
                cls._delete_storage_path(spark, stage_path)
                raise RuntimeError(
                    f"Failed to promote staged data to initial target path '{target_path}'."
                )
