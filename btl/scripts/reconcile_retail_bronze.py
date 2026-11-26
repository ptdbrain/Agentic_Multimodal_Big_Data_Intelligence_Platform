"""Rebuild historical retail rows from verified Bronze on a distributed Spark cluster."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.archive_online_retail_ii import SOURCE
from scripts.prepare_online_retail_ii import SCHEMA


def reconcile(directory, receipt, output, publish_sql=False):
    from config.settings import settings
    from orchestration.source_jobs import write_atomic
    from pyspark.sql import functions as fn
    from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType
    from spark.spark_session import SparkSessionFactory
    from storage.minio_backend import MinIOBackend
    from storage.storage_manager import storage

    if output.exists():
        raise FileExistsError(output)
    if not isinstance(storage.backend, MinIOBackend) or not settings.spark.master.startswith("spark://"):
        raise ValueError("A real MinIO bucket and Spark standalone cluster are required")
    proof = json.loads(receipt.read_text())
    manifest = json.loads((directory / "manifest.json").read_text())
    bucket = storage.backend.bucket
    if manifest != proof["source"] or bucket != proof["bucket"] or bucket != f"{SOURCE}-{proof['run_id']}":
        raise ValueError("Receipt does not bind the prepared source and Bronze bucket")
    rows = manifest["counts"]["distinct"]
    if proof["kafka_acks"] != rows or proof["verified_bronze_rows"] != rows:
        raise ValueError("Kafka and Bronze must match the original input")
    expected = directory / "transaction_lines.parquet"
    with expected.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["parquet_sha256"]:
            raise ValueError("Prepared Parquet has changed")
    objects = [obj["path"] for obj in proof["bronze_objects"]]
    if not objects or len(objects) != len(set(objects)) or any(not p.startswith("bronze/") for p in objects):
        raise ValueError("Expected distinct Bronze object references")
    prefix = f"s3a://{bucket}/"
    comparison_key = "registry/expected.parquet"
    storage.backend.client.fput_object(bucket, comparison_key, str(expected))
    value = storage.backend.client.get_object(bucket, comparison_key)
    try:
        digest = hashlib.sha256()
        for block in value.stream(1024 * 1024):
            digest.update(block)
        if digest.hexdigest() != manifest["parquet_sha256"]:
            raise ValueError("Remote Parquet checksum differs from original")
    finally:
        value.close()
        value.release_conn()

    numeric = {"source_row", "quantity"}
    payload = StructType([
        StructField(name, LongType() if name in numeric else BooleanType() if name == "is_cancellation" else StringType())
        for name in SCHEMA.names
    ])
    envelope = StructType([
        StructField("event_type", StringType()), StructField("source", StringType()), StructField("payload", payload)
    ])
    spark = SparkSessionFactory.get_spark_session(app_name="Verified historical retail Bronze reconciliation")
    try:
        wire = spark.read.schema(envelope).option("multiLine", "true").json([prefix + path for path in objects])
        if wire.where(
            ~fn.col("source").eqNullSafe(SOURCE)
            | ~fn.col("event_type").eqNullSafe("RETAIL_TRANSACTION_IMPORT")
            | fn.col("payload").isNull()
        ).limit(1).count():
            raise ValueError("Bronze contains an invalid envelope")
        data = wire.select("payload.*").withColumns({
            "unit_price": fn.col("unit_price").cast("decimal(18,6)"),
            "invoice_local_time": fn.col("invoice_local_time").cast("timestamp_ntz"),
        }).select(*SCHEMA.names).persist()
        source = spark.read.parquet(prefix + comparison_key).select(*SCHEMA.names)
        if data.count() != rows or data.exceptAll(source).limit(1).count() or source.exceptAll(data).limit(1).count():
            raise ValueError("Bronze rows differ from the prepared Parquet")
        mart = data.groupBy(fn.to_date("invoice_local_time").alias("invoice_day"), "currency").agg(
            fn.count("*").alias("transaction_lines"),
            fn.sum(fn.col("is_cancellation").cast("long")).alias("cancellations"),
            fn.sum((fn.col("quantity").cast("decimal(20,0)")) * fn.col("unit_price")).alias("line_amount"),
        )
        generation = uuid4().hex
        silver_path = prefix + f"silver/{generation}/transactions"
        gold_path = prefix + f"gold/{generation}/transaction_daily_stats"
        data.write.mode("errorifexists").parquet(silver_path)
        mart.write.mode("errorifexists").parquet(gold_path)
        gold_total = spark.read.parquet(gold_path).agg(fn.sum("transaction_lines")).first()[0]
        if gold_total != rows or spark.read.parquet(silver_path).count() != rows:
            raise ValueError("Persisted Silver and Gold failed count reconciliation")
        daily = spark.read.parquet(gold_path).select(
            "invoice_day", "currency", "transaction_lines", "cancellations", "line_amount"
        ).orderBy("invoice_day").collect()
        if publish_sql:
            from serving.retail import publish
            publish([tuple(row) for row in daily], proof["run_id"], manifest["parquet_sha256"], rows)
        write_atomic(output, {
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": proof["run_id"], "spark_master": spark.sparkContext.master,
            "bronze_payloads": rows, "silver_rows": rows, "gold_total_transaction_lines": gold_total,
            "bronze_objects": len(objects), "parquet_sha256": manifest["parquet_sha256"],
            "silver_path": silver_path, "gold_path": gold_path,
            "published_sql": publish_sql,
            "daily": [[day.isoformat(), unit, count, canceled, str(amount)]
                      for day, unit, count, canceled, amount in daily],
        })
    finally:
        spark.stop()


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("prepared_directory", type=Path)
    cli.add_argument("archive_receipt", type=Path)
    cli.add_argument("--output", required=True, type=Path)
    cli.add_argument("--publish-sql", action="store_true", help="Publish the verified daily mart into retail_history")
    args = cli.parse_args()
    reconcile(args.prepared_directory, args.archive_receipt, args.output, args.publish_sql)
