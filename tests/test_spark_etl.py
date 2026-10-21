import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
import pytest
from pyspark.sql.readwriter import DataFrameReader
from spark.etl.cleaner import DataCleaner
from spark.etl.normalizer import EntityNormalizer
from spark.etl.deduplicator import Deduplicator
from spark.spark_session import SparkSessionFactory
from spark.batch.spark_etl_job import SparkBatchETLJob
from spark.storage_safe import SafeParquetIO

pytestmark = pytest.mark.unit


def test_clean_reviews():
    dirty_reviews = [
        {"review_id": "r1", "review_text": "  Good phone  ", "review_title": "  Nice  ", "rating": 5.0},
        {"review_id": "r2", "review_text": "", "rating": 4.0},  # empty text
        {"review_id": "r3", "review_text": None, "rating": 3.0},  # None text
    ]
    cleaned = DataCleaner.clean_reviews(dirty_reviews)
    assert len(cleaned) == 1
    assert cleaned[0]["review_id"] == "r1"
    assert cleaned[0]["review_text"] == "Good phone"
    assert cleaned[0]["review_title"] == "Nice"


def test_validate_reviews_rejects_out_of_range_without_clamping():
    reviews = [
        {"review_id": "r_valid", "review_text": "Great", "rating": 4.5},
        {"review_id": "r_high", "review_text": "Too high", "rating": 10.0},
        {"review_id": "r_low", "review_text": "Too low", "rating": 0.0},
        {"review_id": "r_bad_type", "review_text": "Bad rating", "rating": "not_a_float"},
    ]
    res = DataCleaner.validate_and_clean(reviews)
    assert res["stats"]["valid"] == 1
    assert res["stats"]["invalid"] == 3
    assert res["valid"][0]["review_id"] == "r_valid"
    invalid_ids = {r["review_id"] for r in res["invalid"]}
    assert invalid_ids == {"r_high", "r_low", "r_bad_type"}


def test_normalize_brand():
    assert EntityNormalizer.normalize_brand("apple inc.") == "Apple"
    assert EntityNormalizer.normalize_brand("SAMSUNG") == "Samsung"
    assert EntityNormalizer.normalize_brand("xiaomi") == "Xiaomi"


def test_normalize_price():
    assert EntityNormalizer.normalize_price("19.990.000 VND") == 19990000.0
    assert EntityNormalizer.normalize_price("$1,299.50") == 1299.50
    assert EntityNormalizer.normalize_price("1.299,50 €") == 1299.50
    assert EntityNormalizer.normalize_price(25000000) == 25000000.0


def test_normalize_currency_and_timestamp():
    # Currency normalization
    assert EntityNormalizer.normalize_currency("vnđ") == "VND"
    assert EntityNormalizer.normalize_currency("₫") == "VND"
    assert EntityNormalizer.normalize_currency("$") == "USD"
    assert EntityNormalizer.normalize_currency("us$") == "USD"
    assert EntityNormalizer.normalize_currency("€") == "EUR"
    assert EntityNormalizer.normalize_currency(None, default_currency="VND") == "VND"

    # Timestamp normalization to UTC ISO-8601
    assert EntityNormalizer.normalize_timestamp(1727000000) == "2024-09-22T10:13:20Z"
    assert EntityNormalizer.normalize_timestamp(1727000000000) == "2024-09-22T10:13:20Z"
    assert EntityNormalizer.normalize_timestamp("2026-09-24T19:00:00+07:00") == "2026-09-24T12:00:00Z"
    assert EntityNormalizer.normalize_timestamp("2026/09/24 08:30:00") == "2026-09-24T08:30:00Z"
    assert EntityNormalizer.normalize_timestamp("2026-09-24") == "2026-09-24T00:00:00Z"
    assert EntityNormalizer.normalize_timestamp("not-a-timestamp") is None


def test_deduplicate_reviews():
    reviews = [
        {"review_id": "rv_01", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"},
        {"review_id": "rv_01", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"},  # duplicate id
        {"review_id": "rv_02", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"},  # duplicate hash
    ]
    unique, dups = Deduplicator.deduplicate_reviews(reviews)
    assert len(unique) == 1
    assert dups == 2


def test_deduplicate_retains_latest_record_by_timestamp_and_tiebreaker():
    # Out-of-order updates for the same review_id: 2026-10-03T15:00:00Z must win over 10-01 and 10-02
    records = [
        {
            "review_id": "rv_latest_test",
            "product_id": "p1",
            "user_id": "u1",
            "review_text": "Old v1",
            "rating": 3.0,
            "ingested_at": "2026-10-01T08:00:00Z",
            "event_id": "evt_001",
        },
        {
            "review_id": "rv_latest_test",
            "product_id": "p1",
            "user_id": "u1",
            "review_text": "Newest v3",
            "rating": 5.0,
            "ingested_at": "2026-10-03T15:00:00Z",
            "event_id": "evt_002",
        },
        {
            "review_id": "rv_latest_test",
            "product_id": "p1",
            "user_id": "u1",
            "review_text": "Middle v2",
            "rating": 4.0,
            "ingested_at": "2026-10-02T12:00:00Z",
            "event_id": "evt_003",
        },
    ]
    unique, dup_count = Deduplicator.deduplicate_reviews(records)
    assert len(unique) == 1
    assert dup_count == 2
    assert unique[0]["review_text"] == "Newest v3"
    assert unique[0]["rating"] == 5.0
    assert unique[0]["ingested_at"] == "2026-10-03T15:00:00Z"

    # Tie-breaker on event_id when timestamps are identical
    tied_records = [
        {
            "product_id": "p_tie",
            "product_name": "Phone A",
            "price": 100.0,
            "ingested_at": "2026-10-05T10:00:00Z",
            "event_id": "evt_a",
        },
        {
            "product_id": "p_tie",
            "product_name": "Phone B",
            "price": 200.0,
            "ingested_at": "2026-10-05T10:00:00Z",
            "event_id": "evt_z",
        },
    ]
    unique_tied, dup_tied = Deduplicator.deduplicate_records(
        tied_records, id_field="product_id", hash_fields=["product_id"]
    )
    assert len(unique_tied) == 1
    assert dup_tied == 1
    assert unique_tied[0]["event_id"] == "evt_z"
    assert unique_tied[0]["price"] == 200.0


def test_silver_read_failure_aborts_pipeline_and_preserves_existing_data():
    """Verify that when reading existing Silver Parquet fails (simulating connection loss,
    auth failure, or storage error), the pipeline raises RuntimeError and aborts immediately,
    leaving the existing historical dataset completely untouched.
    """
    spark = SparkSessionFactory.get_spark_session()
    if spark is None:
        pytest.skip("PySpark not available")

    job = SparkBatchETLJob(spark=spark)

    with tempfile.TemporaryDirectory() as tmp_dir:
        silver_path = str(Path(tmp_dir) / "silver_reviews.parquet").replace("\\", "/")

        # 1. Create an existing Silver table with 5 historical reviews
        existing_records = [
            {"review_id": f"rv_hist_{i}", "product_id": f"prod_{i}", "rating": 5.0, "review_text": f"Review {i}", "ingested_at": f"2026-10-01T10:00:0{i}Z"}
            for i in range(1, 6)
        ]
        existing_df = spark.createDataFrame(existing_records)
        existing_df.write.mode("overwrite").parquet(silver_path)

        # Confirm 5 records are written
        assert spark.read.parquet(silver_path).count() == 5

        # 2. Prepare new incoming batch with 2 reviews
        new_records = [
            {"review_id": "rv_new_1", "product_id": "prod_1", "rating": 4.0, "review_text": "New review 1", "ingested_at": "2026-10-21T12:00:00Z"},
            {"review_id": "rv_new_2", "product_id": "prod_2", "rating": 3.0, "review_text": "New review 2", "ingested_at": "2026-10-21T12:05:00Z"},
        ]
        new_batch_df = spark.createDataFrame(new_records)

        # 3. Simulate storage / connection read failure on the existing Silver dataset
        with patch.object(
            DataFrameReader,
            "parquet",
            side_effect=ConnectionError("MinIO connection reset: simulated storage network failure"),
        ):
            with pytest.raises(RuntimeError) as exc_info:
                job._cross_batch_dedup_and_write(
                    new_batch_df=new_batch_df,
                    dataset_name="reviews",
                    silver_path=silver_path,
                    pk="review_id",
                    order_col="ingested_at",
                    replace_existing=False,
                )

            assert "CRITICAL: Failed to read existing Parquet" in str(exc_info.value)
            assert "Aborting pipeline immediately to prevent data loss" in str(exc_info.value)

        # 4. Critical verification: Existing Silver table MUST STILL HAVE all 5 records intact
        preserved_df = spark.read.parquet(silver_path)
        assert preserved_df.count() == 5
        preserved_ids = {row["review_id"] for row in preserved_df.collect()}
        assert preserved_ids == {f"rv_hist_{i}" for i in range(1, 6)}
        assert "rv_new_1" not in preserved_ids
        assert "rv_new_2" not in preserved_ids


def test_silver_non_existent_path_initializes_table_normally():
    """Verify that when the Silver path truly does not exist yet, the pipeline treats it
    as initial table creation and successfully writes the new batch.
    """
    spark = SparkSessionFactory.get_spark_session()
    if spark is None:
        pytest.skip("PySpark not available")

    job = SparkBatchETLJob(spark=spark)

    with tempfile.TemporaryDirectory() as tmp_dir:
        silver_path = str(Path(tmp_dir) / "silver_new_table.parquet").replace("\\", "/")
        assert not Path(silver_path).exists()

        records = [
            {"review_id": "rv_init_1", "product_id": "prod_1", "rating": 5.0, "review_text": "First", "ingested_at": "2026-10-21T10:00:00Z"},
            {"review_id": "rv_init_2", "product_id": "prod_2", "rating": 4.0, "review_text": "Second", "ingested_at": "2026-10-21T10:05:00Z"},
        ]
        new_batch_df = spark.createDataFrame(records)

        res_df = job._cross_batch_dedup_and_write(
            new_batch_df=new_batch_df,
            dataset_name="reviews",
            silver_path=silver_path,
            pk="review_id",
            order_col="ingested_at",
            replace_existing=False,
        )

        assert res_df.count() == 2
        written_df = spark.read.parquet(silver_path)
        assert written_df.count() == 2
        written_ids = {row["review_id"] for row in written_df.collect()}
        assert written_ids == {"rv_init_1", "rv_init_2"}


def test_silver_atomic_swap_and_reconciliation_success():
    """Verify that under normal operation, new batch is reconciled with existing Silver
    via atomic publish swap, deduplicating records by primary key and latest timestamp.
    """
    spark = SparkSessionFactory.get_spark_session()
    if spark is None:
        pytest.skip("PySpark not available")

    job = SparkBatchETLJob(spark=spark)

    with tempfile.TemporaryDirectory() as tmp_dir:
        silver_path = str(Path(tmp_dir) / "silver_reconcile.parquet").replace("\\", "/")

        # Initial dataset: rv_1 (old rating 3.0), rv_2 (rating 4.0)
        initial_records = [
            {"review_id": "rv_1", "product_id": "p1", "rating": 3.0, "review_text": "Old v1", "ingested_at": "2026-10-01T10:00:00Z"},
            {"review_id": "rv_2", "product_id": "p2", "rating": 4.0, "review_text": "Keep v1", "ingested_at": "2026-10-01T10:00:00Z"},
        ]
        spark.createDataFrame(initial_records).write.mode("overwrite").parquet(silver_path)

        # New batch: rv_1 updated (rating 5.0, newer timestamp), rv_3 (brand new)
        new_records = [
            {"review_id": "rv_1", "product_id": "p1", "rating": 5.0, "review_text": "Updated v2", "ingested_at": "2026-10-02T15:00:00Z"},
            {"review_id": "rv_3", "product_id": "p3", "rating": 4.5, "review_text": "Brand new", "ingested_at": "2026-10-02T15:00:00Z"},
        ]
        new_df = spark.createDataFrame(new_records)

        reconciled_df = job._cross_batch_dedup_and_write(
            new_batch_df=new_df,
            dataset_name="reviews",
            silver_path=silver_path,
            pk="review_id",
            order_col="ingested_at",
            replace_existing=False,
        )

        assert reconciled_df.count() == 3
        persisted = spark.read.parquet(silver_path).collect()
        persisted_map = {r["review_id"]: r for r in persisted}

        assert len(persisted_map) == 3
        # Check that rv_1 was updated to latest version
        assert persisted_map["rv_1"]["rating"] == 5.0
        assert persisted_map["rv_1"]["review_text"] == "Updated v2"
        assert persisted_map["rv_2"]["rating"] == 4.0
        assert persisted_map["rv_3"]["rating"] == 4.5

        # Check no staging or backup artifacts remain in directory
        parent_dir = Path(tmp_dir)
        extra_dirs = [p.name for p in parent_dir.glob("silver_reconcile.parquet__*")]
        assert extra_dirs == [], f"Temporary staging/backup dirs were not cleaned up: {extra_dirs}"
