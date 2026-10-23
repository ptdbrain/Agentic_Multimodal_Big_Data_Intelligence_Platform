"""Acceptance Verification Script for Streaming & Advanced Data Quality.
Verifies:
1. Advanced Data Quality:
   - Field-level null statistics (field_null_counts) separated from row-level invalid count (invalid_count).
   - Parse/cast executed BEFORE validation (catching malformed types as cast_error_count / INVALID_TYPE_*).
   - Configurable DQ threshold gate (check_dq_gate) blocking Gold mart generation (DataQualityGateError).
2. Streaming Engine (SparkStructuredStreaming & Stateful Micro-Batch Streaming):
   - Unified S3A configuration via SparkSessionFactory and valid outputMode ('update' with idempotent sink / 'append').
   - Late-arriving data handling via event-time watermark (accepts within watermark, drops beyond watermark).
   - Malformed data types routed to Quarantine without corrupting window metrics.
   - Primary-key deduplication (review_id) within and across micro-batches.
   - Stop & Restart from persistent checkpoint with ZERO data loss and ZERO metric duplication.
"""
import sys
import os
import json
import shutil
import tempfile
from pathlib import Path
from typing import Optional
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage
from spark.spark_session import SparkSessionFactory
from spark.quality.spark_dq import SparkDataQualityEvaluator, DataQualityGateError
from spark.batch.spark_etl_job import SparkBatchETLJob
from spark.analytics.gold_builder import SparkGoldBuilder
from spark.streaming.streaming_job import StreamingJob
from spark.streaming.spark_streaming_job import SparkStreamingJob


def verify_advanced_dq(spark) -> None:
    print("\n--- [Part 1] Verifying Advanced Data Quality (Null Stats vs Row Invalid, Parse/Cast, DQ Gate) ---")
    test_records = [
        # 1. Valid record with 2 NULL optional fields (review_title, helpful_count) -> MUST be VALID row
        {
            "event_id": "evt_dq_1",
            "event_type": "NEW_REVIEW",
            "source": "dq_acceptance",
            "ingested_at": "2026-10-10T10:00:00Z",
            "payload": {
                "review_id": "rv_dq_1",
                "product_id": "prod_1",
                "rating": "5.0",
                "review_text": "Excellent camera and battery life",
                "review_title": None,
                "helpful_count": None,
                "review_date": "2026-10-10T09:00:00Z",
            },
        },
        # 2. Valid record with all fields populated -> MUST be VALID row
        {
            "event_id": "evt_dq_2",
            "event_type": "NEW_REVIEW",
            "source": "dq_acceptance",
            "ingested_at": "2026-10-10T10:01:00Z",
            "payload": {
                "review_id": "rv_dq_2",
                "product_id": "prod_1",
                "rating": "4.0",
                "review_text": "Solid performance",
                "review_title": "Good phone",
                "helpful_count": 3,
                "review_date": "2026-10-10T09:10:00Z",
            },
        },
        # 3. Single row with 3 NULL/empty required fields AND 2 NULL optional fields (5 field nulls, but ONLY 1 invalid row)
        {
            "event_id": "evt_dq_3",
            "event_type": "NEW_REVIEW",
            "source": "dq_acceptance",
            "ingested_at": "2026-10-10T10:02:00Z",
            "payload": {
                "review_id": "rv_dq_3",
                "product_id": None,
                "rating": None,
                "review_text": "   ",
                "review_title": None,
                "helpful_count": None,
                "review_date": "2026-10-10T09:20:00Z",
            },
        },
        # 4. Malformed type strings (rating='five_stars', review_date='not_a_date') -> 0 field nulls before cast, 1 cast error row
        {
            "event_id": "evt_dq_4",
            "event_type": "NEW_REVIEW",
            "source": "dq_acceptance",
            "ingested_at": "2026-10-10T10:03:00Z",
            "payload": {
                "review_id": "rv_dq_4",
                "product_id": "prod_1",
                "rating": "five_stars",
                "review_text": "Malformed rating string",
                "review_title": "Bad type",
                "helpful_count": 1,
                "review_date": "not_a_date",
            },
        },
        # 5. Duplicate primary key (review_id='rv_dq_1') -> 1 duplicate row
        {
            "event_id": "evt_dq_5",
            "event_type": "NEW_REVIEW",
            "source": "dq_acceptance",
            "ingested_at": "2026-10-10T10:04:00Z",
            "payload": {
                "review_id": "rv_dq_1",
                "product_id": "prod_1",
                "rating": "5.0",
                "review_text": "Duplicate review_id entry",
                "review_title": "Dup",
                "helpful_count": 2,
                "review_date": "2026-10-10T09:30:00Z",
            },
        },
    ]

    etl = SparkBatchETLJob(spark=spark)
    unpacked_df = etl.read_bronze_json(test_records)

    # Evaluate via SparkDataQualityEvaluator
    dq_metrics = SparkDataQualityEvaluator.evaluate(unpacked_df, "reviews", primary_key="review_id")
    print(f"  • Field-level Null Counts: {dq_metrics['field_null_counts']}")
    print(f"  • Total Field Nulls (sum across columns): {dq_metrics['total_field_nulls']}")
    print(f"  • Row-level Missing Required (null_row_count): {dq_metrics['null_row_count']}")
    print(f"  • Row-level Type Cast Errors (cast_error_count): {dq_metrics['cast_error_count']}")
    print(
        f"  • Row-level Accounting: total={dq_metrics['total_count']}, "
        f"valid={dq_metrics['valid_count']}, invalid={dq_metrics['invalid_count']} "
        f"(quarantine={dq_metrics['quarantine_count']}, duplicate={dq_metrics['duplicate_count']})"
    )

    assert dq_metrics["total_count"] == 5
    # Field-level nulls: review_title=2, helpful_count=2, product_id=1, rating=1, review_text=1 -> sum = 7
    assert dq_metrics["field_null_counts"]["review_title"] == 2
    assert dq_metrics["field_null_counts"]["helpful_count"] == 2
    assert dq_metrics["field_null_counts"]["product_id"] == 1
    assert dq_metrics["field_null_counts"]["rating"] == 1
    assert dq_metrics["field_null_counts"]["review_text"] == 1
    assert dq_metrics["total_field_nulls"] == 7

    # Row-level missing required fields is strictly 1 (rv_dq_3), NOT 7
    assert dq_metrics["null_row_count"] == 1
    # Row-level cast error is 1 (rv_dq_4)
    assert dq_metrics["cast_error_count"] == 1
    # Row-level quarantine is 2 (rv_dq_3 + rv_dq_4), duplicate is 1 (rv_dq_1 dup), invalid is 3, valid is 2
    assert dq_metrics["quarantine_count"] == 2
    assert dq_metrics["duplicate_count"] == 1
    assert dq_metrics["invalid_count"] == 3
    assert dq_metrics["valid_count"] == 2
    assert dq_metrics["valid_count"] + dq_metrics["invalid_count"] == dq_metrics["total_count"]
    assert dq_metrics["dq_score"] == 40.0

    # Verify parse/cast before validation in SparkBatchETLJob.validate
    valid_df, invalid_df = etl.validate(unpacked_df, "reviews")
    reasons = {r["review_id"]: r["quarantine_reason"] for r in invalid_df.select("review_id", "quarantine_reason").collect()}
    print(f"  • SparkBatchETLJob Quarantine Reasons: {reasons}")
    assert reasons.get("rv_dq_4") == "INVALID_TYPE_RATING"
    assert "rv_dq_3" in reasons

    # Verify DQ Gate blocks Gold when DQ score (40.0%) < min_dq_score (90.0%)
    blocked = False
    try:
        gold_builder = SparkGoldBuilder(spark=spark)
        gold_builder.build_all(
            dq_metrics=[dq_metrics],
            enforce_dq_gate=True,
            min_dq_score=90.0,
            save_gold=False,
        )
    except DataQualityGateError as exc:
        blocked = True
        print(f"  • [OK] DQ Gate successfully blocked Gold generation: {exc}")
    assert blocked is True, "Expected DataQualityGateError to block Gold mart generation!"


def verify_streaming_checkpoint_and_restart(spark, mode: str = "integration") -> None:
    print(f"\n--- [Part 2] Verifying Streaming Late Data, Malformed Types, Dedup & Restart (mode={mode}) ---")
    batch_1 = [
        # 1. Valid review (window 10:00 - 10:01)
        {
            "event_id": "evt_s1",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:00:15Z",
            "payload": {
                "review_id": "rv_stream_001",
                "product_id": "prod_stream_1",
                "user_id": "u1",
                "rating": "5.0",
                "review_title": "Great",
                "review_text": "Works perfectly",
                "review_date": "2026-10-10T10:00:10Z",
                "verified_purchase": "true",
            },
        },
        # 2. Valid negative review (window 10:00 - 10:01)
        {
            "event_id": "evt_s2",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:00:45Z",
            "payload": {
                "review_id": "rv_stream_002",
                "product_id": "prod_stream_1",
                "user_id": "u2",
                "rating": "1.0",
                "review_title": "Bad",
                "review_text": "Stopped working",
                "review_date": "2026-10-10T10:00:40Z",
                "verified_purchase": "true",
            },
        },
        # 3. Intra-batch duplicate key (rv_stream_001) -> must be deduplicated
        {
            "event_id": "evt_s3",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:00:55Z",
            "payload": {
                "review_id": "rv_stream_001",
                "product_id": "prod_stream_1",
                "user_id": "u1",
                "rating": "5.0",
                "review_title": "Great dup",
                "review_text": "Works perfectly duplicate",
                "review_date": "2026-10-10T10:00:50Z",
                "verified_purchase": "true",
            },
        },
        # 4. Malformed data type (rating='BAD_FLOAT') -> must fail parse/cast and route to quarantine
        {
            "event_id": "evt_s4",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:00:30Z",
            "payload": {
                "review_id": "rv_stream_bad_type",
                "product_id": "prod_stream_1",
                "user_id": "u3",
                "rating": "BAD_FLOAT",
                "review_title": "Corrupt",
                "review_text": "Bad rating type",
                "review_date": "2026-10-10T10:00:25Z",
                "verified_purchase": "false",
            },
        },
        # 5. Valid review at 10:12:15Z (window 10:12 - 10:13) -> advances max_event_time to 10:12:15Z, watermark to 10:02:15Z
        {
            "event_id": "evt_s5",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:12:20Z",
            "payload": {
                "review_id": "rv_stream_003",
                "product_id": "prod_stream_1",
                "user_id": "u4",
                "rating": "4.0",
                "review_title": "Good",
                "review_text": "Nice display",
                "review_date": "2026-10-10T10:12:15Z",
                "verified_purchase": "true",
            },
        },
    ]

    batch_2 = [
        # 1. Cross-batch duplicate key after restart (rv_stream_002) -> must be dropped from checkpoint state
        {
            "event_id": "evt_s6",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:12:30Z",
            "payload": {
                "review_id": "rv_stream_002",
                "product_id": "prod_stream_1",
                "user_id": "u2",
                "rating": "1.0",
                "review_title": "Dup after restart",
                "review_text": "Duplicate after restart",
                "review_date": "2026-10-10T10:12:25Z",
                "verified_purchase": "true",
            },
        },
        # 2. Late-arriving data WITHIN watermark (10:05:10Z >= watermark 10:02:15Z) -> must be ACCEPTED
        {
            "event_id": "evt_s7",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:12:35Z",
            "payload": {
                "review_id": "rv_stream_late_ok",
                "product_id": "prod_stream_1",
                "user_id": "u5",
                "rating": "2.0",
                "review_title": "Late within watermark",
                "review_text": "Arrived 7 minutes late but within 10m watermark",
                "review_date": "2026-10-10T10:05:10Z",
                "verified_purchase": "true",
            },
        },
        # 3. Late-arriving data updating window 10:12 - 10:13 (10:12:45Z) -> updates window 10:12-10:13 count 1 -> 2
        {
            "event_id": "evt_s8",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:12:50Z",
            "payload": {
                "review_id": "rv_stream_004",
                "product_id": "prod_stream_1",
                "user_id": "u6",
                "rating": "2.0",
                "review_title": "Second review in 10:12 window",
                "review_text": "Updates 10:12 window metrics",
                "review_date": "2026-10-10T10:12:45Z",
                "verified_purchase": "true",
            },
        },
        # 4. Too-late data BEYOND watermark (09:40:00Z < watermark 10:02:15Z) -> must be DROPPED
        {
            "event_id": "evt_s9",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:12:55Z",
            "payload": {
                "review_id": "rv_stream_too_late",
                "product_id": "prod_stream_1",
                "user_id": "u7",
                "rating": "1.0",
                "review_title": "Too late",
                "review_text": "Arrived 32 minutes late beyond 10m watermark",
                "review_date": "2026-10-10T09:40:00Z",
                "verified_purchase": "true",
            },
        },
        # 5. Malformed timestamp type ('NOT_A_TIMESTAMP') -> must fail parse/cast and route to quarantine
        {
            "event_id": "evt_s10",
            "event_type": "NEW_REVIEW",
            "source": "stream_test",
            "ingested_at": "2026-10-10T10:12:59Z",
            "payload": {
                "review_id": "rv_stream_bad_ts",
                "product_id": "prod_stream_1",
                "user_id": "u8",
                "rating": "5.0",
                "review_title": "Bad date",
                "review_text": "Invalid date string",
                "review_date": "NOT_A_TIMESTAMP",
                "verified_purchase": "true",
            },
        },
    ]

    temp_root = Path(tempfile.mkdtemp(prefix="sentinel_stream_acc_"))
    try:
        # --- A. Verify Stateful Checkpointed StreamingJob ---
        chk_micro = str(temp_root / "micro_checkpoint")
        job1 = StreamingJob(
            checkpoint_dir=chk_micro,
            watermark_seconds=600,
            window_seconds=60,
            output_mode="update",
            strict_validation=True,
        )
        res1 = job1.process_micro_batch(batch_1, batch_id="batch_001")
        print(
            f"  • [StreamingJob Batch 1] valid={res1['batch_valid_count']}, "
            f"invalid={res1['batch_invalid_count']}, dup={res1['batch_duplicate_count']}"
        )
        assert res1["batch_valid_count"] == 3
        assert res1["batch_invalid_count"] == 1
        assert res1["batch_duplicate_count"] == 1
        assert res1["total_processed"] == 3
        assert res1["negative_reviews_count"] == 1

        # Simulate process crash / stop and restart a NEW StreamingJob instance from checkpoint
        restarted_job = StreamingJob(
            checkpoint_dir=chk_micro,
            watermark_seconds=600,
            window_seconds=60,
            output_mode="update",
            strict_validation=True,
        )
        # Re-submitting already committed batch_001 after restart must be idempotent (0 new counts)
        res1_replay = restarted_job.process_micro_batch(batch_1, batch_id="batch_001")
        assert res1_replay["skipped_committed_batch"] is True
        assert res1_replay["total_processed"] == 3
        assert res1_replay["negative_reviews_count"] == 1

        # Process Batch 2 after restart
        res2 = restarted_job.process_micro_batch(batch_2, batch_id="batch_002")
        print(
            f"  • [StreamingJob After Restart Batch 2] valid={res2['batch_valid_count']}, "
            f"invalid={res2['batch_invalid_count']}, dup={res2['batch_duplicate_count']}, "
            f"late_accepted={res2['batch_late_accepted']}, late_dropped={res2['batch_late_dropped']}"
        )
        assert res2["batch_valid_count"] == 2  # rv_stream_late_ok + rv_stream_004
        assert res2["batch_invalid_count"] == 1  # rv_stream_bad_ts
        assert res2["batch_duplicate_count"] == 1  # rv_stream_002 (remembered across restart!)
        assert res2["batch_late_accepted"] == 1  # rv_stream_late_ok (within watermark)
        assert res2["batch_late_dropped"] == 1  # rv_stream_too_late (beyond watermark)
        assert res2["total_processed"] == 5
        assert res2["negative_reviews_count"] == 3  # rv_stream_002 (1.0), rv_stream_late_ok (2.0), rv_stream_004 (2.0)

        # --- B. Verify Real Spark Structured Streaming (SparkStreamingJob) with Checkpoint & Restart ---
        if mode in ("integration", "production"):
            stream_in_rel = "bronze/stream_acceptance_input"
            stream_out_rel = "gold/stream_acceptance_metrics"
            stream_quar_rel = "silver/quarantine/stream_acceptance"
            stream_chk_rel = "checkpoints/stream_acceptance"

            stream_in = SparkSessionFactory.get_s3a_url(stream_in_rel)
            stream_out = SparkSessionFactory.get_s3a_url(stream_out_rel)
            stream_quar = SparkSessionFactory.get_s3a_url(stream_quar_rel)
            stream_chk = SparkSessionFactory.get_s3a_url(stream_chk_rel)

            # Clean any prior test artifacts in MinIO via Hadoop FileSystem
            conf = spark.sparkContext._jsc.hadoopConfiguration()
            path_cls = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
            for p_url in (stream_in, stream_out, stream_quar, stream_chk):
                fs = path_cls(p_url).getFileSystem(conf)
                fs.delete(path_cls(p_url), True)

            storage.backend.write_json(f"{stream_in_rel}/batch_001.json", batch_1)
        else:
            stream_in_dir = temp_root / "spark_stream_in"
            stream_out_dir = temp_root / "spark_stream_out"
            stream_quar_dir = temp_root / "spark_stream_quar"
            stream_chk_dir = temp_root / "spark_stream_chk"
            stream_in_dir.mkdir(parents=True, exist_ok=True)

            with open(stream_in_dir / "batch_001.json", "w", encoding="utf-8") as f:
                json.dump(batch_1, f, ensure_ascii=False)

            stream_in = str(stream_in_dir)
            stream_out = str(stream_out_dir)
            stream_quar = str(stream_quar_dir)
            stream_chk = str(stream_chk_dir)

        spark_stream_job = SparkStreamingJob(
            spark_session=spark,
            checkpoint_dir=stream_chk,
            output_path=stream_out,
            quarantine_path=stream_quar,
            output_mode="update",
        )

        print("  • Starting SparkStreamingJob Run #1 (Batch 1)...")
        spark_stream_job.start_review_stream(
            output_path=stream_out,
            window_duration="1 minute",
            watermark_delay="10 minutes",
            output_mode="update",
            source_path=stream_in,
            trigger_available_now=True,
        )
        spark_stream_job.await_termination()
        spark_stream_job.stop()

        from pyspark.sql import functions as F
        df_run1 = spark.read.parquet(stream_out).withColumn("win_hm", F.date_format("window_start", "HH:mm"))
        rows_run1 = {
            r["win_hm"]: r.asDict()
            for r in df_run1.collect()
        }
        print(f"  • [SparkStreamingJob Run #1 Windows]: {rows_run1}")
        assert "10:00" in rows_run1
        assert rows_run1["10:00"]["review_count"] == 2
        assert float(rows_run1["10:00"]["avg_rating"]) == 3.0
        assert rows_run1["10:00"]["negative_count"] == 1
        assert "10:12" in rows_run1
        assert rows_run1["10:12"]["review_count"] == 1
        assert float(rows_run1["10:12"]["avg_rating"]) == 4.0

        # Write Batch 2 and RESTART SparkStreamingJob from the SAME checkpointLocation
        if mode in ("integration", "production"):
            storage.backend.write_json(f"{stream_in_rel}/batch_002.json", batch_2)
        else:
            with open(Path(stream_in) / "batch_002.json", "w", encoding="utf-8") as f:
                json.dump(batch_2, f, ensure_ascii=False)

        print("  • Restarting SparkStreamingJob Run #2 from Checkpoint (Batch 2)...")
        spark_stream_job.restart(trigger_available_now=True)
        spark_stream_job.await_termination()
        spark_stream_job.stop()

        # Also restart a 3rd time with NO new files to prove restart does not duplicate any metrics
        print("  • Restarting SparkStreamingJob Run #3 from Checkpoint (No new files - idempotency check)...")
        spark_stream_job.restart(trigger_available_now=True)
        spark_stream_job.await_termination()
        spark_stream_job.stop()

        df_final = spark.read.parquet(stream_out).withColumn("win_hm", F.date_format("window_start", "HH:mm"))
        rows_final = {
            r["win_hm"]: r.asDict()
            for r in df_final.collect()
        }
        print(f"  • [SparkStreamingJob Post-Restart Windows]: {rows_final}")

        # Verify Window 10:00-10:01 preserved without data loss or duplication
        assert rows_final["10:00"]["review_count"] == 2
        assert float(rows_final["10:00"]["avg_rating"]) == 3.0
        assert rows_final["10:00"]["negative_count"] == 1

        # Verify Window 10:05-10:06 created from late-arriving event within watermark
        assert "10:05" in rows_final
        assert rows_final["10:05"]["review_count"] == 1
        assert float(rows_final["10:05"]["avg_rating"]) == 2.0
        assert rows_final["10:05"]["negative_count"] == 1

        # Verify Window 10:12-10:13 updated from 1 -> 2 reviews (and cross-batch dup rv_stream_002 was ignored!)
        assert rows_final["10:12"]["review_count"] == 2
        assert float(rows_final["10:12"]["avg_rating"]) == 3.0
        assert rows_final["10:12"]["negative_count"] == 1

        # Verify too-late event (09:40) beyond watermark was dropped
        assert "09:40" not in rows_final

        # Verify Streaming Quarantine captured both malformed type records
        df_quar = spark.read.parquet(stream_quar)
        quar_reasons = {
            r["review_id"]: r["quarantine_reason"]
            for r in df_quar.select("review_id", "quarantine_reason").collect()
        }
        print(f"  • [SparkStreamingJob Quarantine]: {quar_reasons}")
        assert quar_reasons.get("rv_stream_bad_type") == "INVALID_TYPE_RATING"
        assert quar_reasons.get("rv_stream_bad_ts") == "INVALID_TYPE_TIMESTAMP"

    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def run_streaming_dq_acceptance(mode: Optional[str] = None) -> bool:
    effective_mode = (mode or os.environ.get("RUNTIME_MODE", settings.runtime_mode)).strip().lower()
    settings.set_runtime_mode(effective_mode)
    storage.set_mode(effective_mode)

    print("=" * 72)
    print(f"  ACCEPTANCE TEST: STREAMING & ADVANCED DATA QUALITY (MODE: {effective_mode.upper()})")
    print("=" * 72)

    spark = SparkSessionFactory.get_spark_session()
    if spark is None:
        raise RuntimeError("SparkSession is required for streaming & DQ acceptance test.")

    verify_advanced_dq(spark)
    verify_streaming_checkpoint_and_restart(spark, mode=effective_mode)

    print("\n" + "=" * 72)
    print("  ACCEPTANCE TEST PASSED 100%!")
    print("  - Field-level Null Stats separated from Row-level Invalid Count: VERIFIED")
    print("  - Parse/Cast executed BEFORE Validation:                         VERIFIED")
    print("  - Configurable DQ Threshold Gate blocking Gold:                  VERIFIED")
    print("  - Streaming Late Data (within vs beyond Watermark):              VERIFIED")
    print("  - Streaming Malformed Types & Primary-Key Deduplication:         VERIFIED")
    print("  - Streaming Checkpoint Restart (0 data loss, 0 dup metrics):     VERIFIED")
    print("=" * 72)
    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Verify Streaming & Advanced DQ Acceptance")
    parser.add_argument("--mode", choices=["integration", "local", "test"], default=None)
    args = parser.parse_args()
    ok = run_streaming_dq_acceptance(mode=args.mode)
    sys.exit(0 if ok else 1)
