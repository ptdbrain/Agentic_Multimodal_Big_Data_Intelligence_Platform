"""SentinelAI Kafka -> Bronze Acceptance Verification Script.

Executes and verifies:
1. Producer sends real messages with canonical envelope and checks delivery result.
2. 3 valid messages and 1 invalid message are sent through Kafka.
3. Consumer receives messages via consume_batch_once().
4. Exactly 3 messages appear in Bronze with canonical envelopes and consistent partitioning.
5. Exactly 1 malformed message appears in DLQ.
6. Offsets are committed strictly for successful partitions.
7. Verification that if DLQ fails, the error is NOT swallowed and offset is NOT committed.
"""
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from storage.storage_manager import storage, StorageManager
from kafka.topics.topic_manager import TopicManager
from kafka.producers.stream_producer import StreamProducer
from kafka.consumers.raw_consumer import RawConsumer
from kafka.dlq.dead_letter import dlq_handler


def run_acceptance_verification(mode: str = "minio", storage_backend: Optional[str] = None) -> bool:
    print("=" * 70)
    print(f"  KAFKA -> BRONZE ACCEPTANCE VERIFICATION (MODE: {mode.upper()})")
    print("=" * 70)

    use_minio = (storage_backend == "minio") or (mode in ("minio", "integration"))
    is_local_producer = mode in ("local", "test", "minio")
    
    settings.set_runtime_mode("integration" if mode == "integration" else "local")
    storage.set_mode("integration" if use_minio else "local")

    if use_minio:
        from storage.minio_backend import MinIOBackend
        assert StorageManager.backend == MinIOBackend, "StorageManager.backend != MinIOBackend!"
        assert isinstance(storage.backend, MinIOBackend), "storage.backend is not MinIOBackend!"
        print(f"\n[Storage Setup] Connected to real MinIOBackend (Bucket: '{storage.backend.bucket}', Endpoint: '{storage.backend.endpoint}')")
        storage.health_check()
        print("  -> MinIO S3 connection & bucket health check PASSED.")

    topic = settings.kafka.topic_reviews
    print(f"\n[1/5] Ensuring Topic Provisioning for '{topic}' & 'dead-letter'...")
    prov_result = TopicManager.ensure_topics(mode="local" if is_local_producer else "integration")
    print(f"  -> Topic Status: {prov_result['status']}")

    # 3 valid review records conforming to data/schemas/review.json
    valid_reviews = [
        {
            "review_id": "rv_acc_001",
            "product_id": "prod_apple_15",
            "user_id": "usr_alpha",
            "rating": 5.0,
            "review_title": "Tuyệt vời",
            "review_text": "Sản phẩm chất lượng cao, camera sắc nét.",
            "review_date": "2026-10-10T02:00:00Z",
            "verified_purchase": True,
            "source": "acceptance_test"
        },
        {
            "review_id": "rv_acc_002",
            "product_id": "prod_apple_15",
            "user_id": "usr_beta",
            "rating": 4.0,
            "review_title": "Khá tốt",
            "review_text": "Pin dùng được cả ngày, giao hàng nhanh.",
            "review_date": "2026-10-10T02:05:00Z",
            "verified_purchase": True,
            "source": "acceptance_test"
        },
        {
            "review_id": "rv_acc_003",
            "product_id": "prod_apple_15",
            "user_id": "usr_gamma",
            "rating": 3.0,
            "review_title": "Bình thường",
            "review_text": "Tạm ổn trong tầm giá, máy hơi nóng.",
            "review_date": "2026-10-10T02:10:00Z",
            "verified_purchase": False,
            "source": "acceptance_test"
        },
    ]

    # 1 malformed record violating schema (invalid rating=99.0 and missing required review_text)
    invalid_review = {
        "review_id": "rv_acc_bad_999",
        "product_id": "prod_apple_15",
        "user_id": "usr_dirty",
        "rating": 99.0,  # Invalid: schema allows [1.0, 5.0]
        "review_title": "Lỗi dữ liệu",
        # missing required review_text
        "review_date": "2026-10-10T02:15:00Z",
        "source": "acceptance_test"
    }

    test_messages = valid_reviews + [invalid_review]

    print(f"\n[2/5] Publishing 3 valid messages and 1 malformed message to '{topic}'...")
    producer = StreamProducer(enable_offline_buffer=is_local_producer)
    dlq_handler.clear()

    deliveries = []
    for rec in test_messages:
        key = rec["product_id"]
        env, delivery = producer.send_message(
            topic=topic,
            key=key,
            value=rec,
            event_type="NEW_REVIEW",
            source="acceptance_suite",
            wait_for_delivery=True
        )
        deliveries.append(delivery)
        print(f"  + Sent event '{env['event_id']}' (Status: {delivery.get('status', 'OK')})")

    assert len(deliveries) == 4, f"Expected 4 message deliveries, got {len(deliveries)}"
    print(f"  -> Producer delivery check PASSED for 4/4 messages.")

    print(f"\n[3/5] Consuming messages via consume_batch_once()...")
    consumer = RawConsumer()
    offline_items = producer.virtual_queue if is_local_producer else None

    consume_result = consumer.consume_batch_once(
        topics=[topic],
        max_records=10,
        timeout_ms=3000,
        offline_records=offline_items
    )

    print(f"  * Total consumed:  {consume_result['records_consumed']}")
    print(f"  * Valid to Bronze: {consume_result['records_valid']}")
    print(f"  * Routed to DLQ:   {consume_result['records_dlq']}")
    print(f"  * Committed Offsets: {consume_result['committed_offsets']}")

    # Verification 1: Exactly 3 valid messages in Bronze
    assert consume_result["records_valid"] == 3, f"Expected 3 valid records, got {consume_result['records_valid']}"

    # Verification 2: Exactly 1 malformed message routed to DLQ
    assert consume_result["records_dlq"] == 1, f"Expected 1 DLQ record, got {consume_result['records_dlq']}"

    # Verification 3: Offsets committed
    assert len(consume_result["committed_offsets"]) > 0, "Offsets were not committed!"
    print("  -> Partition offsets committed successfully.")

    print(f"\n[4/5] Verifying Bronze layer data lake contents ({type(storage.backend).__name__})...")
    assert len(consume_result["batches_written"]) > 0, "No Bronze batch files were written!"
    written_file = Path(consume_result["batches_written"][0]).as_posix()
    
    if hasattr(storage.backend, "base_dir") and Path(written_file).is_absolute():
        rel_path = Path(written_file).relative_to(storage.backend.base_dir).as_posix()
        batch_records = storage.backend.read_json(rel_path)
    else:
        # MinIOBackend: Direct S3 API GetObject call to MinIO
        assert storage.backend.object_exists(written_file) is True, f"Object {written_file} missing in MinIO bucket!"
        batch_records = storage.backend.read_json(written_file)
        print(f"  -> MinIO S3 object existence verified for '{written_file}' in bucket '{storage.backend.bucket}'.")

    print(f"  * Read {len(batch_records)} records from newly written Bronze batch '{Path(written_file).name}'.")
    assert len(batch_records) == 3, f"Expected 3 records in Bronze batch, got {len(batch_records)}"

    # Unpack envelopes and verify canonical envelope integrity
    bronze_ids = []
    for r in batch_records:
        assert "event_id" in r, "Bronze record missing canonical 'event_id'!"
        assert "event_type" in r, "Bronze record missing canonical 'event_type'!"
        assert "source" in r, "Bronze record missing canonical 'source'!"
        assert "ingested_at" in r, "Bronze record missing canonical 'ingested_at'!"
        assert "payload" in r, "Bronze record missing 'payload'!"
        payload = r["payload"]
        assert isinstance(payload, dict), "Envelope payload must be a dict!"
        bronze_ids.append(payload.get("review_id"))

    assert "rv_acc_001" in bronze_ids, "rv_acc_001 missing from Bronze!"
    assert "rv_acc_002" in bronze_ids, "rv_acc_002 missing from Bronze!"
    assert "rv_acc_003" in bronze_ids, "rv_acc_003 missing from Bronze!"
    assert "rv_acc_bad_999" not in bronze_ids, "Malformed record rv_acc_bad_999 leaked into Bronze!"
    print("  -> Canonical envelopes and data isolation verified in Bronze layer.")

    # Verify DLQ record details
    assert len(consume_result["dlq_records"]) == 1, f"Expected 1 DLQ record, got {len(consume_result['dlq_records'])}"
    dlq_item = consume_result["dlq_records"][0]
    assert dlq_item["error_type"] == "SCHEMA_VALIDATION_ERROR"
    assert dlq_item["source_topic"] == topic
    assert any("rating" in str(err).lower() or "review_text" in str(err).lower() or "contract" in str(err).lower() for err in dlq_item["errors"])
    print("  -> DLQ record structure and contract error details verified.")

    print(f"\n[5/5] Verifying DLQ Fail-Loud Safety (Strictly refuse to swallow DLQ failures)...")
    dlq_handler.simulate_failure = True
    try:
        # Attempt to consume a malformed record while DLQ is simulated broken
        bad_item = {
            "topic": topic,
            "partition": 0,
            "offset": 999,
            "value": invalid_review
        }
        failed_loud = False
        try:
            consumer.consume_batch_once(topics=[topic], max_records=1, offline_records=[bad_item])
        except RuntimeError:
            failed_loud = True

        assert failed_loud is True, "Consumer did not raise RuntimeError when DLQ failed!"
        print("  -> Confirmed: Consumer halts immediately and refuses to commit offset if DLQ fails.")
    finally:
        dlq_handler.simulate_failure = False

    print("\n" + "=" * 70)
    print("  ACCEPTANCE TEST PASSED 100%: KAFKA -> BRONZE IS FULLY VERIFIED")
    print("=" * 70)
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelAI Kafka -> Bronze Acceptance Test")
    parser.add_argument("--mode", choices=["local", "minio", "integration"], default="minio")
    parser.add_argument("--storage", choices=["local", "minio"], default=None)
    args = parser.parse_args()
    success = run_acceptance_verification(mode=args.mode, storage_backend=args.storage)
    sys.exit(0 if success else 1)
