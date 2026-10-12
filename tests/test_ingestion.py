import pytest
import sys
import shutil
import hashlib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ingestion.file_loader.loader import FileLoader
from ingestion.api.collector import APICollector
from ingestion.crawler.crawler import WebCrawler
from kafka.producers.stream_producer import StreamProducer
from kafka.consumers.raw_consumer import RawConsumer

pytestmark = pytest.mark.unit


def test_file_loader_csv_json_parquet():
    sample_dir = REPO_ROOT / "data" / "sample"
    df_csv = FileLoader.load_file(sample_dir / "products.csv")
    assert len(df_csv) > 0
    assert "product_id" in df_csv.columns

    df_json = FileLoader.load_file(sample_dir / "products.json")
    assert len(df_json) == len(df_csv)

    df_pq = FileLoader.load_file(sample_dir / "products.parquet")
    assert len(df_pq) == len(df_csv)


def test_api_collector_pagination():
    collector = APICollector(rate_limit_rps=100.0)
    page1 = collector.fetch_page(page=1, page_size=20)
    assert len(page1["data"]) == 20
    assert page1["has_next"] is True


def test_web_crawler_scrape():
    crawler = WebCrawler()
    scraped = crawler.scrape_product_page("iphone_15_pro")
    assert scraped["product_id"] == "iphone_15_pro"
    assert "stock_status" in scraped


def test_raw_consumer_partitioning():
    test_dir = REPO_ROOT / "storage" / "test_bronze_tmp"
    test_dir.mkdir(parents=True, exist_ok=True)
    try:
        consumer = RawConsumer(bronze_dir=test_dir)
        sample_records = [{"id": 1, "test": "data"}]
        out_file = consumer.save_raw_batch("test.topic", sample_records)
        assert out_file.exists()
        assert "year=" in str(out_file)
    finally:
        if test_dir.exists():
            shutil.rmtree(test_dir)


def test_producer_envelope_structure_event_id_and_partition_key_rules():
    producer = StreamProducer(bootstrap_servers="offline:9092", enable_offline_buffer=True)
    review_payload = {
        "review_id": "rv_1001",
        "product_id": "iphone_15_pro",
        "user_id": "anon_0123456789abcdef",
        "rating": 5.0,
        "review_title": "Excellent",
        "review_text": "Fast performance",
        "review_date": "2026-10-01T09:30:00Z",
    }

    # 1. produce_batch resolves partition key from product_id by default
    sent, _ = producer.produce_batch(
        "raw.reviews",
        [review_payload],
        rate=0,
        event_type="NEW_REVIEW",
        source="unit_producer_test",
    )
    assert sent == 1
    assert len(producer.virtual_queue) == 1

    queued = producer.virtual_queue[0]
    assert queued["topic"] == "raw.reviews"
    assert queued["key"] == "iphone_15_pro"

    envelope = queued["value"]
    assert set(envelope.keys()) == {
        "event_id",
        "event_type",
        "schema_version",
        "source",
        "event_time",
        "ingested_at",
        "payload",
    }
    assert envelope["event_type"] == "NEW_REVIEW"
    assert envelope["schema_version"] == "1.0"
    assert envelope["source"] == "unit_producer_test"
    assert envelope["event_time"] == "2026-10-01T09:30:00Z"
    assert envelope["payload"] == review_payload

    # 2. Verify deterministic event_id rule: evt_<sha256(source|key|event_time)[:24]>
    expected_hash = hashlib.sha256(
        "unit_producer_test|iphone_15_pro|2026-10-01T09:30:00Z".encode("utf-8")
    ).hexdigest()[:24]
    assert envelope["event_id"] == f"evt_{expected_hash}"

    # 3. Verify explicit key_field override (e.g. key_field="review_id")
    producer.clear_virtual_queue()
    producer.produce_batch(
        "raw.reviews",
        [review_payload],
        rate=0,
        key_field="review_id",
        event_type="NEW_REVIEW",
        source="unit_producer_test",
    )
    assert producer.virtual_queue[0]["key"] == "rv_1001"
