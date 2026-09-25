import pytest
import sys
import shutil
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ingestion.file_loader.loader import FileLoader
from ingestion.api.collector import APICollector
from ingestion.crawler.crawler import WebCrawler
from kafka.producers.replay_engine import ReplayEngine
from kafka.consumers.raw_consumer import RawConsumer

def test_file_loader_csv_json_parquet():
    sample_dir = REPO_ROOT / "data" / "sample"
    # Test CSV
    df_csv = FileLoader.load_file(sample_dir / "products.csv")
    assert len(df_csv) > 0
    assert "product_id" in df_csv.columns

    # Test JSON
    df_json = FileLoader.load_file(sample_dir / "products.json")
    assert len(df_json) == len(df_csv)

    # Test Parquet
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
