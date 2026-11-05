import pytest
from ingestion.crawler.mock import MockEcommerceCrawler

pytestmark = pytest.mark.unit


def test_lazada_preserves_missing_facts_and_namespaces_identity():
    from ingestion.crawler.lazada import LazadaCrawler
    crawler = LazadaCrawler()
    row = crawler.standardize_product({'itemId': '123', 'name': 'Phone', 'price': '0', 'priceShow': '₫0', 'categories': [12]})
    assert row['product_id'] == 'lazada_123'
    assert row['price'] == 0
    assert row['currency'] == 'VND'
    assert row.get('rating') is None
    assert row.get('review_count') is None
    assert row.get('brand') is None
    assert row['source_group'] == 'marketplace'
    assert row['acquisition_method'] == 'live'


def test_lazada_missing_or_invalid_id_fails():
    from ingestion.crawler.lazada import LazadaCrawler
    with pytest.raises(ValueError):
        LazadaCrawler().standardize_product({'name': 'Phone'})


def test_base_does_not_invent_price_when_absent(monkeypatch):
    crawler = MockEcommerceCrawler()
    monkeypatch.setattr(crawler, 'search_products', lambda *a, **k: [{'product_id': 'test', 'product_name': 'P'}])
    monkeypatch.setattr(crawler, 'standardize_product', lambda x: x)
    assert crawler.crawl_category('q', reviews_per_product=0)['prices'] == []


def test_listing_price_observation_uses_collection_not_catalog_update(monkeypatch):
    crawler = MockEcommerceCrawler()
    value = {'product_id': 'p', 'price': 42, 'currency': 'VND', 'updated_at': '2000-01-01', 'collected_at': '2026-11-02T00:00:00Z'}
    monkeypatch.setattr(crawler, 'search_products', lambda *a, **k: [value])
    monkeypatch.setattr(crawler, 'standardize_product', lambda x: x)
    row = crawler.crawl_category('q', reviews_per_product=0)['prices'][0]
    assert row['timestamp'] == value['collected_at']
    assert row.get('seller') is None
