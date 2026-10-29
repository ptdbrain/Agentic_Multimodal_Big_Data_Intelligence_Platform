"""Public-network capability check, separate from deterministic CI gates."""
import pytest
from ingestion.crawler.tiki import TikiCrawler

pytestmark=[pytest.mark.integration,pytest.mark.live]


def test_live_tiki_returns_observed_products_without_synthetic_fallback():
    crawler=TikiCrawler(allow_public_fallback=False)
    result=crawler.crawl_category('smartphone',max_products=1,reviews_per_product=1)
    assert result['products'], 'Live Tiki returned no products; source may be blocked/unavailable'
    assert all(p['source']=='tiki_crawler' for p in result['products'])
    assert all(not p['product_id'].startswith('mock_') for p in result['products'])
