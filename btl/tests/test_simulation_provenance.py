import pytest
from data.generator import generate_products, generate_reviews, generate_price_history, generate_streaming_events
from scripts.simulate_dataset_ingestion import scale_dataset_volume

pytestmark = pytest.mark.unit


def test_generators_explicitly_label_all_generated_sources_and_seed():
    products = generate_products(seed=7)
    groups = [products, generate_reviews(products, count=4, inject_anomalies=False, seed=7),
              generate_price_history(products, days_history=1, seed=7), generate_streaming_events(products, count=4, seed=7)]
    for rows in groups:
        assert rows
        for row in rows:
            assert row['source_group'] == 'simulation'
            assert row['acquisition_method'] == 'generated'
            assert row['generator_seed'] == '7'
            assert row['source'] == 'synthetic_generator'
    assert generate_products(seed=7) == products


def test_scaled_real_template_cannot_keep_live_evidence_or_identity():
    original = {'review_id':'tiki_rv_123','product_id':'tiki_1','rating':5,'source':'tiki_crawler',
                'source_group':'marketplace','acquisition_method':'live','evidence_sha256':'abc','source_url':'https://tiki.vn/'}
    rows = scale_dataset_volume([original], 2, 'review_id', 'review', inject_dirty_ratio=0)
    assert all(row['source_group'] == 'simulation' and row['acquisition_method'] == 'generated' for row in rows)
    assert all(row['review_id'].startswith('review_sim_') for row in rows)
    assert all('evidence_sha256' not in row for row in rows)
    assert original['acquisition_method'] == 'live'


def test_invalid_dirty_ratio_is_reported_before_generation():
    with pytest.raises(ValueError):
        scale_dataset_volume([{'review_id':'r'}], 4, 'review_id', 'r', inject_dirty_ratio=2)


def test_legacy_webcrawler_needs_explicit_simulation_opt_in():
    from ingestion.crawler.crawler import WebCrawler
    with pytest.raises(ValueError, match='simulation'):
        WebCrawler()


def test_mock_public_page_and_api_records_are_labelled_generated():
    from ingestion.crawler.mock import MockEcommerceCrawler
    from ingestion.api.collector import APICollector
    row = MockEcommerceCrawler(seed=7).scrape_product_page('fixture')
    assert row['source_group'] == 'simulation'
    assert row['acquisition_method'] == 'generated'
    rows = APICollector(mock_mode=True, total_items=2, rate_limit_rps=0).fetch_page(page_size=2)['data']
    assert all(row['source_group'] == 'simulation' and row['acquisition_method'] == 'generated' for row in rows)
