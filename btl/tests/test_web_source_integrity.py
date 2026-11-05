"""Regression contracts: missing web observations must never become facts."""
import json
import pytest
from ingestion.crawler.generic_html import UniversalProductScraper

pytestmark = pytest.mark.unit


def html(value):
    return '<script type="application/ld+json">' + json.dumps(value) + '</script>'


def test_opengraph_does_not_invent_business_fields():
    result = UniversalProductScraper()._extract_metadata_from_html(
        '<meta property="og:title" content="Phone">', 'https://shop.example/p/1')
    for field in ('rating', 'review_count', 'price', 'currency', 'category', 'brand', 'created_at', 'updated_at'):
        assert result.get(field) is None, field
    assert result['collected_at']


def test_reviews_do_not_invent_rating_time_or_verification():
    rows = UniversalProductScraper().extract_reviews_from_html(html({
        '@type': 'Product', 'review': [{'reviewBody': 'Actual text'}]}), 'p1')
    assert len(rows) == 1
    for field in ('rating', 'review_date', 'verified_purchase', 'helpful_count', 'language', 'created_at'):
        assert rows[0].get(field) is None, field


def test_review_ids_stable_when_page_order_changes():
    scraper = UniversalProductScraper()
    a, b = {'reviewBody': 'First'}, {'reviewBody': 'Second'}
    before = scraper.extract_reviews_from_html(html({'review': [a, b]}), 'p')
    after = scraper.extract_reviews_from_html(html({'review': [b, a]}), 'p')
    assert {r['review_text']: r['review_id'] for r in before} == {r['review_text']: r['review_id'] for r in after}


def test_jsonld_graph_preserves_observed_zero_and_false():
    value = {'@graph': [{'@type': ['Product'], 'name': 'Phone', 'offers': {
        'price': 0, 'priceCurrency': 'USD'}, 'aggregateRating': {'ratingValue': 0, 'reviewCount': 0},
        'review': [{'reviewBody': 'Observed', 'reviewRating': {'ratingValue': 1},
                    'verified_purchase': 'false', 'datePublished': '2026-10-01'}]}]}
    scraper = UniversalProductScraper()
    product = scraper._extract_metadata_from_html(html(value), 'https://shop.example/p')
    assert product['price'] == 0
    assert product['rating'] == 0
    review = scraper.extract_reviews_from_html(html(value), 'p')[0]
    assert review['verified_purchase'] is False
    assert review['review_date'] == '2026-10-01'


def test_nonproduct_page_not_accepted_as_product():
    assert UniversalProductScraper()._extract_metadata_from_html('<title>Access denied</title>', 'https://shop.example/') == {}


def test_bad_review_does_not_discard_other_reviews():
    rows = UniversalProductScraper().extract_reviews_from_html(html({'review': [
        {'reviewBody': 'Bad numeric', 'reviewRating': {'ratingValue': 'bad'}},
        {'reviewBody': 'Good', 'reviewRating': {'ratingValue': 4}}]}), 'p')
    assert {r['review_text'] for r in rows} == {'Bad numeric', 'Good'}
    assert rows[0]['rating_raw'] == 'bad'
    assert rows[0].get('rating') is None


def test_same_sku_on_different_domains_has_distinct_identity():
    value = html({'@type': 'Product', 'sku': '123', 'name': 'Phone'})
    scraper = UniversalProductScraper()
    assert scraper._extract_metadata_from_html(value, 'https://one.example/p')['product_id'] != scraper._extract_metadata_from_html(value, 'https://two.example/p')['product_id']


def test_tiki_cannot_enable_synthetic_fallback():
    from ingestion.crawler.tiki import TikiCrawler
    with pytest.raises(ValueError, match='MockEcommerceCrawler'):
        TikiCrawler(allow_public_fallback=True)


def test_tiki_product_preserves_zero_and_does_not_fill_unobserved_values():
    from ingestion.crawler.tiki import TikiCrawler
    scraper = TikiCrawler()
    row = scraper.standardize_product({'id':123, 'name':'Observed', 'price':0, 'original_price':100})
    assert row['price'] == 0
    for key in ('brand','category','rating','review_count','seller','created_at','updated_at'):
        assert row.get(key) is None, key
    missing = scraper.standardize_product({'id':123,'name':'Observed','original_price':100})
    assert missing.get('price') is None


def test_tiki_negative_price_not_clamped_to_zero():
    from ingestion.crawler.tiki import TikiCrawler
    row = TikiCrawler().standardize_product({'id':123,'name':'Observed','price':-10})
    assert row['price'] == -10


def test_tiki_review_missing_verification_and_helpfulness_remain_unknown():
    from ingestion.crawler.tiki import TikiCrawler
    row = TikiCrawler().standardize_review({'id':123,'product_id':456,'rating':5,'content':'Observed'})
    assert row.get('verified_purchase') is None
    assert row.get('helpful_count') is None
    assert row.get('language') is None
