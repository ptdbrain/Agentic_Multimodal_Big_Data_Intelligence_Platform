import pytest
from ingestion.crawler.tiki import TikiCrawler
from ingestion.crawler.validator import CrawlerDataValidator
from spark.etl.normalizer import EntityNormalizer

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('rating', [None, 0, 99, -1, float('nan'), True, 'not numeric'])
def test_invalid_rating_never_becomes_valid(rating):
    r=TikiCrawler().standardize_review({'id':1,'rating':rating,'content':'actual','created_at':'2026-10-01T00:00:00Z'},'tiki_1')
    assert CrawlerDataValidator.validate_review(r)[0] is False


def test_missing_content_is_not_generated():
    r=TikiCrawler().standardize_review({'id':1,'rating':5},'tiki_1')
    assert not r['review_text']
    assert r['verified_purchase'] is False
    assert CrawlerDataValidator.validate_review(r)[0] is False


@pytest.mark.parametrize('raw', [{'purchased':False},{'status':'approved'},{'purchased':'false'}])
def test_purchase_requires_explicit_true(raw):
    assert TikiCrawler().standardize_review(dict(raw,id=1),'tiki_1')['verified_purchase'] is False


def test_price_parser_retains_negative_sign():
    assert EntityNormalizer.normalize_price('-100 VND') == -100.0
    assert EntityNormalizer.normalize_price('19.990.000 VND') == 19990000
