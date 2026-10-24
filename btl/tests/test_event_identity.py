import pytest
from kafka.producers.stream_producer import StreamProducer
from ingestion.crawler.tiki import TikiCrawler

pytestmark = pytest.mark.unit


def event(payload, key='p1'):
    p = StreamProducer(enable_offline_buffer=True)
    return p.send_message('raw.reviews',key,payload,event_type='NEW_REVIEW')[0]['event_id']


def test_review_identity_is_not_partition_identity():
    a={'review_id':'r1','product_id':'p1','review_date':'2026-10-01T00:00:00Z','rating':1}
    b=dict(a,review_id='r2')
    assert event(a) != event(b)
    assert event(a) == event(a, key='different-route')
    assert event(a) != event(dict(a,rating=5))
    assert event(a) == event(dict(a,review_date='2026-10-01T07:00:00+07:00'))


def test_price_observations_are_stable_and_distinct():
    c=TikiCrawler()
    a={'product_id':'tiki_1','price':100,'updated_at':'2026-10-01T00:00:00Z','seller':'S'}
    b=dict(a,updated_at='2026-10-02T00:00:00Z')
    assert c.standardize_price(a)['price_id'] != c.standardize_price(b)['price_id']
    assert c.standardize_price(a,index=1)['price_id'] == c.standardize_price(a,index=99)['price_id']
