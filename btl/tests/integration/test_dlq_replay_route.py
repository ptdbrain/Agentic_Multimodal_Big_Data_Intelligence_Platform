"""Actual Kafka ACK and Bronze recovery, using isolated synthetic source data."""
import uuid
import pytest
from config.settings import settings
from storage.storage_manager import storage
from kafka.consumers.raw_consumer import RawConsumer
from kafka.topics.topic_manager import TopicManager
from scripts.replay_dlq import replay_dlq_messages

pytestmark = pytest.mark.integration


def test_repaired_lake_dlq_records_are_actually_delivered_and_routed():
    suffix = uuid.uuid4().hex[:12]
    settings.storage.data_lake_bucket = 'dlq-replay-' + suffix
    for dataset in ('products','reviews','prices','events','articles'):
        setattr(settings.kafka,'topic_' + dataset,'dlq-replay-' + suffix + '-' + dataset)
    storage.set_mode('integration')
    TopicManager.ensure_topics(mode='integration')
    provenance = {'source_group':'simulation','acquisition_method':'generated','source':'recovery_acceptance_fixture'}
    price = dict(provenance,price_id='pr',product_id='p',price=100,currency='VND',timestamp='2026-11-01T00:00:00Z')
    review = dict(provenance,review_id='r',product_id='p',user_id='raw-recovery-user',email='private@example.invalid',
                  rating=5,review_title='Synthetic recovery fixture',review_text='Explicit recovery acceptance observation',
                  review_date='2026-11-01T00:00:00Z')
    storage.write_json('bronze/dead-letter/repaired.json',[
        {'payload':{'event_id':'old-price','payload':price}},
        {'payload':{'event_id':'old-review','payload':review}},
    ])
    report = replay_dlq_messages(dry_run=False)
    assert report['eligible'] == report['replayed'] == 2
    assert report['transport'] == 'real_kafka'
    consumer = RawConsumer(group_id='dlq-verify-' + suffix)
    try:
        result = consumer.consume_batch_once(topics=[settings.kafka.topic_reviews,settings.kafka.topic_prices],
                                             max_records=2,timeout_ms=4000)
    finally:
        consumer.close()
    assert result['records_valid'] == result['records_consumed'] == 2
    assert result['records_dlq'] == 0
    by_topic = {topic:[envelope['payload'] for path in paths for envelope in storage.read_json(path)]
                for topic,paths in result['batches_by_topic'].items()}
    assert by_topic[settings.kafka.topic_prices][0]['price_id'] == 'pr'
    observed = by_topic[settings.kafka.topic_reviews][0]
    assert observed['review_id'] == 'r'
    assert observed['user_id'].startswith('anon_') and observed['user_id'] != 'raw-recovery-user'
    assert 'email' not in observed
