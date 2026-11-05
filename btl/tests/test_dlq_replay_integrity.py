"""Replay must inspect actual quarantine shapes and never claim buffered delivery."""
import pandas as pd
import pytest
from config.settings import settings
from storage.storage_manager import storage
from scripts.replay_dlq import replay_dlq_messages

pytestmark = pytest.mark.unit


def review():
    return {'review_id':'r1','product_id':'p','user_id':'anon_redacted','rating':5,'review_title':'Fixture',
            'review_text':'Explicit synthetic recovery fixture','review_date':'2026-11-01T00:00:00Z'}


def test_local_replay_cannot_report_success_without_a_broker():
    storage.write_json('quarantine/fixed.json',[review()])
    with pytest.raises(ValueError,match='integration'):
        replay_dlq_messages(dry_run=False)


def test_dry_run_reads_nested_durable_dlq_and_actual_silver_parquet_quarantine():
    price = {'price_id':'pr','product_id':'p','price':10,'currency':'VND','timestamp':'2026-11-01T00:00:00Z'}
    storage.write_json('bronze/dead-letter/fixed.json',[{'source_topic':settings.kafka.topic_prices,
                       'payload':{'event_id':'e','payload':price}}])
    storage.write_parquet('silver/quarantine/reviews/part-000.parquet',pd.DataFrame([review()]))
    result = replay_dlq_messages(dry_run=True)
    assert result['inspected'] == 2
    assert result['eligible'] == 2
    assert result['replayed'] == 0
    assert result['eligible_by_topic'] == {settings.kafka.topic_prices:1,settings.kafka.topic_reviews:1}


def test_domain_invalid_timestamp_does_not_become_replay_eligible():
    storage.write_json('quarantine/bad.json',[dict(review(),review_date='unparseable')])
    result = replay_dlq_messages(dry_run=True)
    assert result['inspected'] == 1
    assert result['eligible'] == 0
    assert result['failed'] == 1


def test_integration_replay_surfaces_broker_failure_instead_of_offline_success(monkeypatch):
    from kafka.producers.stream_producer import StreamProducer
    storage.write_json('quarantine/fixed.json',[review()])
    settings.set_runtime_mode('integration')

    def offline_or_outage(self):
        if not self.enable_offline_buffer:
            raise RuntimeError('Injected broker unavailable')

    monkeypatch.setattr(StreamProducer,'_init_producer',offline_or_outage)
    with pytest.raises(RuntimeError,match='broker unavailable'):
        replay_dlq_messages(dry_run=False)
