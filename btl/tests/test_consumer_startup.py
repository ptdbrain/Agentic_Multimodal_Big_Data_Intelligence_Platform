"""Unit protocol model for coordinator delay; no real-broker claim is made here."""

from types import SimpleNamespace
import pytest

pytestmark = pytest.mark.unit


def test_fetch_budget_starts_after_assignment_and_preserves_first_records(tmp_path, monkeypatch):
    from kafka.structs import TopicPartition
    from kafka.consumers import raw_consumer

    clock = [0.0]
    tp = TopicPartition('raw.reviews', 0)
    envelope = {'event_id':'assignment-fixture', 'event_type':'NEW_REVIEW',
                'source':'explicit_protocol_fixture', 'ingested_at':'2026-11-19T00:00:00Z',
                'payload':{'review_id':'r','product_id':'p','user_id':'anon_redacted','rating':4,
                           'review_title':'Protocol fixture',
                           'review_text':'Explicit generated protocol fixture','review_date':'2026-11-19T00:00:00Z'}}
    class DelayedCoordinator:
        def __init__(self):
            self.delivered = False
            self.committed = {}
        def assignment(self):
            return {tp} if clock[0] >= 6 else set()
        def poll(self, **kwargs):
            clock[0] += 1
            if self.assignment() and not self.delivered:
                self.delivered = True
                record = SimpleNamespace(topic=tp.topic,partition=0,offset=0,value=envelope,headers=[],timestamp=1795046400000)
                return {tp:[record]}
            return {}
        def end_offsets(self, partitions):
            return {tp:1}
        def position(self, partition):
            return 1 if self.delivered else 0
        def commit(self, offsets):
            self.committed.update(offsets)
        def seek(self, partition, offset):
            pass
    # Only the broker protocol clock/client is controlled. Validation, Bronze
    # serialization and offset-after-durability logic are the production code.
    monkeypatch.setattr(raw_consumer,'time',SimpleNamespace(
        time=lambda:clock[0],monotonic=lambda:clock[0],perf_counter=lambda:clock[0]))
    consumer=raw_consumer.RawConsumer(bronze_dir=tmp_path/'bronze')
    consumer.consumer=DelayedCoordinator()
    result=consumer.consume_batch_once(topics=[tp.topic],max_records=1,timeout_ms=1000)
    assert result['records_consumed']==result['records_valid']==1
    assert result['records_dlq']==0
    assert consumer.consumer.committed[tp].offset==1
    assert result['batches_written']


def test_unassigned_consumer_has_a_bounded_startup_budget(monkeypatch):
    from kafka.consumers import raw_consumer

    clock=[0.0]
    class Unassigned:
        def assignment(self):
            return set()
        def poll(self, **kwargs):
            clock[0]+=1
            return {}
    monkeypatch.setattr(raw_consumer,'time',SimpleNamespace(
        time=lambda:clock[0],monotonic=lambda:clock[0],perf_counter=lambda:clock[0]))
    consumer=raw_consumer.RawConsumer()
    consumer.consumer=Unassigned()
    result=consumer.consume_batch_once(max_records=1,timeout_ms=1000,startup_timeout_ms=3000)
    assert result['records_consumed']==0 and not result['batches_written']
    assert clock[0]==3


@pytest.mark.parametrize('scoped', [False, True])
def test_configured_topic_contract_takes_priority_over_namespace_keywords(monkeypatch, scoped):
    from config.settings import settings
    from kafka.consumers.raw_consumer import RawConsumer

    if scoped:
        monkeypatch.setattr(settings.kafka,'topic_articles','products-review-news')
        topic,expected='products-review-news.run-123','article'
    else:
        monkeypatch.setattr(settings.kafka,'topic_reviews','products-brand-reviews')
        topic,expected='products-brand-reviews','review'
    assert RawConsumer()._resolve_schema_name(topic)==expected
