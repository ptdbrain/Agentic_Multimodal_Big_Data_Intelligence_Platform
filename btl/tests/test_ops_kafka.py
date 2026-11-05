import time
import pytest
from operations.kafka_lifecycle import TopicRegistry, cleanup_plan, apply_cleanup

pytestmark = pytest.mark.unit


class Broker:
    def __init__(self):
        self.deleted = []
        self.active = False
        self.lag = 0

    def safe(self, topic, group):
        return not self.active and self.lag == 0

    def delete(self, topic, group):
        self.deleted.append((topic, group))


def complete(reg, topic="raw.articles.run"):
    reg.register(topic, "run", "web_content", "group", now=0)
    reg.finish(topic, bronze=[{"path": "bronze/file", "sha256": "abc"}], received=2, accepted=2, quarantined=0, now=1)


def test_cleanup_rechecks_liveness_and_never_touches_unknown_or_canonical():
    reg = TopicRegistry("test")
    complete(reg)
    broker = Broker()
    plan = cleanup_plan(reg, broker, lambda proof: True, now=200000)
    assert len(plan["candidates"]) == 1
    broker.active = True
    with pytest.raises(ValueError):
        apply_cleanup(reg, broker, lambda proof: True, plan, now=200001)
    assert not broker.deleted
    broker.active = False
    apply_cleanup(reg, broker, lambda proof: True, plan, now=200002)
    assert broker.deleted == [("raw.articles.run", "group")]
    assert cleanup_plan(reg, broker, lambda proof: True, now=200003)["candidates"] == []
    with pytest.raises(ValueError):
        reg.register("raw.articles", "x", "web_content", "g")
    reg.close()


def test_unverified_failed_active_and_corrupt_evidence_are_retained():
    reg = TopicRegistry("test")
    complete(reg)
    broker = Broker()
    assert not cleanup_plan(reg, broker, lambda proof: False, now=200000)["candidates"]
    broker.lag = 1
    assert not cleanup_plan(reg, broker, lambda proof: True, now=200000)["candidates"]
    broker.lag = 0
    reg.register("raw.articles.second", "r2", "web_content", "g2", now=0)
    assert len(cleanup_plan(reg, broker, lambda proof: True, now=200000)["candidates"]) == 1
    with pytest.raises(ValueError):
        reg.register("raw.articles.run", "other", "web_content", "group")
    with pytest.raises(ValueError):
        reg.register("raw.articles.run", "run", "web_content", "group")
    reg.close()


def test_stale_and_tampered_plans_are_rejected():
    reg = TopicRegistry("test")
    complete(reg)
    broker = Broker()
    plan = cleanup_plan(reg, broker, lambda proof: True, now=200000)
    with pytest.raises(ValueError):
        apply_cleanup(reg, broker, lambda proof: True, plan, now=204000)
    plan["candidates"][0]["topic"] = "raw.reviews"
    with pytest.raises(ValueError):
        apply_cleanup(reg, broker, lambda proof: True, plan, now=200001)
    assert not broker.deleted
    reg.close()


def test_failed_topic_needs_reviewed_durable_archive_and_seven_days():
    reg = TopicRegistry("test")
    broker = Broker()
    reg.register("raw.articles.failed", "failed", "web_content", "g", now=0)
    reg.fail("raw.articles.failed")
    assert not cleanup_plan(reg, broker, lambda proof: True, now=time.time() + 8 * 86400)["candidates"]
    proofs = [{"path": kind, "sha256": "abc", "kind": kind} for kind in ["bronze", "quality", "quarantine"]]
    reg.archive_failure("raw.articles.failed", proofs, "review-ticket-1", lambda proof: True, now=0)
    assert not cleanup_plan(reg, broker, lambda proof: True, now=6 * 86400)["candidates"]
    assert len(cleanup_plan(reg, broker, lambda proof: True, now=8 * 86400)["candidates"]) == 1
    reg.close()


def test_real_adapter_accepts_empty_partition_and_checks_group_delete_result():
    from types import SimpleNamespace
    from kafka.structs import TopicPartition
    from operations.kafka_lifecycle import KafkaMaintenance

    broker = KafkaMaintenance.__new__(KafkaMaintenance)
    partition = TopicPartition("raw.articles.test", 0)
    broker.admin = SimpleNamespace(
        describe_groups=lambda groups: {"g": {"group_state": "Empty", "members": []}},
        list_group_offsets=lambda group: {"g": {}},
        list_topics=lambda: [],
        delete_groups=lambda groups: {"g": "OK"},
    )
    broker.consumer = SimpleNamespace(partitions_for_topic=lambda topic: {0}, end_offsets=lambda parts: {partition: 0})
    assert broker.safe(partition.topic, "g")
    broker.delete(partition.topic, "g")
    broker.admin.delete_groups = lambda groups: {"g": "GroupAuthorizationFailedError"}
    with pytest.raises(RuntimeError):
        broker.delete(partition.topic, "g")
