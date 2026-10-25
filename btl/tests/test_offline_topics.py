import pytest
from kafka.topics.topic_manager import TopicManager

pytestmark = pytest.mark.unit


def test_local_topics_do_not_attempt_network(monkeypatch):
    import kafka.admin
    def forbid(**kwargs):
        raise AssertionError('Local mode attempted real Kafka bootstrap')
    monkeypatch.setattr(kafka.admin,'KafkaAdminClient',forbid)
    result=TopicManager.ensure_topics(mode='local')
    assert result['status'] == 'simulated'
    assert result.get('transport') == 'offline'
