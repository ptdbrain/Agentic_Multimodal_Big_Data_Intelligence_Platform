"""Verify maintenance and backup decisions against offsets accepted by a real broker."""
from contextlib import ExitStack
from uuid import uuid4

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import OffsetAndMetadata, TopicPartition

from config.settings import settings
from operations.backup import kafka_drain_state
from operations.kafka_lifecycle import KafkaMaintenance


@pytest.mark.integration
def test_backup_and_cleanup_require_exact_partition_end():
    servers = settings.kafka.bootstrap_servers
    topic = f"sentinel.test.drain.{uuid4().hex}"
    group = f"{topic}.reader"
    partition = TopicPartition(topic, 0)

    with ExitStack() as stack:
        admin = KafkaAdminClient(bootstrap_servers=servers)
        stack.callback(admin.close)
        admin.create_topics([NewTopic(topic, 1, 1)], wait_for_metadata=True)
        stack.callback(admin.delete_topics, [topic])
        writer = KafkaProducer(bootstrap_servers=servers)
        stack.callback(writer.close)
        broker = KafkaMaintenance(servers)
        stack.callback(broker.close)
        reader = KafkaConsumer(bootstrap_servers=servers, group_id=group, enable_auto_commit=False)
        stack.callback(admin.delete_groups, [group])
        stack.callback(reader.close)
        reader.assign([partition])

        def check(committed, end, expected):
            reader.commit({partition: OffsetAndMetadata(committed, "")})
            assert reader.end_offsets([partition])[partition] == end
            assert reader.committed(partition) == committed
            can_clean = broker.safe(topic, group)
            try:
                offsets = kafka_drain_state(servers, [group], [topic])
            except ValueError as error:
                assert f"committed={committed}, end={end}" in str(error)
                can_back_up = False
            else:
                assert offsets == {f"{group}/{topic}/0": {"committed": committed, "end": end}}
                can_back_up = True
            assert (can_clean, can_back_up) == (expected, expected), (committed, end)

        check(0, 0, True)
        check(1, 0, False)
        writer.send(topic, value=b"drain probe", partition=0).get(timeout=10)
        check(0, 1, False)
        check(2, 1, False)
        check(1, 1, True)
