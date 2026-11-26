"""Verify the real Kafka to PostgreSQL to HTTP price alert path in an isolated schema."""
from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
from threading import Thread
import time
from uuid import uuid4

import pytest

from config.settings import settings
from scripts.run_price_alert_stream import consume_prices
from scripts.verify_operations_stack import webhook_server
from serving.notifications import deliver_notifications
from serving.registry import DatasetRef
from serving.store import ServingStore, connect_dataset

pytestmark = pytest.mark.integration


def test_price_alert_replay_keeps_one_delivery_per_rule(tmp_path):
    import psycopg2
    from psycopg2 import sql
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.producers.stream_producer import StreamProducer
    from operations.migration import read_kafka_cluster_id

    tag = uuid4().hex[:14]
    topic = "price-alert-integration-" + tag
    schema = "price_alert_" + tag
    db = settings.database
    params = dict(host=db.host, port=db.port, user=db.user, password=db.password, dbname=db.database)
    servers = settings.kafka.bootstrap_servers
    binding = DatasetRef(
        id="price-alert-" + tag, label="Labeled integration inputs", kind="commerce", source_group="simulation",
        schema=schema, bucket="simulation", mode="integration",
        connection={**params, "options": "-c search_path=" + schema},
    )
    checked_at = datetime.now(timezone.utc)
    # Keep the candidate on the current UTC date just after midnight.
    clock = max(checked_at - timedelta(seconds=1), checked_at.replace(hour=0, minute=0, second=0, microsecond=0))
    with closing(psycopg2.connect(**params)) as control, KafkaAdminClient(bootstrap_servers=servers) as admin:
        control.autocommit = True
        control.cursor().execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)], wait_for_metadata=True)
            producer = StreamProducer()
            start = time.monotonic()
            try:
                for n, (days, value) in enumerate([(3, 100), (2, 100), (1, 100), (0, 250)]):
                    record = dict(price_id=f"fixture-{n}", product_id="phone", seller="fixture seller",
                                  currency="VND", price=value, timestamp=(clock - timedelta(days=days)).isoformat(),
                                  source_group="simulation", acquisition_method="generated")
                    producer.send_message(topic, "phone", record, event_type="PRICE_UPDATE")
            finally:
                producer.close()

            def receive(consumer_group):
                with closing(KafkaConsumer(
                    topic, bootstrap_servers=servers, group_id=consumer_group, enable_auto_commit=False,
                    auto_offset_reset="earliest",
                )) as consumer:
                    counts = consume_prices(binding, consumer, read_kafka_cluster_id(servers) + ":" + topic, 10)
                    assert sum(counts.values()) == 4
                    assert consumer.committed(TopicPartition(topic, 0)) == 4
                    return counts

            assert receive(topic) == {"historical_observation": 3, "evaluated": 1}
            visibility = time.monotonic() - start
            with closing(connect_dataset(binding)) as db:
                store = ServingStore(db, binding.id, binding.source_group)
                metrics = store.execute(
                    "SELECT event_type,state,revision FROM serving_alerts ORDER BY event_type"
                ).fetchall()
                assert metrics == [("PRICE_ANOMALY", "firing", 1), ("PRICE_IQR_OUTLIER", "firing", 1)]
            assert receive(topic + "-replay") == {"historical_observation": 3, "evaluated": 1}

            receiver_file = tmp_path / "notices.jsonl"
            with webhook_server(receiver_file, port=0) as server:
                receiver = Thread(target=server.serve_forever, daemon=True)
                receiver.start()
                try:
                    url = f"http://127.0.0.1:{server.server_port}/alerts"
                    assert deliver_notifications(binding, url=url) == 2
                    assert deliver_notifications(binding, url=url) == 0
                finally:
                    server.shutdown()
                    receiver.join(timeout=4)
            assert len(receiver_file.read_text().splitlines()) == 2
            print(json.dumps({"kafka_group_offsets": [4, 4], "sql_alerts": [row[0] for row in metrics],
                              "webhook_deliveries": 2, "visibility_seconds_including_receive_window": visibility}))
        finally:
            control.cursor().execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
