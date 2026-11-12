"""Kafka migration must preserve source identity and fail closed on offset loss."""
import hashlib
import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def source():
    return {
        'version': 1, 'bootstrap_servers': 'localhost:9094',
        'starting_offsets': {'raw.reviews': {'0': 4}},
        'ending_offsets': {'raw.reviews': {'0': 6}},
        'schema_version': '1.0',
    }


def fixture_plan(tmp_path, monkeypatch):
    from operations import migration
    old = tmp_path / 'old'
    old.mkdir()
    bronze = tmp_path / 'bronze.jsonl'
    rows = []
    for offset in [4, 5]:
        raw = json.dumps({'event_id': f'e{offset}', 'payload': {'review_id': f'r{offset}'}})
        rows.append({'source_topic': 'raw.reviews', 'source_partition': 0,
                     'source_offset': offset, 'raw_value': raw})
    bronze.write_text('\n'.join(json.dumps(row) for row in rows))
    monkeypatch.setattr(migration, 'read_kafka_bounds', lambda _: {
        'cluster_id': 'fixture-cluster',
        'beginning_offsets': {'raw.reviews': {'0': 0}},
        'ending_offsets': {'raw.reviews': {'0': 6}},
    })
    return migration.make_plan(old, tmp_path / 'new', [bronze], writer_pids=[99999999],
                               engine='spark', kafka_source=source())


def test_kafka_plan_pins_source_raw_payloads_and_schema(tmp_path, monkeypatch):
    plan = fixture_plan(tmp_path, monkeypatch)
    assert plan['source_kind'] == 'kafka'
    assert plan['kafka_source']['starting_offsets'] == {'raw.reviews': {'0': 4}}
    assert plan['kafka_source']['ending_offsets'] == {'raw.reviews': {'0': 6}}
    assert plan['kafka_source']['schema_version'] == '1.0'
    assert len(plan['kafka_input_sha256']) == 64
    assert len(plan['kafka_expected']) == 2
    assert plan['kafka_expected'][0]['event_id'] == 'e4'
    assert plan['kafka_expected'][0]['raw_payload_sha256'] == hashlib.sha256(
        json.dumps({'event_id': 'e4', 'payload': {'review_id': 'r4'}}).encode()).hexdigest()


@pytest.mark.parametrize('begin,end,message', [(5, 6, 'Bronze backfill'), (0, 7, 'frontier'), (0, 5, 'frontier')])
def test_offset_loss_and_frontier_drift_fail_explicitly(begin, end, message):
    from operations.migration import validate_kafka_bounds
    with pytest.raises(ValueError, match=message):
        validate_kafka_bounds(source(), {'beginning_offsets': {'raw.reviews': {'0': begin}},
                                        'ending_offsets': {'raw.reviews': {'0': end}}}, frozen=True)


def test_latest_and_incomplete_partition_manifests_are_rejected():
    from operations.migration import validate_kafka_source
    invalid = source()
    invalid['starting_offsets'] = 'latest'
    with pytest.raises(ValueError, match='explicit'):
        validate_kafka_source(invalid)
    invalid = source()
    invalid['ending_offsets']['raw.reviews']['1'] = 9
    with pytest.raises(ValueError, match='partition'):
        validate_kafka_source(invalid)


def test_resume_requires_actual_kafka_checkpoint_and_same_source(tmp_path, monkeypatch):
    from operations import migration
    plan = fixture_plan(tmp_path, monkeypatch)
    destination = Path(plan['destination'])
    destination.mkdir()
    (destination / 'replay.json').write_text(json.dumps({'engine': 'spark', 'source_kind': 'file'}))
    with pytest.raises(ValueError, match='Kafka'):
        migration.resolve_stream_pointer(tmp_path / 'missing', expected_topics='raw.reviews')
    (destination / 'stream-source.json').write_text(json.dumps({'source_kind': 'file'}))
    pointer = tmp_path / 'pointer.json'
    pointer.write_text(json.dumps({'generation': str(destination)}))
    with pytest.raises(ValueError, match='Kafka'):
        migration.resolve_stream_pointer(pointer, expected_topics='raw.reviews')


@pytest.mark.spark
@pytest.mark.integration
def test_actual_kafka_replay_verify_resume_and_rollback(tmp_path, spark_session):
    """Exercise real Kafka offsets; only a fresh UUID topic and temporary lake are written."""
    import os
    import uuid
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from config.settings import settings
    from operations import migration
    from spark.streaming.spark_streaming_job import SparkStreamingJob

    # Kafka is real; output/maintenance storage is intentionally temporary local.
    settings.set_runtime_mode('test')
    topic = 'sentinel.test.checkpoint-migration.' + uuid.uuid4().hex
    servers = os.environ.get('KAFKA_MIGRATION_TEST_BOOTSTRAP', settings.kafka.bootstrap_servers)
    admin = KafkaAdminClient(bootstrap_servers=servers)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    producer = KafkaProducer(bootstrap_servers=servers)
    try:
        rows = []
        for offset, rating in enumerate([5, 1, 9]):
            value = {'event_id': f'e{offset}', 'review_id': f'r{offset}', 'product_id': 'p1',
                     'rating': rating, 'review_text': 'fixture', 'review_date': '2026-11-01T00:00:10Z',
                     'source_group': 'simulation', 'acquisition_method': 'generated'}
            raw = json.dumps(value)
            sent = producer.send(topic, raw.encode()).get(timeout=15)
            rows.append({'source_topic': topic, 'source_partition': sent.partition,
                         'source_offset': sent.offset, 'raw_value': raw})
        producer.flush()
        bronze = tmp_path / 'bronze.jsonl'
        bronze.write_text('\n'.join(json.dumps(r) for r in rows))
        frozen = {'version': 1, 'bootstrap_servers': servers, 'schema_version': '1.0',
                  'starting_offsets': {topic: {'0': 0}}, 'ending_offsets': {topic: {'0': 3}}}
        empty = tmp_path / 'empty'
        empty.mkdir()
        seed = migration.make_plan(empty, tmp_path / 'old', [bronze], writer_pids=[99999999],
                                   engine='spark', kafka_source=frozen)
        original = migration.replay(seed)
        assert original['business']['output'][0]['review_count'] == 2
        old_inventory = migration.inventory(tmp_path / 'old')
        plan = migration.make_plan(tmp_path / 'old/checkpoint', tmp_path / 'new', [bronze],
                                   writer_pids=[99999999], engine='spark', kafka_source=frozen,
                                   old_output=tmp_path / 'old/metrics', old_quarantine=tmp_path / 'old/quarantine')
        migration.replay(plan)
        assert migration.verify(plan)['passed']
        pointer = tmp_path / 'pointer.json'
        pointer.write_text(json.dumps({'generation': str(tmp_path / 'old')}))
        migration.promote(plan, pointer)
        resolved = migration.resolve_stream_pointer(pointer, expected_topics=topic)
        job = SparkStreamingJob(spark_session=spark_session, checkpoint_dir=resolved['checkpoint_dir'],
                                output_path=resolved['output_path'], quarantine_path=resolved['quarantine_path'])
        try:
            query = job.start_review_stream(kafka_source=resolved['kafka_source'], audit_path=resolved['audit_path'],
                                            trigger_available_now=True)
            job.await_termination(120)
            assert not query.isActive
            assert all(q is None or not q.exception() for q in job.queries)
        finally:
            job.stop()
        assert migration._parquet(tmp_path / 'new/metrics') == original['business']['output']
        assert migration.inventory(tmp_path / 'old') == old_inventory
        migration.rollback(plan, pointer)
        rolled_back = migration.resolve_stream_pointer(pointer, expected_topics=topic)
        assert rolled_back['checkpoint_dir'] == str(tmp_path / 'old/checkpoint')
    finally:
        producer.close()
        admin.delete_topics([topic])
        admin.close()


def generation_with_checkpoints(tmp_path, frozen, frontier):
    generation = tmp_path / 'generation'
    generation.mkdir()
    binding = {'source_kind': 'kafka', 'kafka_source': frozen, 'configuration': {
        'window_seconds': 60, 'watermark_seconds': 600, 'output_mode': 'update', 'negative_threshold': 2.0}}
    (generation / 'stream-source.json').write_text(json.dumps(binding))
    for kind in ('reviews_window', 'reviews_quarantine', 'reviews_audit'):
        checkpoint = generation / 'checkpoint' / kind
        (checkpoint / 'commits').mkdir(parents=True)
        (checkpoint / 'offsets').mkdir()
        (checkpoint / 'commits/0').write_text('v1\n{}')
        (checkpoint / 'offsets/0').write_text('v1\n{}\n' + json.dumps(frontier))
    pointer = tmp_path / 'pointer.json'
    pointer.write_text(json.dumps({'generation': str(generation)}))
    return pointer, generation


def test_resume_checks_current_checkpoint_retention_after_new_batches(tmp_path, monkeypatch):
    from operations import migration
    pointer, generation = generation_with_checkpoints(tmp_path, source(), {'raw.reviews': {'0': 8}})
    monkeypatch.setattr(migration, 'read_kafka_bounds', lambda _: {
        'beginning_offsets': {'raw.reviews': {'0': 7}}, 'ending_offsets': {'raw.reviews': {'0': 10}}})
    assert migration.resolve_stream_pointer(pointer)['checkpoint_dir'] == str(generation / 'checkpoint')
    monkeypatch.setattr(migration, 'read_kafka_bounds', lambda _: {
        'beginning_offsets': {'raw.reviews': {'0': 9}}, 'ending_offsets': {'raw.reviews': {'0': 10}}})
    with pytest.raises(ValueError, match='Bronze backfill'):
        migration.resolve_stream_pointer(pointer)


def test_resume_rejects_tampered_promoted_source_binding(tmp_path, monkeypatch):
    from operations import migration
    pointer, generation = generation_with_checkpoints(tmp_path, source(), {'raw.reviews': {'0': 6}})
    change = json.loads(pointer.read_text())
    change['source_binding_sha256'] = 'a' * 64
    pointer.write_text(json.dumps(change))
    with pytest.raises(ValueError, match='binding'):
        migration.resolve_stream_pointer(pointer)


def test_resume_rejects_same_offsets_from_a_different_kafka_cluster():
    from operations.migration import validate_kafka_bounds
    frozen = dict(source(), cluster_id='original-cluster')
    with pytest.raises(ValueError, match='cluster'):
        validate_kafka_bounds(frozen, {'cluster_id': 'replaced-cluster',
            'beginning_offsets': {'raw.reviews': {'0': 0}}, 'ending_offsets': {'raw.reviews': {'0': 6}}})


@pytest.mark.spark
@pytest.mark.integration
def test_default_kafka_source_fails_on_deleted_starting_offset(tmp_path, spark_session):
    import os
    import uuid
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.structs import TopicPartition
    from config.settings import settings
    from spark.streaming.spark_streaming_job import SparkStreamingJob

    settings.set_runtime_mode('test')
    topic = 'sentinel.test.offset-loss.' + uuid.uuid4().hex
    servers = os.environ.get('KAFKA_MIGRATION_TEST_BOOTSTRAP', settings.kafka.bootstrap_servers)
    settings.kafka.bootstrap_servers = servers
    admin = KafkaAdminClient(bootstrap_servers=servers)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    producer = KafkaProducer(bootstrap_servers=servers)
    query = None
    try:
        for index in range(3):
            producer.send(topic, json.dumps({'event_id': str(index)}).encode()).get(timeout=15)
        producer.flush()
        admin.delete_records({TopicPartition(topic, 0): 2})
        job = SparkStreamingJob(spark_session=spark_session, checkpoint_dir=str(tmp_path / 'checkpoint'))
        raw = job.create_kafka_stream(topics=topic, starting_offsets=json.dumps({topic: {'0': 0}}))
        query = (raw.writeStream.format('memory').queryName('lost_' + uuid.uuid4().hex)
                 .option('checkpointLocation', str(tmp_path / 'raw-checkpoint'))
                 .trigger(availableNow=True).start())
        with pytest.raises(Exception, match='(?is)data.*lost|data.*loss|offset.*range'):
            query.awaitTermination(60)
    finally:
        if query is not None and query.isActive:
            query.stop()
        producer.close()
        admin.delete_topics([topic])
        admin.close()
