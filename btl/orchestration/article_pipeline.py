"""Article transport and serving; commerce mart generations stay independent."""
import uuid
import hashlib
import json
from config.settings import settings
from storage.storage_manager import storage
from kafka.topics.topic_manager import TopicManager
from kafka.producers.stream_producer import StreamProducer
from kafka.consumers.raw_consumer import RawConsumer
from spark.batch.article_etl import publish_articles


def _run_article_pipeline(records, mode='integration', run_id=None):
    if not records:
        raise ValueError('Article source is empty')
    run_id = run_id or str(uuid.uuid4())
    settings.set_runtime_mode(mode)
    storage.set_mode(mode)
    TopicManager.ensure_topics(mode=mode)
    local = mode in ('test', 'local')
    topic = settings.kafka.topic_articles + '.' + hashlib.sha256(run_id.encode()).hexdigest()[:20]
    if not local:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
        try:
            try:
                admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=settings.kafka.replication_factor, topic_configs={"retention.ms":str(settings.kafka.retention_ms),"min.insync.replicas":str(settings.kafka.min_insync_replicas)})])
            except TopicAlreadyExistsError:
                pass
        finally:
            admin.close()
    producer = StreamProducer(enable_offline_buffer=local)
    try:
        sent, _ = producer.produce_batch(topic, records, rate=0,
                                        key_field='article_id', event_type='NEW_ARTICLE', source='web_content')
        offline = producer.virtual_queue if local else None
    finally:
        producer.close()
    if sent != len(records):
        raise RuntimeError('Article delivery count mismatch')
    consumer = RawConsumer(group_id='articles-' + run_id)
    paths, consumed, valid, dlq = [], 0, 0, 0
    try:
        while consumed < sent:
            batch = consumer.consume_batch_once(topics=[topic], max_records=sent,
                                                timeout_ms=4000, offline_records=offline)
            consumed += batch['records_consumed']
            valid += batch['records_valid']
            dlq += batch['records_dlq']
            paths.extend(batch['batches_written'])
            if local or batch['records_consumed'] <= 0:
                break
    finally:
        consumer.close()
    if consumed != sent or valid != sent or dlq:
        raise RuntimeError('Article transport incomplete or quarantined; serving unchanged')
    payloads = []
    for path in paths:
        payloads.extend(envelope['payload'] for envelope in storage.read_json(path))
    # A pre-existing topic must not silently substitute stale messages for this input.
    def hashes(rows):
        return sorted(hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False).encode()).hexdigest() for row in rows)
    if hashes(payloads) != hashes(records):
        raise RuntimeError('Article Bronze content does not match delivered input')
    result = publish_articles(payloads, mode=mode, run_id=run_id, bronze_paths=paths)
    return dict(result, topic=topic, records_delivered=sent, records_consumed=consumed, transport='offline' if local else 'kafka')


def _observed_article_pipeline(records, mode='integration', run_id=None):
    from operations.kafka_lifecycle import TopicRegistry, digest
    from operations.status import OperationStatus
    if not records:
        raise ValueError('Article source is empty')
    settings.set_runtime_mode(mode)
    storage.set_mode(mode)
    run_id = run_id or str(uuid.uuid4())
    topic = settings.kafka.topic_articles + '.' + hashlib.sha256(run_id.encode()).hexdigest()[:20]
    registry = TopicRegistry(mode)
    registered = False
    try:
        registry.register(topic, run_id, 'web_content', 'articles-' + run_id)
        registered = True
        with OperationStatus(mode=mode) as status:
            status.start(run_id, 'articles', 'web_content')
            try:
                result = _run_article_pipeline(records, mode, run_id)
                proofs = [{'path':path,'sha256':digest(storage.read_json(path))} for path in result['bronze_paths']]
                registry.finish(topic, proofs, result['records_consumed'], result['records_delivered'], 0)
                status.finish(run_id, result['records_consumed'], result['records_delivered'], 0)
                return result
            except Exception:
                status.fail(run_id)
                raise
    except Exception:
        if registered:registry.fail(topic)
        raise
    finally:
        registry.close()


def run_article_pipeline(records, mode='integration', run_id=None):
    from operations.backup import writer_admission
    with writer_admission(mode):
        return _observed_article_pipeline(records,mode,run_id)
