"""Real Kafka/Spark/Parquet/PostgreSQL serving with labelled generated observations."""

import uuid
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]


def test_actual_kafka_parquet_sql_progress_and_replay(tmp_path, spark_session):
    import psycopg2
    from config.settings import settings
    from serving.registry import DatasetRef
    from serving.store import ServingStore, connect_dataset
    from kafka.producers.stream_producer import StreamProducer
    from spark.streaming.spark_streaming_job import SparkStreamingJob

    params = dict(
        host=settings.database.host,
        port=settings.database.port,
        dbname=settings.database.database,
        user=settings.database.user,
        password=settings.database.password,
    )
    control = psycopg2.connect(**params)
    schema = "nonai_stream_" + uuid.uuid4().hex[:12]
    control.autocommit = True
    control.cursor().execute("CREATE SCHEMA " + schema)
    binding = DatasetRef(
        schema.replace("_", "-"),
        "Generated streaming integration",
        "commerce",
        "simulation",
        schema,
        "isolated-stream-test",
        "integration",
        {**params, "options": "-c search_path=" + schema},
    )
    conn = connect_dataset(binding)
    store = ServingStore(conn, binding.id, binding.source_group)
    with conn:
        store.initialize()
    store.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600})
    topic = "sentinel-nonai-stream-" + uuid.uuid4().hex
    producer = StreamProducer()
    try:
        for review_id, rating in [("r1", 5), ("r2", 1), ("bad", 99)]:
            producer.send_message(
                topic,
                "p1",
                {
                    "review_id": review_id,
                    "product_id": "p1",
                    "rating": rating,
                    "review_text": "Explicit generated integration observation",
                    "review_date": "2026-11-13T10:00:00Z",
                    "source_group": "simulation",
                    "source_name": "integration_fixture",
                    "acquisition_method": "generated",
                    "generator_seed": "42",
                    "generator_config": '{"version":1}',
                },
                event_type="NEW_REVIEW",
            )
    finally:
        producer.close()
    job = SparkStreamingJob(
        spark_session=spark_session,
        checkpoint_dir=str(tmp_path / "checkpoint"),
        output_path=str(tmp_path / "metrics"),
        quarantine_path=str(tmp_path / "quarantine"),
        serving_binding=binding,
        serving_generation="g",
    )
    try:
        job.start_review_stream(topics=topic, trigger_available_now=True)
        job.await_termination()
        job.observe_progress()
        job.stop()
        assert spark_session.read.parquet(str(tmp_path / "metrics")).count() == 1
        assert spark_session.read.parquet(str(tmp_path / "quarantine")).count() == 1
        assert store.execute("SELECT COUNT(*),SUM(review_count) FROM stream_windows").fetchone() == (1, 2)
        progress = store.execute("SELECT heartbeat_at,last_batch FROM stream_deployments").fetchone()
        assert progress[0] is not None and progress[1] >= 0
        assert store.execute("SELECT COUNT(*) FROM serving_alerts").fetchone()[0] == 2
        assert store.execute("SELECT COUNT(*) FROM serving_alerts WHERE state='firing'").fetchone()[0] == 0
        conn.commit()
        job.restart(trigger_available_now=True)
        job.await_termination()
        job.stop()
        assert store.execute("SELECT COUNT(*),SUM(review_count) FROM stream_windows").fetchone() == (1, 2)
    finally:
        job.stop()
        conn.close()
        control.close()
