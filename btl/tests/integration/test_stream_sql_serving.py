"""Real Kafka/Spark/Parquet/PostgreSQL serving with labelled generated observations."""

import uuid
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.spark]


def cleanup_owned_test_scope(control, schema, topic):
    import re
    from psycopg2 import sql
    from kafka.admin import KafkaAdminClient
    from config.settings import settings

    if not re.fullmatch(r"nonai_(?:stream|alert)_[0-9a-f]{12}", schema) or not re.fullmatch(
        r"sentinel-(?:nonai-stream|causal-test)-[0-9a-f]{32}", topic
    ):
        raise ValueError("Refusing cleanup outside explicitly owned integration scope")
    try:
        admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers, request_timeout_ms=10000)
        try:
            admin.delete_topics([topic])
        finally:
            admin.close()
    finally:
        try:
            control.cursor().execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        finally:
            control.close()


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

        def publish_more(rows):
            producer = StreamProducer()
            try:
                for review_id, rating, clock in rows:
                    producer.send_message(
                        topic,
                        "p1",
                        {
                            "review_id": review_id,
                            "product_id": "p1",
                            "rating": rating,
                            "review_text": "Explicit generated integration observation",
                            "review_date": clock,
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
            job.restart(trigger_available_now=True)
            job.await_termination()
            job.observe_progress()
            progresses = list(job.queries[0].recentProgress)
            job.stop()
            return progresses

        # A within-watermark late observation updates the same SQL window;
        # replaying an already seen review_id must not increase it.
        conn.commit()
        publish_more([("r3", 3, "2026-11-13T10:00:00Z"), ("r1", 5, "2026-11-13T10:00:00Z")])
        assert store.execute("SELECT COUNT(*),SUM(review_count) FROM stream_windows").fetchone() == (1, 3)
        conn.commit()
        publish_more([("future-frontier", 4, "2026-11-13T10:20:00Z")])
        assert store.execute("SELECT COUNT(*),SUM(review_count) FROM stream_windows").fetchone() == (2, 4)
        from serving.store import instant

        watermark = store.execute("SELECT watermark FROM stream_deployments").fetchone()[0]
        assert instant(watermark) >= instant("2026-11-13T10:10:00Z")
        conn.commit()
        progresses = publish_more([("too-late", 1, "2026-11-13T10:00:00Z")])
        assert store.execute("SELECT COUNT(*),SUM(review_count) FROM stream_windows").fetchone() == (2, 4)
        assert any(
            operator.get("numRowsDroppedByWatermark", 0) >= 1
            for progress in progresses
            for operator in progress.get("stateOperators", [])
        )
    finally:
        job.stop()
        conn.close()
        cleanup_owned_test_scope(control, schema, topic)


def test_actual_streaming_causal_alerts_reach_local_receiver_once(tmp_path, spark_session):
    import json, threading
    from datetime import datetime, timedelta, timezone
    import psycopg2
    from config.settings import settings
    from serving.registry import DatasetRef
    from serving.store import ServingStore, connect_dataset
    from serving.notifications import deliver_notifications
    from scripts.verify_operations_stack import webhook_server
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
    control.autocommit = True
    schema = "nonai_alert_" + uuid.uuid4().hex[:12]
    control.cursor().execute("CREATE SCHEMA " + schema)
    binding = DatasetRef(
        schema.replace("_", "-"),
        "Generated causal integration",
        "commerce",
        "simulation",
        schema,
        "isolated-alert-test",
        "integration",
        {**params, "options": "-c search_path=" + schema},
    )
    conn = connect_dataset(binding)
    store = ServingStore(conn, binding.id, binding.source_group)
    with conn:
        store.initialize()
    store.register_stream("g", {"window_seconds": 60, "watermark_seconds": 600})
    start = datetime(2026, 11, 13, 10, tzinfo=timezone.utc)
    observations = [
        (f"baseline-{minute}-{n}", start + timedelta(minutes=minute), rating)
        for minute in range(12)
        for n, rating in enumerate([4, 5])
    ]
    observations += [(f"burst-{n}", start + timedelta(minutes=13), 1 if n < 18 else 5) for n in range(20)]
    observations += [("frontier", start + timedelta(minutes=25), 5)]
    topic = "sentinel-causal-test-" + uuid.uuid4().hex
    producer = StreamProducer()
    try:
        for identity, clock, rating in observations:
            producer.send_message(
                topic,
                "p",
                {
                    "review_id": identity,
                    "product_id": "p",
                    "rating": rating,
                    "review_date": clock.isoformat(),
                    "review_text": "Explicit generated causal integration observation",
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
    server = webhook_server(tmp_path / "business-receipts.jsonl", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        job.start_review_stream(topics=topic, trigger_available_now=True)
        job.await_termination()
        job.observe_progress()
        job.stop()
        assert store.execute("SELECT SUM(review_count) FROM stream_windows").fetchone()[0] == 45
        assert store.execute("SELECT COUNT(*) FROM serving_alerts WHERE state='firing'").fetchone()[0] == 2
        evidence = [
            json.loads(row[0]) for row in store.execute("SELECT evidence_json FROM serving_alerts WHERE state='firing'")
        ]
        assert all(value["baseline_windows"] == 12 and not value["provisional"] for value in evidence)
        conn.commit()
        url = f"http://127.0.0.1:{server.server_port}/alerts"
        assert deliver_notifications(binding, url=url) == 2
        assert deliver_notifications(binding, url=url) == 0
        receipts = [json.loads(line) for line in (tmp_path / "business-receipts.jsonl").read_text().splitlines()]
        assert {row["notification"]["alerts"][0]["labels"]["alertname"] for row in receipts} == {
            "REVIEW_BOMBING",
            "RATING_DROP",
        }
        assert len(receipts) == 2
    finally:
        job.stop()
        conn.close()
        cleanup_owned_test_scope(control, schema, topic)
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
