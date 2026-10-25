import uuid
import pytest
from config.settings import settings
from kafka.producers.stream_producer import StreamProducer
from spark.streaming.spark_streaming_job import SparkStreamingJob

pytestmark = [pytest.mark.integration,pytest.mark.spark]


def test_actual_kafka_source_quarantine_and_checkpoint(tmp_path,spark_session,monkeypatch):
    topic='sentinel-stream-test-'+uuid.uuid4().hex
    monkeypatch.setattr(settings.kafka,'topic_reviews',topic)
    producer=StreamProducer()
    try:
        for review_id,rating in [('r1',5),('r2',1),('bad',99)]:
            producer.send_message(topic,'p1',{'review_id':review_id,'product_id':'p1','rating':rating,
                'review_text':'actual Kafka transport','review_date':'2026-10-01T10:00:00Z'},event_type='NEW_REVIEW')
    finally:producer.close()
    output=str(tmp_path/'metrics');quarantine=str(tmp_path/'quarantine')
    job=SparkStreamingJob(spark_session=spark_session,checkpoint_dir=str(tmp_path/'checkpoint'),
                           output_path=output,quarantine_path=quarantine)
    try:
        job.start_review_stream(topics=topic,trigger_available_now=True)
        job.await_termination();job.stop()
        before=spark_session.read.parquet(output).collect()
        assert sum(r['review_count'] for r in before)==2
        assert spark_session.read.parquet(quarantine).count()==1
        job.restart(trigger_available_now=True);job.await_termination();job.stop()
        assert spark_session.read.parquet(output).collect()==before
    finally:job.stop()
