"""Actual Spark Parquet plus SQLite sink failure/retry, not public-source evidence."""
from datetime import datetime
import sqlite3
import pytest
pytestmark=[pytest.mark.unit,pytest.mark.spark]


def test_actual_spark_sql_sink_failure_is_retryable_without_duplicate_windows(tmp_path,spark_session,monkeypatch):
    from serving.registry import DatasetRegistry
    from serving.store import ServingStore
    from tests.test_serving_registry import registry_file
    from spark.streaming.spark_streaming_job import SparkStreamingJob
    binding=DatasetRegistry.load(registry_file(tmp_path)).datasets['tiki']
    job=SparkStreamingJob(spark_session=spark_session,output_path=str(tmp_path/'metrics'),serving_binding=binding,serving_generation='g')
    schema='window_start timestamp,window_end timestamp,product_id string,source_group string,review_count long,avg_rating double,negative_count long'
    frame=spark_session.createDataFrame([(datetime(2026,11,13,10),datetime(2026,11,13,10,1),'p','marketplace',7,4.0,1)],schema)
    original=ServingStore.write_windows
    def failed(*args,**kwargs):raise RuntimeError('SQL sink deliberately unavailable')
    monkeypatch.setattr(ServingStore,'write_windows',failed)
    with pytest.raises(RuntimeError,match='SQL sink'):job._upsert_window_metrics_batch(frame,0)
    assert (tmp_path/'metrics').exists()
    monkeypatch.setattr(ServingStore,'write_windows',original)
    job._upsert_window_metrics_batch(frame,0)
    job._upsert_window_metrics_batch(frame,0)
    with sqlite3.connect(binding.sqlite_path) as c:
        assert c.execute('SELECT COUNT(*),SUM(review_count) FROM stream_windows').fetchone()==(1,7)
        assert c.execute('SELECT COUNT(*) FROM stream_batches').fetchone()[0]==1
