import pytest
from spark.quality.spark_dq import SparkDataQualityEvaluator as DQ, DataQualityGateError
from spark.analytics.gold_builder import SparkGoldBuilder

pytestmark = pytest.mark.unit


def metrics(**kw):
    return dict(dataset_name='products', records_received=10, records_valid=10,
                records_duplicate=10, intra_batch_duplicate_count=0,
                cross_batch_duplicate_count=10, records_missing=0, dq_score=100, **kw)


def test_replay_is_not_a_quality_duplicate():
    assert DQ.check_dq_gate(metrics())['passed']


def test_missing_and_stale_metrics_fail_closed():
    with pytest.raises(DataQualityGateError):
        DQ.check_dq_gate([], required_datasets=['products'], run_id='run1')
    with pytest.raises(DataQualityGateError):
        DQ.check_dq_gate(metrics(run_id='old'), required_datasets=['products'], run_id='run1')


def test_report_has_no_fabricated_duration():
    assert DQ.generate_report(metrics())['processing_time_ms'] is None
    assert DQ.generate_report(dict(metrics(), processing_time_ms=125))['processing_time_ms'] == 125


def test_gold_does_not_build_without_metrics(monkeypatch):
    from storage.storage_manager import storage
    monkeypatch.setattr(storage, 'read_gold_parquet', lambda _: __import__('pandas').DataFrame())
    builder=SparkGoldBuilder.__new__(SparkGoldBuilder)
    monkeypatch.setattr(builder,'build_product_daily_stats',lambda *a: pytest.fail('Gold writer reached'))
    with pytest.raises(DataQualityGateError):
        builder.build_all(enforce_dq_gate=True)


def test_gold_dq_metrics_require_explicit_current_run(monkeypatch):
    builder = SparkGoldBuilder.__new__(SparkGoldBuilder)
    with pytest.raises(DataQualityGateError, match='run_id'):
        builder.build_all(dq_metrics=[metrics(run_id='run-1')], enforce_dq_gate=True)


@pytest.mark.parametrize('field,value',[('dq_score',float('nan')),('dq_score',float('inf')),('dq_score',101),('records_received',-1),('records_missing',11),('missing_ratio',float('nan'))])
def test_nonfinite_or_impossible_metrics_fail_closed(field,value):
    m=metrics();m[field]=value
    with pytest.raises(DataQualityGateError): DQ.check_dq_gate(m)


def test_gold_storage_failure_never_uses_destructive_fallback(monkeypatch):
    from spark.storage_safe import SafeParquetIO
    class Frame:
        def count(self): return 1
        @property
        def write(self): raise RuntimeError('destructive writer reached')
    builder=SparkGoldBuilder.__new__(SparkGoldBuilder);builder._spark=object()
    # This unit test targets the sink-error branch; validation and lineage have
    # separate real Spark coverage. Keep test doubles out of production logic.
    monkeypatch.setattr(builder,'_add_lineage',lambda frame,*args: frame)
    monkeypatch.setattr(builder,'_validate_mart',lambda *args: None)
    monkeypatch.setattr(builder,'build_product_daily_stats',lambda *args: Frame())
    for name in ['build_brand_daily_stats','build_category_daily_stats','build_price_daily_stats']:
        monkeypatch.setattr(builder,name,lambda *args: Frame())
    def failed_publish(*args,**kw): raise RuntimeError('verified sink failure')
    monkeypatch.setattr(SafeParquetIO,'publish_parquet_atomic',failed_publish)
    with pytest.raises(RuntimeError,match='verified sink failure'):
        builder.build_all()
