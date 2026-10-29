import pandas as pd
import pytest

from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from storage.storage_manager import storage


@pytest.mark.unit
@pytest.mark.parametrize('empty', [False, True])
def test_daily_snapshot_export_removes_obsolete_warehouse_keys(empty):
    wh = WarehouseManager(mode='test'); wh.initialize_schema()
    wh.upsert_dataframe('product_daily_stats', pd.DataFrame([
        {'date':'2026-10-01','product_id':pid,'review_count':1,'run_id':'old'} for pid in ['kept','removed']
    ]), ['date','product_id']); wh.close()
    new = pd.DataFrame([{'date':'2026-10-01','product_id':'kept','review_count':2,'run_id':'new'}])
    if empty:
        new = new.iloc[:0]
    storage.write_gold_parquet('product_daily_stats', new, allow_empty=True)
    export_gold_to_db(marts=['product_daily_stats'], silver_tables=[], mode='test')
    wh = WarehouseManager(mode='test')
    try:
        actual = wh.query('SELECT product_id,run_id FROM product_daily_stats')
        assert actual.to_dict('records') == ([] if empty else [{'product_id':'kept','run_id':'new'}])
    finally:
        wh.close()


@pytest.mark.unit
def test_missing_gold_snapshot_cannot_be_mistaken_for_an_explicit_empty_snapshot():
    wh = WarehouseManager(mode='test'); wh.initialize_schema()
    wh.upsert_dataframe('product_daily_stats', pd.DataFrame([{
        'date':'2026-10-01','product_id':'kept','review_count':1,'run_id':'old',
    }]), ['date','product_id']); wh.close()
    with pytest.raises(ValueError):
        export_gold_to_db(marts=['product_daily_stats'],silver_tables=[],mode='test')
    wh = WarehouseManager(mode='test')
    try:
        assert wh.query('SELECT product_id FROM product_daily_stats')['product_id'].tolist() == ['kept']
    finally:
        wh.close()


@pytest.mark.unit
@pytest.mark.parametrize('change', ['missing_duplicate','missing_nulls','contradictory_score','contradictory_counts'])
def test_dq_cannot_authorize_unmeasured_or_inconsistent_reports(change):
    from spark.quality.spark_dq import SparkDataQualityEvaluator as DQ, DataQualityGateError
    metric = dict(dataset_name='reviews', run_id='run', records_received=10, records_valid=10,
                  records_invalid=0, records_duplicate=0, records_missing=0, dq_score=100)
    if change == 'missing_duplicate': del metric['records_duplicate']
    if change == 'missing_nulls': del metric['records_missing']
    if change == 'contradictory_score': metric.update(records_valid=0, records_invalid=10)
    if change == 'contradictory_counts': metric['records_valid'] = 5
    with pytest.raises(DataQualityGateError):
        DQ.check_dq_gate(metric, run_id='run', required_datasets=['reviews'])


@pytest.mark.unit
@pytest.mark.parametrize('field', ['duplicate', 'missing'])
def test_reported_zero_ratio_cannot_override_nonzero_invalid_record_counts(field):
    from spark.quality.spark_dq import SparkDataQualityEvaluator as DQ, DataQualityGateError
    metric = dict(dataset_name='reviews',records_received=10,records_valid=9,records_invalid=1,
                  records_duplicate=0,records_missing=0,dq_score=90)
    metric['records_'+field] = 1
    metric[field+'_ratio'] = 0
    with pytest.raises(DataQualityGateError):
        DQ.check_dq_gate(metric)


@pytest.mark.unit
def test_report_from_batch_uses_unique_valid_rows_for_consistent_accounting():
    from spark.quality.spark_dq import SparkDataQualityEvaluator as DQ
    report = DQ.generate_report(dict(dataset_name='reviews',input_count=10,valid_count=10,unique_valid_count=9,
        row_invalid_count=1,intra_batch_duplicate_count=1,null_row_count=0,dq_score=90))
    assert report['records_received'] == 10
    assert report['records_valid'] == 9
    assert report['records_invalid'] == report['records_duplicate'] == 1
    assert DQ.check_dq_gate(report,max_duplicate_ratio=.2)['passed']


@pytest.mark.unit
def test_positive_dq_report_cannot_authorize_an_empty_required_input():
    from types import SimpleNamespace
    from spark.analytics.gold_builder import SparkGoldBuilder
    from spark.quality.spark_dq import DataQualityGateError
    metric = dict(dataset_name='reviews',run_id='run',records_received=1,records_valid=1,
                  records_invalid=0,records_duplicate=0,records_missing=0,dq_score=100)
    with pytest.raises(DataQualityGateError, match='reviews.*empty'):
        SparkGoldBuilder(spark=SimpleNamespace()).build_all([],[],[],run_id='run',save_gold=False,
            dq_metrics=[metric],required_datasets=['reviews'])


@pytest.mark.unit
def test_optional_price_read_does_not_hide_permission_errors(monkeypatch):
    import scripts.run_e2e_pipeline as pipeline
    def denied(_):
        raise PermissionError('price source denied')
    monkeypatch.setattr(storage,'read_silver_parquet',denied)
    monkeypatch.setattr(pipeline,'SparkGoldBuilder',lambda:pytest.fail('price error silently ignored'))
    with pytest.raises(PermissionError,match='price source denied'):
        pipeline.stage_5_spark_gold_marts(pd.DataFrame([{'product_id':'p'}]),
                                        pd.DataFrame([{'review_id':'r'}]),None,enforce_dq_gate=False)


@pytest.mark.unit
def test_unlabelled_input_does_not_invent_a_source_run():
    from spark.etl.gold_aggregator import GoldAggregator
    products = pd.DataFrame([{'product_id':'p','brand':'A','category':'Phone','price':10.}])
    reviews = pd.DataFrame([{'review_id':'r','product_id':'p','rating':5.,'review_date':'2026-10-01'}])
    marts = GoldAggregator.build_gold_marts(products, reviews, save_to_storage=False, run_id='analysis')
    for frame in marts.values():
        assert frame['run_id'].eq('analysis').all()
        assert frame['source_run_id'].isna().all()


@pytest.mark.unit
def test_descriptive_and_rating_baselines_use_same_eligible_review_set():
    from analytics.descriptive.stats import DescriptiveStats
    from analytics.anomaly.rating_anomaly import RatingAnomalyDetector
    from analytics.trend.trends import TrendAnalyzer
    reviews = pd.DataFrame([
        {'review_id': f'{day}-{i}', 'product_id':'p', 'rating':5., 'review_date':day}
        for day in ['2026-10-01','2026-10-02'] for i in range(3)
    ] + [{'review_id':None,'product_id':'p','rating':1.,'review_date':'2026-10-01'}]
      + [{'review_id':f'drop{i}','product_id':'p','rating':1.,'review_date':'2026-10-03'} for i in range(3)])
    row = DescriptiveStats.compute_product_metrics(reviews, pd.DataFrame([{'product_id':'p'}])).iloc[0]
    assert row['rating_sample_size'] == row['review_count'] == 9
    assert row['avg_rating'] == 3.67
    assert row['positive_ratio'] == .6667
    summary = DescriptiveStats.compute_summary(pd.DataFrame([{'product_id':'p'}]), reviews)
    assert summary['rating_sample_size'] == 9
    assert summary['avg_rating'] == 3.67
    events = RatingAnomalyDetector.detect_rating_drops(reviews)
    assert len(events) == 1
    assert events[0]['baseline_rating'] == 5.0
    assert events[0]['baseline_sample_size'] == 6
    daily = TrendAnalyzer.daily_review_trends(reviews)
    assert daily.iloc[0]['rating_sample_size'] == daily.iloc[0]['review_count'] == 3
