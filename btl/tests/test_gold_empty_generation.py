import pandas as pd
import pytest

from storage.storage_manager import storage


@pytest.mark.parametrize('engine', ['pandas', pytest.param('spark', marks=pytest.mark.spark)])
def test_new_generation_clears_absent_daily_marts_and_records_empty_generation(engine, request):
    products = pd.DataFrame([{'product_id':'p','brand':'A','category':'Phone','price':10.}])
    reviews = pd.DataFrame([{'review_id':'r','product_id':'p','rating':5.,'review_date':'2026-10-01'}])
    prices = pd.DataFrame([{'product_id':'p','price':10.,'timestamp':'2026-10-01'}])
    def run(revs, price, identity):
        if engine == 'pandas':
            from spark.etl.gold_aggregator import GoldAggregator
            return GoldAggregator.build_gold_marts(products, revs, price, run_id=identity)
        from spark.analytics.gold_builder import SparkGoldBuilder
        spark = request.getfixturevalue('spark_session')
        return SparkGoldBuilder(spark).build_all(revs if not revs.empty else [], products,
                                                price if not price.empty else [], run_id=identity)
    run(reviews, prices, 'old')
    assert not storage.read_gold_parquet('price_daily_stats').empty
    run(reviews.iloc[:0], prices.iloc[:0], 'new')
    for mart in ['product_daily_stats','brand_daily_stats','category_daily_stats','price_daily_stats']:
        frame = storage.read_gold_parquet(mart)
        assert frame.empty
        assert 'date' in frame.columns
    manifest = storage.read_json('gold/_snapshot.json')
    assert manifest['run_id'] == 'new'
    assert manifest['status'] == 'complete'
    assert set(manifest['rows'].values()) == {0}


@pytest.mark.spark
def test_every_consumed_dataset_requires_current_dq_despite_narrow_required_list(spark_session):
    from spark.analytics.gold_builder import SparkGoldBuilder
    from spark.quality.spark_dq import DataQualityGateError
    products = pd.DataFrame([{'product_id':'p','brand':'A','category':'Phone','price':10.}])
    reviews = pd.DataFrame([{'review_id':'r','product_id':'p','rating':5.,'review_date':'2026-10-01'}])
    prices = pd.DataFrame([{'product_id':'p','price':10.,'timestamp':'2026-10-01'}])
    dq = [dict(dataset_name=name,run_id='new',records_received=1,records_valid=1,records_invalid=0,
               records_duplicate=0,records_missing=0,dq_score=100) for name in ['products','reviews']]
    with pytest.raises(DataQualityGateError, match='prices'):
        SparkGoldBuilder(spark_session).build_all(reviews, products, prices, run_id='new',dq_metrics=dq,
                                                required_datasets=['reviews'],save_gold=False)


@pytest.mark.spark
def test_unlabelled_spark_input_keeps_source_run_unknown(spark_session):
    from spark.analytics.gold_builder import SparkGoldBuilder
    reviews = spark_session.createDataFrame([{'product_id':'p','review_id':'r','rating':5.,'review_date':'2026-10-01'}])
    marts = SparkGoldBuilder(spark_session).build_all(reviews, [], [], save_gold=False,run_id='analysis')
    assert marts['product_daily_stats'].first()['source_run_id'] is None


@pytest.mark.spark
def test_current_reports_cannot_authorize_an_older_canonical_price_snapshot(spark_session):
    from spark.analytics.gold_builder import SparkGoldBuilder
    from spark.quality.spark_dq import DataQualityGateError
    from spark.spark_session import SparkSessionFactory
    inputs = {
        'products':pd.DataFrame([{'product_id':'p','brand':'A','category':'Phone','price':10.}]),
        'reviews':pd.DataFrame([{'review_id':'r','product_id':'p','rating':5.,'review_date':'2026-10-01'}]),
        'prices':pd.DataFrame([{'product_id':'p','price':10.,'timestamp':'2026-10-01'}]),
    }
    for name, frame in inputs.items():
        storage.write_silver_parquet(name,frame)
        storage.write_json(f'silver/_snapshot_{name}.json', {'run_id':'old' if name == 'prices' else 'new','rows':1})
    reports = [dict(dataset_name=name,run_id='new',records_received=1,records_valid=1,records_invalid=0,
                    records_duplicate=0,records_missing=0,dq_score=100) for name in inputs]
    paths = {f'silver_{name}':SparkSessionFactory.resolve_storage_path(name,layer='silver') for name in inputs}
    with pytest.raises(DataQualityGateError, match='prices.*snapshot'):
        SparkGoldBuilder(spark_session).build_all(**paths,run_id='new',dq_metrics=reports,save_gold=False)


@pytest.mark.spark
def test_local_pandas_snapshot_replaces_prior_spark_part_files(spark_session):
    from spark.analytics.gold_builder import SparkGoldBuilder
    from spark.etl.gold_aggregator import GoldAggregator
    products = pd.DataFrame([{'product_id':'p','brand':'A','category':'Phone','price':10.}])
    reviews = pd.DataFrame([{'review_id':'r','product_id':'p','rating':5.,'review_date':'2026-10-01'}])
    SparkGoldBuilder(spark_session).build_all(reviews,products,[],run_id='old')
    GoldAggregator.build_gold_marts(products,reviews.iloc[:0],pd.DataFrame(),run_id='new')
    assert storage.read_gold_parquet('product_daily_stats').empty
