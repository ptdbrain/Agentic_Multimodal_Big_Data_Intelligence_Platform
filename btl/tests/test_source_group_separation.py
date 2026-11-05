"""Real and generated observations must never share an analytical generation."""
import pandas as pd
import pytest
from ingestion.provenance import classify_source_group
from spark.etl.gold_aggregator import GoldAggregator

pytestmark = pytest.mark.unit


def test_real_and_simulated_groups_are_rejected_together():
    with pytest.raises(ValueError, match='mixed'):
        classify_source_group([{'source_group':'marketplace','acquisition_method':'live'}],
                              [{'source_group':'simulation','acquisition_method':'generated'}])


def test_real_label_cannot_hide_generated_acquisition():
    with pytest.raises(ValueError, match='acquisition'):
        classify_source_group([{'source_group':'marketplace','acquisition_method':'generated'}])


def test_legacy_unknown_is_not_promoted_to_real():
    assert classify_source_group([{'product_id':'p1','source':'web_crawler'}]) == 'unverified'
    with pytest.raises(ValueError, match='mixed'):
        classify_source_group([{'source_group':'marketplace','acquisition_method':'snapshot_replay'}],
                              [{'source':'old_unknown'}])


def test_pandas_gold_does_not_merge_synthetic_reviews_with_real_catalog():
    products = pd.DataFrame([{'product_id':'p1','brand':'A','category':'Phone','price':10,
                              'source_group':'marketplace','acquisition_method':'live'}])
    reviews = pd.DataFrame([{'review_id':'r1','product_id':'p1','rating':5,'review_date':'2026-11-01',
                             'source_group':'simulation','acquisition_method':'generated'}])
    with pytest.raises(ValueError, match='mixed'):
        GoldAggregator.build_gold_marts(products, reviews, save_to_storage=False)


def test_mock_crawler_labels_all_three_datasets_as_generated():
    from ingestion.crawler.mock import MockEcommerceCrawler
    result = MockEcommerceCrawler(seed=7).crawl_category('phone', max_products=1, reviews_per_product=1)
    for name in ['products','reviews','prices']:
        assert result[name]
        assert classify_source_group(result[name]) == 'simulation'
        assert result[name][0]['generator_seed'] == '7'


def test_tiki_price_inherits_real_collection_provenance():
    from ingestion.crawler.tiki import TikiCrawler
    product = {'product_id':'tiki_1','price':10,'currency':'VND','collected_at':'2026-11-02T00:00:00Z',
               'source_group':'marketplace','source_name':'tiki','acquisition_method':'live',
               'source_url':'https://tiki.vn/product','evidence_sha256':'observed-capture-hash'}
    snapshot = TikiCrawler().standardize_price(product)
    assert classify_source_group([product], [snapshot]) == 'marketplace'
    assert snapshot['evidence_sha256'] == product['evidence_sha256']


def test_persisted_quality_keeps_its_source_group():
    from spark.quality.spark_dq import SparkDataQualityEvaluator
    report = SparkDataQualityEvaluator.generate_report({'run_id':'r','dataset_name':'reviews',
                                                        'source_group':'simulation'})
    assert report['source_group'] == 'simulation'


def test_export_cannot_mix_generated_catalog_into_real_warehouse():
    from database.warehouse import WarehouseManager
    from database.export_gold import export_gold_to_db
    from storage.storage_manager import storage
    real = {'product_id':'real','product_name':'Observed','brand':'A','category':'Phone','price':10,
            'source_group':'marketplace','acquisition_method':'live'}
    wh = WarehouseManager(mode='test'); wh.initialize_schema()
    wh.upsert_dataframe('products', pd.DataFrame([real]), ['product_id']); wh.close()
    fake = dict(real, product_id='sim', source_group='simulation', acquisition_method='generated')
    storage.write_silver_parquet('products', pd.DataFrame([fake]))
    with pytest.raises(ValueError, match='source'):
        export_gold_to_db(marts=[], silver_tables=['products'], mode='test')
    wh = WarehouseManager(mode='test')
    try:
        assert wh.query('SELECT product_id FROM products')['product_id'].tolist() == ['real']
    finally:
        wh.close()

@pytest.mark.parametrize('legacy_group',[None,'','unverified'])
def test_existing_unverified_catalog_requires_a_new_known_source_namespace(legacy_group):
    from storage.storage_manager import storage
    from database.export_gold import export_gold_to_db
    from database.warehouse import WarehouseManager
    legacy={'product_id':'legacy','product_name':'Explicit fixture','brand':'Fixture','category':'Phone','price':100,'currency':'VND',
            'source_group':legacy_group}
    storage.write_silver_parquet('products',pd.DataFrame([legacy]))
    export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    observed=dict(legacy,product_id='observed',source_group='marketplace',acquisition_method='snapshot_replay')
    storage.write_silver_parquet('products',pd.DataFrame([observed]))
    with pytest.raises(ValueError,match='source'):
        export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    wh=WarehouseManager(mode='test')
    try:assert wh.query('SELECT product_id FROM products')['product_id'].tolist()==['legacy']
    finally:wh.close()

def test_empty_unverified_snapshot_can_clear_its_own_legacy_mart():
    from storage.storage_manager import storage
    from database.export_gold import export_gold_to_db
    from database.warehouse import WarehouseManager
    frame=pd.DataFrame([{'date':'2026-11-01','product_id':'legacy','avg_price':None}])
    storage.write_gold_parquet('product_daily_stats',frame)
    export_gold_to_db(marts=['product_daily_stats'],silver_tables=[],mode='test')
    storage.write_gold_parquet('product_daily_stats',frame.iloc[:0],allow_empty=True)
    export_gold_to_db(marts=['product_daily_stats'],silver_tables=[],mode='test')
    wh=WarehouseManager(mode='test')
    try:assert wh.query('SELECT * FROM product_daily_stats').empty
    finally:wh.close()
