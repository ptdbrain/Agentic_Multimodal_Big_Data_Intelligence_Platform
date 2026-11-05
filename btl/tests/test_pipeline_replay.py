import json
import pytest
from data.generator import generate_products,generate_reviews,generate_price_history
from scripts.run_e2e_pipeline import run_end_to_end
from storage.storage_manager import storage
from dashboard.db_connector import DashboardDB

pytestmark = pytest.mark.spark


def test_pipeline_without_optional_prices_clears_previous_price_snapshot(tmp_path, spark_session):
    import pandas as pd
    from database.warehouse import WarehouseManager
    from scripts.create_acceptance_dataset import create

    create(tmp_path / 'inputs')
    storage.write_silver_parquet('prices', pd.DataFrame([{
        'price_id':'old-price','product_id':'old-product','price':77,
        'currency':'VND','timestamp':'2026-10-01T00:00:00Z',
        'source_group':'simulation','acquisition_method':'generated',
    }]))
    wh = WarehouseManager(mode='test')
    try:
        wh.initialize_schema()
        wh.upsert_dataframe('price_daily_stats', pd.DataFrame([{
            'date':'2026-10-01','product_id':'old-product','avg_price':77,'data_points':1,
            'currency':'VND','source_group':'simulation',
        }]), ['date', 'product_id'])
    finally:
        wh.close()

    assert run_end_to_end(
        mode='test', products_path=str(tmp_path / 'inputs/products.json'),
        reviews_path=str(tmp_path / 'inputs/reviews.json'),
        prices_path=str(tmp_path / 'no-prices.json'),
    )
    assert storage.read_gold_parquet('price_daily_stats').empty
    db = DashboardDB(mode='test')
    try:
        assert db.get_price_daily_stats().empty
        assert db.get_kpi_summary()['total_reviews'] == 20
        assert not db.get_product_daily_stats().empty
    finally:
        db.close()


def test_clean_pipeline_replay_preserves_content_and_dq_history(tmp_path,spark_session):
    products=generate_products(seed=7)
    # The generator's catalog may include deliberate duplicate fixtures: select stable entities explicitly.
    products=list({p['product_id']:p for p in products}.values())
    reviews=generate_reviews(products,count=20,seed=7,dirty_ratio=0,duplicate_ratio=0,inject_anomalies=False)
    prices=generate_price_history(products,seed=7)
    paths={}
    for name,records in [('products',products),('reviews',reviews),('prices',prices)]:
        p=tmp_path/f'{name}.json';p.write_text(json.dumps(records));paths[name+'_path']=str(p)
    assert run_end_to_end(mode='test',**paths)
    first={name:storage.read_silver_parquet(name).sort_values(key).reset_index(drop=True)
           for name,key in [('products','product_id'),('reviews','review_id'),('prices','price_id')]}
    first_events = storage.read_gold_parquet('anomaly_events')
    assert not first_events.empty
    first_run = storage.read_gold_parquet('data_quality_metrics')['run_id'].iloc[-1]
    assert first_events['run_id'].eq(first_run).all()
    assert first_events['data_quality_status'].eq('valid').all()
    assert first_events['baseline_sample_size'].gt(0).all()
    report_count=len(storage.read_gold_parquet('data_quality_metrics'))
    assert run_end_to_end(mode='test',**paths)
    for name,key in [('products','product_id'),('reviews','review_id'),('prices','price_id')]:
        after=storage.read_silver_parquet(name).sort_values(key).reset_index(drop=True)
        assert len(after)==len(first[name])
        cols=[c for c in first[name].columns if c not in ('ingested_at','ingested_at_raw','event_id')]
        import pandas as pd
        pd.testing.assert_frame_equal(first[name][cols],after[cols])
    assert len(storage.read_gold_parquet('data_quality_metrics'))==report_count*2
    db=DashboardDB(mode='test')
    try:
        assert db.get_kpi_summary()['total_reviews']==len(reviews)
        latest_events = storage.read_gold_parquet('anomaly_events')
        second_run = storage.read_gold_parquet('data_quality_metrics')['run_id'].iloc[-1]
        assert second_run != first_run
        assert latest_events['run_id'].eq(second_run).all()
        assert set(latest_events['event_id']) == set(first_events['event_id'])
        served = db.get_anomaly_events()
        assert not served.empty
        assert served['run_id'].eq(second_run).all()
        assert served['data_quality_status'].eq('valid').all()
        assert served['baseline_sample_size'].gt(0).all()
        for mart in ['product_daily_stats','brand_daily_stats','category_daily_stats','price_daily_stats']:
            gold = storage.read_gold_parquet(mart)
            warehouse = db.query(f'SELECT * FROM {mart}')
            assert not gold.empty and not warehouse.empty
            assert set(warehouse['run_id']) == {second_run}
            assert set(warehouse['source_run_id']) == set(gold['source_run_id'])
            assert pd.to_datetime(warehouse['data_as_of']).max().date() == gold['data_as_of'].max()
            assert warehouse['time_available'].astype(bool).all()
            if mart != 'price_daily_stats':
                assert warehouse['rating_sample_size'].sum() == gold['rating_sample_size'].sum()
                assert warehouse['weighted_avg_rating'].notna().all()
            if mart == 'product_daily_stats':
                assert set(warehouse['price_source']) == set(gold['price_source'])
    finally: db.close()
