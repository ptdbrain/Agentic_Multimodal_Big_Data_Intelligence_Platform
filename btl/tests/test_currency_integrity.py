"""Money in different or unknown units cannot share an arithmetic result."""
import pandas as pd
import pytest
from analytics.descriptive.stats import DescriptiveStats
from analytics.descriptive.comparative import ComparativeAnalytics
from analytics.trend.price_tracker import PriceTracker
from analytics.anomaly.price_anomaly import PriceAnomalyDetector
from spark.etl.gold_aggregator import GoldAggregator
from spark.etl.normalizer import EntityNormalizer

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('currency', [None, '', 'not money'])
def test_unknown_currency_is_not_defaulted_to_vietnamese_dong(currency):
    assert EntityNormalizer.normalize_currency(currency) is None


def prices():
    return pd.DataFrame([
        {'product_id':'p','brand':'A','category':'Phone','price':25000000,'currency':'VND',
         'timestamp':'2026-11-01T00:00:00Z'},
        {'product_id':'p','brand':'A','category':'Phone','price':1000,'currency':'USD',
         'timestamp':'2026-11-01T01:00:00Z'},
    ])


@pytest.mark.parametrize('operation', ['summary','compare','trajectory','volatility','zscore','iqr','gold'])
def test_incompatible_monetary_units_are_rejected_before_arithmetic(operation):
    rows = prices()
    if operation in ('summary','compare'):
        rows['product_id'] = ['p1','p2']
    callbacks = {
        'summary': lambda: DescriptiveStats.compute_summary(rows, pd.DataFrame()),
        'compare': lambda: ComparativeAnalytics.brand_comparison(rows, pd.DataFrame()),
        'trajectory': lambda: PriceTracker.product_price_trajectory(rows, 'p'),
        'volatility': lambda: PriceTracker.calculate_price_volatility(rows),
        'zscore': lambda: PriceAnomalyDetector.detect_zscore_anomalies(rows),
        'iqr': lambda: PriceAnomalyDetector.detect_iqr_anomalies(rows),
        'gold': lambda: GoldAggregator.build_gold_marts(pd.DataFrame(),pd.DataFrame(),rows,save_to_storage=False),
    }
    with pytest.raises(ValueError, match='currency'):
        callbacks[operation]()


def test_anomaly_description_uses_observed_currency_instead_of_configured_default():
    rows = pd.DataFrame({'product_id':['p']*4,'price':[100,100,100,200],'currency':['USD']*4,
                         'timestamp':['2026-11-01T00:00:00Z']*4})
    result = PriceAnomalyDetector.detect_zscore_anomalies(rows, threshold=3, min_points=4)
    assert len(result) == 1
    assert 'USD' in result[0]['description']
    assert result[0]['currency'] == 'USD'


def test_serving_namespace_cannot_accumulate_products_in_different_currencies():
    from storage.storage_manager import storage
    from database.export_gold import export_gold_to_db
    from database.warehouse import WarehouseManager
    product = {'product_id':'p1','product_name':'USD product','brand':'Fixture','category':'Phone','price':100,'currency':'USD'}
    storage.write_silver_parquet('products',pd.DataFrame([product]))
    export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    storage.write_silver_parquet('products',pd.DataFrame([dict(product,product_id='p2',currency='VND')]))
    with pytest.raises(ValueError, match='currency'):
        export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    wh = WarehouseManager(mode='test')
    try:
        assert wh.query('SELECT product_id,currency FROM products').to_dict('records') == [
            {'product_id':'p1','currency':'USD'}]
    finally:
        wh.close()

def test_serving_namespace_cannot_append_known_currency_to_existing_unknown_unit():
    from storage.storage_manager import storage
    from database.export_gold import export_gold_to_db
    from database.warehouse import WarehouseManager
    product={'product_id':'legacy','product_name':'Explicit fixture','brand':'Fixture','category':'Phone','price':100,'currency':None}
    storage.write_silver_parquet('products',pd.DataFrame([product]))
    export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    storage.write_silver_parquet('products',pd.DataFrame([dict(product,product_id='new',currency='USD')]))
    with pytest.raises(ValueError,match='currency'):
        export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    wh=WarehouseManager(mode='test')
    try:assert wh.query('SELECT product_id FROM products')['product_id'].tolist()==['legacy']
    finally:wh.close()


def test_gold_only_export_cannot_cross_catalog_currency():
    from storage.storage_manager import storage
    from database.export_gold import export_gold_to_db
    product={'product_id':'p','product_name':'Explicit fixture','brand':'Fixture','category':'Phone','price':100,'currency':'USD'}
    storage.write_silver_parquet('products',pd.DataFrame([product]))
    export_gold_to_db(marts=[],silver_tables=['products'],mode='test')
    storage.write_gold_parquet('price_daily_stats',pd.DataFrame([{
        'date':'2026-11-01','product_id':'p','avg_price':1000,'currency':'VND'}]))
    with pytest.raises(ValueError,match='currency'):
        export_gold_to_db(marts=['price_daily_stats'],silver_tables=[],mode='test')

def test_rows_without_any_monetary_observation_do_not_invent_an_unknown_unit():
    from analytics.currency import require_single_currency
    no_amount=pd.DataFrame([{'avg_price':None,'min_price':None,'max_price':None,'currency':None}])
    known=pd.DataFrame([{'price':100,'currency':'VND'}])
    assert require_single_currency(no_amount,known)=='VND'
