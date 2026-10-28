import pandas as pd
import pytest

from dashboard.db_connector import DashboardDB
from dashboard.utils import load_catalog_data, load_reviews_data
from database.warehouse import WarehouseManager

pytestmark = pytest.mark.unit


def test_page_loaders_share_one_warehouse_snapshot_across_concurrent_updates():
    wh = WarehouseManager(mode='test'); wh.initialize_schema()
    wh.upsert_dataframe('products', pd.DataFrame([{'product_id':'p','product_name':'Phone','brand':'Old','category':'Phone','price':10.}]), ['product_id'])
    wh.upsert_dataframe('reviews', pd.DataFrame([{'review_id':'r','product_id':'p','rating':5.}]), ['review_id']); wh.close()
    db = DashboardDB(mode='test')
    try:
        products = load_catalog_data(db=db)
        wh = WarehouseManager(mode='test')
        with wh.transaction():
            wh.upsert_dataframe('products', pd.DataFrame([{'product_id':'p','product_name':'Phone','brand':'New','category':'Phone','price':10.}]), ['product_id'])
            wh.upsert_dataframe('reviews', pd.DataFrame([{'review_id':'r','product_id':'p','rating':1.}]), ['review_id'])
        wh.close()
        reviews = load_reviews_data(db=db)
        assert products.iloc[0]['brand'] == 'Old'
        assert reviews.iloc[0]['rating'] == 5.0
    finally:
        db.close()
