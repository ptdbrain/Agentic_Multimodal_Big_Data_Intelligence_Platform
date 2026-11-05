import pandas as pd
import pytest
from database.warehouse import WarehouseManager

pytestmark = pytest.mark.unit


def test_sqlite_serving_keeps_large_numeric_id_exact():
    wh = WarehouseManager(mode='test')
    try:
        wh.execute('CREATE TABLE observations(id INTEGER PRIMARY KEY, amount REAL)')
        wh.upsert_dataframe('observations',pd.DataFrame({'id':[9007199254740993],'amount':[1.5]}),['id'])
        assert wh.query('SELECT id FROM observations').iloc[0]['id'] == 9007199254740993
    finally:
        wh.close()
