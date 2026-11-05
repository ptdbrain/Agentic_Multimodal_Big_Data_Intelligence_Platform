"""Actual PostgreSQL serialization, conflict ordering and bounded round trips."""
import json
import uuid
import pandas as pd
import pytest
from database.warehouse import WarehouseManager

pytestmark = pytest.mark.integration


@pytest.fixture
def table():
    wh = WarehouseManager(mode='integration')
    name = 'bulk_acceptance_' + uuid.uuid4().hex[:12]
    wh.execute(f'CREATE TABLE {name}(id BIGINT PRIMARY KEY, amount NUMERIC CHECK(amount>=0), metadata TEXT, observed_at TIMESTAMP)')
    try:
        yield wh,name
    finally:
        wh.execute(f'DROP TABLE IF EXISTS {name}')
        wh.close()


def test_large_numeric_identity_is_not_rounded_by_pandas_row_coercion(table):
    wh,name = table
    wh.upsert_dataframe(name,pd.DataFrame({'id':[9007199254740993],'amount':[1.5]}),['id'])
    assert wh.query(f'SELECT id FROM {name}').iloc[0]['id'] == 9007199254740993


def test_thousand_rows_use_bounded_database_round_trips(table):
    import psycopg2.extensions
    wh,name = table
    calls = []

    class CountCursor(psycopg2.extensions.cursor):
        def execute(self, statement, parameters=None):
            calls.append(1)
            return super().execute(statement,parameters)

    wh._conn.cursor_factory = CountCursor
    assert wh.upsert_dataframe(name,pd.DataFrame({'id':range(1000),'amount':[1.5]*1000}),['id']) == 1000
    measured = len(calls)
    assert wh.query(f'SELECT COUNT(*) n FROM {name}').iloc[0]['n'] == 1000
    assert measured <= 6  # metadata lookup plus bounded SQL batches, not one RPC per row


def test_duplicate_keys_keep_input_order_and_nullable_nested_values(table):
    wh,name = table
    rows = pd.DataFrame([
        {'id':1,'amount':1.5,'metadata':['first'],'observed_at':pd.Timestamp('2026-11-01')},
        {'id':2,'amount':2.5,'metadata':None,'observed_at':pd.NaT},
        {'id':1,'amount':3.5,'metadata':{'last':True},'observed_at':pd.Timestamp('2026-11-02')},
    ])
    assert wh.upsert_dataframe(name,rows,['id']) == 3
    result = wh.query(f'SELECT * FROM {name} ORDER BY id')
    assert result['id'].tolist() == [1,2]
    assert result['amount'].tolist() == [3.5,2.5]
    assert json.loads(result.iloc[0]['metadata']) == {'last':True}
    assert result.iloc[1]['metadata'] is None and pd.isna(result.iloc[1]['observed_at'])


def test_later_failed_batch_rolls_back_earlier_batches(table):
    wh,name = table
    wh.upsert_dataframe(name,pd.DataFrame([{'id':1,'amount':1}]),['id'])
    rows = pd.DataFrame({'id':range(1,1002),'amount':[2]*1000+[-1]})
    with pytest.raises(RuntimeError,match='Upsert transaction failed'):
        wh.upsert_dataframe(name,rows,['id'])
    assert wh.query(f'SELECT id,amount FROM {name}').to_dict('records') == [{'id':1,'amount':1.0}]
