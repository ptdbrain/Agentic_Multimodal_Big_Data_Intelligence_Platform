import sqlite3
import pytest
from database.warehouse import WarehouseManager
from config.settings import settings

pytestmark = pytest.mark.unit


def test_existing_dq_schema_migrates_without_losing_history():
    c=sqlite3.connect(settings.database.sqlite_path)
    c.execute('CREATE TABLE data_quality_metrics (batch_id TEXT PRIMARY KEY,dataset_name TEXT,records_received INTEGER,records_valid INTEGER,records_invalid INTEGER,records_duplicate INTEGER,records_missing INTEGER,dq_score REAL,processing_time_ms INTEGER NOT NULL,timestamp TEXT)')
    c.execute("INSERT INTO data_quality_metrics VALUES ('legacy','products',1,1,0,0,0,100,10,'2026-10-01')")
    c.commit();c.close()
    wh=WarehouseManager(mode='test'); wh.initialize_schema(); wh.initialize_schema()
    cols=wh._conn.execute('PRAGMA table_info(data_quality_metrics)').fetchall()
    assert 'run_id' in {r[1] for r in cols}
    assert next(r for r in cols if r[1]=='processing_time_ms')[3] == 0
    assert len(wh.query('SELECT * FROM data_quality_metrics')) == 1
    wh.close()
