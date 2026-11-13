"""Daily-price alerts share the actual warehouse publication transaction."""

import pandas as pd
import pytest
from database.export_gold import export_gold_to_db
from database.warehouse import WarehouseManager
from storage.storage_manager import storage

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("quality,expected", [("valid", 2), ("unverified", 0)])
def test_export_stages_causal_prices_only_with_current_quality_receipt(quality, expected):
    rows = [
        dict(
            date=f"2026-11-{day:02d}",
            product_id="p",
            avg_price=value,
            currency="VND",
            source_group="simulation",
            run_id="g",
        )
        for day, value in [(1, 100), (2, 100), (3, 100), (4, 200)]
    ]
    storage.write_gold_parquet("price_daily_stats", pd.DataFrame(rows))
    storage.write_json(
        "gold/_snapshot.json",
        {
            "run_id": "g",
            "status": "complete",
            "source_group": "simulation",
            "rows": {"price_daily_stats": 4},
            "quality": {"status": quality, "run_id": "g", "required_datasets": ["prices"]},
        },
    )
    export_gold_to_db(marts=["price_daily_stats"], silver_tables=[], mode="test")
    wh = WarehouseManager(mode="test")
    try:
        assert wh.query("SELECT * FROM serving_alerts WHERE state='firing'").shape[0] == expected
        assert wh.query("SELECT * FROM serving_alerts").shape[0] == 8
    finally:
        wh.close()
