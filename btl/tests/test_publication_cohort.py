"""Transport-free policy test: partial exports must not advertise a complete service generation."""

import pandas as pd
import pytest

pytestmark = pytest.mark.unit


def test_partial_service_export_cannot_advance_current_generation(monkeypatch):
    import database.export_gold as exporter
    from database.warehouse import WarehouseManager
    from spark.etl.gold_aggregator import GoldAggregator
    from storage.storage_manager import storage

    products = pd.DataFrame(
        [
            {
                "product_id": "p",
                "product_name": "Explicit policy fixture",
                "brand": "A",
                "category": "Phone",
                "price": 10,
                "currency": "VND",
            }
        ]
    )
    reviews = pd.DataFrame([{"review_id": "r", "product_id": "p", "rating": 5, "review_date": "2026-11-13T00:00:00Z"}])
    GoldAggregator.build_gold_marts(products, reviews, run_id="cohort")
    storage.write_silver_parquet("products", products)
    storage.write_silver_parquet("reviews", reviews)
    # Exercise integration publication policy against an isolated SQLite adapter;
    # real PostgreSQL transport is covered by separate native gates.
    monkeypatch.setattr(exporter, "WarehouseManager", lambda **kwargs: WarehouseManager(mode="test"))
    with pytest.raises(ValueError, match="complete|cohort|partial"):
        exporter.export_gold_to_db(marts=["product_daily_stats"], silver_tables=[], mode="integration")
    wh = WarehouseManager(mode="test")
    try:
        assert wh.query("SELECT COUNT(*) AS n FROM product_daily_stats").iloc[0]["n"] == 0
    finally:
        wh.close()
