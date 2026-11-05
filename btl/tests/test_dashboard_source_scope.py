"""Analytical views must default to real data and expose other scopes explicitly."""

import pandas as pd
import pytest
from database.warehouse import WarehouseManager
from dashboard.db_connector import DashboardDB

pytestmark = pytest.mark.unit


def seed():
    wh = WarehouseManager(mode="test")
    wh.initialize_schema()
    wh.upsert_dataframe(
        "products",
        pd.DataFrame(
            [
                {"product_id": "real", "source_group": "marketplace", "acquisition_method": "live"},
                {"product_id": "sim", "source_group": "simulation", "acquisition_method": "generated"},
                {"product_id": "old", "source_group": None, "acquisition_method": None},
                {"product_id": "wrong", "source_group": "marketplace", "acquisition_method": "generated"},
            ]
        ).assign(product_name="Fixture product", brand="Fixture", category="Phone", price=1, currency="VND"),
        ["product_id"],
    )
    wh.upsert_dataframe(
        "product_daily_stats",
        pd.DataFrame(
            [
                {
                    "date": "2026-11-01",
                    "product_id": "real",
                    "review_count": 2,
                    "avg_rating": 1,
                    "source_group": "marketplace",
                },
                {
                    "date": "2026-11-01",
                    "product_id": "sim",
                    "review_count": 100,
                    "avg_rating": 5,
                    "source_group": "simulation",
                },
            ]
        ),
        ["date", "product_id"],
    )
    wh.close()


def test_real_scope_excludes_simulations_unknowns_and_contradictory_labels():
    seed()
    db = DashboardDB(mode="test", data_scope="real")
    try:
        summary = db.get_kpi_summary()
        assert summary["total_products"] == 1
        assert summary["total_reviews"] == 2
        assert summary["avg_rating"] == 1
        assert db.read_table("products")["product_id"].tolist() == ["real"]
    finally:
        db.close()


def test_all_scope_is_explicit_and_keeps_diagnostics_available():
    seed()
    db = DashboardDB(mode="test", data_scope="all")
    try:
        assert len(db.read_table("products")) == 4
        assert db.get_kpi_summary()["total_reviews"] == 102
    finally:
        db.close()


def test_dashboard_defaults_to_real_and_all_is_a_visible_choice(monkeypatch):
    from pathlib import Path
    from streamlit.testing.v1 import AppTest

    seed()
    monkeypatch.delenv("DASHBOARD_DATA_SCOPE", raising=False)
    page = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py")).run(timeout=20)
    assert not page.exception
    assert page.selectbox[0].value == "real"
    card = next(item.value for item in page.markdown if 'kpi-label">Sản phẩm' in item.value)
    assert ">1</div>" in card
    page.selectbox[0].set_value("all").run()
    card = next(item.value for item in page.markdown if 'kpi-label">Sản phẩm' in item.value)
    assert ">4</div>" in card


def test_pipeline_bronze_sample_respects_selected_source_scope():
    # Controlled fixtures exercise UI filtering; they are not live-crawl evidence.
    from pathlib import Path
    from streamlit.testing.v1 import AppTest
    from config.settings import settings
    from storage.storage_manager import storage

    wh = WarehouseManager(mode="test")
    wh.initialize_schema()
    wh.close()
    storage.write_bronze_json(
        settings.kafka.topic_reviews,
        [
            {"review_id": "observed-fixture", "source_group": "marketplace", "acquisition_method": "live"},
            {"review_id": "generated-fixture", "source_group": "simulation", "acquisition_method": "generated"},
        ],
    )
    page = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/pages/2_Pipeline.py"))
    page.session_state["dashboard_data_scope"] = "real"
    page.run(timeout=20)
    assert not page.exception
    frames = [item.value for item in page.dataframe if "review_id" in item.value.columns]
    assert len(frames) == 1
    assert frames[0]["review_id"].tolist() == ["observed-fixture"]


def test_raw_group_without_acquisition_is_unverified_in_samples():
    from dashboard.utils import filter_source_scope

    frame = pd.DataFrame([{"source_group": "marketplace", "review_id": "unknown"}])
    assert filter_source_scope(frame, "real").empty
    assert len(filter_source_scope(frame, "unverified")) == 1


def test_legacy_raw_table_without_acquisition_is_not_promoted_to_real():
    wh = WarehouseManager(mode="test")
    wh.execute("CREATE TABLE products(product_id TEXT PRIMARY KEY,source_group TEXT)")
    wh.execute("INSERT INTO products VALUES('old','marketplace')")
    wh.close()
    db = DashboardDB(mode="test", data_scope="real")
    try:
        assert db.read_table("products").empty
    finally:
        db.close()
