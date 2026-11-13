"""Real Streamlit widget interactions over explicit isolated warehouse fixtures."""

from pathlib import Path
import pandas as pd
import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[1]


def app():
    from streamlit.testing.v1 import AppTest

    return AppTest.from_file(str(ROOT / "dashboard/app.py")).run(timeout=25)


def seed():
    from database.warehouse import WarehouseManager
    from tests.test_basic_analytics import inputs

    p, r, prices = inputs()
    p = p.assign(product_name=p["product_id"])
    r = r.loc[r["review_id"].ne("orphan")]
    warehouse = WarehouseManager(mode="test")
    warehouse.initialize_schema()
    warehouse.upsert_dataframe("products", p, ["product_id"])
    warehouse.upsert_dataframe("reviews", r, ["review_id"])
    warehouse.upsert_dataframe("price_daily_stats", prices.loc[prices["currency"].notna()], ["date", "product_id"])
    warehouse.close()


def test_basic_page_registered_and_filters_change_kpis_and_exports():
    seed()
    view = app().switch_page("pages/9_Basic_Analytics.py").run(timeout=25)
    assert not view.exception
    assert any(t.value == "Phân tích cơ bản" for t in view.title)
    # The first table is the selected dimension comparison, independently checked.
    assert set(view.dataframe[0].value["brand"]) == {"A", "B"}
    view.multiselect(key="basic_brands").set_value(["B"]).run(timeout=25)
    assert not view.exception
    assert view.dataframe[0].value["brand"].tolist() == ["B"]
    assert any("0 mẫu điểm hợp lệ" in x.value for x in view.markdown)
    assert any("Chưa đủ" in x.value or "chưa đủ" in x.value for x in view.markdown)
    view.multiselect(key="basic_brands").set_value([]).run(timeout=25)
    view.selectbox(key="basic_price_product").set_value("p1").run(timeout=25)
    assert not view.exception
    history = next(d.value for d in view.dataframe if {"date", "avg_price"}.issubset(d.value.columns))
    assert history["avg_price"].tolist() == [100, 80]
    view.checkbox(key="basic_date_filter").check().run(timeout=25)
    view.date_input(key="basic_dates").set_value(
        (pd.Timestamp("2026-01-02").date(), pd.Timestamp("2026-01-03").date())
    ).run(timeout=25)
    assert not view.exception
    assert any("2 mẫu điểm hợp lệ" in x.value for x in view.markdown)
    history = next(d.value for d in view.dataframe if {"date", "avg_price"}.issubset(d.value.columns))
    assert history["avg_price"].tolist() == [80]


def test_empty_basic_page_has_no_sample_fallback_or_exceptions():
    from database.warehouse import WarehouseManager

    warehouse = WarehouseManager(mode="test")
    warehouse.initialize_schema()
    warehouse.close()
    view = app().switch_page("pages/9_Basic_Analytics.py").run(timeout=25)
    assert not view.exception
    assert any("Chưa có dữ liệu" in m.value for m in view.markdown)
    assert not view.dataframe


def test_selected_product_price_export_matches_the_display(monkeypatch):
    from dashboard import ui

    seed()
    downloads = {}
    original = ui.download

    def observe(frame, filename="du-lieu.csv"):
        downloads[filename] = frame.copy(deep=True)
        return original(frame, filename)

    monkeypatch.setattr(ui, "download", observe)
    view = app().switch_page("pages/9_Basic_Analytics.py").run(timeout=25)
    view.selectbox(key="basic_price_product").set_value("p2").run(timeout=25)
    assert not view.exception
    assert downloads["bien-dong-gia-co-ban.csv"]["product_id"].tolist() == ["p2"]
    assert downloads["lich-su-gia-san-pham.csv"]["product_id"].unique().tolist() == ["p2"]
