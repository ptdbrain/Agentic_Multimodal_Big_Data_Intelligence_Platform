"""Registry selection changes the actual SQLite test connection, never global credentials."""

import os, shutil, sqlite3
from pathlib import Path
import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[1]


def test_app_selects_commerce_and_article_sources_independently(tmp_path, monkeypatch):
    from tests.test_serving_registry import registry_file
    from serving.registry import DatasetRegistry
    from serving.store import ServingStore
    from database.warehouse import WarehouseManager
    from config.settings import settings
    from streamlit.testing.v1 import AppTest

    wh = WarehouseManager(mode="test")
    wh.initialize_schema()
    wh.close()
    config = registry_file(tmp_path)
    registry = DatasetRegistry.load(config)
    for key, item in registry.datasets.items():
        shutil.copyfile(settings.database.sqlite_path, item.sqlite_path)
        with sqlite3.connect(item.sqlite_path) as c:
            if item.kind == "commerce":
                method = "generated" if item.source_group == "simulation" else "live"
                c.execute(
                    "INSERT INTO products(product_id,product_name,brand,category,price,currency,source_group,acquisition_method) VALUES(?,?,?,?,?,?,?,?)",
                    ("p", key, key, "Phone", 10, "VND", item.source_group, method),
                )
                c.execute(
                    "INSERT INTO reviews(review_id,product_id,rating,review_date,source_group,acquisition_method) VALUES(?,?,?,?,?,?)",
                    ("r", "p", 4, "2026-11-13", item.source_group, method),
                )
            else:
                c.execute(
                    "INSERT INTO articles(article_id,source_group,source_name,source_url,acquisition_method,title,body,collected_at,content_sha256,run_id) VALUES('a','web_content','Fixture','https://example.test','live','Web fixture','Explicit isolated test','2026-11-13','hash','run')"
                )
            store = ServingStore(c, key, item.source_group)
            store.initialize()
    monkeypatch.setenv("SERVING_DATASETS_CONFIG", str(config))
    before = {k: v for k, v in os.environ.items() if k.startswith("POSTGRES") or k == "PGOPTIONS"}
    app = AppTest.from_file(str(ROOT / "dashboard/app.py")).run(timeout=25)
    assert not app.exception
    app.switch_page("pages/9_Basic_Analytics.py").run(timeout=25)
    assert not app.exception
    assert app.selectbox(key="serving_commerce").value == "tiki"
    assert any("tiki" in str(frame.value) for frame in app.dataframe)
    app.selectbox(key="dashboard_data_scope").set_value("simulation").run(timeout=25)
    assert not app.exception
    assert app.selectbox(key="serving_commerce").value == "simulation"
    assert any("simulation" in str(frame.value) for frame in app.dataframe)
    app.selectbox(key="dashboard_data_scope").set_value("real").run(timeout=25)
    app.switch_page("pages/8_Web_Content.py").run(timeout=25)
    assert not app.exception
    assert any("Web fixture" in str(frame.value) for frame in app.dataframe)
    assert before == {k: v for k, v in os.environ.items() if k.startswith("POSTGRES") or k == "PGOPTIONS"}
    app.switch_page("pages/10_Streaming.py").run(timeout=25)
    assert not app.exception
    assert any("Chưa có generation" in item.value for item in app.markdown)
    app.switch_page("pages/11_Search.py").run(timeout=25)
    assert not app.exception
    assert any("Chỉ mục chưa sẵn sàng" in item.value for item in app.markdown)
    app.switch_page('pages/6_Anomalies.py').run(timeout=25)
    assert not app.exception
    assert any('Chưa có đánh giá thống kê' in item.value for item in app.info)
    app.switch_page('pages/12_Lineage.py').run(timeout=25)
    assert not app.exception
    assert any('Chưa có receipt' in item.value for item in app.markdown)
