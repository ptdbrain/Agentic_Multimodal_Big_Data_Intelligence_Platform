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
    app.switch_page("pages/6_Anomalies.py").run(timeout=25)
    assert not app.exception
    assert any("Chưa có đánh giá thống kê" in item.value for item in app.info)
    app.switch_page("pages/12_Lineage.py").run(timeout=25)
    assert not app.exception
    assert any("Chưa có receipt" in item.value for item in app.markdown)
    # Exercise actual buttons against pinned Parquet/JSON, rather than only empty-state rendering.
    import hashlib, json
    from datetime import date
    import pandas as pd
    from serving.lineage import stage_lineage

    binding = registry.datasets["tiki"]
    lake_root = tmp_path / "ui-evidence"
    binding.environment_file.write_text(
        binding.environment_file.read_text() + "\nLOCAL_DATA_DIR=" + str(lake_root) + "\n"
    )
    bronze = lake_root / "bronze/reviews/batch.json"
    silver = lake_root / "silver/serving_snapshots/ui/reviews/data.parquet"
    gold = lake_root / "gold/serving_snapshots/ui/product_daily_stats/data.parquet"
    for path in (bronze, silver, gold):
        path.parent.mkdir(parents=True, exist_ok=True)
    bronze.write_text(
        json.dumps(
            [
                {
                    "event_id": "ui-event",
                    "payload": {
                        "review_id": "r",
                        "product_id": "p",
                        "rating": 4,
                        "review_date": "2026-11-13T00:00:00Z",
                    },
                }
            ]
        )
    )
    pd.DataFrame(
        [
            {
                "event_id": "ui-event",
                "review_id": "r",
                "product_id": "p",
                "rating": 4,
                "review_date": "2026-11-13T00:00:00Z",
            }
        ]
    ).to_parquet(silver)
    pd.DataFrame([{"date": "2026-11-13", "product_id": "p", "review_count": 1, "avg_rating": 4}]).to_parquet(gold)

    def reference(path):
        raw = path.read_bytes()
        return {
            "path": path.relative_to(lake_root).as_posix(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size": len(raw),
        }

    with sqlite3.connect(binding.sqlite_path) as connection:
        store = ServingStore(connection, binding.id, binding.source_group)
        stage_lineage(
            store,
            "ui",
            {
                "run_id": "ui",
                "bronze": [reference(bronze)],
                "silver": [reference(silver)],
                "gold": [reference(gold)],
                "tables": {"reviews": [reference(silver)["path"]]},
                "marts": {"product_daily_stats": [reference(gold)["path"]]},
            },
        )
    app.run(timeout=25)
    next(widget for widget in app.selectbox if widget.label == "Loại bản ghi").set_value("reviews").run(timeout=25)
    next(widget for widget in app.text_input if widget.label == "Mã bản ghi chính xác").set_value("r").run(timeout=25)
    next(widget for widget in app.button if widget.label == "Kiểm tra bằng chứng").click().run(timeout=25)
    assert not app.exception
    assert any("verified_winner" in item.value for item in app.markdown)
    next(widget for widget in app.radio if widget.label == "Kiểu truy vết").set_value("Số liệu tổng hợp").run(
        timeout=25
    )
    app.date_input[0].set_value(date(2026, 11, 13)).run(timeout=25)
    next(widget for widget in app.text_input if widget.label == "product_id").set_value("p").run(timeout=25)
    next(widget for widget in app.button if widget.label == "Kiểm tra bằng chứng").click().run(timeout=25)
    assert not app.exception
    assert any("verified_aggregate" in item.value for item in app.markdown)
    assert any(frame.value.get("review_id", pd.Series(dtype=str)).tolist() == ["r"] for frame in app.dataframe)
