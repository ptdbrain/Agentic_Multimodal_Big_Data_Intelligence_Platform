"""User-facing filtering preserves unknown facts and searches literal Vietnamese text."""

import pandas as pd
import pytest

pytestmark = pytest.mark.unit


def catalog():
    return pd.DataFrame(
        [
            {
                "product_id": "p1",
                "product_name": "Ốp lưng điện thoại",
                "brand": "Apple",
                "category": "Phụ kiện",
                "source_name": "tiki",
                "price": 100,
                "currency": "VND",
                "avg_rating": 4.5,
            },
            {
                "product_id": "p2",
                "product_name": "Điện thoại [mới]",
                "brand": "Nokia",
                "category": "Điện thoại",
                "source_name": "lazada",
                "price": 200,
                "currency": "VND",
                "avg_rating": None,
            },
        ]
    )


def test_product_search_is_accent_insensitive_and_literal():
    from dashboard.experience import filter_catalog

    assert filter_catalog(catalog(), query="op lung")["product_id"].tolist() == ["p1"]
    assert filter_catalog(catalog(), query="[moi]")["product_id"].tolist() == ["p2"]


def test_combined_filters_preserve_missing_rating_until_requested():
    from dashboard.experience import filter_catalog

    data = catalog()
    before = data.copy(deep=True)
    assert len(filter_catalog(data)) == 2
    assert filter_catalog(data, brands=["Apple"], sources=["tiki"], minimum_rating=4)["product_id"].tolist() == ["p1"]
    assert filter_catalog(data, minimum_rating=1)["product_id"].tolist() == ["p1"]
    pd.testing.assert_frame_equal(data, before)


def test_pagination_bounds_reset_a_page_after_narrowing_results():
    from dashboard.experience import page_slice

    data = catalog()
    shown, page, pages = page_slice(data, page=8, size=1)
    assert page == 2 and pages == 2 and shown["product_id"].tolist() == ["p2"]
    shown, page, pages = page_slice(data.iloc[:0], page=8, size=20)
    assert shown.empty and page == 1 and pages == 1


def test_kpi_html_escapes_external_text():
    from dashboard.utils import create_kpi_card

    card = create_kpi_card("<script>bad</script>", "<b>2</b>", "<img src=x>")
    assert "<script>" not in card and "<img" not in card
    assert "&lt;script&gt;" in card


def test_csv_export_preserves_data_and_neutralizes_spreadsheet_formulas():
    from dashboard.experience import csv_download_bytes
    from io import StringIO

    data = pd.DataFrame({"text": ["=1+1", "Điện thoại"], "value": [-2, 3]})
    exported = pd.read_csv(StringIO(csv_download_bytes(data).decode("utf-8-sig")))
    assert exported.loc[0, "text"] == "'=1+1"
    assert exported.loc[1, "text"] == "Điện thoại"
    assert exported.loc[0, "value"] == -2
    assert data.loc[0, "text"] == "=1+1"


def test_product_page_search_and_reset_keep_real_result_counts():
    from pathlib import Path
    from streamlit.testing.v1 import AppTest
    from database.warehouse import WarehouseManager

    warehouse = WarehouseManager(mode="test")
    warehouse.initialize_schema()
    warehouse.upsert_dataframe("products", catalog().drop(columns=["avg_rating"]), ["product_id"])
    warehouse.close()
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/pages/3_Products.py")).run(timeout=20)
    assert not app.exception
    search = next(x for x in app.text_input if x.label == "Tìm sản phẩm")
    search.set_value("op lung").run()
    assert not app.exception
    assert app.dataframe[0].value["product_id"].tolist() == ["p1"]
    next(x for x in app.button if x.label == "Xóa bộ lọc").click().run()
    assert not app.exception
    assert len(app.dataframe[0].value) == 2
    assert app.dataframe[0].value["avg_rating"].isna().all()


def test_purchase_verification_accepts_database_boolean_values_without_inventing_unknowns():
    from dashboard.experience import purchase_status

    assert [purchase_status(value) for value in [True, 1, "true"]] == ["Đã xác minh"] * 3
    assert [purchase_status(value) for value in [False, 0, "false"]] == ["Chưa xác minh"] * 3
    assert [purchase_status(value) for value in [None, pd.NA, "unknown"]] == ["Chưa có thông tin"] * 3


def test_registered_navigation_preserves_scope_and_review_filters_work():
    from pathlib import Path
    from streamlit.testing.v1 import AppTest
    from database.warehouse import WarehouseManager

    warehouse = WarehouseManager(mode="test")
    warehouse.initialize_schema()
    warehouse.upsert_dataframe(
        "reviews",
        pd.DataFrame(
            [
                {
                    "review_id": "r1",
                    "product_id": "p1",
                    "rating": 5,
                    "review_text": "Đóng gói đẹp",
                    "review_date": "2026-11-01",
                },
                {
                    "review_id": "r2",
                    "product_id": "p2",
                    "rating": 2,
                    "review_text": "Giao hàng chậm",
                    "review_date": "2026-11-02",
                },
            ]
        ),
        ["review_id"],
    )
    warehouse.close()
    app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard/app.py")).run(timeout=20)
    app.switch_page("pages/4_Reviews.py").run(timeout=20)
    assert not app.exception
    assert app.session_state["dashboard_data_scope"] == "all"
    app.text_input(key="reviews_query").set_value("dong goi").run()
    assert app.dataframe[0].value["review_id"].tolist() == ["r1"]
    app.multiselect(key="reviews_stars").set_value([2]).run()
    assert not app.exception
    assert not app.dataframe
    next(button for button in app.button if button.label == "Xóa bộ lọc").click().run()
    assert set(app.dataframe[0].value["review_id"]) == {"r1", "r2"}
    app.switch_page("pages/8_Web_Content.py").run(timeout=20)
    assert not app.exception
    assert any("Chưa có bài viết" in item.value for item in app.markdown)


def test_price_comparison_keeps_units_separate_and_excludes_unknown_currency():
    from dashboard.experience import price_comparison

    frame = pd.DataFrame(
        [
            {"price": 100, "currency": "USD", "avg_rating": 4.5, "review_count": 3},
            {"price": 2000000, "currency": "VND", "avg_rating": 4, "review_count": 2},
            {"price": 300, "currency": None, "avg_rating": 4, "review_count": 1},
            {"price": -5, "currency": "USD", "avg_rating": 4, "review_count": 1},
        ]
    )
    result = price_comparison(frame)
    assert result["currency"].tolist() == ["USD", "VND"]
    assert result["price"].tolist() == [100, 2000000]
    assert pd.isna(frame.loc[2, "currency"])
