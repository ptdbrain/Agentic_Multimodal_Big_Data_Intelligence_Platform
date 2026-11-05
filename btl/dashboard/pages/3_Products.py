"""Explore the catalog with explicit source scope and combinable filters."""

import pandas as pd
import plotly.express as px
import streamlit as st
from analytics.descriptive.stats import DescriptiveStats
from dashboard import ui
from dashboard.experience import filter_catalog, options, page_slice, clear_keys, price_comparison
from dashboard.utils import load_catalog_data, load_reviews_data, render_data_sources, render_analytics_quality

ui.setup_page(
    "Khám phá sản phẩm", "Tìm sản phẩm, so sánh điểm đánh giá và đọc dữ liệu giá theo đúng đơn vị quan sát.", "products"
)
with ui.warehouse() as db:
    products = load_catalog_data(db=db)
    reviews = load_reviews_data(db=db)
render_data_sources(products, reviews)
if products.empty:
    ui.empty_state(
        "Chưa có sản phẩm trong phạm vi này", "Chọn một phạm vi dữ liệu khác hoặc hoàn tất lượt thu thập sản phẩm."
    )
    ui.footer()
    st.stop()
metrics = DescriptiveStats.compute_product_metrics(reviews, products)
with st.container(border=True):
    query = st.text_input(
        "Tìm sản phẩm",
        placeholder="Tên sản phẩm, thương hiệu hoặc mã sản phẩm…",
        key="catalog_query",
        on_change=clear_keys,
        args=("catalog_page",),
    )
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1, 0.7])
    brands = c1.multiselect(
        "Thương hiệu", options(metrics, "brand"), key="catalog_brands", on_change=clear_keys, args=("catalog_page",)
    )
    categories = c2.multiselect(
        "Danh mục", options(metrics, "category"), key="catalog_categories", on_change=clear_keys, args=("catalog_page",)
    )
    sources = c3.multiselect(
        "Nguồn", options(metrics, "source_name"), key="catalog_sources", on_change=clear_keys, args=("catalog_page",)
    )
    with c4:
        st.caption("Bộ lọc")
        st.button(
            "Xóa bộ lọc",
            key="catalog_reset",
            on_click=clear_keys,
            args=(
                "catalog_query",
                "catalog_brands",
                "catalog_categories",
                "catalog_sources",
                "catalog_minimum",
                "catalog_page",
            ),
            width="stretch",
        )
with st.expander("Thêm bộ lọc điểm đánh giá"):
    minimum = st.slider(
        "Điểm tối thiểu",
        0.0,
        5.0,
        0.0,
        0.5,
        key="catalog_minimum",
        help="Chọn 0 để giữ cả sản phẩm chưa có điểm.",
        on_change=clear_keys,
        args=("catalog_page",),
    )
filtered = filter_catalog(
    metrics, query=query, brands=brands, categories=categories, sources=sources, minimum_rating=minimum
)
c1, c2, c3 = st.columns(3)
with c1:
    ui.metric("Sản phẩm tìm thấy", ui.number(len(filtered)), f"Trong {len(products):,} sản phẩm của phạm vi đang chọn")
with c2:
    ui.metric("Thương hiệu", ui.number(filtered["brand"].nunique()) if "brand" in filtered else "Chưa có dữ liệu")
with c3:
    sample = pd.to_numeric(filtered.get("rating_sample_size", pd.Series(dtype=float)), errors="coerce").sum()
    ui.metric("Mẫu điểm hợp lệ", ui.number(sample), "Giữ riêng số đánh giá trên sàn và số đã thu")
st.write("")
catalog_tab, position_tab = st.tabs(["Danh mục sản phẩm", "So sánh giá & đánh giá"])
with catalog_tab:
    if filtered.empty:
        ui.empty_state("Không tìm thấy sản phẩm", "Thử từ khóa ngắn hơn hoặc xóa bộ lọc hiện tại.", "⌕")
    else:
        toolbar, export = st.columns([3, 1])
        with toolbar:
            st.caption(f"{len(filtered):,} sản phẩm phù hợp · Giá giữ nguyên đơn vị nguồn")
        with export:
            ui.download(filtered, "san-pham.csv")
        size = 20
        _, current, pages = page_slice(filtered, st.session_state.get("catalog_page", 1), size)
        if st.session_state.get("catalog_page", 1) != current:
            st.session_state["catalog_page"] = current
        ui.table(
            page_slice(filtered, current, size)[0],
            [
                "product_id",
                "product_name",
                "brand",
                "category",
                "price",
                "currency",
                "avg_rating",
                "review_count",
                "catalog_review_count",
                "rating_sample_size",
                "source_name",
            ],
        )
        a, b = st.columns([1, 4])
        with a:
            st.number_input("Trang", min_value=1, max_value=pages, step=1, key="catalog_page")
        with b:
            st.caption(f"Trang {current}/{pages} · Tối đa {size} sản phẩm mỗi trang")
with position_tab:
    if {"price", "avg_rating", "review_count", "brand"}.issubset(filtered):
        plotted = price_comparison(filtered)
        st.caption(
            "Mỗi đơn vị tiền tệ có một biểu đồ riêng. Chỉ hiển thị sản phẩm có giá, đơn vị tiền tệ và điểm hợp lệ; không tự quy đổi giá."
        )
        if not plotted.empty:
            ui.chart(
                px.scatter(
                    plotted,
                    x="price",
                    y="avg_rating",
                    size="review_count",
                    color="brand",
                    facet_col="currency",
                    hover_name="product_name",
                    labels={"price": "Giá theo đơn vị nguồn", "avg_rating": "Điểm trung bình", "brand": "Thương hiệu"},
                    color_discrete_sequence=ui.COLORS,
                ).update_xaxes(matches=None),
                400,
            )
        else:
            ui.empty_state(
                "Chưa đủ dữ liệu để so sánh", "Biểu đồ cần cả giá và điểm đánh giá hợp lệ của cùng sản phẩm."
            )
render_analytics_quality(metrics)
ui.footer()
