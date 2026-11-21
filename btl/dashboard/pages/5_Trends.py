import pandas as pd
import streamlit as st
import plotly.express as px
from analytics.basic import build_basic_report, price_sample_by_day, source_groups
from analytics.trend.trends import TrendAnalyzer
from dashboard import ui
from dashboard.experience import options
from dashboard.utils import load_reviews_data, render_data_sources, render_analytics_quality

ui.setup_page(
    "Diễn biến & xu hướng", "Theo dõi điểm đánh giá theo thời gian với cửa sổ quan sát và độ bao phủ rõ ràng.", "trends"
)
with ui.warehouse() as db:
    reviews = load_reviews_data(db=db)
    products = db.read_table("products")
    prices = db.read_table("price_daily_stats")
render_data_sources(reviews)
if reviews.empty and prices.empty:
    ui.empty_state(
        "Chưa đủ dữ liệu để phân tích xu hướng", "Cần đánh giá có ngày quan sát hợp lệ trong phạm vi đang chọn."
    )
    ui.footer()
    st.stop()
with st.container(border=True):
    a, b = st.columns(2)
    window = a.slider("Cửa sổ trung bình (ngày)", 3, 30, 7, 1)
    threshold = b.slider("Ngưỡng đánh giá tích cực (sao)", 3.0, 5.0, 4.0, 0.5)
    st.caption("Phân nhóm tích cực/tiêu cực theo điểm số. Ngày thiếu không được coi là ngày có 0 đánh giá.")
    groups = source_groups(products, reviews, prices)
    source = st.selectbox("Nhóm nguồn", groups, key="trend_source") if len(groups) > 1 else next(iter(groups), None)
    controls = st.columns(3)
    selected = {}
    for control, column, label in zip(controls, ("brand", "category", "seller"), ("Thương hiệu", "Danh mục", "Nhà bán")):
        selected[column] = control.multiselect(label, options(products, column), key=f"trend_{column}")
    st.caption("Các bộ lọc phân nhóm theo danh mục hiện tại. Khi áp dụng cho lịch sử, nhà bán là nhà bán đang được ghi nhận cho sản phẩm.")
if source is not None:
    review_groups = reviews.get("source_group", pd.Series("unverified", index=reviews.index, dtype=object))
    reviews = reviews.loc[review_groups.fillna("unverified").replace("", "unverified").eq(source)]
if any(selected.values()) and not reviews.empty:
    catalog_groups = products.get("source_group", pd.Series("unverified", index=products.index, dtype=object))
    catalog = products.loc[catalog_groups.fillna("unverified").replace("", "unverified").eq(source)]
    for dimension, values in selected.items():
        if values:
            catalog = catalog.loc[catalog[dimension].isin(values)]
    if "product_id" in reviews and "product_id" in catalog:
        reviews = reviews.loc[reviews["product_id"].isin(catalog["product_id"])]
    else:
        reviews = reviews.iloc[:0]
try:
    price_report = build_basic_report(
        products, None, prices, source_group=source, brands=selected["brand"],
        categories=selected["category"], sellers=selected["seller"],
    )
except ValueError as error:
    st.warning(str(error))
else:
    sampled = price_sample_by_day(price_report.price_daily)
    if not sampled.empty:
        with st.container(border=True):
            st.subheader("Giá quan sát theo ngày")
            st.caption("Mỗi sản phẩm có trọng số như nhau. Cột vào mẫu và rời mẫu so với ngày có quan sát trước đó giúp nhận biết khi thành phần mẫu thay đổi.")
            ui.chart(px.line(
                sampled, x="date", y="avg_price", facet_row="currency", markers=True,
                hover_data=["product_count", "common_products", "entered_products", "exited_products"],
                labels={"date": "Ngày (UTC)", "avg_price": "Giá trung bình", "currency": "Đơn vị"},
            ).update_yaxes(matches=None))
            ui.table(sampled)
            ui.download(sampled, "xu-huong-gia-theo-nhom.csv")
daily = TrendAnalyzer.daily_review_trends(
    reviews, rolling_window=window, positive_threshold=threshold, negative_threshold=2.0
)
if daily.empty:
    ui.empty_state(
        "Chưa có mốc thời gian hợp lệ",
        "Các bản ghi thiếu thời gian vẫn được giữ trong dữ liệu, không được đưa vào xu hướng theo ngày.",
    )
    ui.footer()
    st.stop()
insufficient = daily["trend_status"].eq("insufficient_data")
if insufficient.any():
    st.warning("Chưa đủ độ bao phủ theo ngày hoặc số mẫu; đường trung bình được ẩn tại các ngày chưa đủ dữ liệu.")
plot = daily.copy()
plot.loc[insufficient, "rolling_avg_rating"] = float("nan")
with st.container(border=True):
    st.subheader("Phản hồi tích cực & tiêu cực")
    ui.chart(
        px.line(
            daily,
            x="date",
            y=["positive_count", "negative_count"],
            color_discrete_map={"positive_count": "#20b5a2", "negative_count": "#ea8b83"},
            labels={"date": "Ngày", "value": "Số đánh giá", "variable": "Nhóm điểm"},
        ),
        340,
    )
with st.container(border=True):
    st.subheader("Diễn biến điểm trung bình")
    st.caption("Trung bình trượt được tính theo số mẫu đánh giá, không trung bình đều giữa các ngày.")
    ui.chart(
        px.line(
            plot,
            x="date",
            y=["avg_rating", "rolling_avg_rating"],
            color_discrete_map={"avg_rating": "#b7becf", "rolling_avg_rating": "#7560e9"},
            labels={"date": "Ngày", "value": "Điểm đánh giá", "variable": "Chuỗi"},
        ),
        320,
    )
with st.expander("Dữ liệu và độ bao phủ cửa sổ"):
    ui.download(daily, "xu-huong.csv")
    ui.table(
        daily[
            [
                "date",
                "rating_sample_size",
                "rolling_rating_sample_size",
                "observed_days",
                "missing_days",
                "baseline_start",
                "baseline_end",
                "trend_status",
            ]
        ]
    )
render_analytics_quality(reviews)
ui.footer()
