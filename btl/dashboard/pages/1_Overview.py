import streamlit as st
import plotly.express as px
from analytics.trend.trends import TrendAnalyzer
from dashboard import ui
from dashboard.utils import render_analytics_quality

ui.setup_page("Tổng quan phân tích", "Nắm bắt mức độ bao phủ dữ liệu và diễn biến đánh giá theo thời gian.", "overview")
with ui.warehouse() as db:
    kpis = db.get_kpi_summary()
    products = db.get_product_daily_stats()
    categories = db.get_category_daily_stats()
for col, (title, value, note) in zip(
    st.columns(4),
    [
        ("Đánh giá đã thu", ui.number(kpis["total_reviews"]), "Từ dữ liệu đã xử lý"),
        ("Sản phẩm", ui.number(kpis["total_products"]), "Trong phạm vi đang chọn"),
        ("Thương hiệu", ui.number(kpis["total_brands"]), "Đã có thống kê"),
        ("Điểm trung bình", ui.number(kpis["avg_rating"], 2), f"{kpis['rating_sample_size']:,} mẫu điểm hợp lệ"),
    ],
):
    with col:
        ui.metric(title, value, note)
st.write("")
left, right = st.columns([1.6, 1], gap="large")
with left:
    with st.container(border=True):
        st.subheader("Đánh giá theo ngày")
        if not products.empty and {"date", "review_count"}.issubset(products):
            daily = TrendAnalyzer.daily_volume_trends(products)
            insufficient = daily["trend_status"].eq("insufficient_data")
            if insufficient.any():
                st.warning("Chưa đủ độ bao phủ theo ngày; đường trung bình được ẩn tại các ngày thiếu dữ liệu.")
            plotted = daily.copy()
            plotted.loc[insufficient, "rolling_avg_count"] = float("nan")
            ui.chart(
                px.line(
                    plotted,
                    x="date",
                    y=["review_count", "rolling_avg_count"],
                    labels={"date": "Ngày", "value": "Đánh giá", "variable": "Chuỗi"},
                    color_discrete_map={"review_count": "#7560e9", "rolling_avg_count": "#20b5a2"},
                )
            )
            with st.expander("Độ bao phủ cửa sổ 7 ngày"):
                st.caption("Trung bình tính trên ngày có quan sát. Ngày thiếu được giữ là chưa biết.")
                ui.table(
                    daily[["date", "observed_days", "missing_days", "baseline_start", "baseline_end", "trend_status"]]
                )
        else:
            ui.empty_state("Chưa có diễn biến theo ngày", "Cần đánh giá có thời gian quan sát hợp lệ để hiển thị.")
with right:
    with st.container(border=True):
        st.subheader("Danh mục sản phẩm")
        if not categories.empty and {"category", "product_count"}.issubset(categories):
            # Latest date avoids counting the same catalog once per historical day.
            snapshot = categories
            if "date" in snapshot:
                snapshot = snapshot.loc[snapshot["date"].eq(snapshot["date"].max())]
            counts = snapshot.groupby("category", as_index=False)["product_count"].sum()
            ui.chart(
                px.pie(counts, values="product_count", names="category", hole=0.7, color_discrete_sequence=ui.COLORS)
            )
            st.caption("Cơ cấu danh mục tại ngày thống kê gần nhất.")
        else:
            ui.empty_state("Chưa có cơ cấu danh mục", "Thống kê danh mục sẽ xuất hiện sau khi dữ liệu được xử lý.")
render_analytics_quality(products)
ui.footer()
