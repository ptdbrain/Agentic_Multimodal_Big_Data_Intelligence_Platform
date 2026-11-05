import streamlit as st
import plotly.express as px
from analytics.trend.trends import TrendAnalyzer
from dashboard import ui
from dashboard.utils import load_reviews_data, render_data_sources, render_analytics_quality

ui.setup_page(
    "Diễn biến & xu hướng", "Theo dõi điểm đánh giá theo thời gian với cửa sổ quan sát và độ bao phủ rõ ràng.", "trends"
)
with ui.warehouse() as db:
    reviews = load_reviews_data(db=db)
render_data_sources(reviews)
if reviews.empty:
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
