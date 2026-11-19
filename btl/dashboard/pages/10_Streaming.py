"""Committed stream serving, distinct from upstream collection freshness."""

import pandas as pd
import streamlit as st
import plotly.express as px
from dashboard import ui

ui.setup_page("Luồng đang xử lý", "Kết quả cửa sổ đã lưu và tiến độ các sink của nguồn đang chọn.", "streaming")
st.caption(
    "Trang cập nhật mỗi 10 giây. Thời gian xử lý luồng và lịch thu thập trên sàn là hai phép đo riêng; nguồn thu hằng ngày không trở thành quan sát thị trường liên tục."
)


@st.fragment(run_every="10s")
def render():
    with ui.warehouse() as db:
        deployments = db.read_table("stream_deployments")
        windows = db.read_table("stream_windows")
    if deployments.empty:
        ui.empty_state(
            "Chưa có generation streaming được đăng ký",
            "Cấu hình dịch vụ streaming cho dataset này. Không dùng dữ liệu batch thay cho tiến độ luồng.",
        )
        return
    current = deployments.loc[deployments["state"].ne("retired")]
    if len(current) != 1:
        st.error("Không xác định được duy nhất generation hiện hành; cần kiểm tra migration.")
        return
    deployment = current.iloc[0]
    windows = windows.loc[windows["generation"].eq(deployment["generation"])] if not windows.empty else windows
    if not windows.empty:
        frontier = deployment.get("last_batch")
        windows = windows.iloc[0:0] if pd.isna(frontier) else windows.loc[windows["batch_id"].le(frontier)]
    a, b, c = st.columns(3)
    with a:
        ui.metric("Generation", str(deployment["generation"]), str(deployment["state"]))
    with b:
        ui.metric(
            "Tiến độ đã xác nhận", deployment.get("heartbeat_at") or "Chưa có", "Chờ các sink đạt cùng frontier nguồn"
        )
    with c:
        ui.metric("Watermark", deployment.get("watermark") or "Chưa có", "Theo tiến độ Spark đã commit")
    if windows.empty:
        ui.empty_state("Chưa có cửa sổ hợp lệ đã phục vụ", "Luồng chưa nhận đủ dữ liệu hoặc chưa commit kết quả.")
        return
    watermark = pd.to_datetime(deployment.get("watermark"), utc=True, errors="coerce")
    ends = pd.to_datetime(windows["window_end"], utc=True, errors="coerce")
    windows = windows.copy()
    windows["window_status"] = "Chưa xác định"
    if not pd.isna(watermark):
        windows["window_status"] = ends.le(watermark).map({True: "Đã kết thúc theo watermark", False: "Đang mở"})
    ui.chart(
        px.line(
            windows.sort_values("window_start"),
            x="window_start",
            y="review_count",
            color="product_id",
            labels={"window_start": "Bắt đầu cửa sổ", "review_count": "Số đánh giá", "product_id": "Sản phẩm"},
        )
    )
    ui.table(
        windows,
        [
            "product_id",
            "window_start",
            "window_end",
            "review_count",
            "avg_rating",
            "negative_count",
            "window_status",
            "batch_id",
            "updated_at",
        ],
    )
    ui.download(windows, "cua-so-streaming.csv")


render()
ui.footer()
