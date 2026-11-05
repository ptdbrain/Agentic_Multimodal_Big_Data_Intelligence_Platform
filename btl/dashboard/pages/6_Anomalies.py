import pandas as pd
import streamlit as st
from dashboard import ui
from dashboard.experience import options, search_frame, clear_keys

ui.setup_page(
    "Theo dõi bất thường",
    "Khám phá các tín hiệu đã được phát hiện, kèm ngữ cảnh và mẫu dữ liệu dùng để so sánh.",
    "anomalies",
)
with ui.warehouse() as db:
    events = db.get_anomaly_events()
records = events.to_dict("records") if not events.empty else []
kind = events.get("anomaly_type", pd.Series(dtype=str)).fillna("").astype(str)
for col, (label, value, note) in zip(
    st.columns(4),
    [
        ("Sự kiện đã ghi nhận", len(events), "Tối đa 100 sự kiện gần nhất"),
        ("Tăng đột biến", int(kind.str.contains("BURST|BOMBING", regex=True).sum()), "Theo quy tắc phát hiện"),
        ("Sụt giảm điểm", int(kind.str.contains("RATING_DROP", regex=False).sum()), "So sánh với lịch sử"),
        ("Bất thường giá", int(kind.str.contains("PRICE", regex=False).sum()), "Giữ đơn vị tiền tệ nguồn"),
    ],
):
    with col:
        ui.metric(label, ui.number(value), note)
st.write("")
if events.empty:
    ui.empty_state(
        "Chưa ghi nhận sự kiện bất thường",
        "Điều này chưa đủ để kết luận dữ liệu bình thường. Việc phát hiện cần lịch sử và đầu vào đạt chất lượng.",
        "✓",
    )
    ui.footer()
    st.stop()
with st.container(border=True):
    a, b, c = st.columns([2, 1, 0.65])
    query = a.text_input("Tìm sự kiện", placeholder="Mã đối tượng hoặc mô tả…", key="events_query")
    selected = b.multiselect("Loại bất thường", options(events, "anomaly_type"), key="events_types")
    with c:
        st.caption("Bộ lọc")
        st.button("Xóa bộ lọc", on_click=clear_keys, args=("events_query", "events_types"), width="stretch")
shown = search_frame(events, query, ["entity_id", "event_id", "description", "anomaly_type"])
if selected:
    shown = shown.loc[shown["anomaly_type"].isin(selected)]
with st.container(border=True):
    st.subheader("Nhật ký phát hiện")
    if shown.empty:
        ui.empty_state("Không có sự kiện phù hợp", "Thử thay từ khóa hoặc xóa bộ lọc.", "⌕")
    else:
        ui.download(shown, "bat-thuong.csv")
        ui.table(
            shown,
            [
                "event_id",
                "entity_id",
                "anomaly_type",
                "score",
                "timestamp",
                "description",
                "run_id",
                "data_quality_status",
                "baseline_sample_size",
                "baseline_sample_unit",
                "baseline_method",
                "baseline_start",
                "baseline_end",
                "current_sample_size",
            ],
        )
        with st.expander("Chi tiết đối tượng"):
            entity = st.selectbox("Chọn đối tượng", options(shown, "entity_id"))
            ui.table(shown.loc[shown["entity_id"].astype(str).eq(entity)])
ui.footer()
