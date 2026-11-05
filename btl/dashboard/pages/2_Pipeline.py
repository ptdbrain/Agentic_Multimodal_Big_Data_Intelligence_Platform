import pandas as pd
import plotly.express as px
import streamlit as st
from storage.storage_manager import storage
from config.settings import settings
from dashboard import ui
from dashboard.utils import get_datalake_metrics, filter_source_scope

ui.setup_page(
    "Luồng dữ liệu", "Theo dõi dữ liệu đang lưu trong các tầng xử lý và đọc báo cáo chất lượng đã ghi nhận.", "pipeline"
)
with ui.warehouse() as db:
    quality = db.get_data_quality_metrics()
    scope = db.data_scope
try:
    stats = get_datalake_metrics()
except (ConnectionError, RuntimeError, OSError):
    st.error("Chưa đọc được thống kê lưu trữ. Kiểm tra kết nối và làm mới dữ liệu.")
    ui.footer()
    st.stop()


def size(value):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024:
            return f"{value:,.1f} {unit}"
        value /= 1024
    return f"{value:,.1f} PB"


total = sum(stats[layer]["size_bytes"] for layer in ["bronze", "silver", "gold"])
for col, layer, label in zip(
    st.columns(4),
    ["all", "bronze", "silver", "gold"],
    ["Tổng dung lượng", "Bronze · Dữ liệu gốc", "Silver · Đã làm sạch", "Gold · Đã tổng hợp"],
):
    with col:
        amount = total if layer == "all" else stats[layer]["size_bytes"]
        count = (
            sum(stats[k]["files"] for k in ["bronze", "silver", "gold"]) if layer == "all" else stats[layer]["files"]
        )
        ui.metric(label, size(amount), f"{count:,} tệp đang lưu trong namespace kết nối")
st.write("")
a, b = st.columns([1.4, 1], gap="large")
with a:
    with st.container(border=True):
        st.subheader("Phân bổ lưu trữ")
        layers = pd.DataFrame(
            {
                "Tầng": ["Bronze", "Silver", "Gold"],
                "Dung lượng (byte)": [stats[x]["size_bytes"] for x in ["bronze", "silver", "gold"]],
                "Số tệp": [stats[x]["files"] for x in ["bronze", "silver", "gold"]],
            }
        )
        if total:
            ui.chart(px.bar(layers, x="Tầng", y="Dung lượng (byte)", color="Tầng", color_discrete_sequence=ui.COLORS))
        else:
            ui.empty_state("Chưa có tệp trong Data Lake", "Các tầng sẽ có dữ liệu khi nguồn được thu thập và xử lý.")
with b:
    with st.container(border=True):
        st.subheader("Hành trình dữ liệu")
        for title, description in [
            ("01 · Thu thập", "Sàn thương mại, dữ liệu sinh và nội dung web"),
            ("02 · Bronze", "Giữ bản gốc cùng thông tin nguồn"),
            ("03 · Silver", "Chuẩn hóa, kiểm tra và khử trùng"),
            ("04 · Gold", "Tổng hợp chỉ số và tín hiệu bất thường"),
            ("05 · Phục vụ", "Kho dữ liệu và dashboard phân tích"),
        ]:
            st.markdown("**" + title + "**")
            st.caption(description)
with st.container(border=True):
    st.subheader("Chất lượng đầu vào đã ghi nhận")
    if quality.empty:
        ui.empty_state("Chưa có báo cáo xử lý", "Báo cáo chất lượng sẽ xuất hiện sau khi pipeline hoàn tất.")
    else:
        ui.table(
            quality,
            [
                "dataset_name",
                "records_received",
                "records_valid",
                "records_invalid",
                "records_duplicate",
                "dq_score",
                "timestamp",
            ],
        )
with st.expander("Mẫu đánh giá Bronze và thông tin vận hành"):
    st.caption(
        "Quét tối đa 1.000 bản ghi, hiển thị tối đa 10 bản khớp phạm vi nguồn. Đây không phải luồng theo dõi trực tiếp."
    )
    rows = storage.read_bronze_records(settings.kafka.topic_reviews, limit=1000)
    sample = filter_source_scope(pd.DataFrame(rows), scope).head(10) if rows else pd.DataFrame()
    if sample.empty:
        st.info("Chưa có bản ghi phù hợp trong phần dữ liệu được quét.")
    else:
        ui.table(sample, ["review_id", "product_id", "rating", "review_date", "source"])
ui.footer()
