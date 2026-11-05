"""Shared visual language and accessible, native Streamlit controls."""

from contextlib import contextmanager
from html import escape
from pathlib import Path
import pandas as pd
import streamlit as st

COLORS = ["#7560e9", "#20b5a2", "#f3b85b", "#679af5", "#c683d8", "#ea8b83"]
SCOPES = {
    "real": "Dữ liệu thật",
    "simulation": "Dữ liệu giả lập",
    "unverified": "Chưa kiểm chứng",
    "all": "Tất cả · Chẩn đoán",
}
TABLE_LABELS = {
    "product_id": "Mã sản phẩm",
    "product_name": "Sản phẩm",
    "brand": "Thương hiệu",
    "category": "Danh mục",
    "price": "Giá",
    "currency": "Đơn vị",
    "avg_rating": "Điểm trung bình",
    "review_count": "Đánh giá đã thu",
    "catalog_review_count": "Đánh giá trên sàn",
    "rating_sample_size": "Mẫu điểm hợp lệ",
    "rating_availability_status": "Trạng thái điểm",
    "source_name": "Nguồn",
    "source_group": "Nhóm nguồn",
    "source_url": "Liên kết nguồn",
    "acquisition_method": "Cách thu thập",
    "collected_at": "Thu thập lúc",
    "review_id": "Mã đánh giá",
    "rating": "Điểm",
    "review_title": "Tiêu đề",
    "review_text": "Nội dung",
    "review_date": "Ngày đánh giá",
    "verified_purchase": "Mua hàng xác minh",
    "article_id": "Mã bài viết",
    "title": "Tiêu đề",
    "body": "Nội dung",
    "published_at": "Xuất bản lúc",
    "event_id": "Mã sự kiện",
    "entity_id": "Đối tượng",
    "anomaly_type": "Loại bất thường",
    "score": "Điểm bất thường",
    "timestamp": "Thời điểm",
    "observed_days": "Ngày có quan sát",
    "missing_days": "Ngày thiếu dữ liệu",
    "baseline_start": "Bắt đầu cửa sổ",
    "baseline_end": "Kết thúc cửa sổ",
    "trend_status": "Trạng thái xu hướng",
    "rolling_rating_sample_size": "Mẫu điểm trong cửa sổ",
    "description": "Mô tả",
    "date": "Ngày",
    "dataset_name": "Tập dữ liệu",
    "records_received": "Đầu vào",
    "records_valid": "Hợp lệ",
    "records_invalid": "Không hợp lệ",
    "records_quarantined": "Cách ly",
    "records_duplicate": "Trùng lặp",
    "dq_score": "Chất lượng (%)",
    "run_id": "Lượt xử lý",
}


def metric_html(title, value, note=None):
    return (
        '<div class="kpi-card"><div class="kpi-label">'
        + escape(str(title))
        + '</div><div class="kpi-value">'
        + escape(str(value))
        + "</div>"
        + ('<div class="kpi-note">' + escape(str(note)) + "</div>" if note is not None else "")
        + "</div>"
    )


def metric(title, value, note=None):
    st.markdown(metric_html(title, value, note), unsafe_allow_html=True)


def number(value, digits=0):
    if value is None or pd.isna(value):
        return "Chưa có dữ liệu"
    return f"{value:,.{digits}f}"


def setup_page(title, description, key):
    st.set_page_config(page_title=title + " · Sentinel", page_icon=":material/monitoring:", layout="wide")
    css = (Path(__file__).parent / "assets/theme.css").read_text()
    st.markdown("<style>" + css + "</style>", unsafe_allow_html=True)
    from dashboard.utils import render_sidebar_source

    render_sidebar_source()
    scope = st.session_state.get("dashboard_data_scope", "real")
    st.markdown('<div class="eyebrow">SENTINEL / ' + escape(title.upper()) + "</div>", unsafe_allow_html=True)
    st.title(title)
    st.markdown('<div class="page-description">' + escape(description) + "</div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="page-context"><span class="badge '
        + scope
        + '">'
        + SCOPES[scope]
        + "</span><span>Nguồn và thời gian quan sát được giữ lại trong dữ liệu</span></div>",
        unsafe_allow_html=True,
    )


def sidebar():
    import os
    from dashboard.navigation import page_groups
    from dashboard.utils import open_dashboard_db

    st.sidebar.markdown(
        '<div class="brand"><div class="brand-symbol">S</div><div><div class="brand-name">Sentinel<span style="color:#a596ff">AI</span></div><div class="brand-tagline">Data Intelligence</div></div></div>',
        unsafe_allow_html=True,
    )
    for group, pages in page_groups():
        st.sidebar.markdown('<div class="nav-section">' + group + "</div>", unsafe_allow_html=True)
        for _, page in pages:
            st.sidebar.page_link(page, label=page.title, icon=page.icon, width="stretch")
    st.sidebar.divider()
    default = os.environ.get("DASHBOARD_DATA_SCOPE", "real")
    if default not in SCOPES:
        raise ValueError("Unknown DASHBOARD_DATA_SCOPE")
    st.sidebar.selectbox(
        "Phạm vi dữ liệu",
        list(SCOPES),
        index=list(SCOPES).index(default),
        format_func=SCOPES.get,
        key="dashboard_data_scope",
        help="Dữ liệu thật gồm bản thu trực tiếp và bản phát lại có nhãn. Dữ liệu sinh được tách riêng.",
    )
    st.sidebar.caption("Bản phát lại vẫn giữ nhãn và thời gian của nguồn.")
    st.sidebar.button("Làm mới dữ liệu", icon=":material/refresh:", width="stretch")
    try:
        db = open_dashboard_db()
        try:
            label = db.get_active_source_label()
            integration = db.is_integration
        finally:
            db.close()
        st.sidebar.success("PostgreSQL Warehouse · Đã kết nối" if integration else "SQLite Warehouse · Local/Test")
        with st.sidebar.expander("Thông tin kết nối"):
            st.caption(label)
    except ConnectionError:
        st.sidebar.error("Chưa kết nối được kho dữ liệu")


@contextmanager
def warehouse():
    from dashboard.utils import open_dashboard_db

    try:
        db = open_dashboard_db()
    except ConnectionError as error:
        empty_state("Không thể kết nối dữ liệu", "Kiểm tra dịch vụ kho dữ liệu, sau đó bấm Làm mới dữ liệu.", "!")
        with st.expander("Chi tiết kết nối"):
            st.code(str(error))
        st.stop()
    try:
        with st.spinner("Đang đọc dữ liệu…"):
            yield db
    finally:
        db.close()


def empty_state(title, description, icon="↗"):
    st.markdown(
        '<div class="empty-state"><div class="empty-icon">'
        + escape(icon)
        + '</div><div class="empty-title">'
        + escape(title)
        + '</div><div class="empty-copy">'
        + escape(description)
        + "</div></div>",
        unsafe_allow_html=True,
    )


def chart(figure, height=320):
    figure.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=12, r=12, t=30, b=20),
        font=dict(family="Inter, system-ui, sans-serif", color="#7c879e", size=12),
        colorway=COLORS,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=-0.18, x=0, title_text=""),
        title=dict(font=dict(size=14, color="#26324c")),
    )
    names = {
        "review_count": "Đánh giá đã thu",
        "rolling_avg_count": "Trung bình 7 ngày",
        "avg_rating": "Điểm theo ngày",
        "rolling_avg_rating": "Trung bình trượt",
        "positive_count": "Tích cực",
        "negative_count": "Tiêu cực",
    }
    for trace in figure.data:
        original = trace.name
        if original in names:
            trace.name = names[original]
            if trace.hovertemplate:
                trace.hovertemplate = trace.hovertemplate.replace(original, names[original])
    figure.update_xaxes(showgrid=False, zeroline=False, title_font_size=11)
    figure.update_yaxes(gridcolor="#eef0f6", zeroline=False, title_font_size=11)
    st.plotly_chart(figure, width="stretch", config={"displayModeBar": False, "responsive": True})


def table(frame, columns=None, height=None):
    if columns is not None:
        frame = frame[[c for c in columns if c in frame]]
    config = {key: st.column_config.Column(label) for key, label in TABLE_LABELS.items() if key in frame}
    if "source_url" in frame:
        config["source_url"] = st.column_config.LinkColumn("Liên kết nguồn", display_text="Mở nguồn")
    if "avg_rating" in frame:
        config["avg_rating"] = st.column_config.NumberColumn("Điểm trung bình", format="%.2f ★")
    kwargs = {"height": height} if height else {}
    st.dataframe(frame, width="stretch", hide_index=True, column_config=config, **kwargs)


def download(frame, filename="du-lieu.csv"):
    from dashboard.experience import csv_download_bytes

    st.download_button("Xuất CSV", csv_download_bytes(frame), filename, mime="text/csv", icon=":material/download:")


def footer():
    st.markdown(
        '<div class="app-footer"><span>SentinelAI · Không gian phân tích dữ liệu</span><span>Thời gian chuẩn UTC · Trường chưa biết được giữ nguyên</span></div>',
        unsafe_allow_html=True,
    )
