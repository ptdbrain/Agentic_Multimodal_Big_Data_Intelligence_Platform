"""Single route registry for the application and sidebar."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent


def page_groups():
    import streamlit as st
    from dashboard.views.home import render_home

    return [
        (
            "KHÔNG GIAN LÀM VIỆC",
            [
                (
                    "home",
                    st.Page(
                        render_home, title="Trang chủ", icon=":material/space_dashboard:", url_path="home", default=True
                    ),
                ),
                (
                    "overview",
                    st.Page(
                        str(ROOT / "pages/1_Overview.py"),
                        title="Tổng quan",
                        icon=":material/monitoring:",
                        url_path="overview",
                    ),
                ),
            ],
        ),
        (
            "PHÂN TÍCH DỮ LIỆU",
            [
                (
                    "search",
                    st.Page(
                        str(ROOT / "pages/11_Search.py"),
                        title="Tìm kiếm toàn văn",
                        icon=":material/search:",
                        url_path="search",
                    ),
                ),
                (
                    "basic",
                    st.Page(
                        str(ROOT / "pages/9_Basic_Analytics.py"),
                        title="Phân tích cơ bản",
                        icon=":material/bar_chart:",
                        url_path="basic-analytics",
                    ),
                ),
                (
                    "products",
                    st.Page(
                        str(ROOT / "pages/3_Products.py"),
                        title="Sản phẩm",
                        icon=":material/shopping_bag:",
                        url_path="products",
                    ),
                ),
                (
                    "reviews",
                    st.Page(
                        str(ROOT / "pages/4_Reviews.py"),
                        title="Đánh giá",
                        icon=":material/rate_review:",
                        url_path="reviews",
                    ),
                ),
                (
                    "trends",
                    st.Page(
                        str(ROOT / "pages/5_Trends.py"),
                        title="Xu hướng",
                        icon=":material/auto_graph:",
                        url_path="trends",
                    ),
                ),
                (
                    "articles",
                    st.Page(
                        str(ROOT / "pages/8_Web_Content.py"),
                        title="Nội dung web",
                        icon=":material/article:",
                        url_path="articles",
                    ),
                ),
            ],
        ),
        (
            "VẬN HÀNH",
            [
                (
                    "streaming",
                    st.Page(
                        str(ROOT / "pages/10_Streaming.py"),
                        title="Luồng đang xử lý",
                        icon=":material/stream:",
                        url_path="streaming",
                    ),
                ),
                (
                    "anomalies",
                    st.Page(
                        str(ROOT / "pages/6_Anomalies.py"),
                        title="Bất thường",
                        icon=":material/report:",
                        url_path="anomalies",
                    ),
                ),
                (
                    "lineage",
                    st.Page(str(ROOT / "pages/12_Lineage.py"), title="Truy vết dữ liệu",
                            icon=":material/fact_check:", url_path="lineage"),
                ),
                (
                    "pipeline",
                    st.Page(
                        str(ROOT / "pages/2_Pipeline.py"),
                        title="Luồng dữ liệu",
                        icon=":material/account_tree:",
                        url_path="pipeline",
                    ),
                ),
                (
                    "quality",
                    st.Page(
                        str(ROOT / "pages/7_Data_Quality.py"),
                        title="Chất lượng dữ liệu",
                        icon=":material/verified_user:",
                        url_path="quality",
                    ),
                ),
            ],
        ),
    ]
