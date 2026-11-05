from html import escape
import pandas as pd
import plotly.express as px
import streamlit as st
from dashboard import ui


def render_home():
    ui.setup_page(
        "Không gian dữ liệu của bạn",
        "Theo dõi thị trường, khám phá sản phẩm và nắm bắt những tín hiệu quan trọng từ dữ liệu đã thu thập.",
        "home",
    )
    with ui.warehouse() as db:
        kpis = db.get_kpi_summary()
        products = db.read_table("products")
        reviews = db.read_table("reviews")
        articles = db.read_table("articles", limit=1000)
        daily = db.get_product_daily_stats()
        incidents = db.get_anomaly_events()
    for col, (title, value, note) in zip(
        st.columns(4),
        [
            ("Sản phẩm", ui.number(kpis["total_products"]), "Trong danh mục đang chọn"),
            ("Đánh giá", ui.number(kpis["total_reviews"]), "Đã được thu thập và xử lý"),
            ("Điểm trung bình", ui.number(kpis["avg_rating"], 2), f"{kpis['rating_sample_size']:,} mẫu điểm hợp lệ"),
            (
                "Chất lượng dữ liệu",
                f"{kpis['avg_dq_score']:.1f}%" if kpis["avg_dq_score"] is not None else "Chưa có dữ liệu",
                "Theo báo cáo xử lý đã ghi nhận",
            ),
        ],
    ):
        with col:
            ui.metric(title, value, note)
    st.write("")
    left, right = st.columns([2.1, 1], gap="large")
    with left:
        with st.container(border=True):
            st.subheader("Nhịp độ đánh giá")
            st.caption("Số đánh giá theo ngày quan sát · Ngày thiếu dữ liệu không được điền thành 0")
            if not daily.empty and {"date", "review_count"}.issubset(daily):
                data = daily.groupby("date", as_index=False)["review_count"].sum()
                ui.chart(
                    px.area(
                        data,
                        x="date",
                        y="review_count",
                        color_discrete_sequence=["#7560e9"],
                        labels={"date": "Ngày", "review_count": "Đánh giá"},
                    ),
                    330,
                )
            else:
                ui.empty_state(
                    "Chưa có diễn biến theo ngày", "Dữ liệu có thời gian quan sát hợp lệ sẽ xuất hiện tại đây."
                )
    with right:
        with st.container(border=True):
            st.subheader("Dữ liệu trong phạm vi")
            st.caption("Số bản ghi đang được phục vụ")
            provider = products["source_name"].dropna().nunique() if "source_name" in products else None
            for title, value, description in [
                (
                    "Danh mục sản phẩm",
                    len(products),
                    "Sản phẩm từ " + (str(provider) + " nguồn đã ghi nhận" if provider else "nguồn đang chọn"),
                ),
                ("Đánh giá sản phẩm", len(reviews), "Văn bản và điểm số quan sát được"),
                ("Nội dung web", len(articles), "Bài viết độc lập · Tối đa 1.000 bài gần nhất"),
            ]:
                st.markdown(
                    '<div class="source-card"><strong>'
                    + escape(title)
                    + "</strong><p>"
                    + escape(description)
                    + '</p><div class="source-number">'
                    + f"{value:,}"
                    + "</div></div>",
                    unsafe_allow_html=True,
                )
    st.write("")
    with st.container(border=True):
        title, action = st.columns([4, 1])
        with title:
            st.subheader("Khám phá danh mục")
            st.caption("Một góc nhìn nhanh vào dữ liệu sản phẩm đang được phục vụ")
        with action:
            from dashboard.navigation import page_groups

            page = next(p for _, group in page_groups() for key, p in group if key == "products")
            st.page_link(page, label="Xem sản phẩm", icon=":material/arrow_forward:")
        if products.empty:
            ui.empty_state(
                "Danh mục đang trống", "Chọn phạm vi dữ liệu khác hoặc hoàn tất một lượt thu thập để bắt đầu."
            )
        else:
            ui.table(products.head(6), ["product_name", "brand", "category", "price", "currency", "source_name"])
    with st.expander("Ghi chú khi sử dụng dữ liệu"):
        st.write(
            "Phạm vi Dữ liệu thật không bao gồm dữ liệu sinh hoặc chưa kiểm chứng. Bản phát lại được giữ nhãn riêng."
        )
        st.write(
            "Không có bản ghi bất thường chưa đủ để kết luận thị trường bình thường; việc phát hiện cần lịch sử và chất lượng đầu vào phù hợp."
        )
        st.caption(f"{len(incidents)} sự kiện bất thường trong tối đa 100 sự kiện gần nhất của phạm vi đang chọn.")
    ui.footer()
