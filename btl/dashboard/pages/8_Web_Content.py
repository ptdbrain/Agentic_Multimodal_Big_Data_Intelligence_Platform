import pandas as pd
import plotly.express as px
import streamlit as st
from dashboard import ui
from dashboard.experience import options, search_frame, clear_keys

ui.setup_page(
    "Khám phá nội dung web",
    "Đọc bài viết và thảo luận độc lập, với nguồn gốc và thời gian được giữ rõ ràng.",
    "articles",
)
with ui.warehouse() as db:
    articles = db.read_table("articles", order_by="collected_at", limit=1000)
    daily = db.read_table("article_daily_stats", order_by="date")
if articles.empty:
    ui.empty_state(
        "Chưa có bài viết trong phạm vi này",
        "Bài viết được phục vụ riêng với đánh giá sản phẩm. Chọn phạm vi khác hoặc hoàn tất thu thập nội dung web.",
    )
    ui.footer()
    st.stop()
with st.container(border=True):
    a, b, c = st.columns([2, 1, 0.65])
    query = a.text_input("Tìm bài viết", placeholder="Từ khóa trong tiêu đề hoặc nội dung…", key="articles_query")
    sources = b.multiselect("Nguồn bài viết", options(articles, "source_name"), key="articles_sources")
    with c:
        st.caption("Bộ lọc")
        st.button("Xóa bộ lọc", on_click=clear_keys, args=("articles_query", "articles_sources"), width="stretch")
shown = search_frame(articles, query, ["title", "body", "source_name"])
if sources:
    shown = shown.loc[shown["source_name"].isin(sources)]
known = int(pd.to_datetime(shown["published_at"], errors="coerce", utc=True).notna().sum())
for col, (title, value, note) in zip(
    st.columns(3),
    [
        ("Bài viết phù hợp", ui.number(len(shown)), "Trong tối đa 1.000 bài gần nhất"),
        ("Nguồn nội dung", ui.number(shown["source_name"].nunique()), "Các nguồn có trong kết quả"),
        ("Có thời gian xuất bản", ui.number(known), "Thời gian thu thập được giữ riêng"),
    ],
):
    with col:
        ui.metric(title, value, note)
st.write("")
with st.container(border=True):
    st.subheader("Thư viện bài viết")
    if shown.empty:
        ui.empty_state("Không tìm thấy bài viết", "Thay từ khóa hoặc xóa bộ lọc nguồn.", "⌕")
    else:
        ui.download(shown, "bai-viet.csv")
        ui.table(shown, ["title", "source_name", "published_at", "collected_at", "source_url"], height=350)
        selected = st.selectbox(
            "Đọc bài viết",
            shown["article_id"].tolist(),
            format_func=lambda key: str(shown.loc[shown["article_id"].eq(key), "title"].iloc[0]),
            key="article_detail",
        )
        record = shown.loc[shown["article_id"].eq(selected)].iloc[0]
        with st.expander("Nội dung và nguồn bài viết", expanded=True):
            st.subheader(str(record["title"]))
            st.text(str(record["body"]))
            st.caption(f"Nguồn: {record['source_name']} · Thu thập lúc: {record.get('collected_at')}")
            from urllib.parse import urlsplit

            link = str(record.get("source_url") or "")
            if urlsplit(link).scheme in ("http", "https"):
                st.link_button("Mở bài gốc", link, icon=":material/open_in_new:")
with st.expander("Xuất bản theo ngày và thông tin dữ liệu"):
    st.caption(
        "Thống kê toàn bộ lịch sử của nguồn đang chọn, không áp dụng từ khóa và giới hạn 1.000 bài của thư viện. Chỉ bài có ngày xuất bản quan sát được mới tham gia thống kê. Thời gian chuẩn UTC; ngày thiếu giờ/múi giờ không được biến thành thời điểm chính xác."
    )
    counts = daily.loc[daily["source_name"].isin(sources)] if sources and not daily.empty else daily
    if not counts.empty:
        ui.chart(
            px.bar(
                counts,
                x="date",
                y="article_count",
                color="source_name",
                labels={"date": "Ngày", "article_count": "Bài viết", "source_name": "Nguồn"},
                color_discrete_sequence=ui.COLORS,
            )
        )
    else:
        st.info("Chưa có thống kê ngày xuất bản hợp lệ.")
ui.footer()
