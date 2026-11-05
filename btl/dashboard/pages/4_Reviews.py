import pandas as pd
import plotly.express as px
import streamlit as st
from dashboard import ui
from dashboard.experience import search_frame, clear_keys, purchase_status
from dashboard.utils import load_reviews_data, render_data_sources

ui.setup_page(
    "Lắng nghe đánh giá", "Đọc phản hồi thực tế, khám phá phân bố điểm và tìm những đánh giá bạn quan tâm.", "reviews"
)
with ui.warehouse() as db:
    reviews = load_reviews_data(db=db)
render_data_sources(reviews)
if reviews.empty:
    ui.empty_state(
        "Chưa có đánh giá trong phạm vi này", "Dữ liệu đánh giá sẽ xuất hiện khi nguồn thu thập và xử lý thành công."
    )
    ui.footer()
    st.stop()
with st.container(border=True):
    a, b, c = st.columns([2, 1.3, 0.65])
    query = a.text_input("Tìm đánh giá", placeholder="Nội dung, tiêu đề hoặc mã sản phẩm…", key="reviews_query")
    stars = b.multiselect("Điểm đánh giá", [1, 2, 3, 4, 5], key="reviews_stars")
    with c:
        st.caption("Bộ lọc")
        st.button("Xóa bộ lọc", on_click=clear_keys, args=("reviews_query", "reviews_stars"), width="stretch")
shown = search_frame(reviews, query, ["review_id", "product_id", "review_title", "review_text"])
if stars:
    shown = shown.loc[pd.to_numeric(shown["rating"], errors="coerce").isin(stars)]
ratings = pd.to_numeric(shown.get("rating", pd.Series(dtype=float)), errors="coerce")
valid = ratings[ratings.between(1, 5)]
for col, (title, value, note) in zip(
    st.columns(3),
    [
        ("Đánh giá phù hợp", ui.number(len(shown)), f"Trong {len(reviews):,} đánh giá đang phục vụ"),
        (
            "Điểm trung bình",
            ui.number(valid.mean(), 2) if len(valid) else "Chưa có dữ liệu",
            f"{len(valid):,} điểm hợp lệ",
        ),
        ("Đánh giá 4–5 sao", ui.number(int(valid.ge(4).sum())), "Phân nhóm theo điểm số, không phải dự đoán cảm xúc"),
    ],
):
    with col:
        ui.metric(title, value, note)
st.write("")
left, right = st.columns([1.3, 1], gap="large")
with left:
    with st.container(border=True):
        st.subheader("Phân bố điểm đánh giá")
        if not valid.empty:
            counts = valid.value_counts().sort_index().rename_axis("rating").reset_index(name="count")
            ui.chart(
                px.bar(
                    counts,
                    x="rating",
                    y="count",
                    text="count",
                    color_discrete_sequence=["#7560e9"],
                    labels={"rating": "Điểm đánh giá", "count": "Số đánh giá"},
                )
            )
        else:
            ui.empty_state("Chưa có điểm hợp lệ", "Điểm thiếu hoặc ngoài khoảng 1–5 được giữ ngoài thống kê.")
with right:
    with st.container(border=True):
        st.subheader("Xác minh mua hàng")
        if "verified_purchase" in shown:
            counts = (
                shown["verified_purchase"]
                .map(purchase_status)
                .value_counts()
                .rename_axis("status")
                .reset_index(name="count")
            )
            ui.chart(px.pie(counts, names="status", values="count", hole=0.72, color_discrete_sequence=ui.COLORS))
        else:
            ui.empty_state(
                "Nguồn chưa cung cấp xác minh", "Không suy đoán trạng thái mua hàng từ điểm hoặc nội dung đánh giá."
            )
with st.container(border=True):
    st.subheader("Danh sách đánh giá")
    if shown.empty:
        ui.empty_state("Không tìm thấy đánh giá", "Thử xóa bộ lọc hoặc tìm một từ khóa khác.", "⌕")
    else:
        ui.download(shown, "danh-gia.csv")
        ui.table(
            shown,
            ["review_id", "product_id", "rating", "review_title", "review_text", "review_date", "source_name"],
            height=380,
        )
        with st.expander("Đọc nội dung đầy đủ"):
            identifiers = shown["review_id"].astype(str).tolist()
            selected = st.selectbox("Chọn đánh giá", identifiers, key="review_detail")
            record = shown.loc[shown["review_id"].astype(str).eq(selected)].iloc[0]
            st.write(str(record.get("review_title") or "Đánh giá không có tiêu đề"))
            st.text(str(record.get("review_text") or "Chưa có nội dung"))
ui.footer()
