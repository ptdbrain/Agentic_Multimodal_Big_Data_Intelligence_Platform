"""Dataset-bound full-text search against a verified immutable Elasticsearch generation."""

import hashlib, json
import pandas as pd
import streamlit as st
from dashboard import ui
from serving.search import search_snapshot

ui.setup_page("Tìm kiếm toàn văn", "Tìm sản phẩm, đánh giá và bài viết trong chỉ mục đã được đối soát.", "search")
entity = st.selectbox(
    "Loại nội dung",
    ["products", "reviews", "articles"],
    format_func=lambda x: {"products": "Sản phẩm", "reviews": "Đánh giá", "articles": "Bài viết"}[x],
    key="search_entity",
)
with ui.warehouse(kind="web" if entity == "articles" else "commerce") as sources:
    if not hasattr(sources, "context"):
        ui.empty_state("Chưa cấu hình registry phục vụ", "Chọn nguồn qua registry để giữ tìm kiếm đúng dataset.")
        st.stop()
    binding = sources.context.resolve(entity)
    db = sources._db(entity)
    if not db._columns("search_jobs"):
        ui.empty_state(
            "Chưa có chỉ mục đã xuất bản", "Pipeline cần hoàn tất lần xuất bản và tác vụ lập chỉ mục cho nguồn này."
        )
        st.stop()
    jobs = db.read_table("search_jobs")
    published = db.read_table("serving_generation")
    candidates = jobs.loc[jobs["entity_kind"].eq(entity) & jobs["state"].eq("complete")].sort_values(
        "completed_at", ascending=False
    )
if candidates.empty:
    ui.empty_state(
        "Chỉ mục chưa sẵn sàng", "Kiểm tra hàng đợi lập chỉ mục; trang không dùng dữ liệu trong bộ nhớ thay thế."
    )
    ui.table(jobs, ["entity_kind", "generation", "state", "attempts", "error_kind"])
    st.stop()
job = candidates.iloc[0]
if not published.empty and str(job["generation"]) != str(published.iloc[0]["generation"]):
    st.warning(
        "Chỉ mục đang chậm hơn phiên dữ liệu SQL mới nhất. Kết quả bên dưới giữ phiên và thời điểm đã lập chỉ mục."
    )
st.caption(f"Nguồn: {binding.label} · Generation: {job['generation']} · Hoàn tất chỉ mục: {job['completed_at']}")
query = st.text_input("Từ khóa", key="search_text", placeholder="Nhập từ khóa trong tên hoặc nội dung…", max_chars=500)
a, b = st.columns(2)
brand = a.text_input("Thương hiệu chính xác (tùy chọn)", key="search_brand") if entity == "products" else ""
minimum = (
    b.number_input("Điểm tối thiểu", min_value=1.0, max_value=5.0, value=1.0, step=0.5, key="search_minimum")
    if entity == "reviews"
    else None
)
filters = {"brand": brand} if brand.strip() else {}
fingerprint = hashlib.sha256(
    json.dumps([binding.id, entity, job["index_name"], query, filters, minimum], sort_keys=True).encode()
).hexdigest()
if st.session_state.get("search_fingerprint") != fingerprint:
    st.session_state["search_fingerprint"] = fingerprint
    st.session_state["search_cursors"] = [None]
    st.session_state["search_page"] = 0
page = st.session_state["search_page"]
cursors = st.session_state["search_cursors"]
try:
    result = search_snapshot(
        binding,
        entity,
        str(job["index_name"]),
        str(job["generation"]),
        query,
        filters=filters,
        min_rating=minimum,
        after=cursors[page],
    )
except Exception as error:
    st.error("Không truy vấn được chỉ mục Elasticsearch của nguồn đã chọn. Vui lòng kiểm tra dịch vụ và thử lại.")
    st.stop()
st.caption(f"{result['total']:,} kết quả · Trang {page + 1} · Tối đa 20 bản ghi/trang")
frame = pd.DataFrame(result["records"])
if frame.empty:
    ui.empty_state("Không có kết quả phù hợp", "Đổi từ khóa hoặc bộ lọc.")
else:
    frame = frame[[name for name in frame if not name.startswith("__")]]
    ui.table(frame)
    ui.download(frame, "ket-qua-tim-kiem-trang.csv")
previous, next_column = st.columns(2)
if previous.button("Trang trước", disabled=page == 0, key="search_previous"):
    st.session_state["search_page"] = page - 1
    st.rerun()
if next_column.button(
    "Trang sau", disabled=not result["next_after"] or (page + 1) * 20 >= result["total"], key="search_next"
):
    if len(cursors) == page + 1:
        cursors.append(result["next_after"])
    else:
        cursors[page + 1] = result["next_after"]
    st.session_state["search_page"] = page + 1
    st.rerun()
ui.footer()
