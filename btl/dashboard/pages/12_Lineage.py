"""Read-only checksum verification of archived records and aggregate membership."""

import json
import pandas as pd
import streamlit as st
from dashboard import ui
from serving.lake import DatasetLake
from serving.lineage import trace_record, trace_aggregate, MART_GRAINS
from serving.store import ServingStore

ui.setup_page("Truy vết dữ liệu", "Đối soát bản ghi và số liệu tổng hợp bằng snapshot cùng checksum đã lưu.", "lineage")
mode = st.radio("Kiểu truy vết", ["Bản ghi", "Số liệu tổng hợp"], horizontal=True)
if mode == "Bản ghi":
    entity = st.selectbox("Loại bản ghi", ["products", "reviews", "prices", "articles"])
else:
    mart = st.selectbox("Bảng tổng hợp", list(MART_GRAINS))
    entity = "articles" if mart == "article_daily_stats" else "prices" if mart == "price_daily_stats" else "reviews"
with ui.warehouse(kind="web" if entity == "articles" else "commerce") as sources:
    db = sources._db(entity) if hasattr(sources, "_db") else sources
    binding = getattr(db, "binding", None)
    if binding is None:
        ui.empty_state("Chưa chọn dataset có registry", "Cấu hình registry để truy vết đúng bucket và schema.")
        st.stop()
    receipts = db.read_table("publication_lineage", order_by="created_at", limit=100)
    if receipts.empty:
        ui.empty_state(
            "Chưa có receipt truy vết", "Bản ghi cũ giữ trạng thái chưa xác định; cần lần xuất dữ liệu mới có receipt."
        )
        st.stop()
    generation = st.selectbox("Phiên xuất dữ liệu", receipts["generation"].tolist())
    if mode == "Bản ghi":
        record_id = st.text_input(
            "Mã bản ghi chính xác",
            max_chars=500,
            help="product_id, review_id, price_id hoặc article_id tương ứng với loại đã chọn.",
        )
        page = st.number_input("Trang đối tượng Bronze", min_value=1, value=1, step=1)
        st.caption(
            "Mỗi trang kiểm tra tối đa 100 đối tượng. Chỉ xác nhận bản ghi Silver khi có snapshot và liên kết sự kiện hoặc phiên bản nội dung phù hợp; dữ liệu cũ thiếu bằng chứng giữ trạng thái chưa xác định."
        )
        ready = bool(record_id.strip())
    else:
        a, b = st.columns(2)
        day = a.date_input("Ngày tổng hợp").isoformat()
        dimension = MART_GRAINS[mart][1]
        value = b.text_input(dimension, max_chars=500)
        page = st.number_input("Trang bản ghi đóng góp", min_value=1, value=1, step=1)
        ready = bool(value.strip())
        with st.expander("Gợi ý grain từ SQL hiện hành"):
            ui.table(db.read_table(mart, order_by="date", limit=30), list(MART_GRAINS[mart]))
    if st.button("Kiểm tra bằng chứng", disabled=not ready):
        try:
            store, lake = ServingStore(db._conn, binding.id, binding.source_group), DatasetLake(binding)
            if mode == "Bản ghi":
                result = trace_record(
                    store,
                    lake,
                    generation,
                    entity,
                    record_id.strip(),
                    max_objects=100,
                    object_offset=(int(page) - 1) * 100,
                )
            else:
                result = trace_aggregate(
                    store,
                    lake,
                    generation,
                    mart,
                    {"date": day, dimension: value.strip()},
                    offset=(int(page) - 1) * 100,
                    limit=100,
                )
        except Exception as error:
            st.error(
                f"Không thể xác minh bằng chứng ({type(error).__name__}). Không hiển thị dữ liệu chưa được xác minh."
            )
        else:
            st.write("Kết quả:", result["status"])
            if mode == "Bản ghi":
                if result.get("truncated"):
                    st.info(
                        "Còn đối tượng chưa quét. Tăng số trang để tiếp tục; kết quả không đại diện cho toàn bộ lịch sử."
                    )
                if result.get("silver_record") is not None:
                    with st.expander("Bản ghi trong snapshot Silver"):
                        st.json(json.dumps(result["silver_record"], default=str, ensure_ascii=False))
                for row in result["records"][:100]:
                    with st.expander(f"{row['object_path']} · bản ghi {row['record_index']}"):
                        st.code(row["object_sha256"], language=None)
                        st.json(row["payload"])
                frame = pd.DataFrame(
                    [{**row, "payload": json.dumps(row["payload"], ensure_ascii=False)} for row in result["records"]]
                )
                with st.expander("Receipt các tầng dữ liệu"):
                    st.json(result.get("receipt", {}))
            else:
                st.caption(
                    f"Tổng thành viên đã đối soát: {result.get('total_members', 'Chưa xác định')} · Trang {page}"
                )
                frame = pd.DataFrame(result.get("records", []))
                ui.table(frame)
                st.json(json.dumps(result.get("checks", {}), default=str, ensure_ascii=False))
                if result.get("unchecked_fields"):
                    st.info("Các trường chưa đối soát bằng recipe này: " + ", ".join(result["unchecked_fields"]))
                with st.expander("Grain, điều kiện chọn và công thức đối soát"):
                    st.json(json.dumps(result.get("recipe", {}), default=str, ensure_ascii=False))
                    st.code(result.get("recipe_sha256", ""), language=None)
                st.caption("Có thể dùng mã thành viên ở trên trong chế độ Bản ghi để truy tiếp từ Silver tới Bronze.")
            if not frame.empty:
                ui.download(frame, "bang-chung-truy-vet.csv")
ui.footer()
