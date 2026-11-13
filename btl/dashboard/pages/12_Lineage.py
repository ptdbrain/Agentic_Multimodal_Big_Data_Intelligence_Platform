"""Read-only, checksum-verified publication input evidence."""

import json
import pandas as pd
import streamlit as st
from dashboard import ui
from serving.lake import DatasetLake
from serving.lineage import trace_record
from serving.store import ServingStore

ui.setup_page("Truy vết dữ liệu", "Kiểm tra nguồn gốc bằng receipt đã lưu và checksum của đối tượng Bronze.", "lineage")
entity = st.selectbox("Loại bản ghi", ["products", "reviews", "prices", "articles"])
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
    record_id = st.text_input(
        "Mã bản ghi chính xác",
        max_chars=500,
        help="product_id, review_id, price_id hoặc article_id tương ứng với loại đã chọn.",
    )
    st.caption(
        "Bản ghi tìm thấy là đầu vào của phiên xuất. Receipt đầu vào chưa chứng minh bản ghi đó thắng bước khử trùng Silver hoặc đóng góp vào một chỉ số tổng hợp cụ thể."
    )
    if st.button("Kiểm tra bằng chứng", disabled=not record_id.strip()):
        try:
            result = trace_record(
                ServingStore(db._conn, binding.id, binding.source_group),
                DatasetLake(binding),
                generation,
                entity,
                record_id.strip(),
            )
        except Exception as error:
            st.error(
                f"Không thể xác minh bằng chứng ({type(error).__name__}). Không hiển thị dữ liệu chưa được xác minh."
            )
        else:
            st.write("Kết quả:", result["status"])
            if result.get("truncated"):
                st.warning("Đã đạt giới hạn 500 đối tượng; kết quả chưa bao phủ toàn bộ phiên.")
            for row in result["records"][:100]:
                with st.expander(f"{row['object_path']} · bản ghi {row['record_index']}"):
                    st.code(row["object_sha256"], language=None)
                    st.json(row["payload"])
            if result["records"]:
                frame = pd.DataFrame(
                    [{**row, "payload": json.dumps(row["payload"], ensure_ascii=False)} for row in result["records"]]
                )
                ui.download(frame, "bang-chung-bronze.csv")
            with st.expander("Receipt các tầng dữ liệu"):
                st.json(result.get("receipt", {}))
ui.footer()
