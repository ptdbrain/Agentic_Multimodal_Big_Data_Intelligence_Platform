import pandas as pd
import plotly.express as px
import streamlit as st
from spark.quality.metrics import DataQualityEvaluator
from spark.quality.rules import RuleRegistry, REVIEW_QUALITY_RULES, PRODUCT_QUALITY_RULES
from dashboard import ui
from dashboard.utils import load_reviews_data, load_catalog_data, render_analytics_quality

ui.setup_page(
    "Chất lượng & nguồn gốc",
    "Hiểu mức độ tin cậy của dữ liệu, từ đầu vào đến các bản ghi đang được phục vụ.",
    "quality",
)
with ui.warehouse() as db:
    reviews = load_reviews_data(db=db)
    products = load_catalog_data(db=db)
    reports = db.get_data_quality_metrics()
choice = st.radio("Tập dữ liệu", ["Đánh giá", "Sản phẩm"], horizontal=True)
dataset = "reviews" if choice == "Đánh giá" else "products"
frame = reviews if dataset == "reviews" else products
records = frame.to_dict("records") if not frame.empty else []
rules = RuleRegistry.get_rules(dataset) or (REVIEW_QUALITY_RULES if dataset == "reviews" else PRODUCT_QUALITY_RULES)
result = DataQualityEvaluator.evaluate_batch(
    dataset, records, rules, primary_key="review_id" if dataset == "reviews" else "product_id"
)
score = f"{result['dq_score']:.1f}%" if records else "Chưa có dữ liệu"
for col, (title, value, note) in zip(
    st.columns(4),
    [
        ("Bản ghi đang phục vụ", ui.number(len(records)), "Trong phạm vi nguồn đang chọn"),
        ("Bản ghi hợp lệ", ui.number(result["records_valid"]), "Theo quy tắc kiểm tra hiện tại"),
        ("Bản ghi trùng", ui.number(result["records_duplicate"]), "Kiểm tra theo khóa định danh"),
        ("Điểm chất lượng", score, "Không suy ra 100% khi chưa có dữ liệu"),
    ],
):
    with col:
        ui.metric(title, value, note)
st.write("")
with st.container(border=True):
    st.subheader("Báo cáo từ các lượt xử lý")
    st.caption(
        "Bao gồm dữ liệu đầu vào, cách ly và thời gian quan sát thiếu. Các mã lỗi có thể cùng xuất hiện trên một bản ghi."
    )
    if reports.empty:
        ui.empty_state("Chưa có báo cáo chất lượng", "Báo cáo được ghi lại khi dữ liệu đi qua pipeline.")
    else:
        shown = reports.loc[reports["dataset_name"].eq(dataset)]
        if shown.empty:
            st.info("Chưa có báo cáo cho tập dữ liệu đang chọn.")
        else:
            ui.table(shown)
a, b = st.columns([1.6, 1], gap="large")
with a:
    with st.container(border=True):
        st.subheader("Kiểm tra dữ liệu đang phục vụ")
        st.caption("Đánh giá lại bản ghi hiện tại; bản ghi đã cách ly được thống kê trong báo cáo xử lý.")
        evaluations = pd.DataFrame(result["rule_evaluations"])
        if not evaluations.empty:
            ui.table(evaluations)
        else:
            ui.empty_state("Chưa có kết quả kiểm tra", "Kết quả quy tắc sẽ có khi tập dữ liệu đang chọn có bản ghi.")
with b:
    with st.container(border=True):
        st.subheader("Hiểu các trạng thái")
        st.markdown("**Hợp lệ**")
        st.caption("Đáp ứng những quy tắc đang áp dụng.")
        st.markdown("**Cách ly**")
        st.caption("Giữ lại bản ghi lỗi để có thể kiểm tra và sửa.")
        st.markdown("**Chưa biết**")
        st.caption("Nguồn thiếu thông tin; không tự biến thành 0 hoặc một giá trị phỏng đoán.")
        st.markdown("**Bản phát lại**")
        st.caption("Dữ liệu thật đã thu trước đó, giữ nhãn và thời gian gốc.")
render_analytics_quality(frame)
ui.footer()
