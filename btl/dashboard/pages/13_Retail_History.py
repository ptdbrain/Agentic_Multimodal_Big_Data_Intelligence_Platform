"""A dashboard for the attributed Online Retail II invoice-line mart."""

from decimal import Decimal

import plotly.express as px
import streamlit as st
from psycopg2 import Error as PostgreSQLError

from dashboard import ui
from serving.retail import SOURCE_URL, read_daily

ui.setup_page("Giao dịch lịch sử", "Online Retail II · Dữ liệu của nhà bán lẻ trực tuyến tại Anh", "retail-history")
if st.session_state.get("dashboard_data_scope") not in {"real", "all"}:
    ui.empty_state("Không có giao dịch lịch sử trong phạm vi này", "Chọn Dữ liệu thật trên thanh bên.")
    st.stop()
try:
    daily = read_daily()
except (ConnectionError, PostgreSQLError):
    ui.empty_state("Chưa kết nối được kho giao dịch", "Kiểm tra kết nối PostgreSQL và thử lại.")
    st.stop()
if daily.empty:
    ui.empty_state("Chưa có phiên dữ liệu giao dịch", "Bảng tổng hợp sẽ xuất hiện sau khi đối soát và xuất bản.")
    st.stop()

start, end = daily["invoice_day"].min(), daily["invoice_day"].max()
period = st.date_input("Khoảng ngày trên hóa đơn", (start, end), min_value=start, max_value=end)
if len(period) != 2:
    st.info("Chọn đủ ngày đầu và ngày cuối.")
    st.stop()
shown = daily.loc[daily["invoice_day"].between(*period)]
st.caption(f"Phiên {daily['id'].iloc[0]} · Đơn vị GBP · Ngày giữ theo giờ gốc trên hóa đơn")
left, center, right = st.columns(3)
with left:
    ui.metric("Dòng giao dịch", ui.number(shown["transaction_lines"].sum()))
with center:
    ui.metric("Dòng có mã hủy", ui.number(shown["cancellations"].sum()))
with right:
    amount = sum(shown["line_amount"], Decimal(0))
    ui.metric("Giá trị dòng có dấu", f"{amount:,.2f} GBP", "Tổng số lượng × đơn giá, gồm cả dòng âm")
if shown.empty:
    st.info("Khoảng ngày này không có hóa đơn quan sát.")
else:
    ui.chart(px.bar(shown, x="invoice_day", y="transaction_lines",
                    labels={"invoice_day": "Ngày trên hóa đơn", "transaction_lines": "Dòng giao dịch"}))
    ui.table(shown, ["invoice_day", "currency", "transaction_lines", "cancellations", "line_amount"])
    ui.download(shown, "online-retail-ii-theo-ngay.csv")
st.markdown("Daqing Chen, UCI Machine Learning Repository, 2012. CC BY 4.0. [Nguồn Online Retail II](" + SOURCE_URL + ")")
ui.footer()
