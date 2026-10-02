import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.utils import create_kpi_card
from dashboard.db_connector import DashboardDB

st.set_page_config(page_title="Executive Overview", page_icon="📊", layout="wide")
st.title("📊 Executive Overview")

# Load verified analytical data from PostgreSQL DW / Gold Marts
db = DashboardDB()
kpis = db.get_kpi_summary()
p_stats = db.get_product_daily_stats()
b_stats = db.get_brand_daily_stats()
c_stats = db.get_category_daily_stats()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(create_kpi_card("Total Reviews", f"{kpis['total_reviews']:,}"), unsafe_allow_html=True)
with c2:
    st.markdown(create_kpi_card("Total Products", f"{kpis['total_products']:,}"), unsafe_allow_html=True)
with c3:
    st.markdown(create_kpi_card("Total Brands", f"{kpis['total_brands']}"), unsafe_allow_html=True)
with c4:
    st.markdown(create_kpi_card("Average Rating", f"⭐ {kpis['avg_rating']} / 5.0"), unsafe_allow_html=True)

st.divider()

col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Daily Review Volume Trend")
    if not p_stats.empty and 'date' in p_stats.columns and 'review_count' in p_stats.columns:
        daily_trends = p_stats.groupby('date')['review_count'].sum().reset_index().sort_values('date')
        daily_trends['rolling_7d'] = daily_trends['review_count'].rolling(window=7, min_periods=1).mean().round(1)
        fig = px.line(daily_trends, x="date", y=["review_count", "rolling_7d"],
                      labels={"value": "Reviews", "date": "Date"},
                      title="Review Volume & 7-Day Moving Average",
                      color_discrete_map={"review_count": "#38bdf8", "rolling_7d": "#f59e0b"})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No daily review trends available yet. Run the Spark Batch pipeline to populate.")

with col_chart2:
    st.subheader("Products by Category Share")
    if not c_stats.empty and 'category' in c_stats.columns and 'product_count' in c_stats.columns:
        cat_counts = c_stats.groupby('category')['product_count'].sum().reset_index()
        fig_pie = px.pie(cat_counts, values="product_count", names="category", title="Category Distribution", hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No category distribution available yet. Run the Spark Batch pipeline to populate.")
