import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.utils import load_catalog_data, load_reviews_data, create_kpi_card
from analytics.descriptive.stats import DescriptiveStats
from analytics.trend.trends import TrendAnalyzer

st.set_page_config(page_title="Executive Overview", page_icon="📊", layout="wide")
st.title("📊 Executive Overview")

prods = load_catalog_data()
revs = load_reviews_data()

summary = DescriptiveStats.compute_summary(prods, revs)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(create_kpi_card("Total Reviews", f"{summary['total_reviews']:,}"), unsafe_allow_html=True)
with c2:
    st.markdown(create_kpi_card("Total Products", f"{summary['total_products']:,}"), unsafe_allow_html=True)
with c3:
    st.markdown(create_kpi_card("Total Brands", f"{summary['total_brands']}"), unsafe_allow_html=True)
with c4:
    st.markdown(create_kpi_card("Average Rating", f"⭐ {summary['avg_rating']} / 5.0"), unsafe_allow_html=True)

st.divider()

col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Daily Review Volume Trend")
    daily_trends = TrendAnalyzer.daily_review_trends(revs)
    if not daily_trends.empty:
        fig = px.line(daily_trends, x="date", y=["review_count", "rolling_avg_count"],
                      labels={"value": "Reviews", "date": "Date"},
                      title="Review Volume & 7-Day Moving Average",
                      color_discrete_map={"review_count": "#38bdf8", "rolling_avg_count": "#f59e0b"})
        st.plotly_chart(fig, use_container_width=True)

with col_chart2:
    st.subheader("Products by Category Share")
    if not prods.empty:
        cat_counts = prods["category"].value_counts().reset_index()
        cat_counts.columns = ["category", "count"]
        fig_pie = px.pie(cat_counts, values="count", names="category", title="Category Distribution", hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)
