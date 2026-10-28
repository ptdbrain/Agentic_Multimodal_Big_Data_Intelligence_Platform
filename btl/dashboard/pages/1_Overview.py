from dashboard.utils import render_analytics_quality
import streamlit as st
import plotly.express as px
from analytics.trend.trends import TrendAnalyzer
from dashboard.utils import create_kpi_card, render_sidebar_source
from dashboard.db_connector import DashboardDB

st.set_page_config(page_title="Executive Overview", page_icon="📊", layout="wide")
st.title("📊 Executive Overview")

render_sidebar_source()

# Load verified analytical data from PostgreSQL DW / Gold Marts
db = DashboardDB()
kpis = db.get_kpi_summary()
p_stats = db.get_product_daily_stats()
b_stats = db.get_brand_daily_stats()
c_stats = db.get_category_daily_stats()
db.close()
render_analytics_quality(p_stats)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(create_kpi_card("Total Reviews", f"{kpis['total_reviews']:,}"), unsafe_allow_html=True)
with c2:
    st.markdown(create_kpi_card("Total Products", f"{kpis['total_products']:,}"), unsafe_allow_html=True)
with c3:
    st.markdown(create_kpi_card("Total Brands", f"{kpis['total_brands']}"), unsafe_allow_html=True)
with c4:
    rating = f"⭐ {kpis['avg_rating']} / 5.0" if kpis['avg_rating'] is not None else 'Unavailable'
    st.markdown(create_kpi_card("Average Rating", rating, f"Sample: {kpis['rating_sample_size']:,} reviews"), unsafe_allow_html=True)

st.divider()

col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Daily Review Volume Trend")
    if not p_stats.empty and 'date' in p_stats.columns and 'review_count' in p_stats.columns:
        daily_trends = TrendAnalyzer.daily_volume_trends(p_stats)
        if not daily_trends.empty:
            insufficient = daily_trends['trend_status'] == 'insufficient_data'
            if insufficient.any():
                st.warning('Insufficient calendar coverage; smoothed volume is hidden for those dates.')
            st.caption('7 calendar days; average over observed days. Missing days remain unknown.')
            with st.expander('Volume window coverage'):
                st.dataframe(daily_trends[['date', 'observed_days', 'missing_days', 'baseline_start',
                                          'baseline_end', 'trend_status']], use_container_width=True)
            plotted = daily_trends.copy()
            plotted.loc[insufficient, 'rolling_avg_count'] = float('nan')
            fig = px.line(plotted, x="date", y=["review_count", "rolling_avg_count"],
                          labels={"value": "Reviews", "date": "Date"},
                          title="Review Volume & 7-Day Moving Average",
                          color_discrete_map={"review_count": "#38bdf8", "rolling_avg_count": "#f59e0b"})
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
