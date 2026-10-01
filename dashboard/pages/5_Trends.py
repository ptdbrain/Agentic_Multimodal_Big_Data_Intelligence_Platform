import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.utils import load_reviews_data
from analytics.trend.trends import TrendAnalyzer

st.set_page_config(page_title="Trend Analytics", page_icon="📈", layout="wide")
st.title("📈 Temporal & Trend Analytics")

revs = load_reviews_data()

if revs.empty:
    st.warning("No review data available to analyze trends. Please run the E2E pipeline.")
    st.stop()

# Interactive Controls
col_ctrl1, col_ctrl2 = st.columns(2)
with col_ctrl1:
    window_days = st.slider("Rolling Window (Days)", min_value=3, max_value=30, value=7, step=1)
with col_ctrl2:
    sentiment_cut = st.slider("Positive Rating Cutoff (Stars)", min_value=3.0, max_value=5.0, value=4.0, step=0.5)

daily = TrendAnalyzer.daily_review_trends(
    revs,
    rolling_window=window_days,
    positive_threshold=sentiment_cut,
    negative_threshold=2.0
)

st.subheader("Daily Review Volume & Positive/Negative Trajectory")
if not daily.empty:
    fig_vol = px.line(daily, x="date", y=["positive_count", "negative_count"],
                      labels={"value": "Reviews Count", "date": "Date"},
                      title=f"Daily Sentiment Trajectory (>= {sentiment_cut}★ Positive vs <= 2.0★ Negative)",
                      color_discrete_map={"positive_count": "#34d399", "negative_count": "#f87171"})
    st.plotly_chart(fig_vol, use_container_width=True)

    st.subheader(f"Average Rating Evolution ({window_days}-Day Moving Window)")
    fig_rating = px.line(daily, x="date", y=["avg_rating", "rolling_avg_rating"],
                         labels={"value": "Rating", "date": "Date"},
                         title=f"Daily Average Rating & {window_days}-Day Smoothed Trend",
                         color_discrete_map={"avg_rating": "#94a3b8", "rolling_avg_rating": "#38bdf8"})
    st.plotly_chart(fig_rating, use_container_width=True)
else:
    st.info("No temporal records found in review dates.")

