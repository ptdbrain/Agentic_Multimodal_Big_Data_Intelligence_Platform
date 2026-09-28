import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.utils import load_reviews_data
from analytics.trend.trends import TrendAnalyzer

st.set_page_config(page_title="Trend Analytics", page_icon="📈", layout="wide")
st.title("📈 Temporal & Trend Analytics")

revs = load_reviews_data()
daily = TrendAnalyzer.daily_review_trends(revs)

st.subheader("Daily Review Volume & Positive/Negative Trajectory")
if not daily.empty:
    fig_vol = px.line(daily, x="date", y=["positive_count", "negative_count"],
                      labels={"value": "Reviews Count", "date": "Date"},
                      title="Daily Sentiment Trajectory (Positive vs Negative)",
                      color_discrete_map={"positive_count": "#34d399", "negative_count": "#f87171"})
    st.plotly_chart(fig_vol, use_container_width=True)

    st.subheader("Average Rating Evolution (7-Day Moving Window)")
    fig_rating = px.line(daily, x="date", y=["avg_rating", "rolling_avg_rating"],
                         labels={"value": "Rating", "date": "Date"},
                         title="Daily Average Rating & Trend Smooth",
                         color_discrete_map={"avg_rating": "#94a3b8", "rolling_avg_rating": "#38bdf8"})
    st.plotly_chart(fig_rating, use_container_width=True)
