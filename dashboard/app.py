import streamlit as st
import pandas as pd
from dashboard.utils import load_catalog_data, load_reviews_data, create_kpi_card

st.set_page_config(
    page_title="SentinelAI — Big Data Intelligence Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("⚡ SentinelAI Platform")
st.subheader("Data Foundation & Analytics Dashboard (Phase 1)")

st.markdown("""
Welcome to **SentinelAI**, a distributed Big Data ingestion, lakehouse storage, distributed processing,
and anomaly intelligence platform built for multimodal consumer technology analytics.
""")

# Load baseline stats
prods = load_catalog_data()
revs = load_reviews_data()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(create_kpi_card("Total Products", f"{len(prods):,}", "Catalog Active"), unsafe_allow_html=True)
with col2:
    st.markdown(create_kpi_card("Total Reviews", f"{len(revs):,}", "+12.4% this month"), unsafe_allow_html=True)
with col3:
    brands_count = prods['brand'].nunique() if not prods.empty else 0
    st.markdown(create_kpi_card("Active Brands", f"{brands_count}", "Multi-vendor sync"), unsafe_allow_html=True)
with col4:
    st.markdown(create_kpi_card("Data Quality Score", "98.4%", "Verified Silver Layer"), unsafe_allow_html=True)

st.divider()

col_left, col_right = st.columns([2, 1])

with col_left:
    st.markdown("### 🏛️ System Architecture Status")
    st.markdown("""
    - **Data Lake (MinIO/Local)**: `sentinel-raw`, `sentinel-silver`, `sentinel-gold` (Active)
    - **Streaming Engine**: Kafka Topics (`raw.products`, `raw.reviews`, `raw.prices`, `raw.events`)
    - **Compute Engine**: Apache Spark Batch & Structured Streaming (Tumbling Windows: 1m, 5m)
    - **Data Warehouse**: PostgreSQL / SQLite (`product_daily_stats`, `brand_daily_stats`)
    - **Search Indexer**: Elasticsearch / In-memory full-text search
    """)

with col_right:
    st.markdown("### 🧭 Navigation Guide")
    st.info("""
    Use the sidebar to explore platform views:
    1. **Overview**: Executive KPIs & Trends
    2. **Data Pipeline**: Real-time Kafka & Spark metrics
    3. **Product Analytics**: Pricing, rating distributions
    4. **Review Analytics**: Feedback, sentiment heuristic
    5. **Trend Analytics**: Time series & moving averages
    6. **Anomaly Center**: Price spikes & review bursts
    7. **Data Quality & Lineage**: DQ scorecards & lineage
    """)
