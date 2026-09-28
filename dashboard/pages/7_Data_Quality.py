import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.utils import create_kpi_card

st.set_page_config(page_title="Data Quality & Lineage", page_icon="🛡️", layout="wide")
st.title("🛡️ Data Quality Scorecard & End-to-End Lineage")

st.subheader("Data Quality Scorecard (Batch Silver Processing)")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(create_kpi_card("Records Received", "1,200", "Batch #20260928"), unsafe_allow_html=True)
with col2:
    st.markdown(create_kpi_card("Valid Records", "1,182", "Passed rules"), unsafe_allow_html=True)
with col3:
    st.markdown(create_kpi_card("Duplicate Records", "18", "Filtered"), unsafe_allow_html=True)
with col4:
    st.markdown(create_kpi_card("Overall DQ Score", "98.5%", "High Integrity"), unsafe_allow_html=True)

st.divider()

col_rules, col_lineage = st.columns([1, 1])

with col_rules:
    st.subheader("Validation Rules Evaluation")
    rule_data = pd.DataFrame([
        {"Rule Name": "NonNullRule(product_id)", "Status": "PASSED", "Pass Rate": "100.0%"},
        {"Rule Name": "NonNullRule(review_text)", "Status": "PASSED", "Pass Rate": "99.8%"},
        {"Rule Name": "RangeRule(rating [1.0, 5.0])", "Status": "PASSED", "Pass Rate": "100.0%"},
        {"Rule Name": "PositiveNumberRule(price)", "Status": "PASSED", "Pass Rate": "100.0%"},
        {"Rule Name": "CompositeHashDeduplication", "Status": "FILTERED", "Pass Rate": "98.5%"}
    ])
    st.dataframe(rule_data, use_container_width=True)

with col_lineage:
    st.subheader("Data Lineage Traceability")
    st.markdown("""
    ```
    [Data Sources] (Web / API / Files)
           │
           ▼
    [Kafka Topics] (raw.reviews / raw.products)
           │
           ▼
    [Bronze Layer] (MinIO S3 Raw JSON, partitioned by hour)
           │
           ▼
    [Spark Batch ETL] (Cleaning, Normalization, Deduplication)
           │
           ▼
    [Silver Layer] (Cleaned Parquet, Schema Enforced)
           │
           ▼
    [Gold Layer] (Marts: product_daily_stats, brand_daily_stats)
           │
           ▼
    [PostgreSQL DW] ──► [Streamlit Dashboard & API]
    ```
    """)
