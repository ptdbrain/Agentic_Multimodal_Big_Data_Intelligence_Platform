import streamlit as st
import pandas as pd
from dashboard.utils import load_reviews_data, load_catalog_data, create_kpi_card
from spark.quality.metrics import DataQualityEvaluator
from spark.quality.rules import RuleRegistry, REVIEW_QUALITY_RULES, PRODUCT_QUALITY_RULES

st.set_page_config(page_title="Data Quality & Lineage", page_icon="🛡️", layout="wide")
st.title("🛡️ Data Quality Scorecard & End-to-End Lineage")

revs = load_reviews_data()
prods = load_catalog_data()

# Evaluate Quality Dynamically
selected_dataset = st.radio("Select Target Dataset for Audit", ["Reviews", "Products"], horizontal=True)

if selected_dataset == "Reviews":
    records = revs.to_dict(orient="records") if not revs.empty else []
    rules = RuleRegistry.get_rules("reviews") or REVIEW_QUALITY_RULES
    eval_res = DataQualityEvaluator.evaluate_batch("reviews", records, rules, primary_key="review_id")
else:
    records = prods.to_dict(orient="records") if not prods.empty else []
    rules = RuleRegistry.get_rules("products") or PRODUCT_QUALITY_RULES
    eval_res = DataQualityEvaluator.evaluate_batch("products", records, rules, primary_key="product_id")

st.subheader(f"Data Quality Scorecard ({selected_dataset})")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(create_kpi_card("Records Received", f"{eval_res['records_received']:,}", f"Time: {eval_res['processing_time_ms']}ms"), unsafe_allow_html=True)
with col2:
    st.markdown(create_kpi_card("Valid Records", f"{eval_res['records_valid']:,}", "Passed validation"), unsafe_allow_html=True)
with col3:
    st.markdown(create_kpi_card("Duplicate Records", f"{eval_res['records_duplicate']:,}", "Identified"), unsafe_allow_html=True)
with col4:
    score_color = "🟢 High Integrity" if eval_res['dq_score'] >= 90 else "🟡 Requires Attention"
    st.markdown(create_kpi_card("Overall DQ Score", f"{eval_res['dq_score']}%", score_color), unsafe_allow_html=True)

st.divider()

col_rules, col_lineage = st.columns([1, 1])

with col_rules:
    st.subheader("Validation Rules Evaluation")
    if eval_res["rule_evaluations"]:
        df_rule_res = pd.DataFrame(eval_res["rule_evaluations"])
        st.dataframe(df_rule_res, use_container_width=True)
    else:
        st.info("No active rules registered for this dataset.")

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

