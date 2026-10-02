import streamlit as st
import time
import pandas as pd
import plotly.graph_objects as go
from dashboard.utils import get_datalake_metrics
from dashboard.db_connector import DashboardDB
from storage.storage_manager import storage

st.set_page_config(page_title="Data Pipeline & Velocity", page_icon="🚀", layout="wide")
st.title("🚀 Data Pipeline & Storage Observability")

st.markdown("""
Infrastructure monitoring of Data Lake storage footprint (Bronze, Silver, Gold), Data Quality scorecards, and batch/stream processing throughput.
""")

db = DashboardDB()
stats = get_datalake_metrics()
dq_metrics = db.get_data_quality_metrics()

col_gauge, col_stats = st.columns([1, 1])

total_files = stats["bronze"]["files"] + stats["silver"]["files"] + stats["gold"]["files"]
total_kb = (stats["bronze"]["size_bytes"] + stats["silver"]["size_bytes"] + stats["gold"]["size_bytes"]) / 1024.0

with col_gauge:
    st.subheader("Data Lake Storage Health")
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = round(total_kb, 1),
        number = {'suffix': " KB"},
        title = {'text': "Total Storage Footprint"},
        gauge = {
            'axis': {'range': [None, max(500, total_kb * 1.5)]},
            'bar': {'color': "#38bdf8"},
            'steps' : [
                {'range': [0, 200], 'color': "#1e293b"},
                {'range': [200, 500], 'color': "#0f766e"}
            ]
        }
    ))
    st.plotly_chart(fig, use_container_width=True)

with col_stats:
    st.subheader("Data Lake Layer Breakdown")
    lake_data = {
        "Data Lake Layer": ["Bronze (Raw Ingest)", "Silver (Cleaned Parquet)", "Gold (Aggregated Marts)"],
        "File Count": [f"{stats['bronze']['files']} files", f"{stats['silver']['files']} files", f"{stats['gold']['files']} files"],
        "Storage Footprint": [f"{stats['bronze']['size_bytes'] / 1024:.1f} KB", f"{stats['silver']['size_bytes'] / 1024:.1f} KB", f"{stats['gold']['size_bytes'] / 1024:.1f} KB"],
        "Backend Status": [f"🟢 {type(storage.backend).__name__} (Active)" if stats['bronze']['files'] > 0 else "⚪ Standby",
                           f"🟢 {type(storage.backend).__name__} (Sync)" if stats['silver']['files'] > 0 else "⚪ Awaiting Batch",
                           f"🟢 {type(storage.backend).__name__} (Serving)" if stats['gold']['files'] > 0 else "⚪ Awaiting Batch"]
    }
    st.table(pd.DataFrame(lake_data))

st.divider()

# Data Quality Scorecard
st.subheader("🛡️ Verified Data Quality Scorecard")
if not dq_metrics.empty:
    cols_to_show = [c for c in ["batch_id", "dataset_name", "records_received", "records_valid", "records_invalid", "records_duplicate", "dq_score", "timestamp"] if c in dq_metrics.columns]
    st.dataframe(dq_metrics[cols_to_show], use_container_width=True)
else:
    st.info("No DQ scorecards recorded yet. Run the Spark Batch ETL job to generate data quality metrics.")

st.divider()
st.subheader("Live Event Stream Feed")
bronze_revs = storage.read_bronze_records("raw.reviews", limit=10)
if bronze_revs:
    df_stream = pd.DataFrame(bronze_revs)
    display_cols = [c for c in ["review_id", "product_id", "rating", "review_date", "source"] if c in df_stream.columns]
    st.dataframe(df_stream[display_cols] if display_cols else df_stream, use_container_width=True)
else:
    st.info("No active event streams detected in Bronze. Ingest raw batches to populate.")
