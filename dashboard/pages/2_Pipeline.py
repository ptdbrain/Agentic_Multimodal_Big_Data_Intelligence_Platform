import streamlit as st
import time
import pandas as pd
import plotly.graph_objects as go
from dashboard.utils import get_datalake_metrics, load_reviews_data
from storage.storage_manager import storage

st.set_page_config(page_title="Data Pipeline & Velocity", page_icon="🚀", layout="wide")
st.title("🚀 Data Pipeline & Real-Time Monitoring")

st.markdown("""
Real-time infrastructure monitoring of Data Lake storage footprint (Bronze, Silver, Gold) and stream velocity.
""")

stats = get_datalake_metrics()
revs = load_reviews_data()

col_gauge, col_stats = st.columns([1, 1])

total_files = stats["bronze"]["files"] + stats["silver"]["files"] + stats["gold"]["files"]
total_kb = (stats["bronze"]["size_bytes"] + stats["silver"]["size_bytes"] + stats["gold"]["size_bytes"]) / 1024.0

with col_gauge:
    st.subheader("Data Lake Storage Health")
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = round(total_kb, 1),
        number = {'suffix': " KB"},
        title = {'text': "Total Storage Utilized"},
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
        "Lakehouse Layer": ["Bronze (Raw Ingest)", "Silver (Cleaned Parquet)", "Gold (Aggregated Marts)"],
        "File Count": [f"{stats['bronze']['files']} files", f"{stats['silver']['files']} files", f"{stats['gold']['files']} files"],
        "Storage Footprint": [f"{stats['bronze']['size_bytes'] / 1024:.1f} KB", f"{stats['silver']['size_bytes'] / 1024:.1f} KB", f"{stats['gold']['size_bytes'] / 1024:.1f} KB"],
        "Health Status": ["🟢 Active" if stats['bronze']['files'] > 0 else "⚪ Standby",
                          "🟢 Synchronized" if stats['silver']['files'] > 0 else "⚪ Awaiting Batch",
                          "🟢 Serving" if stats['gold']['files'] > 0 else "⚪ Awaiting Batch"]
    }
    st.table(pd.DataFrame(lake_data))

st.divider()
st.subheader("Live Event Stream Feed Simulation")
bronze_revs = storage.read_bronze_records("raw.reviews", limit=10)
if bronze_revs:
    df_stream = pd.DataFrame(bronze_revs)
    display_cols = [c for c in ["review_id", "product_id", "rating", "review_date", "source"] if c in df_stream.columns]
    st.dataframe(df_stream[display_cols], use_container_width=True)
elif not revs.empty:
    sample_stream = revs.tail(10).copy()
    sample_stream["Status"] = "PROCESSED_IN_SILVER"
    display_cols = [c for c in ["review_id", "product_id", "rating", "review_date", "Status"] if c in sample_stream.columns]
    st.dataframe(sample_stream[display_cols], use_container_width=True)
else:
    st.info("No active event streams detected. Run the ingestion pipeline to stream live events.")

