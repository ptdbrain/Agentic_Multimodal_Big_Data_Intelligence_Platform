import streamlit as st
import time
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(page_title="Data Pipeline & Velocity", page_icon="🚀", layout="wide")
st.title("🚀 Data Pipeline & Real-Time Monitoring")

st.markdown("""
Real-time monitoring of Kafka streaming throughput, Spark micro-batch processing, and ingestion latency.
""")

col_gauge, col_stats = st.columns([1, 1])

with col_gauge:
    st.subheader("Kafka Ingestion Throughput")
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = 245,
        title = {'text': "Throughput (msg/sec)"},
        delta = {'reference': 150},
        gauge = {
            'axis': {'range': [None, 500]},
            'bar': {'color': "#38bdf8"},
            'steps' : [
                {'range': [0, 100], 'color': "#1e293b"},
                {'range': [100, 300], 'color': "#0f766e"},
                {'range': [300, 500], 'color': "#0369a1"}],
            'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 450}
        }
    ))
    st.plotly_chart(fig, use_container_width=True)

with col_stats:
    st.subheader("Streaming Metrics (Last 5 Minutes)")
    metrics_data = {
        "Metric": ["Spark Processing Latency", "Kafka Consumer Lag", "Failed / Dropped Records", "Active Partitions", "Current Velocity"],
        "Value": ["142 ms", "12 records", "0 records", "10 partitions", "245 events/sec"],
        "Health": ["🟢 Optimal", "🟢 Low", "🟢 Excellent", "🟢 Balanced", "🟢 High"]
    }
    st.table(pd.DataFrame(metrics_data))

st.divider()
st.subheader("Live Event Stream Feed Simulation")
sample_stream = pd.DataFrame([
    {"Timestamp": "2026-09-28 10:24:12", "Topic": "raw.reviews", "Product": "iphone_15_pro", "Rating": 5.0, "Status": "ACK"},
    {"Timestamp": "2026-09-28 10:24:13", "Topic": "raw.prices", "Product": "galaxy_s24_ultra", "Price": "29,990,000 VND", "Status": "ACK"},
    {"Timestamp": "2026-09-28 10:24:14", "Topic": "raw.events", "Event": "PRICE_UPDATE", "Payload": "Delta: 0.0%", "Status": "ACK"},
    {"Timestamp": "2026-09-28 10:24:15", "Topic": "raw.reviews", "Product": "macbook_pro_m3", "Rating": 4.0, "Status": "ACK"}
])
st.dataframe(sample_stream, use_container_width=True)
