import streamlit as st
import pandas as pd
from dashboard.db_connector import DashboardDB

st.set_page_config(page_title="Anomaly Center", page_icon="🚨", layout="wide")
st.title("🚨 Anomaly Center & Incident Intelligence")

st.markdown("""
Pre-computed analytical anomaly incidents served directly from **PostgreSQL DW (`anomaly_events`)** / **Gold Layer**:
**Review Bursts**, **Price Outliers**, and **Sudden Rating Drops**.
""")

db = DashboardDB()
df_anom = db.get_anomaly_events()

all_anomalies = df_anom.to_dict(orient="records") if not df_anom.empty else []

# Overview metrics
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Total Detected Incidents", len(all_anomalies))
with c2:
    burst_count = sum(1 for a in all_anomalies if "BURST" in str(a.get("anomaly_type", "")) or "BOMBING" in str(a.get("anomaly_type", "")))
    st.metric("Review Bursts / Spikes", burst_count)
with c3:
    drop_count = sum(1 for a in all_anomalies if "RATING_DROP" in str(a.get("anomaly_type", "")))
    st.metric("Rating Drops", drop_count)
with c4:
    price_count = sum(1 for a in all_anomalies if "PRICE" in str(a.get("anomaly_type", "")))
    st.metric("Price Outliers", price_count)

st.divider()

if not df_anom.empty:
    st.subheader(f"Active Incidents ({len(df_anom)} events recorded)")
    display_cols = [c for c in ["event_id", "entity_type", "entity_id", "anomaly_type", "score", "timestamp", "description"] if c in df_anom.columns]
    st.dataframe(df_anom[display_cols] if display_cols else df_anom, use_container_width=True)

    # Dynamic Drill-Down
    st.divider()
    st.subheader("Incident Drill-Down")
    if "entity_id" in df_anom.columns:
        affected_ids = sorted(list(set(str(e) for e in df_anom["entity_id"].dropna().unique())))
        if affected_ids:
            selected_entity = st.selectbox("Inspect Affected Entity", affected_ids)
            entity_events = df_anom[df_anom["entity_id"] == selected_entity]
            st.write(f"Audit log for **{selected_entity}**:")
            st.dataframe(entity_events, use_container_width=True)
else:
    st.success("✅ No anomalies detected in warehouse. All metrics operating within normal baseline bounds.")
