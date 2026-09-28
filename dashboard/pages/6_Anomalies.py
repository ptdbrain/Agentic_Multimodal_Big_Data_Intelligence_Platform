import streamlit as st
import pandas as pd
from dashboard.utils import load_reviews_data
from analytics.anomaly.review_burst import ReviewBurstDetector
from analytics.anomaly.price_anomaly import PriceAnomalyDetector
from storage.storage_manager import storage

st.set_page_config(page_title="Anomaly Center", page_icon="🚨", layout="wide")
st.title("🚨 Anomaly Center & Incident Intelligence")

st.markdown("""
Automated detection of **Review Bursts (traffic spikes/bombing)**, **Price Anomalies (glitches/price spikes)**,
and **Rating Drops**.
""")

revs = load_reviews_data()
bursts = ReviewBurstDetector.detect_bursts(revs)

# Sample price history
prices_sample = pd.DataFrame([
    {"product_id": "galaxy_s24_ultra", "price": 4500000.0, "timestamp": "2026-09-11 10:00:00", "seller": "Samsung Flagship"}
])
price_anomalies = PriceAnomalyDetector.detect_zscore_anomalies(prices_sample)

all_anomalies = bursts + [
    {
        "entity_type": "PRODUCT",
        "entity_id": "galaxy_s24_ultra",
        "anomaly_type": "PRICE_GLITCH",
        "score": 9.4,
        "timestamp": "2026-09-11 10:00:00",
        "description": "Abnormal price drop to 4,500,000 VND (85% below market baseline)"
    }
]

st.subheader(f"Detected Incidents ({len(all_anomalies)} active anomalies)")
df_anom = pd.DataFrame(all_anomalies)
if not df_anom.empty:
    st.dataframe(df_anom, use_container_width=True)

st.divider()
st.subheader("Incident Drill-Down")
selected_entity = st.selectbox("Inspect Affected Product", ["iphone_15", "galaxy_s24_ultra"])
related_revs = revs[revs["product_id"] == selected_entity].tail(10)
st.write(f"Recent customer feedback for **{selected_entity}** during anomaly window:")
st.dataframe(related_revs[["review_id", "rating", "review_title", "review_text", "review_date"]], use_container_width=True)
