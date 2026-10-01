import streamlit as st
import pandas as pd
from dashboard.utils import load_reviews_data, load_catalog_data, load_prices_data
from analytics.anomaly.review_burst import ReviewBurstDetector
from analytics.anomaly.price_anomaly import PriceAnomalyDetector
from analytics.anomaly.rating_anomaly import RatingAnomalyDetector

st.set_page_config(page_title="Anomaly Center", page_icon="🚨", layout="wide")
st.title("🚨 Anomaly Center & Incident Intelligence")

st.markdown("""
Automated multi-detector intelligence: **Review Bursts (traffic spikes & bombing)**,
**Price Outliers (Z-score & IQR)**, and **Sudden Rating Drops**.
""")

revs = load_reviews_data()
prods = load_catalog_data()
prices = load_prices_data()

all_anomalies = []

# 1. Run Review Burst & Bombing Detector
if not revs.empty:
    burst_events = ReviewBurstDetector.detect_bursts(revs)
    all_anomalies.extend(burst_events)

# 2. Run Rating Drop Detector
if not revs.empty:
    rating_drops = RatingAnomalyDetector.detect_rating_drops(revs)
    all_anomalies.extend(rating_drops)

# 3. Run Price Anomaly Detector
if not prices.empty:
    price_anoms = PriceAnomalyDetector.detect_zscore_anomalies(prices)
    all_anomalies.extend(price_anoms)

# Overview metrics
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Total Detected Incidents", len(all_anomalies))
with c2:
    burst_count = sum(1 for a in all_anomalies if "BURST" in a.get("anomaly_type", "") or "BOMBING" in a.get("anomaly_type", ""))
    st.metric("Review Bursts / Spikes", burst_count)
with c3:
    drop_count = sum(1 for a in all_anomalies if a.get("anomaly_type") == "RATING_DROP")
    st.metric("Rating Drops", drop_count)
with c4:
    price_count = sum(1 for a in all_anomalies if "PRICE" in a.get("anomaly_type", ""))
    st.metric("Price Outliers", price_count)

st.divider()

if all_anomalies:
    st.subheader(f"Active Incidents ({len(all_anomalies)} events detected)")
    df_anom = pd.DataFrame(all_anomalies)
    st.dataframe(df_anom, use_container_width=True)

    # Dynamic Drill-Down
    st.divider()
    st.subheader("Incident Drill-Down")
    affected_ids = sorted(list(set(str(a["entity_id"]) for a in all_anomalies if "entity_id" in a)))
    selected_entity = st.selectbox("Inspect Affected Product", affected_ids)

    if not revs.empty and "product_id" in revs.columns:
        related_revs = revs[revs["product_id"] == selected_entity].tail(10)
        display_rev_cols = [c for c in ["review_id", "rating", "review_title", "review_text", "review_date"] if c in related_revs.columns]
        st.write(f"Recent customer feedback for **{selected_entity}**:")
        st.dataframe(related_revs[display_rev_cols], use_container_width=True)
else:
    st.success("✅ No anomalies detected in current dataset. All metrics operating within normal baseline bounds.")

