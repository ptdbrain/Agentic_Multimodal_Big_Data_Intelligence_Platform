import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.utils import load_reviews_data

st.set_page_config(page_title="Review Analytics", page_icon="💬", layout="wide")
st.title("💬 Review Analytics & Feedback Intelligence")

revs = load_reviews_data()

c1, c2 = st.columns(2)

with c1:
    st.subheader("Rating Distribution (1 - 5 Stars)")
    if not revs.empty:
        r_counts = revs["rating"].value_counts().sort_index().reset_index()
        r_counts.columns = ["Rating", "Count"]
        fig = px.bar(r_counts, x="Rating", y="Count", text="Count", color="Rating",
                     color_continuous_scale="Blues", title="Distribution of Star Ratings")
        st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Language & Verified Buyer Split")
    if not revs.empty:
        v_counts = revs["verified_purchase"].value_counts().reset_index()
        v_counts.columns = ["Verified Purchase", "Count"]
        v_counts["Verified Purchase"] = v_counts["Verified Purchase"].map({True: "Verified Buyer", False: "Unverified"})
        fig2 = px.pie(v_counts, values="Count", names="Verified Purchase", hole=0.4, title="Buyer Verification Ratio")
        st.plotly_chart(fig2, use_container_width=True)

st.divider()
st.subheader("Customer Reviews Feed")
st.dataframe(revs[["review_id", "product_id", "rating", "review_title", "review_text", "language", "review_date"]], use_container_width=True)
