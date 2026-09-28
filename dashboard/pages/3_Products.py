import streamlit as st
import pandas as pd
import plotly.express as px
from dashboard.utils import load_catalog_data, load_reviews_data
from analytics.descriptive.stats import DescriptiveStats

st.set_page_config(page_title="Product Analytics", page_icon="📱", layout="wide")
st.title("📱 Product Analytics Explorer")

prods = load_catalog_data()
revs = load_reviews_data()

metrics_df = DescriptiveStats.compute_product_metrics(revs, prods)

# Filters
col_f1, col_f2 = st.columns(2)
with col_f1:
    brands = ["All"] + sorted(prods["brand"].unique().tolist())
    selected_brand = st.selectbox("Filter by Brand", brands)
with col_f2:
    categories = ["All"] + sorted(prods["category"].unique().tolist())
    selected_cat = st.selectbox("Filter by Category", categories)

filtered = metrics_df.copy()
if selected_brand != "All":
    filtered = filtered[filtered["brand"] == selected_brand]
if selected_cat != "All":
    filtered = filtered[filtered["category"] == selected_cat]

st.subheader(f"Catalog Products ({len(filtered)} items)")
st.dataframe(filtered[["product_id", "product_name", "brand", "category", "price", "avg_rating", "review_count"]], use_container_width=True)

st.divider()
st.subheader("Price vs Average Rating Distribution")
fig_scatter = px.scatter(
    filtered, x="price", y="avg_rating", size="review_count", color="brand",
    hover_name="product_name", labels={"price": "Price (VND)", "avg_rating": "Average Rating"},
    title="Product Positioning Matrix (Price vs Rating vs Volume)"
)
st.plotly_chart(fig_scatter, use_container_width=True)
