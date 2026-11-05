"""Application entrypoint with one route registry and a custom shared sidebar."""

import streamlit as st
from dashboard.navigation import page_groups

st.set_page_config(page_title="SentinelAI · Phân tích dữ liệu", page_icon=":material/monitoring:", layout="wide")
pages = page_groups()
st.navigation({label: [page for _, page in entries] for label, entries in pages}, position="hidden").run()
