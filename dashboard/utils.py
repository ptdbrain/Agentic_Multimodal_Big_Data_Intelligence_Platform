import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from storage.storage_manager import storage

def load_catalog_data() -> pd.DataFrame:
    from dashboard.db_connector import DashboardDB
    db = DashboardDB()
    df_prods = db.query("SELECT * FROM products")
    if not df_prods.empty:
        return df_prods
        
    df_prods = storage.read_silver_parquet("products")
    if df_prods.empty:
        sample_path = Path(__file__).resolve().parent.parent / "data" / "sample" / "products.parquet"
        if sample_path.exists():
            df_prods = pd.read_parquet(sample_path)
    return df_prods

def load_reviews_data() -> pd.DataFrame:
    from dashboard.db_connector import DashboardDB
    db = DashboardDB()
    df_revs = db.query("SELECT * FROM reviews")
    if not df_revs.empty:
        return df_revs
        
    df_revs = storage.read_silver_parquet("reviews")
    if df_revs.empty:
        sample_path = Path(__file__).resolve().parent.parent / "data" / "sample" / "reviews.parquet"
        if sample_path.exists():
            df_revs = pd.read_parquet(sample_path)
    return df_revs

def load_prices_data() -> pd.DataFrame:
    df_prices = storage.read_silver_parquet("prices")
    if df_prices.empty:
        sample_path = Path(__file__).resolve().parent.parent / "data" / "sample" / "prices.parquet"
        if sample_path.exists():
            df_prices = pd.read_parquet(sample_path)
    return df_prices

def load_gold_mart(mart_name: str) -> pd.DataFrame:
    return storage.read_gold_parquet(mart_name)

def get_datalake_metrics() -> dict:
    return storage.get_storage_stats()

def create_kpi_card(title: str, value: str, delta: str = None) -> str:
    return f"""
    <div style="background-color: #1e293b; padding: 18px; border-radius: 10px; border-left: 5px solid #38bdf8; margin-bottom: 10px;">
        <span style="font-size: 13px; color: #94a3b8; text-transform: uppercase; font-weight: 600;">{title}</span>
        <div style="font-size: 28px; font-weight: 700; color: #f8fafc; margin-top: 4px;">{value}</div>
        {f'<div style="font-size: 12px; color: #34d399; margin-top: 4px;">{delta}</div>' if delta else ''}
    </div>
    """

