import logging
from typing import Optional
import pandas as pd
from config.settings import settings

logger = logging.getLogger(__name__)

class DashboardDB:
    """Dashboard data source connecting to PostgreSQL warehouse.
    Falls back to SQLite or Gold Parquet when PostgreSQL is unavailable.
    Guarantees no hardcoded KPIs and adheres to Plan L-LIV.
    """
    
    def __init__(self):
        self._conn = None
        self._source = None  # 'postgres', 'sqlite', 'parquet'
        self._connect()
    
    def _connect(self):
        # Try PostgreSQL first
        try:
            import psycopg2
            self._conn = psycopg2.connect(
                host=settings.database.host,
                port=settings.database.port,
                user=settings.database.user,
                password=settings.database.password,
                database=settings.database.database
            )
            self._source = 'postgres'
            return
        except Exception:
            pass
        
        # Try SQLite
        try:
            import sqlite3
            db_path = settings.database.sqlite_path
            if db_path.exists():
                self._conn = sqlite3.connect(str(db_path))
                self._source = 'sqlite'
                return
        except Exception:
            pass
        
        # Parquet fallback
        self._source = 'parquet'
    
    def query(self, sql: str) -> pd.DataFrame:
        if self._conn:
            try:
                return pd.read_sql_query(sql, self._conn)
            except Exception:
                return pd.DataFrame()
        return pd.DataFrame()
    
    def get_product_daily_stats(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            res = self.query("SELECT * FROM product_daily_stats ORDER BY date DESC")
            if not res.empty:
                return res
        from storage.storage_manager import storage
        return storage.read_gold_parquet("product_daily_stats")
    
    def get_brand_daily_stats(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            res = self.query("SELECT * FROM brand_daily_stats ORDER BY date DESC")
            if not res.empty:
                return res
        from storage.storage_manager import storage
        return storage.read_gold_parquet("brand_daily_stats")

    def get_category_daily_stats(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            res = self.query("SELECT * FROM category_daily_stats ORDER BY date DESC")
            if not res.empty:
                return res
        from storage.storage_manager import storage
        return storage.read_gold_parquet("category_daily_stats")

    def get_price_daily_stats(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            res = self.query("SELECT * FROM price_daily_stats ORDER BY date DESC")
            if not res.empty:
                return res
        from storage.storage_manager import storage
        return storage.read_gold_parquet("price_daily_stats")
    
    def get_anomaly_events(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            res = self.query("SELECT * FROM anomaly_events ORDER BY timestamp DESC LIMIT 100")
            if not res.empty:
                return res
        from storage.storage_manager import storage
        return storage.read_gold_parquet("anomaly_events")
    
    def get_data_quality_metrics(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            res = self.query("SELECT * FROM data_quality_metrics ORDER BY timestamp DESC LIMIT 50")
            if not res.empty:
                return res
        from storage.storage_manager import storage
        return storage.read_gold_parquet("data_quality_metrics")
    
    def get_pipeline_metrics(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            res = self.query("SELECT * FROM pipeline_metrics ORDER BY timestamp DESC LIMIT 100")
            if not res.empty:
                return res
        return pd.DataFrame()
    
    def get_kpi_summary(self) -> dict:
        """Get live KPI values from database or Gold marts, never hardcoded."""
        total_products = 0
        total_reviews = 0
        total_brands = 0
        avg_dq = 0.0
        rating = 0.0

        if self._source in ('postgres', 'sqlite'):
            try:
                products = self.query("SELECT COUNT(*) as cnt FROM products")
                if not products.empty and int(products['cnt'].iloc[0]) > 0:
                    total_products = int(products['cnt'].iloc[0])
                
                reviews = self.query("SELECT SUM(review_count) as total_rc, AVG(avg_rating) as ar FROM product_daily_stats")
                if not reviews.empty and reviews['total_rc'].iloc[0] is not None:
                    total_reviews = int(reviews['total_rc'].iloc[0])
                    rating = float(reviews['ar'].iloc[0]) if reviews['ar'].iloc[0] is not None else 0.0
                
                brands = self.query("SELECT COUNT(DISTINCT brand) as cnt FROM brand_daily_stats")
                if not brands.empty and int(brands['cnt'].iloc[0]) > 0:
                    total_brands = int(brands['cnt'].iloc[0])
                
                dq = self.query("SELECT AVG(dq_score) as avg_dq FROM data_quality_metrics")
                if not dq.empty and dq['avg_dq'].iloc[0] is not None:
                    avg_dq = float(dq['avg_dq'].iloc[0])
            except Exception:
                pass

        # Fallback to Gold Marts if DB query yielded 0
        if total_products == 0 or total_reviews == 0:
            p_stats = self.get_product_daily_stats()
            if not p_stats.empty:
                if 'product_id' in p_stats.columns:
                    total_products = p_stats['product_id'].nunique()
                if 'review_count' in p_stats.columns:
                    total_reviews = int(p_stats['review_count'].sum())
                if 'avg_rating' in p_stats.columns:
                    rating = float(p_stats['avg_rating'].mean())

            b_stats = self.get_brand_daily_stats()
            if not b_stats.empty and 'brand' in b_stats.columns:
                total_brands = b_stats['brand'].nunique()

            dq_stats = self.get_data_quality_metrics()
            if not dq_stats.empty and 'dq_score' in dq_stats.columns:
                avg_dq = float(dq_stats['dq_score'].mean())

        return {
            "total_products": total_products,
            "total_reviews": total_reviews,
            "total_brands": total_brands,
            "avg_dq_score": round(avg_dq, 1),
            "avg_rating": round(rating, 2)
        }
