import logging
from typing import Optional
import pandas as pd
from config.settings import settings

logger = logging.getLogger(__name__)

class DashboardDB:
    """Dashboard data source connecting to PostgreSQL warehouse.
    Falls back to SQLite or Parquet when PostgreSQL is unavailable.
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
            return self.query("SELECT * FROM product_daily_stats ORDER BY date DESC")
        from storage.storage_manager import storage
        return storage.read_gold_parquet("product_daily_stats")
    
    def get_brand_daily_stats(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            return self.query("SELECT * FROM brand_daily_stats ORDER BY date DESC")
        from storage.storage_manager import storage
        return storage.read_gold_parquet("brand_daily_stats")
    
    def get_anomaly_events(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            return self.query("SELECT * FROM anomaly_events ORDER BY timestamp DESC LIMIT 100")
        return pd.DataFrame()
    
    def get_data_quality_metrics(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            return self.query("SELECT * FROM data_quality_metrics ORDER BY timestamp DESC LIMIT 50")
        return pd.DataFrame()
    
    def get_pipeline_metrics(self) -> pd.DataFrame:
        if self._source in ('postgres', 'sqlite'):
            return self.query("SELECT * FROM pipeline_metrics ORDER BY timestamp DESC LIMIT 100")
        return pd.DataFrame()
    
    def get_kpi_summary(self) -> dict:
        """Get real KPI values from database, not hardcoded."""
        if self._source in ('postgres', 'sqlite'):
            try:
                products = self.query("SELECT COUNT(*) as cnt FROM products")
                total_products = int(products['cnt'].iloc[0]) if not products.empty else 0
                
                reviews = self.query("SELECT COUNT(*) as cnt, AVG(review_count) as avg_rc FROM product_daily_stats")
                total_reviews = int(reviews['cnt'].iloc[0]) if not reviews.empty else 0
                
                brands = self.query("SELECT COUNT(DISTINCT brand) as cnt FROM brand_daily_stats")
                total_brands = int(brands['cnt'].iloc[0]) if not brands.empty else 0
                
                dq = self.query("SELECT AVG(dq_score) as avg_dq FROM data_quality_metrics")
                avg_dq = float(dq['avg_dq'].iloc[0]) if not dq.empty and dq['avg_dq'].iloc[0] is not None else 0.0
                
                avg_rating = self.query("SELECT AVG(avg_rating) as ar FROM product_daily_stats")
                rating = float(avg_rating['ar'].iloc[0]) if not avg_rating.empty and avg_rating['ar'].iloc[0] is not None else 0.0
                
                return {
                    "total_products": total_products,
                    "total_reviews": total_reviews,
                    "total_brands": total_brands,
                    "avg_dq_score": round(avg_dq, 1),
                    "avg_rating": round(rating, 2)
                }
            except Exception:
                pass
        return {"total_products": 0, "total_reviews": 0, "total_brands": 0, "avg_dq_score": 0.0, "avg_rating": 0.0}
