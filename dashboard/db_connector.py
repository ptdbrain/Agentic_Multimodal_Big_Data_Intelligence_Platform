import logging
from typing import Optional
import pandas as pd
from config.settings import settings

logger = logging.getLogger(__name__)

class DashboardDB:
    """Dashboard data source with strict mode separation:
      - Integration/Production mode (default): Connects strictly to PostgreSQL warehouse.
        Raises ConnectionError if PostgreSQL is unavailable. Never falls back to SQLite or local Parquet.
      - Local/Test mode (explicitly declared via mode='local'/'test' or use_postgres=False):
        Connects to SQLite warehouse.
    """
    
    def __init__(self, mode: Optional[str] = None, use_postgres: Optional[bool] = None):
        self._conn = None
        self._source: Optional[str] = None  # 'postgres' or 'sqlite'
        self._mode = mode
        self._use_postgres = use_postgres
        self._connect()
    
    def _connect(self):
        target_engine = settings.database.resolve_db_engine(
            explicit_mode=self._mode,
            use_postgres=self._use_postgres
        )

        if target_engine == "postgres":
            try:
                import psycopg2
            except ModuleNotFoundError as e:
                raise ConnectionError(
                    "PostgreSQL driver 'psycopg2' is not installed; DashboardDB cannot connect in PostgreSQL mode."
                ) from e

            try:
                self._conn = psycopg2.connect(
                    host=settings.database.host,
                    port=settings.database.port,
                    user=settings.database.user,
                    password=settings.database.password,
                    database=settings.database.database,
                    connect_timeout=10
                )
                self._source = "postgres"
                return
            except Exception as e:
                raise ConnectionError(
                    f"DashboardDB failed to connect to PostgreSQL at "
                    f"{settings.database.host}:{settings.database.port}/{settings.database.database}: {e}"
                ) from e

        # Explicit local / unit-test SQLite mode ONLY
        try:
            import sqlite3
            db_path = settings.database.sqlite_path
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(db_path))
            self._source = "sqlite"
        except Exception as e:
            raise ConnectionError(
                f"DashboardDB failed to connect to SQLite at {settings.database.sqlite_path}: {e}"
            ) from e

    @property
    def is_integration(self) -> bool:
        return self._source == "postgres"

    @property
    def source_name(self) -> str:
        return self._source or "unknown"

    def get_active_source_label(self) -> str:
        """Returns explicit readable label for the active database engine."""
        if self._source == "postgres":
            return f"PostgreSQL Warehouse ({settings.database.user}@{settings.database.host}:{settings.database.port}/{settings.database.database})"
        elif self._source == "sqlite":
            return f"SQLite Warehouse (Local/Test: {settings.database.sqlite_path})"
        return "Unknown Warehouse Source"

    def health_check(self) -> bool:
        """Verifies live database connection."""
        if not self._conn:
            raise ConnectionError("DashboardDB connection is not initialized.")
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            return True
        except Exception as e:
            raise ConnectionError(f"DashboardDB health check failed ({self._source}): {e}") from e
    
    def query(self, sql: str) -> pd.DataFrame:
        """Executes query strictly against configured warehouse.
        In integration mode, only PostgreSQL is accepted; never falls back to SQLite or local files.
        """
        if not self._conn:
            raise ConnectionError("DashboardDB has no active database connection.")
        try:
            return pd.read_sql_query(sql, self._conn)
        except Exception as e:
            err_msg = str(e).lower()
            if self.is_integration:
                if any(k in err_msg for k in ("connection", "closed", "server", "operationalerror", "terminated")):
                    raise ConnectionError(f"PostgreSQL connection error: {e}") from e
                if "does not exist" in err_msg or "undefinedtable" in err_msg:
                    logger.warning(f"PostgreSQL table not initialized: {e}")
                    return pd.DataFrame()
                raise RuntimeError(f"PostgreSQL query failed: {e}") from e
            else:
                if any(k in err_msg for k in ("connection", "closed", "server", "operationalerror", "terminated")):
                    if "no such table" not in err_msg:
                        raise ConnectionError(f"SQLite connection error: {e}") from e
                return pd.DataFrame()
    
    def get_product_daily_stats(self) -> pd.DataFrame:
        return self.query("SELECT * FROM product_daily_stats ORDER BY date DESC")
    
    def get_brand_daily_stats(self) -> pd.DataFrame:
        return self.query("SELECT * FROM brand_daily_stats ORDER BY date DESC")

    def get_category_daily_stats(self) -> pd.DataFrame:
        return self.query("SELECT * FROM category_daily_stats ORDER BY date DESC")

    def get_price_daily_stats(self) -> pd.DataFrame:
        return self.query("SELECT * FROM price_daily_stats ORDER BY date DESC")
    
    def get_anomaly_events(self) -> pd.DataFrame:
        return self.query("SELECT * FROM anomaly_events ORDER BY timestamp DESC LIMIT 100")
    
    def get_data_quality_metrics(self) -> pd.DataFrame:
        return self.query("SELECT * FROM data_quality_metrics ORDER BY timestamp DESC LIMIT 50")
    
    def get_pipeline_metrics(self) -> pd.DataFrame:
        return self.query("SELECT * FROM pipeline_metrics ORDER BY timestamp DESC LIMIT 100")
    
    def get_kpi_summary(self) -> dict:
        """Get live KPI values strictly from the configured Warehouse database."""
        total_products = 0
        total_reviews = 0
        total_brands = 0
        avg_dq = 0.0
        rating = 0.0

        products = self.query("SELECT COUNT(*) as cnt FROM products")
        if not products.empty and products['cnt'].iloc[0] is not None:
            total_products = int(products['cnt'].iloc[0])
        
        reviews = self.query("SELECT SUM(review_count) as total_rc, AVG(avg_rating) as ar FROM product_daily_stats")
        if not reviews.empty and reviews['total_rc'].iloc[0] is not None:
            total_reviews = int(reviews['total_rc'].iloc[0])
            rating = float(reviews['ar'].iloc[0]) if reviews['ar'].iloc[0] is not None else 0.0
        
        brands = self.query("SELECT COUNT(DISTINCT brand) as cnt FROM brand_daily_stats")
        if not brands.empty and brands['cnt'].iloc[0] is not None:
            total_brands = int(brands['cnt'].iloc[0])
        
        dq = self.query("SELECT AVG(dq_score) as avg_dq FROM data_quality_metrics")
        if not dq.empty and dq['avg_dq'].iloc[0] is not None:
            avg_dq = float(dq['avg_dq'].iloc[0])

        return {
            "total_products": total_products,
            "total_reviews": total_reviews,
            "total_brands": total_brands,
            "avg_dq_score": round(avg_dq, 1),
            "avg_rating": round(rating, 2)
        }

    def close(self):
        if self._conn:
            self._conn.close()
