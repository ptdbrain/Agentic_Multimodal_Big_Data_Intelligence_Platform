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

    def __init__(self, mode: Optional[str] = None, use_postgres: Optional[bool] = None, data_scope: str = "all"):
        if data_scope not in ("real", "simulation", "unverified", "all"):
            raise ValueError("Unknown dashboard data scope")
        self.data_scope = data_scope
        self._scope_columns = {}
        self._conn = None
        self._source: Optional[str] = None  # 'postgres' or 'sqlite'
        self._mode = mode
        self._use_postgres = use_postgres
        self._connect()

    def _connect(self):
        target_engine = settings.database.resolve_db_engine(explicit_mode=self._mode, use_postgres=self._use_postgres)

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
                    connect_timeout=10,
                )
                self._conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
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
            self._conn.execute("BEGIN")
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

    _SERVING_TABLES = {
        "products",
        "reviews",
        "articles",
        "article_daily_stats",
        "article_pipeline_runs",
        "product_daily_stats",
        "brand_daily_stats",
        "category_daily_stats",
        "price_daily_stats",
        "anomaly_events",
        "data_quality_metrics",
        "pipeline_metrics",
    }

    def _columns(self, table):
        if table not in self._SERVING_TABLES:
            raise ValueError("Unknown serving table")
        if table not in self._scope_columns:
            cur = self._conn.cursor()
            try:
                if self.is_integration:
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=%s",
                        (table,),
                    )
                    names = {row[0] for row in cur.fetchall()}
                else:
                    cur.execute(f"PRAGMA table_info({table})")
                    names = {row[1] for row in cur.fetchall()}
                self._scope_columns[table] = names
            finally:
                cur.close()
        return self._scope_columns[table]

    def _table_source(self, table):
        columns = self._columns(table)
        if self.data_scope == "all" or not columns:
            return table
        if "source_group" not in columns:
            condition = "1=1" if self.data_scope == "unverified" else "1=0"
        else:
            observed = "TRIM(source_group) IN ('marketplace','web_content')"
            simulated = "TRIM(source_group) = 'simulation'"
            if "acquisition_method" in columns:
                observed += " AND TRIM(acquisition_method) IN ('live','snapshot_replay')"
                simulated += " AND TRIM(acquisition_method) = 'generated'"
            elif table in ("products", "reviews", "articles"):
                observed = simulated = "FALSE"
            if self.data_scope == "real":
                condition = observed
            elif self.data_scope == "simulation":
                condition = simulated
            else:
                condition = f"NOT (COALESCE(({observed}), FALSE) OR COALESCE(({simulated}), FALSE))"
        return f"(SELECT * FROM {table} WHERE {condition})"

    def read_table(self, table, order_by=None, descending=True, limit=None):
        columns = self._columns(table)
        if not columns:
            return self.query(f"SELECT * FROM {table} LIMIT 0")
        statement = f"SELECT * FROM {self._table_source(table)} AS scoped_data"
        if order_by:
            if order_by not in columns:
                raise ValueError("Unknown ordering column")
            statement += f" ORDER BY {order_by} " + ("DESC" if descending else "ASC")
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
                raise ValueError("Positive row limit required")
            statement += f" LIMIT {limit}"
        return self.query(statement)

    def get_product_daily_stats(self) -> pd.DataFrame:
        return self.read_table("product_daily_stats", order_by="date")

    def get_brand_daily_stats(self) -> pd.DataFrame:
        return self.read_table("brand_daily_stats", order_by="date")

    def get_category_daily_stats(self) -> pd.DataFrame:
        return self.read_table("category_daily_stats", order_by="date")

    def get_price_daily_stats(self) -> pd.DataFrame:
        return self.read_table("price_daily_stats", order_by="date")

    def get_anomaly_events(self) -> pd.DataFrame:
        return self.read_table("anomaly_events", order_by="timestamp", limit=100)

    def get_data_quality_metrics(self) -> pd.DataFrame:
        return self.query(
            f"SELECT * FROM {self._table_source('data_quality_metrics')} q WHERE NOT EXISTS (SELECT 1 FROM {self._table_source('data_quality_metrics')} newer WHERE newer.dataset_name = q.dataset_name AND newer.timestamp > q.timestamp) ORDER BY timestamp DESC"
        )

    def get_pipeline_metrics(self) -> pd.DataFrame:
        return self.read_table("pipeline_metrics", order_by="timestamp", limit=100)

    def get_kpi_summary(self) -> dict:
        """Get live KPI values strictly from the configured Warehouse database."""
        total_products = 0
        total_reviews = 0
        total_brands = 0
        avg_dq = None
        rating = None
        sample_size = 0

        products = self.query(f"SELECT COUNT(*) as cnt FROM {self._table_source('products')} AS scoped_products")
        if not products.empty and products["cnt"].iloc[0] is not None:
            total_products = int(products["cnt"].iloc[0])

        reviews = self.query(f"""
            SELECT SUM(review_count) AS total_rc,
                   SUM(CASE WHEN avg_rating BETWEEN 1 AND 5 AND COALESCE(rating_sample_size,review_count) > 0
                            THEN COALESCE(rating_sample_size,review_count) ELSE 0 END) AS sample_size,
                   SUM(CASE WHEN avg_rating BETWEEN 1 AND 5 AND COALESCE(rating_sample_size,review_count) > 0
                            THEN avg_rating * COALESCE(rating_sample_size,review_count) ELSE 0 END) AS rating_sum
            FROM {self._table_source("product_daily_stats")} AS scoped_stats
        """)
        if not reviews.empty and reviews["total_rc"].iloc[0] is not None:
            total_reviews = int(reviews["total_rc"].iloc[0])
            sample_size = int(reviews["sample_size"].iloc[0] or 0)
            if sample_size:
                rating = float(reviews["rating_sum"].iloc[0]) / sample_size

        brands = self.query(
            f"SELECT COUNT(DISTINCT brand) as cnt FROM {self._table_source('brand_daily_stats')} AS scoped_brands"
        )
        if not brands.empty and brands["cnt"].iloc[0] is not None:
            total_brands = int(brands["cnt"].iloc[0])

        dq = self.query(
            f"SELECT AVG(dq_score) as avg_dq FROM {self._table_source('data_quality_metrics')} AS scoped_quality"
        )
        if not dq.empty and dq["avg_dq"].iloc[0] is not None:
            avg_dq = float(dq["avg_dq"].iloc[0])

        return {
            "total_products": total_products,
            "total_reviews": total_reviews,
            "total_brands": total_brands,
            "avg_dq_score": round(avg_dq, 1) if avg_dq is not None else None,
            "avg_rating": round(rating, 2) if rating is not None else None,
            "rating_sample_size": sample_size,
        }

    def close(self):
        if self._conn:
            self._conn.close()
