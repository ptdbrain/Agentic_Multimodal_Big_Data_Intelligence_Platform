import datetime
import json
import logging
import sqlite3
from contextlib import contextmanager, nullcontext
from decimal import Decimal
from typing import Optional, List
import pandas as pd
import numpy as np
from config.settings import settings

sqlite3.register_adapter(Decimal, float)
sqlite3.register_adapter(pd.Timestamp, lambda ts: str(ts))
logger = logging.getLogger(__name__)


def _database_value(value, sqlite: bool):
    """Serialize nullable/nested Parquet scalars consistently for both drivers."""
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or pd.isna(value):
        return None
    if sqlite and isinstance(value, Decimal):
        return float(value)
    if sqlite and isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date)):
        return str(value)
    return value


class WarehouseManager:
    """Manages Gold data export to Data Warehouse with strict mode separation:
      - Integration/Production mode (default): Real PostgreSQL only (NO silent fallback to SQLite)
      - Local/Test mode (explicitly declared via use_postgres=False or mode='local'/'test'): SQLite
    """
    
    def __init__(self, use_postgres: Optional[bool] = None, mode: Optional[str] = None):
        self._conn = None
        self._engine_type = None  # 'postgres' or 'sqlite'
        
        target_engine = settings.database.resolve_db_engine(
            explicit_mode=mode,
            use_postgres=use_postgres
        )
        if target_engine == "postgres":
            self._connect_postgres()
        else:
            self._connect_sqlite()
    
    def _connect_postgres(self):
        """Connect strictly to real PostgreSQL. Raises ConnectionError if unavailable."""
        try:
            import psycopg2
        except ModuleNotFoundError as e:
            raise ConnectionError(
                "PostgreSQL driver 'psycopg2' is not installed; cannot run in PostgreSQL integration mode."
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
            self._engine_type = 'postgres'
            logger.info(f"Connected to PostgreSQL at {settings.database.host}:{settings.database.port}")
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL at {settings.database.host}:{settings.database.port}/"
                f"{settings.database.database}: {e}"
            ) from e
    
    def _connect_sqlite(self):
        """Connect to SQLite for explicit local or unit-test mode."""
        import sqlite3
        db_path = settings.database.sqlite_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        # Allow serving readers to retain a consistent snapshot while an export commits.
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._engine_type = 'sqlite'
        logger.info(f"Connected to SQLite (explicit local/test mode) at {db_path}")

    def health_check(self) -> bool:
        """Actively checks database connection health via SELECT 1."""
        if not self._conn:
            raise ConnectionError("Warehouse database connection is not initialized.")
        try:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception as e:
            raise ConnectionError(f"Warehouse health check failed ({self._engine_type}): {e}") from e

    @contextmanager
    def transaction(self):
        """Commit a complete export together; individual upserts join this scope."""
        if getattr(self, '_export_transaction', False):
            yield
            return
        self._export_transaction = True
        try:
            with self._conn:
                yield
        finally:
            self._export_transaction = False
    
    def initialize_schema(self):
        """Run schema.sql to create tables."""
        schema_path = settings.repo_root / "database" / "schema.sql"
        if not schema_path.exists():
            logger.warning(f"Schema file not found at {schema_path}")
            return
            
        with open(schema_path, "r", encoding="utf-8") as f:
            ddl = f.read()
        cursor = self._conn.cursor()
        if self._engine_type == 'sqlite':
            cursor.executescript(ddl)
        else:
            cursor.execute(ddl)
        self._conn.commit()
        self._migrate_dq_schema()
        self._migrate_anomaly_schema()
        self._migrate_analytics_schema()
        self._migrate_silver_schema()

    def _postgres_columns(self, cursor, table: str) -> dict:
        """Inspect the active schema without taking an ALTER TABLE lock."""
        cursor.execute('''SELECT column_name,is_nullable FROM information_schema.columns
                          WHERE table_schema=current_schema() AND table_name=%s''', (table,))
        return dict(cursor.fetchall())

    def _migrate_silver_schema(self):
        """Preserve normalization evidence in serving; legacy values stay null."""
        common = {"normalization_status": "VARCHAR(30)", "normalization_errors": "TEXT",
                  "time_available": "BOOLEAN", "ingestion_time_available": "BOOLEAN",
                  "ingested_at_raw": "TEXT"}
        extras = {
            "products": {"price_raw": "TEXT", "created_at_raw": "TEXT", "updated_at_raw": "TEXT",
                         "timestamp_raw": "TEXT", "ingested_at": "TIMESTAMP"},
            "reviews": {"rating_raw": "TEXT", "review_date_raw": "TEXT"},
        }
        with self._conn:
            cur = self._conn.cursor()
            for table, fields in extras.items():
                additions = dict(common, **fields)
                if self._engine_type == 'sqlite':
                    existing = {row[1] for row in cur.execute(f'PRAGMA table_info({table})').fetchall()}
                    for name, sql_type in additions.items():
                        if name not in existing:
                            cur.execute(f'ALTER TABLE {table} ADD COLUMN {name} {sql_type}')
                else:
                    existing = self._postgres_columns(cur, table)
                    for name, sql_type in additions.items():
                        if name not in existing:
                            cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {sql_type}')

    def _migrate_analytics_schema(self):
        """Keep Gold lineage available in serving; legacy values stay unknown."""
        common = {"run_id": "VARCHAR(100)", "source_run_id": "VARCHAR(100)",
                  "data_as_of": "DATE", "time_available": "BOOLEAN"}
        tables = ("product_daily_stats", "brand_daily_stats", "category_daily_stats", "price_daily_stats")
        with self._conn:
            cur = self._conn.cursor()
            for table in tables:
                additions = dict(common)
                if table != "price_daily_stats":
                    additions.update(rating_sample_size="INTEGER", weighted_avg_rating="NUMERIC(3,2)")
                if table == "product_daily_stats":
                    additions["price_source"] = "VARCHAR(30)"
                if self._engine_type == "sqlite":
                    existing = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
                    for name, sql_type in additions.items():
                        if name not in existing:
                            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type}")
                else:
                    existing = self._postgres_columns(cur, table)
                    for name, sql_type in additions.items():
                        if name not in existing:
                            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {sql_type}")

    def _migrate_anomaly_schema(self):
        """Add anomaly lineage fields without rewriting existing incidents."""
        additions = {
            "run_id": "VARCHAR(100)",
            "data_quality_status": "VARCHAR(30)",
            "baseline_window_hours": "INTEGER",
            "baseline_window_days": "INTEGER",
            "baseline_sample_size": "INTEGER",
            "baseline_sample_unit": "VARCHAR(30)",
            "baseline_method": "VARCHAR(30)",
            "baseline_start": "TIMESTAMP",
            "baseline_end": "TIMESTAMP",
            "baseline_observed_days": "INTEGER",
            "current_sample_size": "INTEGER",
        }
        with self._conn:
            cur = self._conn.cursor()
            if self._engine_type == "sqlite":
                existing = {r[1] for r in cur.execute("PRAGMA table_info(anomaly_events)").fetchall()}
                for name, sql_type in additions.items():
                    if name not in existing:
                        cur.execute(f"ALTER TABLE anomaly_events ADD COLUMN {name} {sql_type}")
            else:
                existing = self._postgres_columns(cur, 'anomaly_events')
                for name, sql_type in additions.items():
                    if name not in existing:
                        cur.execute(f"ALTER TABLE anomaly_events ADD COLUMN IF NOT EXISTS {name} {sql_type}")

    def _migrate_dq_schema(self):
        """Idempotent additive DQ migration, preserving legacy history and indexes."""
        additions = {"run_id": "VARCHAR(100)", "timing_status": "VARCHAR(30)",
                     "records_quarantined": "INTEGER", "records_missing_observation_time": "INTEGER",
                     "untimed_silver_count": "INTEGER", "normalization_error_counts": "TEXT",
                     "quarantine_reason_counts": "TEXT",
                     "intra_batch_duplicate_count": "INTEGER DEFAULT 0",
                     "cross_batch_duplicate_count": "INTEGER DEFAULT 0"}
        with self._conn:
            cur = self._conn.cursor()
            if self._engine_type == "sqlite":
                import re
                info = cur.execute("PRAGMA table_info(data_quality_metrics)").fetchall()
                if any(r[1] == "processing_time_ms" and r[3] for r in info):
                    original = cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='data_quality_metrics'").fetchone()[0]
                    indexes = cur.execute("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name='data_quality_metrics' AND sql IS NOT NULL").fetchall()
                    ddl = re.sub(r"CREATE TABLE(?: IF NOT EXISTS)? [\"`]?data_quality_metrics[\"`]?", "CREATE TABLE dq_migration_stage", original, flags=re.I)
                    ddl = re.sub(r"(processing_time_ms\s+\w+)\s+NOT NULL", r"\1", ddl, flags=re.I)
                    cur.execute(ddl)
                    columns = ",".join('"' + r[1].replace('"', '""') + '"' for r in info)
                    cur.execute(f"INSERT INTO dq_migration_stage ({columns}) SELECT {columns} FROM data_quality_metrics")
                    cur.execute("DROP TABLE data_quality_metrics")
                    cur.execute("ALTER TABLE dq_migration_stage RENAME TO data_quality_metrics")
                    for (index_sql,) in indexes:
                        cur.execute(index_sql)
                existing = {r[1] for r in info}
                for name, sql_type in additions.items():
                    if name not in existing:
                        cur.execute(f"ALTER TABLE data_quality_metrics ADD COLUMN {name} {sql_type}")
            else:
                existing = self._postgres_columns(cur, 'data_quality_metrics')
                for name, sql_type in additions.items():
                    if name not in existing:
                        cur.execute(f"ALTER TABLE data_quality_metrics ADD COLUMN IF NOT EXISTS {name} {sql_type}")
                if existing.get('processing_time_ms') == 'NO':
                    cur.execute("ALTER TABLE data_quality_metrics ALTER COLUMN processing_time_ms DROP NOT NULL")

    def upsert_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        conflict_keys: List[str],
        update_columns: Optional[List[str]] = None
    ):
        """UPSERT (INSERT ON CONFLICT UPDATE) for PostgreSQL.
        For explicit SQLite local/test mode, uses transactional DELETE + INSERT.
        """
        if df.empty:
            return 0
        
        if "date" in df.columns:
            df = df.copy()
            df["date"] = df["date"].astype(str)
        
        rows_affected = 0
        try:
            with (nullcontext() if getattr(self, '_export_transaction', False) else self._conn):
                cursor = self._conn.cursor()
                
                # Discover table schema dynamically
                try:
                    if self._engine_type == 'sqlite':
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        table_cols = {r[1] for r in cursor.fetchall()}
                    else:
                        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table_name,))
                        table_cols = {r[0] for r in cursor.fetchall()}
                except Exception:
                    table_cols = set()

                if not table_cols:
                    try:
                        if self._engine_type == 'sqlite':
                            df.head(0).to_sql(table_name, self._conn, if_exists='append', index=False)
                            cursor.execute(f"PRAGMA table_info({table_name})")
                            table_cols = {r[1] for r in cursor.fetchall()}
                        else:
                            col_defs = []
                            for col, dtype in df.dtypes.items():
                                pg_type = "NUMERIC" if "float" in str(dtype) or "int" in str(dtype) else "TEXT"
                                col_defs.append(f'"{col}" {pg_type}')
                            if col_defs:
                                cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)})')
                                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table_name,))
                                table_cols = {r[0] for r in cursor.fetchall()}
                    except Exception as e:
                        logger.warning(f"Could not auto-create warehouse table {table_name}: {e}")

                # Align column names with schema.sql aliases
                if "positive_ratio" in table_cols and "positive_ratio" not in df.columns and "high_rating_ratio" in df.columns:
                    df = df.copy()
                    df["positive_ratio"] = df["high_rating_ratio"]
                if "negative_ratio" in table_cols and "negative_ratio" not in df.columns and "low_rating_ratio" in df.columns:
                    df = df.copy()
                    df["negative_ratio"] = df["low_rating_ratio"]

                if table_cols:
                    columns = [c for c in df.columns if c in table_cols]
                else:
                    columns = list(df.columns)

                if not columns:
                    return 0

                conflict_keys = [k for k in conflict_keys if k in columns]
                
                if self._engine_type == 'postgres':
                    if conflict_keys:
                        update_cols = update_columns or [c for c in columns if c not in conflict_keys]
                        placeholders = ", ".join(["%s"] * len(columns))
                        col_names = ", ".join(columns)
                        conflict_clause = ", ".join(conflict_keys)
                        
                        if update_cols:
                            update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                            sql = f"""
                                INSERT INTO {table_name} ({col_names})
                                VALUES ({placeholders})
                                ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}
                            """
                        else:
                            sql = f"""
                                INSERT INTO {table_name} ({col_names})
                                VALUES ({placeholders})
                                ON CONFLICT ({conflict_clause}) DO NOTHING
                            """
                    else:
                        placeholders = ", ".join(["%s"] * len(columns))
                        col_names = ", ".join(columns)
                        sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
                    
                    for _, row in df.iterrows():
                        val_tuple = tuple(_database_value(row[c], sqlite=False) for c in columns)
                        cursor.execute(sql, val_tuple)
                        rows_affected += 1
                    
                else:
                    placeholders = ", ".join(["?"] * len(columns))
                    col_names = ", ".join(columns)
                    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

                    if conflict_keys:
                        null_clause = " OR ".join([f"{k} IS NULL" for k in conflict_keys])
                        cursor.execute(f"DELETE FROM {table_name} WHERE {null_clause}")

                    for _, row in df.iterrows():
                        if conflict_keys:
                            where_clause = " AND ".join([f"{k} = ?" for k in conflict_keys])
                            del_vals = tuple(_database_value(row[k], sqlite=True) for k in conflict_keys)
                            cursor.execute(f"DELETE FROM {table_name} WHERE {where_clause}", del_vals)
                        
                        val_tuple = tuple(_database_value(row[c], sqlite=True) for c in columns)
                        cursor.execute(insert_sql, val_tuple)
                        rows_affected += 1

        except Exception as e:
            try:
                self._conn.rollback()
            except Exception:
                pass
            logger.error(f"Transaction failed for warehouse table '{table_name}': {e}")
            raise RuntimeError(f"Upsert transaction failed for '{table_name}': {e}") from e
        
        logger.info(f"Upserted {rows_affected} rows into {table_name}")
        return rows_affected
    
    def query(self, sql: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        return pd.read_sql_query(sql, self._conn)
    
    def execute(self, sql: str):
        """Execute a statement."""
        cursor = self._conn.cursor()
        cursor.execute(sql)
        self._conn.commit()
    
    def close(self):
        if self._conn:
            self._conn.close()
