import datetime
import json
import logging
import sqlite3
from decimal import Decimal
from typing import Optional, List
import pandas as pd
from config.settings import settings

sqlite3.register_adapter(Decimal, float)
sqlite3.register_adapter(pd.Timestamp, lambda ts: str(ts))
logger = logging.getLogger(__name__)


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
            with self._conn:
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
                        val_tuple = tuple(None if pd.isna(row[c]) else row[c] for c in columns)
                        cursor.execute(sql, val_tuple)
                        rows_affected += 1
                    
                else:
                    placeholders = ", ".join(["?"] * len(columns))
                    col_names = ", ".join(columns)
                    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

                    def _to_sqlite_val(v):
                        if v is None:
                            return None
                        if isinstance(v, (dict, list)):
                            return json.dumps(v, ensure_ascii=False)
                        if pd.isna(v):
                            return None
                        if isinstance(v, Decimal):
                            return float(v)
                        if isinstance(v, (pd.Timestamp, datetime.datetime, datetime.date)):
                            return str(v)
                        return v

                    if conflict_keys:
                        null_clause = " OR ".join([f"{k} IS NULL" for k in conflict_keys])
                        cursor.execute(f"DELETE FROM {table_name} WHERE {null_clause}")

                    for _, row in df.iterrows():
                        if conflict_keys:
                            where_clause = " AND ".join([f"{k} = ?" for k in conflict_keys])
                            del_vals = tuple(_to_sqlite_val(row[k]) for k in conflict_keys)
                            cursor.execute(f"DELETE FROM {table_name} WHERE {where_clause}", del_vals)
                        
                        val_tuple = tuple(_to_sqlite_val(row[c]) for c in columns)
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
