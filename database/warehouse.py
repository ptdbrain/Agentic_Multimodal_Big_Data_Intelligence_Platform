import logging
from typing import Optional, List, Dict, Any
import pandas as pd
from config.settings import settings

logger = logging.getLogger(__name__)

class WarehouseManager:
    """Manages Gold data export to PostgreSQL with proper UPSERT logic.
    Falls back to SQLite for tests when PostgreSQL is unavailable.
    """
    
    def __init__(self, use_postgres: Optional[bool] = None):
        """Try PostgreSQL first, fall back to SQLite."""
        self._conn = None
        self._engine_type = None  # 'postgres' or 'sqlite'
        
        if use_postgres is None:
            # Auto-detect: try postgres, fall back to sqlite
            try:
                self._connect_postgres()
            except Exception:
                self._connect_sqlite()
        elif use_postgres:
            self._connect_postgres()
        else:
            self._connect_sqlite()
    
    def _connect_postgres(self):
        """Connect to real PostgreSQL."""
        import psycopg2
        self._conn = psycopg2.connect(
            host=settings.database.host,
            port=settings.database.port,
            user=settings.database.user,
            password=settings.database.password,
            database=settings.database.database
        )
        self._engine_type = 'postgres'
        logger.info(f"Connected to PostgreSQL at {settings.database.host}:{settings.database.port}")
    
    def _connect_sqlite(self):
        """Connect to SQLite fallback."""
        import sqlite3
        db_path = settings.database.sqlite_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._engine_type = 'sqlite'
        logger.info(f"Connected to SQLite at {db_path}")
    
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
    
    def upsert_dataframe(self, table_name: str, df: pd.DataFrame, 
                         conflict_keys: List[str], update_columns: Optional[List[str]] = None):
        """UPSERT (INSERT ON CONFLICT UPDATE) for PostgreSQL.
        For SQLite, uses DELETE + INSERT as fallback.
        
        Plan says: NO more if_exists='replace'. Must do proper UPSERT.
        """
        if df.empty:
            return 0
        
        if "date" in df.columns:
            df = df.copy()
            df["date"] = df["date"].astype(str)
        
        rows_affected = 0
        
        if self._engine_type == 'postgres':
            # Real PostgreSQL UPSERT using INSERT ... ON CONFLICT ... DO UPDATE
            columns = list(df.columns)
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
            
            cursor = self._conn.cursor()
            for _, row in df.iterrows():
                cursor.execute(sql, tuple(row[c] for c in columns))
                rows_affected += 1
            self._conn.commit()
            
        else:
            # SQLite: DELETE matching rows then INSERT
            cursor = self._conn.cursor()
            for _, row in df.iterrows():
                where_clause = " AND ".join([f"{k} = ?" for k in conflict_keys])
                cursor.execute(f"DELETE FROM {table_name} WHERE {where_clause}",
                             tuple(row[k] for k in conflict_keys))
                
                columns = list(df.columns)
                placeholders = ", ".join(["?"] * len(columns))
                col_names = ", ".join(columns)
                cursor.execute(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
                             tuple(row[c] for c in columns))
                rows_affected += 1
            self._conn.commit()
        
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
