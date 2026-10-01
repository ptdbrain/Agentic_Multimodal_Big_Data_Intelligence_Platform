import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List
from config.settings import settings
from storage.storage_manager import storage

def export_gold_to_db(
    marts: Optional[List[str]] = None,
    silver_tables: Optional[List[str]] = None,
    db_file: Optional[Path] = None
):
    """Syncs Gold Parquet marts and Silver tables to PostgreSQL / SQLite DW.
    Dynamically discovers all available gold marts from storage if not explicitly provided.
    """
    target_db = db_file or settings.database.sqlite_path
    target_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target_db)

    # 1. Discover or use provided Gold Marts
    target_marts = marts if marts is not None else storage.list_gold_marts()
    if not target_marts:
        # Fallback to standard marts if storage directory empty
        target_marts = ["product_daily_stats", "brand_daily_stats", "category_daily_stats"]

    for mart in target_marts:
        df = storage.read_gold_parquet(mart)
        if not df.empty:
            if "date" in df.columns:
                df["date"] = df["date"].astype(str)
            df.to_sql(mart, conn, if_exists="replace", index=False)
            print(f"Exported {len(df)} rows to database table '{mart}'")

    # 2. Export Silver Reference Tables
    tables_to_sync = silver_tables if silver_tables is not None else ["products"]
    for tbl in tables_to_sync:
        df_tbl = storage.read_silver_parquet(tbl)
        if not df_tbl.empty:
            df_tbl.to_sql(tbl, conn, if_exists="replace", index=False)
            print(f"Exported {len(df_tbl)} rows to table '{tbl}'")

    conn.close()
    print("Database sync completed.")

if __name__ == "__main__":
    export_gold_to_db()

