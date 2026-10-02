import pandas as pd
from pathlib import Path
from typing import Optional, List
from config.settings import settings
from storage.storage_manager import storage
from database.warehouse import WarehouseManager

def export_gold_to_db(
    marts: Optional[List[str]] = None,
    silver_tables: Optional[List[str]] = None,
    db_file: Optional[Path] = None,
    use_postgres: Optional[bool] = None
):
    """Syncs Gold Parquet marts and Silver tables to PostgreSQL / SQLite DW.
    Dynamically discovers all available gold marts from storage if not explicitly provided.
    """
    if db_file:
        settings.database.sqlite_path = db_file
        
    wh = WarehouseManager(use_postgres=use_postgres)
    wh.initialize_schema()
    
    # Define conflict keys for known tables
    conflict_keys_map = {
        "product_daily_stats": ["date", "product_id"],
        "brand_daily_stats": ["date", "brand"],
        "category_daily_stats": ["date", "category"],
        "price_daily_stats": ["date", "product_id"],
        "anomaly_events": ["event_id"],
        "data_quality_metrics": ["batch_id"],
        "products": ["product_id"]
    }

    # 1. Discover or use provided Gold Marts
    target_marts = marts if marts is not None else storage.list_gold_marts()
    if not target_marts:
        target_marts = [
            "product_daily_stats", 
            "brand_daily_stats", 
            "category_daily_stats", 
            "price_daily_stats",
            "anomaly_events",
            "data_quality_metrics"
        ]

    for mart in target_marts:
        try:
            df = storage.read_gold_parquet(mart)
            if not df.empty:
                keys = conflict_keys_map.get(mart, ["id"])
                wh.upsert_dataframe(mart, df, conflict_keys=keys)
                print(f"Exported {len(df)} rows to database table '{mart}'")
        except Exception as e:
            print(f"Warning: Failed exporting mart '{mart}': {e}")

    # 2. Export Silver Reference Tables
    tables_to_sync = silver_tables if silver_tables is not None else ["products"]
    for tbl in tables_to_sync:
        df_tbl = storage.read_silver_parquet(tbl)
        if not df_tbl.empty:
            keys = conflict_keys_map.get(tbl, ["product_id"])
            wh.upsert_dataframe(tbl, df_tbl, conflict_keys=keys)
            print(f"Exported {len(df_tbl)} rows to table '{tbl}'")

    wh.close()
    print("Database sync completed.")

if __name__ == "__main__":
    export_gold_to_db()
