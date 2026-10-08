from pathlib import Path
from typing import Optional, List
from config.settings import settings
from storage.storage_manager import storage
from database.warehouse import WarehouseManager

def export_gold_to_db(
    marts: Optional[List[str]] = None,
    silver_tables: Optional[List[str]] = None,
    db_file: Optional[Path] = None,
    use_postgres: Optional[bool] = None,
    mode: Optional[str] = None
):
    """Syncs Gold Parquet marts and Silver tables to PostgreSQL (or SQLite ONLY in explicit local/test mode).
    Fails loudly if the target database or storage backend is unreachable.
    """
    if db_file:
        settings.database.sqlite_path = db_file
        if use_postgres is None and mode is None:
            use_postgres = False
        
    wh = WarehouseManager(use_postgres=use_postgres, mode=mode)
    # Strictly verify warehouse health before performing export
    wh.health_check()
    wh.initialize_schema()
    
    conflict_keys_map = {
        "product_daily_stats": ["date", "product_id"],
        "brand_daily_stats": ["date", "brand"],
        "category_daily_stats": ["date", "category"],
        "price_daily_stats": ["date", "product_id"],
        "anomaly_events": ["event_id"],
        "data_quality_metrics": ["batch_id"],
        "products": ["product_id"],
        "reviews": ["review_id"]
    }

    canonical_marts = [
        "product_daily_stats", 
        "brand_daily_stats", 
        "category_daily_stats", 
        "price_daily_stats",
        "anomaly_events",
        "data_quality_metrics"
    ]
    if marts is not None:
        target_marts = marts
    else:
        available = set(storage.list_gold_marts())
        target_marts = [m for m in canonical_marts if m in available] or canonical_marts

    synced_tables = {}
    try:
        for mart in target_marts:
            try:
                df = storage.read_gold_parquet(mart)
            except Exception as e:
                raise RuntimeError(f"Failed to read Gold mart '{mart}' from storage: {e}") from e

            if not df.empty:
                keys = conflict_keys_map.get(mart, ["id"])
                try:
                    rows = wh.upsert_dataframe(mart, df, conflict_keys=keys)
                    synced_tables[mart] = rows
                    print(f"Exported {rows} rows to database table '{mart}' ({wh._engine_type})")
                except Exception as e:
                    raise RuntimeError(f"Export to warehouse failed for table '{mart}': {e}") from e

        tables_to_sync = silver_tables if silver_tables is not None else ["products", "reviews"]
        for tbl in tables_to_sync:
            try:
                df_tbl = storage.read_silver_parquet(tbl)
            except Exception as e:
                raise RuntimeError(f"Failed to read Silver table '{tbl}' from storage: {e}") from e

            if not df_tbl.empty:
                keys = conflict_keys_map.get(tbl, ["id"])
                try:
                    rows = wh.upsert_dataframe(tbl, df_tbl, conflict_keys=keys)
                    synced_tables[tbl] = rows
                    print(f"Exported {rows} rows to table '{tbl}' ({wh._engine_type})")
                except Exception as e:
                    raise RuntimeError(f"Export to warehouse failed for table '{tbl}': {e}") from e
    finally:
        wh.close()

    print(f"Database sync completed ({wh._engine_type}): synced {len(synced_tables)} tables.")
    return synced_tables

if __name__ == "__main__":
    export_gold_to_db()
