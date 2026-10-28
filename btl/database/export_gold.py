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
        # Read and validate the entire group before opening the warehouse write
        # transaction. A failed/partial Gold generation cannot advance serving.
        frames = {}
        run_ids = set()
        unknown_run_marts = []
        from storage.write_lock import write_lock
        from spark.spark_session import SparkSessionFactory
        with write_lock(SparkSessionFactory.resolve_storage_path('', layer='gold')):
            try:
                generation = storage.read_json('gold/_snapshot.json')
            except FileNotFoundError:
                generation = None
            if generation is not None and generation.get('status') != 'complete':
                raise ValueError('Gold run publication is incomplete; serving snapshot retained')
            for mart in target_marts:
                try:
                    frame = storage.read_gold_parquet(mart)
                except Exception as exc:
                    raise RuntimeError(f"Failed to read Gold mart '{mart}' from storage: {exc}") from exc
                frames[mart] = frame
                if mart in canonical_marts[:4] and not set(conflict_keys_map[mart]).issubset(frame.columns):
                    raise ValueError(f"Gold snapshot '{mart}' is missing or has no required grain schema")
                if mart in canonical_marts[:4] and not frame.empty:
                    known = set(frame['run_id'].dropna().astype(str)) if 'run_id' in frame else set()
                    if len(known) > 1 or (known and frame['run_id'].isna().any()):
                        raise ValueError(f"Gold mart '{mart}' contains mixed run IDs")
                    run_ids.update(known)
                    if not known:
                        unknown_run_marts.append(mart)
            if len(run_ids) > 1 or (run_ids and unknown_run_marts):
                raise ValueError('Gold marts belong to different run IDs; retry after complete publication')
            if generation:
                for name, frame in frames.items():
                    if name in canonical_marts[:4]:
                        if name not in generation['rows'] or len(frame) != generation['rows'][name]:
                            raise ValueError('Gold run manifest row counts do not match published snapshots')
                        if not frame.empty and (set(frame['run_id']) != {generation['run_id']}):
                            raise ValueError('Gold run manifest does not match published lineage')
        tables_to_sync = silver_tables if silver_tables is not None else ['products', 'reviews']
        for table in tables_to_sync:
            try:
                frames[table] = storage.read_silver_parquet(table)
            except Exception as exc:
                raise RuntimeError(f"Failed to read Silver table '{table}' from storage: {exc}") from exc
        with wh.transaction():
            for name, frame in frames.items():
                if name in canonical_marts[:4]:
                    # Daily marts are complete snapshots; incidents/DQ remain history.
                    wh._conn.cursor().execute(f'DELETE FROM {name}')
                if frame.empty:
                    if name in canonical_marts[:4]:
                        synced_tables[name] = 0
                    continue
                try:
                    rows = wh.upsert_dataframe(name, frame, conflict_keys=conflict_keys_map.get(name, ['id']))
                    synced_tables[name] = rows
                    print(f"Prepared {rows} rows for database table '{name}' ({wh._engine_type})")
                except Exception as exc:
                    raise RuntimeError(f"Export to warehouse failed for table '{name}': {exc}") from exc
    finally:
        wh.close()

    print(f"Database sync completed ({wh._engine_type}): synced {len(synced_tables)} tables.")
    return synced_tables

if __name__ == "__main__":
    export_gold_to_db()
