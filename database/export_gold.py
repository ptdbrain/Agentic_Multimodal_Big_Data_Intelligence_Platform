import sqlite3
import pandas as pd
from pathlib import Path
from config.settings import settings
from storage.storage_manager import storage

def export_gold_to_db():
    """Syncs Gold Parquet tables to PostgreSQL / SQLite DW."""
    db_file = settings.database.sqlite_path
    conn = sqlite3.connect(db_file)

    for mart in ["product_daily_stats", "brand_daily_stats", "category_daily_stats"]:
        df = storage.read_gold_parquet(mart)
        if not df.empty:
            df["date"] = df["date"].astype(str)
            df.to_sql(mart, conn, if_exists="replace", index=False)
            print(f"Exported {len(df)} rows to database table '{mart}'")

    # Export products catalog
    df_prods = storage.read_silver_parquet("products")
    if not df_prods.empty:
        df_prods.to_sql("products", conn, if_exists="replace", index=False)
        print(f"Exported {len(df_prods)} rows to table 'products'")

    conn.close()
    print("Database sync completed.")

if __name__ == "__main__":
    export_gold_to_db()
