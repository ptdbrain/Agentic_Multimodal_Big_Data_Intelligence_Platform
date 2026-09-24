import os
import sys
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings

def initialize_database():
    schema_path = REPO_ROOT / "database" / "schema.sql"
    with open(schema_path, "r", encoding="utf-8") as f:
        ddl = f.read()

    db_file = settings.database.sqlite_path
    db_file.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Connecting to database: {db_file}")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.executescript(ddl)
    conn.commit()
    conn.close()
    print("Database tables initialized successfully.")

if __name__ == "__main__":
    initialize_database()
