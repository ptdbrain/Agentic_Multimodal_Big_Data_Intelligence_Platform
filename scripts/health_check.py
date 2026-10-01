import sys
import sqlite3
from pathlib import Path
from typing import Dict, Any, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings

def check_system_health(timeout_ms: int = 1500) -> Dict[str, Any]:
    """Checks health status of all infrastructure and platform components dynamically."""
    status = {}
    
    # 1. Database Check
    try:
        conn = sqlite3.connect(settings.database.sqlite_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        status["database"] = {"status": "HEALTHY", "tables_count": len(tables), "path": str(settings.database.sqlite_path)}
    except Exception as e:
        status["database"] = {"status": "UNHEALTHY", "error": str(e)}

    # 2. Data Lake Storage Check
    lake_dir = settings.storage.local_data_dir
    silver_dir = lake_dir / "silver"
    gold_dir = lake_dir / "gold"
    status["storage"] = {
        "status": "HEALTHY" if lake_dir.exists() else "UNHEALTHY",
        "path": str(lake_dir),
        "silver_ready": silver_dir.exists(),
        "gold_ready": gold_dir.exists()
    }

    # 3. Kafka Broker Connectivity
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers, request_timeout_ms=timeout_ms)
        topics = admin.list_topics()
        admin.close()
        status["kafka"] = {"status": "HEALTHY", "mode": "cluster", "topics_count": len(topics)}
    except Exception:
        status["kafka"] = {"status": "STANDALONE_FALLBACK", "mode": "virtual_queue"}

    print("================ SENTINEL HEALTH REPORT ================")
    for comp, res in status.items():
        print(f"[{comp.upper()}]: {res['status']} | {res}")
    print("========================================================")
    return status

if __name__ == "__main__":
    check_system_health()

