import sys
import sqlite3
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
from config.settings import settings

def check_system_health():
    """Checks health status of all infrastructure and platform components."""
    status = {}
    
    # 1. Database Check
    try:
        conn = sqlite3.connect(settings.database.sqlite_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        status["database"] = {"status": "HEALTHY", "tables_count": len(tables)}
    except Exception as e:
        status["database"] = {"status": "UNHEALTHY", "error": str(e)}

    # 2. Data Lake Storage Check
    lake_dir = settings.storage.local_data_dir
    status["storage"] = {
        "status": "HEALTHY" if lake_dir.exists() else "UNHEALTHY",
        "path": str(lake_dir)
    }

    # 3. Kafka Broker Connectivity
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers, request_timeout_ms=1000)
        admin.list_topics()
        admin.close()
        status["kafka"] = {"status": "HEALTHY", "mode": "cluster"}
    except Exception:
        status["kafka"] = {"status": "STANDALONE_FALLBACK", "mode": "virtual_queue"}

    print("================ SENTINEL HEALTH REPORT ================")
    for comp, res in status.items():
        print(f"[{comp.upper()}]: {res['status']} | {res}")
    print("========================================================")
    return status

if __name__ == "__main__":
    check_system_health()
