import sys
import argparse
from pathlib import Path
from typing import Dict

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings
from kafka import KafkaConsumer
from storage.minio_backend import MinIOBackend
from database.warehouse import WarehouseManager

def check_kafka(strict: bool = False) -> bool:
    print(f"Checking Kafka ({settings.kafka.bootstrap_servers})...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            request_timeout_ms=5000,
            session_timeout_ms=2000,
            bootstrap_timeout_ms=3000
        )
        topics = consumer.topics()
        consumer.close()
        print(f"  [OK] Kafka is UP. Topics: {topics}")
        return True
    except Exception as e:
        print(f"  [FAIL] Kafka is DOWN: {e}")
        if strict:
            raise ConnectionError(f"Kafka health check failed at {settings.kafka.bootstrap_servers}: {e}") from e
        return False

def check_minio(strict: bool = False) -> bool:
    print(f"Checking MinIO ({settings.storage.endpoint}, bucket='{settings.storage.data_lake_bucket}')...")
    try:
        backend = MinIOBackend(
            endpoint=settings.storage.endpoint,
            access_key=settings.storage.access_key,
            secret_key=settings.storage.secret_key,
            secure=settings.storage.secure,
            bucket=settings.storage.data_lake_bucket,
            enable_local_mirror=False
        )
        backend.health_check()
        print(f"  [OK] MinIO is UP. Bucket '{settings.storage.data_lake_bucket}' verified.")
        return True
    except Exception as e:
        print(f"  [FAIL] MinIO is DOWN: {e}")
        if strict:
            raise ConnectionError(f"MinIO health check failed at {settings.storage.endpoint}: {e}") from e
        return False

def check_postgres(strict: bool = False) -> bool:
    print(f"Checking PostgreSQL ({settings.database.host}:{settings.database.port}/{settings.database.database})...")
    try:
        wh = WarehouseManager(use_postgres=True, mode="integration")
        wh.health_check()
        wh.close()
        print("  [OK] PostgreSQL is UP (SELECT 1 succeeded).")
        return True
    except Exception as e:
        print(f"  [FAIL] PostgreSQL is DOWN: {e}")
        if strict:
            raise ConnectionError(
                f"PostgreSQL health check failed at {settings.database.host}:{settings.database.port}/"
                f"{settings.database.database}: {e}"
            ) from e
        return False

def check_spark() -> bool:
    print(f"Checking Spark ({settings.spark.master})...")
    try:
        if settings.spark.master.startswith("local"):
            print("  [OK] Spark is configured for local[*] execution.")
            return True
        print(f"  [OK] Spark master configured at {settings.spark.master}.")
        return True
    except Exception as e:
        print(f"  [FAIL] Spark check failed: {e}")
        return False

def verify_integration_health(require_kafka: bool = False) -> Dict[str, bool]:
    """Performs mandatory pre-flight health checks for Integration Mode.
    Raises ConnectionError immediately if MinIO or PostgreSQL (or Kafka when required) is unreachable.
    """
    print("----------------------------------------------------------")
    print("[Pre-Flight] Running Real Infrastructure Health Checks...")
    print("----------------------------------------------------------")
    failures = []
    status = {}

    try:
        status["minio"] = check_minio(strict=True)
    except Exception as e:
        status["minio"] = False
        failures.append(f"MinIO ({settings.storage.endpoint}): {e}")

    try:
        status["postgres"] = check_postgres(strict=True)
    except Exception as e:
        status["postgres"] = False
        failures.append(f"PostgreSQL ({settings.database.host}:{settings.database.port}): {e}")

    if require_kafka:
        try:
            status["kafka"] = check_kafka(strict=True)
        except Exception as e:
            status["kafka"] = False
            failures.append(f"Kafka ({settings.kafka.bootstrap_servers}): {e}")

    status["spark"] = check_spark()

    if failures:
        joined = "\n  - ".join(failures)
        raise ConnectionError(
            f"Integration pre-flight health check FAILED. Required services are unreachable:\n  - {joined}\n"
            f"Refusing to fall back to local files or SQLite in integration mode."
        )

    print("[Pre-Flight] All required integration services are HEALTHY.\n")
    return status

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SentinelAI Infrastructure Health Check")
    parser.add_argument("--require-kafka", action="store_true", help="Also require Kafka to be up for exit code 0")
    args = parser.parse_args()

    print("Running SentinelAI Health Checks...")
    k_ok = check_kafka(strict=False)
    m_ok = check_minio(strict=False)
    p_ok = check_postgres(strict=False)
    s_ok = check_spark()
    print("Done.")

    if not m_ok or not p_ok or (args.require_kafka and not k_ok):
        sys.exit(1)
