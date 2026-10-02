import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from kafka import KafkaConsumer
from minio import Minio
import requests

def check_kafka():
    print("Checking Kafka...")
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=settings.kafka.bootstrap_servers,
            request_timeout_ms=1000,
            session_timeout_ms=1000
        )
        topics = consumer.topics()
        print(f"Kafka is UP. Topics: {topics}")
        return True
    except Exception as e:
        print(f"Kafka is DOWN: {e}")
        return False

def check_minio():
    print("Checking MinIO...")
    try:
        client = Minio(
            settings.storage.endpoint,
            access_key=settings.storage.access_key,
            secret_key=settings.storage.secret_key,
            secure=settings.storage.secure
        )
        exists = client.bucket_exists(settings.storage.data_lake_bucket)
        print(f"MinIO is UP. Bucket '{settings.storage.data_lake_bucket}' exists: {exists}")
        return True
    except Exception as e:
        print(f"MinIO is DOWN: {e}")
        return False

def check_postgres():
    print("Checking PostgreSQL...")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=settings.database.host,
            port=settings.database.port,
            user=settings.database.user,
            password=settings.database.password,
            dbname=settings.database.database,
            connect_timeout=5
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        conn.close()
        print("PostgreSQL is UP.")
        return True
    except ModuleNotFoundError:
        print("PostgreSQL driver (psycopg2) not installed in local environment. Skipping live check.")
        return False
    except Exception as e:
        print(f"PostgreSQL is DOWN: {e}")
        return False

def check_spark():
    print("Checking Spark...")
    try:
        if settings.spark.master.startswith("local"):
            print("Spark is configured for local mode. No external master to check.")
            return True
        else:
            print("Spark master is remote. Not performing deep connectivity check.")
            return True
    except Exception as e:
        print(f"Spark check failed: {e}")
        return False

if __name__ == "__main__":
    print("Running SentinelAI Health Checks...")
    check_kafka()
    check_minio()
    check_postgres()
    check_spark()
    print("Done.")
