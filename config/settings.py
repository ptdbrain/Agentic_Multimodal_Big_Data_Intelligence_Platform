import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent

def get_env(key: str, default: str) -> str:
    return os.environ.get(key, default)

@dataclass
class KafkaSettings:
    bootstrap_servers: str = get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    client_id: str = get_env("KAFKA_CLIENT_ID", "sentinel-producer")
    group_id: str = get_env("KAFKA_GROUP_ID", "sentinel-consumers")
    topic_products: str = get_env("KAFKA_TOPIC_PRODUCTS", "raw.products")
    topic_reviews: str = get_env("KAFKA_TOPIC_REVIEWS", "raw.reviews")
    topic_prices: str = get_env("KAFKA_TOPIC_PRICES", "raw.prices")
    topic_events: str = get_env("KAFKA_TOPIC_EVENTS", "raw.events")

@dataclass
class StorageSettings:
    endpoint: str = get_env("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = get_env("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = get_env("MINIO_SECRET_KEY", "minioadmin")
    secure: bool = get_env("MINIO_SECURE", "false").lower() == "true"
    bucket_raw: str = get_env("MINIO_BUCKET_RAW", "sentinel-raw")
    bucket_silver: str = get_env("MINIO_BUCKET_SILVER", "sentinel-silver")
    bucket_gold: str = get_env("MINIO_BUCKET_GOLD", "sentinel-gold")
    local_data_dir: Path = REPO_ROOT / get_env("LOCAL_DATA_DIR", "storage/datalake")

@dataclass
class DatabaseSettings:
    host: str = get_env("POSTGRES_HOST", "localhost")
    port: int = int(get_env("POSTGRES_PORT", "5432"))
    user: str = get_env("POSTGRES_USER", "sentinel")
    password: str = get_env("POSTGRES_PASSWORD", "sentinelpass")
    database: str = get_env("POSTGRES_DB", "sentinel_dw")
    use_sqlite_fallback: bool = get_env("USE_SQLITE_FALLBACK", "true").lower() == "true"
    sqlite_path: Path = REPO_ROOT / get_env("SQLITE_DB_PATH", "database/sentinel.db")

@dataclass
class Settings:
    app_name: str = "SentinelAI"
    env: str = get_env("ENV", "development")
    repo_root: Path = REPO_ROOT
    kafka: KafkaSettings = KafkaSettings()
    storage: StorageSettings = StorageSettings()
    database: DatabaseSettings = DatabaseSettings()

settings = Settings()
