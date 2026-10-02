import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent

def get_env(key: str, default: str) -> str:
    return os.environ.get(key, default)

@dataclass
class KafkaSettings:
    bootstrap_servers: str = get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
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
    data_lake_bucket: str = get_env("DATA_LAKE_BUCKET", "sentinel-data")
    backend: str = get_env("STORAGE_BACKEND", "minio")
    local_data_dir: Path = REPO_ROOT / get_env("LOCAL_DATA_DIR", "storage/datalake")

    @property
    def bucket_raw(self) -> str:
        return self.data_lake_bucket

    @property
    def bucket_silver(self) -> str:
        return self.data_lake_bucket

    @property
    def bucket_gold(self) -> str:
        return self.data_lake_bucket

@dataclass
class DatabaseSettings:
    host: str = get_env("POSTGRES_HOST", "localhost")
    port: int = int(get_env("POSTGRES_PORT", "5432"))
    user: str = get_env("POSTGRES_USER", "sentinel")
    password: str = get_env("POSTGRES_PASSWORD", "sentinelpass")
    database: str = get_env("POSTGRES_DB", "sentinel_dw")
    sqlite_path: Path = REPO_ROOT / get_env("SQLITE_DB_PATH", "database/sentinel.db")

@dataclass
class AnalyticsSettings:
    price_zscore_threshold: float = float(get_env("PRICE_ZSCORE_THRESHOLD", "3.0"))
    price_iqr_k: float = float(get_env("PRICE_IQR_K", "1.5"))
    price_min_data_points: int = int(get_env("PRICE_MIN_DATA_POINTS", "3"))
    burst_window_hours: int = int(get_env("BURST_WINDOW_HOURS", "1"))
    burst_threshold_multiplier: float = float(get_env("BURST_THRESHOLD_MULTIPLIER", "3.0"))
    burst_min_reviews: int = int(get_env("BURST_MIN_REVIEWS", "5"))
    bombing_rating_threshold: float = float(get_env("BOMBING_RATING_THRESHOLD", "2.5"))
    rating_drop_threshold: float = float(get_env("RATING_DROP_THRESHOLD", "0.5"))
    rating_drop_min_reviews: int = int(get_env("RATING_DROP_MIN_REVIEWS", "3"))
    rating_drop_window_days: int = int(get_env("RATING_DROP_WINDOW_DAYS", "7"))
    trend_rolling_window: int = int(get_env("TREND_ROLLING_WINDOW", "7"))
    sentiment_positive_threshold: float = float(get_env("SENTIMENT_POSITIVE_THRESHOLD", "4.0"))
    sentiment_negative_threshold: float = float(get_env("SENTIMENT_NEGATIVE_THRESHOLD", "2.0"))
    default_currency: str = get_env("DEFAULT_CURRENCY", "VND")

@dataclass
class QualitySettings:
    min_dq_score: float = float(get_env("MIN_DQ_SCORE", "90.0"))
    max_duplicate_ratio: float = float(get_env("MAX_DUPLICATE_RATIO", "0.05"))
    max_missing_ratio: float = float(get_env("MAX_MISSING_RATIO", "0.05"))

@dataclass
class SparkSettings:
    master: str = get_env("SPARK_MASTER", "local[*]")
    app_name: str = get_env("SPARK_APP_NAME", "SentinelAI-Spark")
    batch_interval_sec: int = int(get_env("SPARK_BATCH_INTERVAL_SEC", "60"))

@dataclass
class IngestionSettings:
    default_batch_size: int = int(get_env("INGESTION_BATCH_SIZE", "50"))
    max_api_pages: int = int(get_env("INGESTION_MAX_API_PAGES", "10"))
    rate_limit_rps: float = float(get_env("INGESTION_RATE_LIMIT_RPS", "20.0"))
    request_timeout_sec: int = int(get_env("INGESTION_REQUEST_TIMEOUT_SEC", "15"))

@dataclass
class SearchSettings:
    es_host: str = get_env("ES_HOST", "localhost")
    es_port: int = int(get_env("ES_PORT", "9200"))
    default_search_limit: int = int(get_env("SEARCH_DEFAULT_LIMIT", "20"))

@dataclass
class CrawlerSettings:
    tiki_base_url: str = get_env("TIKI_BASE_URL", "https://tiki.vn")
    tiki_products_endpoint: str = get_env("TIKI_PRODUCTS_ENDPOINT", "/api/v2/products")
    tiki_reviews_endpoint: str = get_env("TIKI_REVIEWS_ENDPOINT", "/api/v2/reviews")
    user_agent: str = get_env(
        "CRAWLER_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    request_timeout_sec: int = int(get_env("CRAWLER_TIMEOUT_SEC", "15"))
    rate_limit_rps: float = float(get_env("CRAWLER_RATE_LIMIT_RPS", "2.0"))
    max_retries: int = int(get_env("CRAWLER_MAX_RETRIES", "3"))
    retry_backoff_sec: float = float(get_env("CRAWLER_RETRY_BACKOFF_SEC", "1.5"))

@dataclass
class Settings:
    app_name: str = "SentinelAI"
    env: str = get_env("ENV", "development")
    repo_root: Path = REPO_ROOT
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    analytics: AnalyticsSettings = field(default_factory=AnalyticsSettings)
    quality: QualitySettings = field(default_factory=QualitySettings)
    spark: SparkSettings = field(default_factory=SparkSettings)
    ingestion: IngestionSettings = field(default_factory=IngestionSettings)
    search: SearchSettings = field(default_factory=SearchSettings)
    crawler: CrawlerSettings = field(default_factory=CrawlerSettings)

settings = Settings()


