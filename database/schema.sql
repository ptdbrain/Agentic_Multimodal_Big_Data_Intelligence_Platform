-- SentinelAI Data Warehouse Schema
-- Designed for PostgreSQL & SQLite compatibility

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(100) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    price NUMERIC(15, 2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'VND',
    rating NUMERIC(3, 2),
    review_count INTEGER DEFAULT 0,
    seller VARCHAR(255),
    source VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_daily_stats (
    date DATE NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    review_count INTEGER DEFAULT 0,
    avg_rating NUMERIC(3, 2),
    min_price NUMERIC(15, 2),
    max_price NUMERIC(15, 2),
    avg_price NUMERIC(15, 2),
    rating_std NUMERIC(4, 2),
    positive_ratio NUMERIC(5, 4),
    negative_ratio NUMERIC(5, 4),
    PRIMARY KEY (date, product_id)
);

CREATE TABLE IF NOT EXISTS brand_daily_stats (
    date DATE NOT NULL,
    brand VARCHAR(100) NOT NULL,
    product_count INTEGER DEFAULT 0,
    review_count INTEGER DEFAULT 0,
    avg_rating NUMERIC(3, 2),
    avg_price NUMERIC(15, 2),
    PRIMARY KEY (date, brand)
);

CREATE TABLE IF NOT EXISTS category_daily_stats (
    date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    product_count INTEGER DEFAULT 0,
    review_count INTEGER DEFAULT 0,
    avg_rating NUMERIC(3, 2),
    avg_price NUMERIC(15, 2),
    PRIMARY KEY (date, category)
);

CREATE TABLE IF NOT EXISTS anomaly_events (
    event_id VARCHAR(100) PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(100) NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    score NUMERIC(5, 2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    batch_id VARCHAR(100) PRIMARY KEY,
    dataset_name VARCHAR(100) NOT NULL,
    records_received INTEGER NOT NULL,
    records_valid INTEGER NOT NULL,
    records_invalid INTEGER NOT NULL,
    records_duplicate INTEGER NOT NULL,
    records_missing INTEGER NOT NULL,
    dq_score NUMERIC(5, 2) NOT NULL,
    processing_time_ms INTEGER NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_id VARCHAR(100) PRIMARY KEY,
    stage VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(15, 4) NOT NULL,
    unit VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prod_daily_date ON product_daily_stats(date);
CREATE INDEX IF NOT EXISTS idx_brand_daily_date ON brand_daily_stats(date);
CREATE INDEX IF NOT EXISTS idx_cat_daily_date ON category_daily_stats(date);
CREATE INDEX IF NOT EXISTS idx_anomaly_timestamp ON anomaly_events(timestamp);

CREATE TABLE IF NOT EXISTS price_daily_stats (
    date DATE NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    min_price NUMERIC(15, 2),
    max_price NUMERIC(15, 2),
    avg_price NUMERIC(15, 2),
    price_std NUMERIC(15, 2),
    data_points INTEGER DEFAULT 0,
    PRIMARY KEY (date, product_id)
);

CREATE INDEX IF NOT EXISTS idx_price_daily_date ON price_daily_stats(date);
