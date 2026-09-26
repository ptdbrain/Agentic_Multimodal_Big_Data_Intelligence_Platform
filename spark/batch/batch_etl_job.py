import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any

from spark.etl.cleaner import DataCleaner
from spark.etl.normalizer import EntityNormalizer
from spark.etl.deduplicator import Deduplicator
from spark.quality.metrics import DataQualityEvaluator
from spark.quality.rules import REVIEW_QUALITY_RULES, PRODUCT_QUALITY_RULES
from storage.storage_manager import storage

class BatchETLJob:
    """Executes End-to-End Batch ETL:
    Bronze Raw -> Clean -> Normalize -> Deduplicate -> Validate -> Silver Parquet
    """

    @staticmethod
    def run_pipeline(products_file: str, reviews_file: str) -> Dict[str, Any]:
        print(">>> Starting SentinelAI Batch ETL Pipeline")
        
        # 1. Read Bronze Raw Data
        with open(products_file, "r", encoding="utf-8") as f:
            raw_products = json.load(f)
        with open(reviews_file, "r", encoding="utf-8") as f:
            raw_reviews = json.load(f)

        print(f"Bronze Raw Ingested: {len(raw_products)} products, {len(raw_reviews)} reviews")

        # 2. Products ETL
        cleaned_prods = DataCleaner.clean_products(raw_products)
        norm_prods = [EntityNormalizer.normalize_product(p) for p in cleaned_prods]
        dq_prods = DataQualityEvaluator.evaluate_batch("products", norm_prods, PRODUCT_QUALITY_RULES)
        df_silver_prods = pd.DataFrame(dq_prods["valid_records"])
        storage.write_silver_parquet("products", df_silver_prods)

        # 3. Reviews ETL
        cleaned_revs = DataCleaner.clean_reviews(raw_reviews)
        dedup_revs, dup_count = Deduplicator.deduplicate_reviews(cleaned_revs)
        dq_revs = DataQualityEvaluator.evaluate_batch("reviews", dedup_revs, REVIEW_QUALITY_RULES)
        df_silver_revs = pd.DataFrame(dq_revs["valid_records"])
        storage.write_silver_parquet("reviews", df_silver_revs)

        print(f"Silver Parquet Generated: {len(df_silver_prods)} products, {len(df_silver_revs)} reviews")
        print(f"Products DQ Score: {dq_prods['dq_score']}% | Reviews DQ Score: {dq_revs['dq_score']}%")

        return {
            "products_processed": len(df_silver_prods),
            "reviews_processed": len(df_silver_revs),
            "products_dq": dq_prods,
            "reviews_dq": dq_revs
        }
