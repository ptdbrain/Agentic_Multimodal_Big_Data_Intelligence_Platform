import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Union, List, Optional

from spark.etl.cleaner import DataCleaner
from spark.etl.normalizer import EntityNormalizer
from spark.etl.deduplicator import Deduplicator
from spark.quality.metrics import DataQualityEvaluator
from spark.quality.rules import RuleRegistry, REVIEW_QUALITY_RULES, PRODUCT_QUALITY_RULES
from storage.storage_manager import storage
from ingestion.file_loader.loader import FileLoader

class BatchETLJob:
    """Executes End-to-End Batch ETL:
    Bronze Raw -> Clean -> Normalize -> Deduplicate -> Validate -> Silver Parquet
    Flexible across file formats (JSON, CSV, Parquet) and in-memory structures.
    """

    @staticmethod
    def _to_records(source: Union[str, Path, List[Dict[str, Any]], pd.DataFrame]) -> List[Dict[str, Any]]:
        if isinstance(source, list):
            return list(source)
        if isinstance(source, pd.DataFrame):
            return source.to_dict(orient="records")
        p = Path(source)
        if not p.exists():
            raise FileNotFoundError(f"Source file not found: {p}")
        return FileLoader.load_records(p)

    @classmethod
    def run_pipeline(
        cls,
        products_source: Union[str, Path, List[Dict[str, Any]], pd.DataFrame],
        reviews_source: Union[str, Path, List[Dict[str, Any]], pd.DataFrame],
        product_rules: Optional[List] = None,
        review_rules: Optional[List] = None,
        save_silver: bool = True
    ) -> Dict[str, Any]:
        print(">>> Starting SentinelAI Batch ETL Pipeline")
        
        # 1. Ingest Bronze Raw Data dynamically
        raw_products = cls._to_records(products_source)
        raw_reviews = cls._to_records(reviews_source)

        print(f"Bronze Raw Ingested: {len(raw_products)} products, {len(raw_reviews)} reviews")

        # 2. Products ETL
        cleaned_prods = DataCleaner.clean_products(raw_products)
        norm_prods = [EntityNormalizer.normalize_product(p) for p in cleaned_prods]
        p_rules = product_rules or RuleRegistry.get_rules("products") or PRODUCT_QUALITY_RULES
        dq_prods = DataQualityEvaluator.evaluate_batch("products", norm_prods, p_rules)
        df_silver_prods = pd.DataFrame(dq_prods["valid_records"])
        if save_silver:
            storage.write_silver_parquet("products", df_silver_prods)

        # 3. Reviews ETL
        cleaned_revs = DataCleaner.clean_reviews(raw_reviews)
        dedup_revs, dup_count = Deduplicator.deduplicate_reviews(cleaned_revs)
        r_rules = review_rules or RuleRegistry.get_rules("reviews") or REVIEW_QUALITY_RULES
        dq_revs = DataQualityEvaluator.evaluate_batch("reviews", dedup_revs, r_rules)
        df_silver_revs = pd.DataFrame(dq_revs["valid_records"])
        if save_silver:
            storage.write_silver_parquet("reviews", df_silver_revs)

        print(f"Silver Parquet Generated: {len(df_silver_prods)} products, {len(df_silver_revs)} reviews")
        print(f"Products DQ Score: {dq_prods['dq_score']}% | Reviews DQ Score: {dq_revs['dq_score']}%")

        return {
            "products_processed": len(df_silver_prods),
            "reviews_processed": len(df_silver_revs),
            "products_dq": dq_prods,
            "reviews_dq": dq_revs
        }

