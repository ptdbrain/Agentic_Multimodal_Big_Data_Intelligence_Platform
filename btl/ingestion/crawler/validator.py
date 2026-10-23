"""Crawler Data Validator - Pre-Spark Data Contract & Schema Guard.

Strictly verifies field names, data types, product IDs, prices, customer reviews,
and schema compliance for crawled payloads BEFORE ingestion into Kafka,
Bronze Data Lake, or Spark ETL.
"""
from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from data.schemas.validator import SchemaValidator
from ingestion.privacy import is_anonymized_user_id

PRODUCT_ID_RE = re.compile(r"^tiki_\d+$", re.IGNORECASE)
ALLOWED_CURRENCIES: Set[str] = {"VND", "USD"}
REQUIRED_PRODUCT_FIELDS: Set[str] = {
    "product_id",
    "product_name",
    "brand",
    "category",
    "price",
    "currency",
    "source",
}
REQUIRED_PRICE_FIELDS: Set[str] = {
    "price_id",
    "product_id",
    "price",
    "currency",
    "timestamp",
}
REQUIRED_REVIEW_FIELDS: Set[str] = {
    "review_id",
    "product_id",
    "user_id",
    "rating",
    "review_title",
    "review_text",
    "review_date",
}


def _is_valid_iso_timestamp(ts: Any) -> bool:
    if not isinstance(ts, str) or not ts.strip():
        return False
    try:
        clean_ts = ts.replace("Z", "+00:00")
        datetime.fromisoformat(clean_ts)
        return True
    except (ValueError, TypeError):
        return False


def _is_valid_number(val: Any, min_val: float = 0.0) -> bool:
    if isinstance(val, bool) or val is None:
        return False
    if not isinstance(val, (int, float)):
        return False
    if math.isnan(val) or math.isinf(val):
        return False
    return val >= min_val


class CrawlerDataValidator:
    """Validator enforcing SentinelAI Data Contracts on crawled products, prices, and reviews."""

    @classmethod
    def validate_product(
        cls, record: Dict[str, Any], enforce_tiki_id_format: bool = False
    ) -> Tuple[bool, List[str]]:
        """Validate a single product record against canonical contract and data types."""
        errors: List[str] = []

        if not isinstance(record, dict):
            return False, ["Product record must be a dictionary"]

        # 1. Required field names check
        missing_fields = REQUIRED_PRODUCT_FIELDS - set(record.keys())
        if missing_fields:
            errors.append(f"Missing required product fields: {sorted(missing_fields)}")

        # 2. Product ID validation
        pid = record.get("product_id")
        if not isinstance(pid, str) or not pid.strip():
            errors.append(f"Invalid product_id: must be non-empty string, got {type(pid).__name__}")
        else:
            if enforce_tiki_id_format and not PRODUCT_ID_RE.match(pid.strip()):
                errors.append(f"Invalid product_id format: '{pid}' does not match pattern '^tiki_\\d+$'")

        # 3. Product Name validation
        name = record.get("product_name")
        if not isinstance(name, str) or len(name.strip()) < 2:
            errors.append(f"Invalid product_name: must be a string with length >= 2, got '{name}'")

        # 4. Brand and Category validation
        brand = record.get("brand")
        if not isinstance(brand, str) or not brand.strip():
            errors.append(f"Invalid brand: must be non-empty string, got '{brand}'")

        category = record.get("category")
        if not isinstance(category, str) or not category.strip():
            errors.append(f"Invalid category: must be non-empty string, got '{category}'")

        # 5. Price validation
        price = record.get("price")
        if not _is_valid_number(price, min_val=0.0):
            errors.append(f"Invalid price: must be finite number >= 0, got {price}")

        # 6. Currency validation
        currency = record.get("currency")
        if not isinstance(currency, str) or currency.upper() not in ALLOWED_CURRENCIES:
            errors.append(f"Invalid currency: must be one of {ALLOWED_CURRENCIES}, got '{currency}'")

        # 7. Optional fields type checks
        if "rating" in record and record["rating"] is not None:
            rating = record["rating"]
            if not _is_valid_number(rating, min_val=0.0) or float(rating) > 5.0:
                errors.append(f"Invalid rating: must be between 0.0 and 5.0, got {rating}")

        if "review_count" in record and record["review_count"] is not None:
            rc = record["review_count"]
            if isinstance(rc, bool) or not isinstance(rc, int) or rc < 0:
                errors.append(f"Invalid review_count: must be non-negative integer, got {rc}")

        if "created_at" in record and record["created_at"]:
            if not _is_valid_iso_timestamp(record["created_at"]):
                errors.append(f"Invalid created_at timestamp: '{record['created_at']}'")

        if "updated_at" in record and record["updated_at"]:
            if not _is_valid_iso_timestamp(record["updated_at"]):
                errors.append(f"Invalid updated_at timestamp: '{record['updated_at']}'")

        # 8. SchemaValidator (data/schemas/product.json)
        try:
            is_schema_valid, schema_errs = SchemaValidator.validate_record(record, "product")
            if not is_schema_valid:
                errors.extend(schema_errs)
        except Exception as exc:
            errors.append(f"SchemaValidator execution error: {exc}")

        return len(errors) == 0, errors

    @classmethod
    def validate_price(cls, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a single price observation record."""
        errors: List[str] = []

        if not isinstance(record, dict):
            return False, ["Price record must be a dictionary"]

        # 1. Required field names check
        missing_fields = REQUIRED_PRICE_FIELDS - set(record.keys())
        if missing_fields:
            errors.append(f"Missing required price fields: {sorted(missing_fields)}")

        # 2. Price ID and Product ID check
        price_id = record.get("price_id")
        if not isinstance(price_id, str) or not price_id.strip():
            errors.append(f"Invalid price_id: must be non-empty string, got {type(price_id).__name__}")

        product_id = record.get("product_id")
        if not isinstance(product_id, str) or not product_id.strip():
            errors.append(f"Invalid product_id: must be non-empty string, got {type(product_id).__name__}")

        # 3. Price amount check
        price = record.get("price")
        if not _is_valid_number(price, min_val=0.0):
            errors.append(f"Invalid price: must be finite number >= 0, got {price}")

        # 4. Currency check
        currency = record.get("currency")
        if not isinstance(currency, str) or not currency.strip():
            errors.append(f"Invalid currency: must be non-empty string, got '{currency}'")

        # 5. Timestamp check
        ts = record.get("timestamp")
        if not _is_valid_iso_timestamp(ts):
            errors.append(f"Invalid timestamp: must be valid ISO-8601 string, got '{ts}'")

        # 6. SchemaValidator (data/schemas/price.json)
        try:
            is_schema_valid, schema_errs = SchemaValidator.validate_record(record, "price")
            if not is_schema_valid:
                errors.extend(schema_errs)
        except Exception as exc:
            errors.append(f"SchemaValidator execution error: {exc}")

        return len(errors) == 0, errors

    @classmethod
    def validate_review(
        cls, record: Dict[str, Any], enforce_tiki_id_format: bool = False
    ) -> Tuple[bool, List[str]]:
        """Validate a single genuine customer review record."""
        errors: List[str] = []

        if not isinstance(record, dict):
            return False, ["Review record must be a dictionary"]

        # 1. Required field names check
        missing_fields = REQUIRED_REVIEW_FIELDS - set(record.keys())
        if missing_fields:
            errors.append(f"Missing required review fields: {sorted(missing_fields)}")

        # 2. Review ID and Product ID check
        review_id = record.get("review_id")
        if not isinstance(review_id, str) or not review_id.strip():
            errors.append(f"Invalid review_id: must be non-empty string, got {type(review_id).__name__}")

        product_id = record.get("product_id")
        if not isinstance(product_id, str) or not product_id.strip():
            errors.append(f"Invalid product_id: must be non-empty string, got {type(product_id).__name__}")
        elif enforce_tiki_id_format and not PRODUCT_ID_RE.match(product_id.strip()):
            errors.append(f"Invalid product_id format in review: '{product_id}'")

        # 3. User ID PII anonymization check
        uid = record.get("user_id")
        if not isinstance(uid, str) or not uid.strip():
            errors.append(f"Invalid user_id: must be non-empty string, got {type(uid).__name__}")
        elif not is_anonymized_user_id(uid):
            errors.append(f"Review user_id '{uid}' is not properly anonymized/hashed")

        # 4. Rating check (1.0 to 5.0)
        rating = record.get("rating")
        if not _is_valid_number(rating, min_val=1.0) or float(rating) > 5.0:
            errors.append(f"Invalid rating: must be between 1.0 and 5.0, got {rating}")

        # 5. Text content check
        review_text = record.get("review_text")
        if not isinstance(review_text, str) or len(review_text.strip()) < 1:
            errors.append(f"Invalid review_text: must have minLength >= 1, got '{review_text}'")

        # 6. Timestamp check
        review_date = record.get("review_date")
        if not _is_valid_iso_timestamp(review_date):
            errors.append(f"Invalid review_date: must be valid ISO-8601 string, got '{review_date}'")

        # 7. SchemaValidator (data/schemas/review.json)
        try:
            is_schema_valid, schema_errs = SchemaValidator.validate_record(record, "review")
            if not is_schema_valid:
                errors.extend(schema_errs)
        except Exception as exc:
            errors.append(f"SchemaValidator execution error: {exc}")

        return len(errors) == 0, errors

    @classmethod
    def validate_crawled_dataset(
        cls,
        products: List[Dict[str, Any]],
        prices: List[Dict[str, Any]],
        reviews: Optional[List[Dict[str, Any]]] = None,
        enforce_tiki_id_format: bool = False,
    ) -> Dict[str, Any]:
        """Validate full crawled payload (products, price history, and customer reviews) before Spark ingestion."""
        valid_products: List[Dict[str, Any]] = []
        invalid_products: List[Dict[str, Any]] = []
        valid_prices: List[Dict[str, Any]] = []
        invalid_prices: List[Dict[str, Any]] = []
        valid_reviews: List[Dict[str, Any]] = []
        invalid_reviews: List[Dict[str, Any]] = []
        all_errors: List[str] = []

        valid_product_ids: Set[str] = set()

        # Validate products
        for idx, prod in enumerate(products):
            ok, errs = cls.validate_product(prod, enforce_tiki_id_format=enforce_tiki_id_format)
            if ok:
                valid_products.append(prod)
                valid_product_ids.add(prod["product_id"])
            else:
                invalid_products.append({"index": idx, "record": prod, "errors": errs})
                all_errors.extend([f"Product[{idx}]: {e}" for e in errs])

        # Validate prices
        for idx, pr in enumerate(prices):
            ok, errs = cls.validate_price(pr)
            if ok:
                pid = pr["product_id"]
                if valid_product_ids and pid not in valid_product_ids:
                    ref_err = f"Price product_id '{pid}' does not exist in valid products set"
                    invalid_prices.append({"index": idx, "record": pr, "errors": [ref_err]})
                    all_errors.append(f"Price[{idx}]: {ref_err}")
                else:
                    valid_prices.append(pr)
            else:
                invalid_prices.append({"index": idx, "record": pr, "errors": errs})
                all_errors.extend([f"Price[{idx}]: {e}" for e in errs])

        # Validate reviews (if provided)
        if reviews is not None:
            for idx, rev in enumerate(reviews):
                ok, errs = cls.validate_review(rev, enforce_tiki_id_format=enforce_tiki_id_format)
                if ok:
                    pid = rev["product_id"]
                    if valid_product_ids and pid not in valid_product_ids:
                        ref_err = f"Review product_id '{pid}' does not exist in valid products set"
                        invalid_reviews.append({"index": idx, "record": rev, "errors": [ref_err]})
                        all_errors.append(f"Review[{idx}]: {ref_err}")
                    else:
                        valid_reviews.append(rev)
                else:
                    invalid_reviews.append({"index": idx, "record": rev, "errors": errs})
                    all_errors.extend([f"Review[{idx}]: {e}" for e in errs])

        is_valid = (
            len(invalid_products) == 0
            and len(invalid_prices) == 0
            and len(invalid_reviews) == 0
            and len(valid_products) > 0
        )

        return {
            "is_valid": is_valid,
            "valid_products": valid_products,
            "invalid_products": invalid_products,
            "valid_prices": valid_prices,
            "invalid_prices": invalid_prices,
            "valid_reviews": valid_reviews,
            "invalid_reviews": invalid_reviews,
            "errors": all_errors,
            "stats": {
                "products_total": len(products),
                "products_valid": len(valid_products),
                "products_invalid": len(invalid_products),
                "prices_total": len(prices),
                "prices_valid": len(valid_prices),
                "prices_invalid": len(invalid_prices),
                "reviews_total": len(reviews) if reviews is not None else 0,
                "reviews_valid": len(valid_reviews),
                "reviews_invalid": len(invalid_reviews),
            },
        }

    @classmethod
    def verify_pre_spark_guard(
        cls,
        products: List[Dict[str, Any]],
        prices: List[Dict[str, Any]],
        reviews: Optional[List[Dict[str, Any]]] = None,
        enforce_tiki_id_format: bool = False,
        strict: bool = True,
    ) -> bool:
        """Pre-Spark execution guard.
        Raises ValueError if validation fails in strict mode.
        """
        report = cls.validate_crawled_dataset(
            products=products,
            prices=prices,
            reviews=reviews,
            enforce_tiki_id_format=enforce_tiki_id_format,
        )
        if not report["is_valid"]:
            msg = (
                f"Pre-Spark Validation Guard FAILED: "
                f"{report['stats']['products_invalid']} invalid products, "
                f"{report['stats']['prices_invalid']} invalid prices, "
                f"{report['stats']['reviews_invalid']} invalid reviews. "
                f"Errors: {report['errors'][:5]}"
            )
            if strict:
                raise ValueError(msg)
            return False
        return True


def validate_crawler_payloads(
    products: List[Dict[str, Any]],
    prices: List[Dict[str, Any]],
    reviews: Optional[List[Dict[str, Any]]] = None,
    enforce_tiki_id_format: bool = False,
    strict: bool = True,
) -> Dict[str, Any]:
    """Functional interface for pre-Spark data verification."""
    report = CrawlerDataValidator.validate_crawled_dataset(
        products=products,
        prices=prices,
        reviews=reviews,
        enforce_tiki_id_format=enforce_tiki_id_format,
    )
    if strict and not report["is_valid"]:
        raise ValueError(
            f"Pre-Spark validation failed: {report['stats']['products_invalid']} invalid products, "
            f"{report['stats']['prices_invalid']} invalid prices, "
            f"{report['stats']['reviews_invalid']} invalid reviews. Errors: {report['errors'][:5]}"
        )
    return report
