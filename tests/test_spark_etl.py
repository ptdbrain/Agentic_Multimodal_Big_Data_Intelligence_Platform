import pytest
from spark.etl.cleaner import DataCleaner
from spark.etl.normalizer import EntityNormalizer
from spark.etl.deduplicator import Deduplicator

def test_clean_reviews():
    dirty_reviews = [
        {"review_id": "r1", "review_text": "Good phone", "rating": 5.0},
        {"review_id": "r2", "review_text": "", "rating": 4.0}, # empty text
        {"review_id": "r3", "review_text": None, "rating": 3.0} # None text
    ]
    cleaned = DataCleaner.clean_reviews(dirty_reviews)
    assert len(cleaned) == 1
    assert cleaned[0]["review_id"] == "r1"

def test_normalize_brand():
    assert EntityNormalizer.normalize_brand("apple inc.") == "Apple"
    assert EntityNormalizer.normalize_brand("SAMSUNG") == "Samsung"
    assert EntityNormalizer.normalize_brand("xiaomi") == "Xiaomi"

def test_normalize_price():
    assert EntityNormalizer.normalize_price("19.990.000 VND") == 19990000.0
    assert EntityNormalizer.normalize_price(25000000) == 25000000.0

def test_deduplicate_reviews():
    reviews = [
        {"review_id": "rv_01", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"},
        {"review_id": "rv_01", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"}, # duplicate id
        {"review_id": "rv_02", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"}  # duplicate hash
    ]
    unique, dups = Deduplicator.deduplicate_reviews(reviews)
    assert len(unique) == 1
    assert dups == 2
