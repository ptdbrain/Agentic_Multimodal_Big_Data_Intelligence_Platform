import pytest
from spark.etl.cleaner import DataCleaner
from spark.etl.normalizer import EntityNormalizer
from spark.etl.deduplicator import Deduplicator

pytestmark = pytest.mark.unit


def test_clean_reviews():
    dirty_reviews = [
        {"review_id": "r1", "review_text": "  Good phone  ", "review_title": "  Nice  ", "rating": 5.0},
        {"review_id": "r2", "review_text": "", "rating": 4.0},  # empty text
        {"review_id": "r3", "review_text": None, "rating": 3.0},  # None text
    ]
    cleaned = DataCleaner.clean_reviews(dirty_reviews)
    assert len(cleaned) == 1
    assert cleaned[0]["review_id"] == "r1"
    assert cleaned[0]["review_text"] == "Good phone"
    assert cleaned[0]["review_title"] == "Nice"


def test_validate_reviews_rejects_out_of_range_without_clamping():
    reviews = [
        {"review_id": "r_valid", "review_text": "Great", "rating": 4.5},
        {"review_id": "r_high", "review_text": "Too high", "rating": 10.0},
        {"review_id": "r_low", "review_text": "Too low", "rating": 0.0},
        {"review_id": "r_bad_type", "review_text": "Bad rating", "rating": "not_a_float"},
    ]
    res = DataCleaner.validate_and_clean(reviews)
    assert res["stats"]["valid"] == 1
    assert res["stats"]["invalid"] == 3
    assert res["valid"][0]["review_id"] == "r_valid"
    invalid_ids = {r["review_id"] for r in res["invalid"]}
    assert invalid_ids == {"r_high", "r_low", "r_bad_type"}


def test_normalize_brand():
    assert EntityNormalizer.normalize_brand("apple inc.") == "Apple"
    assert EntityNormalizer.normalize_brand("SAMSUNG") == "Samsung"
    assert EntityNormalizer.normalize_brand("xiaomi") == "Xiaomi"


def test_normalize_price():
    assert EntityNormalizer.normalize_price("19.990.000 VND") == 19990000.0
    assert EntityNormalizer.normalize_price("$1,299.50") == 1299.50
    assert EntityNormalizer.normalize_price("1.299,50 €") == 1299.50
    assert EntityNormalizer.normalize_price(25000000) == 25000000.0


def test_normalize_currency_and_timestamp():
    # Currency normalization
    assert EntityNormalizer.normalize_currency("vnđ") == "VND"
    assert EntityNormalizer.normalize_currency("₫") == "VND"
    assert EntityNormalizer.normalize_currency("$") == "USD"
    assert EntityNormalizer.normalize_currency("us$") == "USD"
    assert EntityNormalizer.normalize_currency("€") == "EUR"
    assert EntityNormalizer.normalize_currency(None, default_currency="VND") == "VND"

    # Timestamp normalization to UTC ISO-8601
    assert EntityNormalizer.normalize_timestamp(1727000000) == "2024-09-22T10:13:20Z"
    assert EntityNormalizer.normalize_timestamp(1727000000000) == "2024-09-22T10:13:20Z"
    assert EntityNormalizer.normalize_timestamp("2026-09-24T19:00:00+07:00") == "2026-09-24T12:00:00Z"
    assert EntityNormalizer.normalize_timestamp("2026/09/24 08:30:00") == "2026-09-24T08:30:00Z"
    assert EntityNormalizer.normalize_timestamp("2026-09-24") == "2026-09-24T00:00:00Z"
    assert EntityNormalizer.normalize_timestamp("not-a-timestamp") is None


def test_deduplicate_reviews():
    reviews = [
        {"review_id": "rv_01", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"},
        {"review_id": "rv_01", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"},  # duplicate id
        {"review_id": "rv_02", "product_id": "p1", "user_id": "u1", "review_text": "Hello", "review_date": "2026-09-24"},  # duplicate hash
    ]
    unique, dups = Deduplicator.deduplicate_reviews(reviews)
    assert len(unique) == 1
    assert dups == 2


def test_deduplicate_retains_latest_record_by_timestamp_and_tiebreaker():
    # Out-of-order updates for the same review_id: 2026-10-03T15:00:00Z must win over 10-01 and 10-02
    records = [
        {
            "review_id": "rv_latest_test",
            "product_id": "p1",
            "user_id": "u1",
            "review_text": "Old v1",
            "rating": 3.0,
            "ingested_at": "2026-10-01T08:00:00Z",
            "event_id": "evt_001",
        },
        {
            "review_id": "rv_latest_test",
            "product_id": "p1",
            "user_id": "u1",
            "review_text": "Newest v3",
            "rating": 5.0,
            "ingested_at": "2026-10-03T15:00:00Z",
            "event_id": "evt_002",
        },
        {
            "review_id": "rv_latest_test",
            "product_id": "p1",
            "user_id": "u1",
            "review_text": "Middle v2",
            "rating": 4.0,
            "ingested_at": "2026-10-02T12:00:00Z",
            "event_id": "evt_003",
        },
    ]
    unique, dup_count = Deduplicator.deduplicate_reviews(records)
    assert len(unique) == 1
    assert dup_count == 2
    assert unique[0]["review_text"] == "Newest v3"
    assert unique[0]["rating"] == 5.0
    assert unique[0]["ingested_at"] == "2026-10-03T15:00:00Z"

    # Tie-breaker on event_id when timestamps are identical
    tied_records = [
        {
            "product_id": "p_tie",
            "product_name": "Phone A",
            "price": 100.0,
            "ingested_at": "2026-10-05T10:00:00Z",
            "event_id": "evt_a",
        },
        {
            "product_id": "p_tie",
            "product_name": "Phone B",
            "price": 200.0,
            "ingested_at": "2026-10-05T10:00:00Z",
            "event_id": "evt_z",
        },
    ]
    unique_tied, dup_tied = Deduplicator.deduplicate_records(
        tied_records, id_field="product_id", hash_fields=["product_id"]
    )
    assert len(unique_tied) == 1
    assert dup_tied == 1
    assert unique_tied[0]["event_id"] == "evt_z"
    assert unique_tied[0]["price"] == 200.0
