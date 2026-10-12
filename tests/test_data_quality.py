import pytest
from spark.quality.rules import NonNullRule, RangeRule, PositiveNumberRule
from spark.quality.metrics import DataQualityEvaluator

pytestmark = pytest.mark.unit


def test_data_quality_rules():
    rule_null = NonNullRule("product_id")
    rule_range = RangeRule("rating", 1.0, 5.0)

    ok1, _ = rule_null.validate({"product_id": "p1"})
    assert ok1 is True
    ok2, _ = rule_null.validate({"product_id": ""})
    assert ok2 is False

    ok3, _ = rule_range.validate({"rating": 4.5})
    assert ok3 is True
    ok4, _ = rule_range.validate({"rating": 6.5})
    assert ok4 is False


def test_data_quality_evaluator():
    records = [
        {"product_id": "p1", "rating": 5.0},
        {"product_id": "p2", "rating": 10.0},  # invalid range
        {"product_id": None, "rating": 4.0},  # missing id
    ]
    rules = [NonNullRule("product_id"), RangeRule("rating", 1.0, 5.0)]
    res = DataQualityEvaluator.evaluate_batch("test", records, rules)
    assert res["records_received"] == 3
    assert res["records_valid"] == 1
    assert res["records_invalid"] == 2
    assert res["dq_score"] == 33.33


def test_dq_counts_nulls_duplicates_and_multi_error_row_counted_once():
    """Verifies that:
    1. Field-level null counts accurately count every null/empty field cell.
    2. Duplicate rows are counted accurately in records_duplicate.
    3. A single row failing MULTIPLE rules + type cast errors is counted ONLY ONCE in records_invalid.
    """
    rules = [
        NonNullRule("review_id"),
        NonNullRule("product_id"),
        NonNullRule("review_text"),
        RangeRule("rating", 1.0, 5.0),
        PositiveNumberRule("price", allow_zero=False),
    ]
    records = [
        # Row 1: completely valid
        {
            "review_id": "rv_1",
            "product_id": "p1",
            "review_text": "Great product",
            "rating": "4.5",
            "price": "1000.0",
            "review_date": "2026-10-01T10:00:00Z",
        },
        # Row 2: duplicate primary key (rv_1)
        {
            "review_id": "rv_1",
            "product_id": "p1",
            "review_text": "Duplicate review",
            "rating": 5.0,
            "price": 1000.0,
            "review_date": "2026-10-01T11:00:00Z",
        },
        # Row 3: single row with MULTIPLE simultaneous failures (product_id=None, review_text="",
        # rating=99.0 out of range, price=-50 negative, review_date="bad-date" cast error)
        {
            "review_id": "rv_multi_err",
            "product_id": None,
            "review_text": "   ",
            "rating": 99.0,
            "price": -50.0,
            "review_date": "not-a-timestamp",
        },
    ]

    res = DataQualityEvaluator.evaluate_batch("reviews", records, rules, primary_key="review_id")

    assert res["records_received"] == 3
    assert res["records_valid"] == 1
    assert res["records_duplicate"] == 1
    # Row 2 (duplicate) + Row 3 (multi-error) = 2 invalid rows total (Row 3 is NOT counted multiple times)
    assert res["records_invalid"] == 2
    assert res["records_valid"] + res["records_invalid"] == res["records_received"]

    # Field-level null counts: product_id is None once, review_text is blank once -> total_field_nulls == 2
    assert res["field_null_counts"]["product_id"] == 1
    assert res["field_null_counts"]["review_text"] == 1
    assert res["field_null_counts"]["review_id"] == 0
    assert res["total_field_nulls"] == 2
    assert res["cast_error_count"] == 1
