import pytest
from spark.quality.rules import NonNullRule, RangeRule
from spark.quality.metrics import DataQualityEvaluator

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
        {"product_id": "p2", "rating": 10.0}, # invalid range
        {"product_id": None, "rating": 4.0}   # missing id
    ]
    rules = [NonNullRule("product_id"), RangeRule("rating", 1.0, 5.0)]
    res = DataQualityEvaluator.evaluate_batch("test", records, rules)
    assert res["records_received"] == 3
    assert res["records_valid"] == 1
    assert res["records_invalid"] == 2
    assert res["dq_score"] == 33.33
