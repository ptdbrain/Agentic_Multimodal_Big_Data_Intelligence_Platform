import pytest
import sys
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from data.schemas.versioning import registry

def test_registry_schemas_exist():
    for entity in ["product", "review", "price", "event"]:
        schema = registry.get_schema(entity)
        assert schema is not None
        assert "properties" in schema

def test_valid_product_validation():
    sample_product = {
        "product_id": "test_phone_01",
        "product_name": "Test Smartphone Pro",
        "brand": "Apple",
        "category": "Smartphone",
        "price": 20000000.0,
        "currency": "VND",
        "source": "unit_test"
    }
    is_valid, errors = registry.validate_record("product", sample_product)
    assert is_valid
    assert len(errors) == 0

def test_invalid_review_missing_text():
    sample_review = {
        "review_id": "rv_test_01",
        "product_id": "test_phone_01",
        "user_id": "usr_100",
        "rating": 5.0,
        "review_date": "2026-09-24T12:00:00Z"
        # missing review_text
    }
    is_valid, errors = registry.validate_record("review", sample_review)
    assert not is_valid
    assert any("review_text" in e for e in errors)
