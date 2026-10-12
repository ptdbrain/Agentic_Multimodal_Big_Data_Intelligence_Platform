import pytest
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from data.schemas.versioning import registry
from data.schemas.validator import SchemaValidator
from kafka.consumers.raw_consumer import RawConsumer

pytestmark = pytest.mark.unit


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
        "source": "unit_test",
    }
    is_valid, errors = registry.validate_record("product", sample_product)
    assert is_valid
    assert len(errors) == 0

    ok_jsonschema, errs_jsonschema = SchemaValidator.validate_record(sample_product, "product")
    assert ok_jsonschema
    assert errs_jsonschema == []


def test_invalid_review_missing_text():
    sample_review = {
        "review_id": "rv_test_01",
        "product_id": "test_phone_01",
        "user_id": "usr_100",
        "rating": 5.0,
        "review_title": "Great",
        "review_date": "2026-09-24T12:00:00Z",
        # missing review_text
    }
    is_valid, errors = registry.validate_record("review", sample_review)
    assert not is_valid
    assert any("review_text" in e for e in errors)

    ok_schema, schema_errs = SchemaValidator.validate_record(sample_review, "review")
    assert not ok_schema
    assert any("review_text" in e for e in schema_errs)


def test_contract_missing_required_fields_all_entities():
    # 1. Product missing brand & price
    ok_p, errs_p = registry.validate_record(
        "product",
        {"product_id": "p1", "product_name": "Phone", "category": "Smartphone", "currency": "VND", "source": "s"},
    )
    assert not ok_p
    assert any("brand" in e for e in errs_p)
    assert any("price" in e for e in errs_p)

    # 2. Price missing timestamp
    ok_pr, errs_pr = SchemaValidator.validate_record(
        {"price_id": "pr_1", "product_id": "p1", "price": 100.0, "currency": "VND"},
        "price",
    )
    assert not ok_pr
    assert any("timestamp" in e for e in errs_pr)

    # 3. Envelope missing ingested_at and payload
    consumer = RawConsumer()
    ok_env, errs_env = consumer.validate_envelope(
        {"event_id": "evt_1", "event_type": "NEW_REVIEW", "source": "test"}
    )
    assert not ok_env
    assert any("ingested_at" in e for e in errs_env)
    assert any("payload" in e for e in errs_env)


def test_contract_wrong_field_types_detected():
    # String instead of number for price, and bool instead of integer for review_count
    bad_product = {
        "product_id": "p1",
        "product_name": "Phone",
        "brand": "Apple",
        "category": "Smartphone",
        "price": "not_a_number",
        "review_count": True,
        "currency": "VND",
        "source": "unit_test",
    }
    ok_reg, errs_reg = registry.validate_record("product", bad_product)
    assert not ok_reg
    assert any("price" in e for e in errs_reg)
    assert any("review_count" in e for e in errs_reg)

    ok_val, errs_val = SchemaValidator.validate_record(bad_product, "product")
    assert not ok_val
    assert len(errs_val) > 0

    # Out-of-range rating (rating = 9.5 > 5.0)
    bad_review = {
        "review_id": "rv_1",
        "product_id": "p1",
        "user_id": "anon_1234567890abcdef",
        "rating": 9.5,
        "review_title": "Title",
        "review_text": "Valid text",
        "review_date": "2026-09-24T12:00:00Z",
    }
    ok_r_reg, errs_r_reg = registry.validate_record("review", bad_review)
    assert not ok_r_reg
    assert any("rating" in e for e in errs_r_reg)

    ok_r_val, _ = SchemaValidator.validate_record(bad_review, "review")
    assert not ok_r_val


def test_contract_wrong_schema_version_detected():
    valid_product_wrong_ver = {
        "schema_version": "2.0",
        "product_id": "p1",
        "product_name": "Phone",
        "brand": "Apple",
        "category": "Smartphone",
        "price": 15000000.0,
        "currency": "VND",
        "source": "unit_test",
    }
    ok_reg, errs_reg = registry.validate_record("product", valid_product_wrong_ver, expected_version="1.0")
    assert not ok_reg
    assert any("schema_version" in e for e in errs_reg)

    ok_val, errs_val = SchemaValidator.validate_record(valid_product_wrong_ver, "product", expected_version="1.0")
    assert not ok_val
    assert any("schema_version" in e for e in errs_val)

    consumer = RawConsumer()
    bad_envelope = {
        "event_id": "evt_123",
        "event_type": "NEW_PRODUCT",
        "schema_version": "99.0",
        "source": "unit_test",
        "ingested_at": "2026-10-12T00:00:00Z",
        "payload": {
            "product_id": "p1",
            "product_name": "Phone",
            "brand": "Apple",
            "category": "Smartphone",
            "price": 15000000.0,
            "currency": "VND",
            "source": "unit_test",
        },
    }
    ok_msg, msg_errs, _ = consumer.validate_message("raw.products", bad_envelope)
    assert not ok_msg
    assert any("schema_version" in e for e in msg_errs)
