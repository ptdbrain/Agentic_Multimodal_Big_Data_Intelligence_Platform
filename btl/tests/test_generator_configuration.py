"""Simulation provenance must record effective generation parameters, not an empty config."""
from datetime import datetime,timezone
import json
import pytest
from data.generator import generate_products,generate_reviews,generate_price_history,generate_streaming_events

pytestmark = pytest.mark.unit


def test_review_configuration_describes_the_actual_clean_duplicate_and_invalid_workload():
    products=generate_products(seed=7)
    reviews=generate_reviews(products,count=100,seed=7,inject_anomalies=False,dirty_ratio=.1,duplicate_ratio=.2)
    config=json.loads(reviews[0]['generator_config'])
    assert config['requested_count'] == config['output_records'] == 100
    assert config['effective_dirty_records'] == 10
    assert config['effective_duplicate_records'] == 20
    assert config['inject_anomalies'] is False
    assert config['input_products_sha256']
    assert {row['generator_config'] for row in reviews} == {reviews[0]['generator_config']}
    assert reviews == generate_reviews(products,count=100,seed=7,inject_anomalies=False,dirty_ratio=.1,duplicate_ratio=.2)


def test_product_configuration_records_reference_clock_currency_and_input_fingerprint():
    metadata=[{'id':'fixture','name':'Fixture','brand':'Fixture','category':'Phone','base_price':100}]
    first=generate_products(metadata,reference_date=datetime(2026,1,1,tzinfo=timezone.utc),currency='USD',seed=7)
    config=json.loads(first[0]['generator_config'])
    assert config['reference_date'] == '2026-01-01T00:00:00+00:00'
    assert config['currency'] == 'USD'
    assert config['output_records'] == 1
    changed=generate_products([dict(metadata[0],base_price=200)],seed=7)
    assert json.loads(changed[0]['generator_config'])['metadata_sha256'] != config['metadata_sha256']


def test_event_and_price_generators_bind_the_input_catalog_and_parameters():
    products=generate_products(seed=7)
    events=generate_streaming_events(products,count=20,seed=7)
    prices=generate_price_history(products,days_history=2,seed=7)
    event_config=json.loads(events[0]['generator_config'])
    price_config=json.loads(prices[0]['generator_config'])
    assert event_config['requested_count'] == event_config['output_records'] == 20
    assert price_config['days_history'] == 2
    assert price_config['output_records'] == len(prices)
    assert event_config['input_products_sha256'] == price_config['input_products_sha256']


def test_price_simulation_keeps_small_usd_values_positive_and_inherits_catalog_currency():
    metadata=[{'id':'fixture','name':'Fixture','brand':'Fixture','category':'Phone','base_price':100}]
    products=generate_products(metadata,currency='USD',seed=7)
    prices=generate_price_history(products,days_history=10,seed=7)
    assert {row['currency'] for row in prices} == {'USD'}
    assert all(row['price'] > 0 for row in prices)


def test_price_simulation_does_not_relabel_catalog_money_without_fx_conversion():
    products=generate_products(currency='VND',seed=7)
    with pytest.raises(ValueError,match='currency'):
        generate_price_history(products,days_history=2,currency='USD',seed=7)
