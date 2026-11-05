"""A time-bounded stream must exercise invalid records throughout its prefix."""
from collections import Counter
import json
import pytest

pytestmark = pytest.mark.unit


def fixture(index,bad=False):
    return {'review_id':str(index),'product_id':'p','user_id':'anon_redacted','rating':9 if bad else 4,
            'review_title':'Fixture','review_text':'Explicit workload fixture','review_date':'2026-01-01T00:00:00Z'}


def test_invalid_records_are_evenly_represented_before_a_duration_limit_truncates_the_tail():
    from scripts.benchmark import spread_schema_failures
    source=[fixture(i) for i in range(96)]+[fixture(i+96,True) for i in range(4)]
    ordered=spread_schema_failures(source)
    assert len(ordered) == 100
    assert sum(row['rating']==9 for row in ordered[:25]) == 1
    assert sum(row['rating']==9 for row in ordered[:50]) == 2
    assert sum(row['rating']==9 for row in ordered[:75]) == 3
    assert Counter(json.dumps(row,sort_keys=True) for row in ordered) == Counter(json.dumps(row,sort_keys=True) for row in source)


def test_stream_layout_handles_all_valid_all_invalid_and_empty_without_inventing_rows():
    from scripts.benchmark import spread_schema_failures
    for source in [[],[fixture(1)],[fixture(1,True),fixture(2,True)]]:
        assert spread_schema_failures(source) == source
