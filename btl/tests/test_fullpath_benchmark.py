"""Synthetic full-route workload accounting is independently checkable."""
import json
import pytest

pytestmark = pytest.mark.unit


def test_fullpath_fixture_preserves_exact_duplicate_and_invalid_time_ratios(tmp_path):
    from scripts.benchmark_fullpath import write_workload
    paths, manifest = write_workload(tmp_path, 100)
    rows = [json.loads(line) for line in paths['reviews_path'].read_text().splitlines()]
    assert len(rows) == 100
    assert rows[92:95] == rows[:3]
    assert len({row['review_id'] for row in rows[:92]}) == 92
    assert [row['review_date'] for row in rows[95:]] == ['unparseable']*5
    assert all(row['source_group'] == 'simulation' and row['acquisition_method'] == 'generated' for row in rows)
    assert manifest['expected_reviews'] == {'input':100,'silver':92,'quarantine':5,'duplicates':3}
    assert manifest['products']['records'] == 100
    assert manifest['prices']['records'] == 10
    copy = tmp_path/'repeat'
    copy.mkdir()
    _, repeated = write_workload(copy, 100)
    assert manifest == repeated


def test_existing_fullpath_report_is_rejected_before_any_service_is_contacted(tmp_path):
    from scripts.benchmark_fullpath import run_benchmark
    output = tmp_path/'existing.json'
    output.write_text('original evidence\n')
    with pytest.raises(FileExistsError):
        run_benchmark(100, output)
    assert output.read_text() == 'original evidence\n'


@pytest.mark.parametrize('count', [0, -100, 101])
def test_workload_rejects_sizes_that_cannot_have_exact_accounting(tmp_path, count):
    from scripts.benchmark_fullpath import write_workload
    destination = tmp_path/'inputs'
    destination.mkdir()
    with pytest.raises(ValueError):
        write_workload(destination, count)
    assert list(destination.iterdir()) == []


def test_existing_velocity_report_is_not_replaced_or_contact_services(tmp_path):
    from scripts.benchmark_fullpath import run_velocity
    report = tmp_path/'velocity.json'
    report.write_text('preserved\n')
    with pytest.raises(FileExistsError):
        run_velocity(50,120,report)
    assert report.read_text() == 'preserved\n'
