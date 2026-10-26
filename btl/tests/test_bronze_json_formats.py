import json

import pytest

from spark.batch.spark_etl_job import SparkBatchETLJob

pytestmark = pytest.mark.spark


def test_bronze_jsonl_reads_every_line_instead_of_only_the_first_object(spark_session, tmp_path):
    records = [{'review_id': f'r{i}', 'product_id': 'p1', 'rating': 4.0} for i in range(3)]
    path = tmp_path/'reviews.jsonl'
    path.write_text('\n'.join(json.dumps(row) for row in records)+'\n')
    actual = SparkBatchETLJob(spark_session).read_bronze_json(path)
    assert {row['review_id'] for row in actual.collect()} == {'r0', 'r1', 'r2'}


def test_bronze_mixed_json_and_jsonl_paths_do_not_lose_rows(spark_session, tmp_path):
    array = tmp_path/'products.json'
    lines = tmp_path/'more.jsonl'
    array.write_text(json.dumps([{'product_id':'p1'}, {'product_id':'p2'}], indent=2))
    lines.write_text('{"product_id":"p3"}\n{"product_id":"p4"}\n')
    actual = SparkBatchETLJob(spark_session).read_bronze_json([array, lines])
    assert {row['product_id'] for row in actual.collect()} == {'p1', 'p2', 'p3', 'p4'}
