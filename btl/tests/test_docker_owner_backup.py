from pathlib import PurePosixPath

import pytest


def test_backup_archives_owner_history_without_sweeping_other_app_state():
    from scripts.backup_docker_owner import capture_tree

    assert capture_tree("/var/lib/sentinel/captures") == PurePosixPath("/var/lib/sentinel/captures")
    assert capture_tree("/var/lib/sentinel/live/one/captures") == PurePosixPath("/var/lib/sentinel/live/one")
    for path in ("/", "/var/lib/sentinel", "/var/lib/sentinel/../private/captures", "/private/captures"):
        with pytest.raises(ValueError):
            capture_tree(path)


def test_every_job_must_write_to_the_warehouse_and_bucket_being_backed_up():
    from scripts.backup_docker_owner import validate_targets

    baseline = {"POSTGRES_HOST": "postgres", "POSTGRES_PORT": "5432", "POSTGRES_DB": "warehouse",
                "MINIO_ENDPOINT": "minio:9000", "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"}
    job = {"namespace": "docker-web", "target": {"bucket": "lake-web", "postgres_schema": "source_web"}}
    env = dict(baseline, SOURCE_JOB_NAMESPACE="docker-web", DATA_LAKE_BUCKET="lake-web", PGOPTIONS="-c search_path=source_web")
    validate_targets(baseline, [job], [env])
    for key, other in [("POSTGRES_DB", "other"), ("DATA_LAKE_BUCKET", "other"), ("PGOPTIONS", "-c search_path=wrong")]:
        with pytest.raises(ValueError):
            validate_targets(baseline, [job], [dict(env, **{key: other})])
