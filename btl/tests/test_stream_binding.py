"""A registry CLI must not combine SQL for one dataset with another process runtime."""

import pytest

pytestmark = pytest.mark.unit


def test_stream_binding_requires_private_runtime_and_dataset_paths(tmp_path):
    from serving.runtime import validate_stream_binding
    from serving.registry import DatasetRegistry
    from tests.test_serving_registry import registry_file

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    with pytest.raises(ValueError, match="dataset|runtime"):
        validate_stream_binding(binding, {}, {})
    with pytest.raises(ValueError, match="path|lake"):
        validate_stream_binding(
            binding,
            {"RUNTIME_MODE": "test", "SQLITE_DB_PATH": str(binding.sqlite_path)},
            {"checkpoint_dir": str(tmp_path / "outside")},
        )


def test_stream_binding_accepts_its_local_runtime_but_rejects_other_lake(tmp_path):
    from serving.runtime import validate_stream_binding
    from serving.registry import DatasetRegistry
    from scripts.run_scheduled_backup import environment
    from tests.test_serving_registry import registry_file

    binding = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    root = tmp_path / "owned-lake"
    binding.environment_file.write_text(binding.environment_file.read_text() + "\nLOCAL_DATA_DIR=" + str(root) + "\n")
    env = environment(binding.environment_file)
    paths = {name: str(root / name) for name in ("checkpoint_dir", "output_path", "quarantine_path")}
    validate_stream_binding(binding, env, paths)
    with pytest.raises(ValueError, match="escapes"):
        validate_stream_binding(binding, env, {**paths, "output_path": str(tmp_path / "another-lake")})


def test_stream_runtime_schema_must_equal_sql_sink_schema(tmp_path):
    from dataclasses import replace
    from serving.registry import DatasetRegistry
    from serving.runtime import validate_stream_binding
    from scripts.prepare_application import _env
    from tests.test_serving_registry import registry_file

    local = DatasetRegistry.load(registry_file(tmp_path)).datasets["tiki"]
    binding = replace(local, sqlite_path=None, mode="integration")
    keys = (
        "RUNTIME_MODE",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "PGOPTIONS",
        "DATA_LAKE_BUCKET",
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "KAFKA_BOOTSTRAP_SERVERS",
    )
    env = dict.fromkeys(keys, "explicit-test-value")
    env.update(RUNTIME_MODE="integration", DATA_LAKE_BUCKET=binding.bucket, PGOPTIONS="-c search_path=other_schema",
               MINIO_ENDPOINT_DOCKER=env["MINIO_ENDPOINT"])
    binding.environment_file.write_text(_env(env))
    paths = {
        name: f"s3a://{binding.bucket}/fixture/{name}" for name in ("checkpoint_dir", "output_path", "quarantine_path")
    }
    with pytest.raises(ValueError, match="schema"):
        validate_stream_binding(binding, env, paths)
    env["PGOPTIONS"] = "-c search_path=" + binding.schema
    binding.environment_file.write_text(_env(env))
    validate_stream_binding(binding, env, paths)
    docker_env = dict(env, RUNNING_IN_DOCKER="true", MINIO_ENDPOINT_DOCKER="restored-lake:9000")
    binding.environment_file.write_text(_env(docker_env))
    validate_stream_binding(binding, docker_env, paths)
    with pytest.raises(ValueError, match="dataset|runtime"):
        validate_stream_binding(binding, dict(docker_env, MINIO_ENDPOINT_DOCKER="original-lake:9000"), paths)
    with pytest.raises(ValueError, match="dataset|runtime"):
        validate_stream_binding(binding, dict(docker_env, MINIO_SECURE="true"), paths)


def test_generation_pins_every_sink_and_semantic_setting_before_restart(tmp_path):
    from serving.runtime import stream_configuration
    from tests.test_stream_serving import store

    paths = {name: f's3a://fixture/{name}' for name in ('checkpoint_dir', 'output_path', 'quarantine_path')}
    base = dict(window_seconds=60, watermark_seconds=600, topic='reviews', paths=paths,
                output_mode='update', negative_threshold=2.0, kafka_cluster_id='original-cluster')
    s = store(tmp_path)
    s.register_stream('g', stream_configuration(**base))
    for changed in [dict(base, paths=dict(paths, quarantine_path='s3a://fixture/other')),
                    dict(base, negative_threshold=1.0), dict(base, output_mode='complete'),
                    dict(base, kafka_cluster_id='replacement-cluster'), dict(base, audit_path='/tmp/new-audit')]:
        with pytest.raises(ValueError, match='configuration'):
            s.register_stream('g', stream_configuration(**changed))
    assert stream_configuration(**base)['serving_protocol'] == 3
    s.conn.close()
