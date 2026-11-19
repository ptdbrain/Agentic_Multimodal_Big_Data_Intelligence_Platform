"""Private staging configuration is exclusive and never enables a second collector."""

from pathlib import Path
import stat
import pytest, yaml

pytestmark = pytest.mark.unit


def test_bootstrap_creates_private_docker_bound_files_without_overwriting(tmp_path):
    from scripts.prepare_application import prepare
    from scripts.run_scheduled_backup import environment

    root = tmp_path / "private"
    prepare(root)
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    for path in root.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    source = yaml.safe_load((root / "source-jobs.yml").read_text())
    serving = yaml.safe_load((root / "serving-datasets.yml").read_text())
    assert {job["adapter"] for job in source["jobs"]} == {"tiki", "web", "simulation"}
    assert source["root"] == "/var/lib/sentinel/captures"
    assert len(serving["datasets"]) == 3
    for job in source["jobs"]:
        assert job["environment_file"] == "/private/" + job["id"] + ".env"
        env = environment(root / (job["id"] + ".env"))
        assert env["POSTGRES_HOST"] == "postgres"
        assert env["MINIO_ENDPOINT"] == "minio:9000"
        assert env["DATA_LAKE_BUCKET"] == job["target"]["bucket"]
        assert env["PGOPTIONS"] == "-c search_path=" + job["target"]["postgres_schema"]
    compose = environment(root / "compose.env")
    assert compose["SENTINEL_PRIVATE_DIR"] == str(root.resolve())
    assert len(compose["POSTGRES_PASSWORD"]) >= 32
    before = (root / "compose.env").read_bytes()
    with pytest.raises(FileExistsError):
        prepare(root)
    assert (root / "compose.env").read_bytes() == before


def test_each_fresh_deployment_receives_a_unique_kafka_cluster_identity(tmp_path):
    import re
    from scripts.prepare_application import prepare
    from scripts.run_scheduled_backup import environment

    identities=[]
    for name in ('one','two'):
        root=tmp_path/name
        prepare(root)
        identity=environment(root/'compose.env')['KAFKA_CLUSTER_ID']
        assert re.fullmatch(r'[A-Za-z0-9_-]{22}',identity)
        identities.append(identity)
    assert identities[0]!=identities[1]
    services=yaml.safe_load((Path(__file__).resolve().parents[1]/'docker-compose.yml').read_text())['services']
    assert 'KAFKA_CLUSTER_ID' in services['kafka']['environment']['CLUSTER_ID']
