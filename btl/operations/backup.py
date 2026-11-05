"""Coordinated, immutable recovery bundles and explicitly fenced restore.

The fence covers participating application writers. External database clients or
object-store writers must be stopped by the operator before maintenance.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
import uuid

LOCK_KEY = 1397048150
REQUIRED_ROLES = {"warehouse", "lake", "checkpoints", "orchestration", "registry"}


def _hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _safe(root, relative):
    value = PurePosixPath(relative)
    if not relative or value.is_absolute() or ".." in value.parts or "\\" in relative:
        raise ValueError("Unsafe recovery path")
    root = Path(root).resolve()
    target = root.joinpath(*value.parts)
    current = root
    for part in value.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("Recovery path contains symlink")
    if not target.resolve().is_relative_to(root):
        raise ValueError("Recovery path escapes root")
    return target


def _copy_directory(source, destination):
    source, destination = Path(source), Path(destination)
    if source.is_symlink() or not source.is_dir():
        raise ValueError("Source directory missing or symlink")
    destination.mkdir(parents=True, exist_ok=False)
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise ValueError("Source symlink is not a recovery artifact")
        target = destination / path.relative_to(source)
        if path.is_dir():
            target.mkdir(exist_ok=True)
        elif path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
        else:
            raise ValueError("Only regular files may be backed up")


def _pg(component):
    prefix = component.get("env_prefix", "POSTGRES_")
    values = {
        name: os.environ.get(prefix + key, default)
        for name, key, default in [
            ("host", "HOST", "127.0.0.1"),
            ("port", "PORT", "5432"),
            ("user", "USER", ""),
            ("password", "PASSWORD", ""),
            ("dbname", "DB", ""),
        ]
    }
    if not values["dbname"] or not values["user"]:
        raise ValueError("PostgreSQL environment is incomplete")
    return values


def _pg_run(component, command, extra):
    values = _pg(component)
    executable = str(Path(component["binaries_dir"]) / command) if component.get("binaries_dir") else command
    environment = dict(
        os.environ,
        PGHOST=values["host"],
        PGPORT=values["port"],
        PGUSER=values["user"],
        PGPASSWORD=values["password"],
        PGDATABASE=values["dbname"],
    )
    result = subprocess.run([executable, *extra], env=environment, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"{command} failed (code {result.returncode}); credentials/output withheld")


def _pg_fingerprint(component):
    """Multiset of every table's canonical rows, independent of physical order."""
    import psycopg2
    from psycopg2 import sql

    result = {}

    def encode(value):
        if isinstance(value, (bytes, memoryview)):
            return bytes(value).hex()
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    with psycopg2.connect(**_pg(component)) as connection:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        cursor = connection.cursor()
        cursor.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1,2"
        )
        tables = cursor.fetchall()
        for schema, table in tables:
            cursor.execute(sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
            hashes = []
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                hashes.extend(
                    hashlib.sha256(
                        json.dumps(row, default=encode, ensure_ascii=False, separators=(",", ":")).encode()
                    ).digest()
                    for row in rows
                )
            result[schema + "." + table] = {
                "rows": len(hashes),
                "sha256": hashlib.sha256(b"".join(sorted(hashes))).hexdigest(),
                "columns": [column.name for column in cursor.description],
            }
    return result


def _minio(component):
    from minio import Minio

    prefix = component.get("env_prefix", "MINIO_")
    endpoint = os.environ.get(prefix + "ENDPOINT")
    if not endpoint:
        raise ValueError("MinIO endpoint environment is required")
    secure = os.environ.get(prefix + "SECURE", "false").lower()
    if secure not in ("true", "false"):
        raise ValueError("Invalid MinIO secure setting")
    return Minio(
        endpoint,
        access_key=os.environ.get(prefix + "ACCESS_KEY"),
        secret_key=os.environ.get(prefix + "SECRET_KEY"),
        secure=secure == "true",
    )


def _identity(component):
    kind = component["kind"]
    if kind == "directory":
        return {"path": str(Path(component["path"]).resolve())}
    if kind == "postgres":
        values = _pg(component)
        return {key: values[key] for key in ("host", "port", "dbname")}
    if kind == "minio":
        return {
            "endpoint": os.environ.get(component.get("env_prefix", "MINIO_") + "ENDPOINT"),
            "bucket": component["bucket"],
        }
    raise ValueError("Unknown backup component kind")


@contextmanager
def _admission(mode, exclusive=False):
    from database.warehouse import WarehouseManager

    warehouse = WarehouseManager(mode=mode)
    cursor = warehouse._conn.cursor()
    handle = None
    try:
        if warehouse._engine_type == "postgres":
            function = "pg_try_advisory_lock" if exclusive else "pg_try_advisory_lock_shared"
            cursor.execute(f"SELECT {function}(%s)", (LOCK_KEY,))
            if not cursor.fetchone()[0]:
                raise RuntimeError("Maintenance/writer admission is busy")
            warehouse._conn.commit()
        else:
            from config.settings import settings

            lock = Path(str(settings.database.sqlite_path) + ".maintenance.lock")
            lock.parent.mkdir(parents=True, exist_ok=True)
            handle = lock.open("a")
            try:
                fcntl.flock(handle, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError("Maintenance/writer admission is busy") from error
        yield warehouse
    finally:
        if warehouse._engine_type == "postgres":
            # Closing the dedicated connection also releases a session lock.
            warehouse._conn.rollback()
        if handle:
            fcntl.flock(handle, fcntl.LOCK_UN)
            handle.close()
        warehouse.close()


@contextmanager
def writer_admission(mode=None):
    with _admission(mode, exclusive=False):
        yield


@contextmanager
def maintenance_guard(mode=None):
    """Reject active logical runs; shared locks reject active participating writers."""
    with _admission(mode, exclusive=True) as warehouse:
        cursor = warehouse._conn.cursor()
        if warehouse._engine_type == "postgres":
            cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname=current_schema()")
        else:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        if "pipeline_run_lease" in tables:
            cursor.execute("SELECT COUNT(*) FROM pipeline_run_lease")
            if cursor.fetchone()[0]:
                raise ValueError("Active or unrecovered canonical run lease")
        if "ops_topic_registry" in tables:
            cursor.execute("SELECT payload_json FROM ops_topic_registry")
            for row in cursor.fetchall():
                payload = json.loads(row[0])
                if payload.get("state", payload.get("status")) == "running":
                    raise ValueError("Active or unrecovered topic run")
        yield {
            "writers_stopped": True,
            "drained": True,
            "lease_count": 0,
            "scope": "participating-writers; external writers must be stopped",
        }


def _guard(guard):
    if guard is None:
        raise ValueError("A coordinated maintenance guard is required")
    return guard()


def _quiet(state):
    if (
        not isinstance(state, dict)
        or not state.get("writers_stopped")
        or not state.get("drained")
        or state.get("lease_count") != 0
    ):
        raise ValueError("Writers must be stopped, drained and have no active leases")


def create_backup(target, components, guard=None, inventory=None):
    started = time.monotonic()
    roles = {row.get("role") for row in components}
    if not REQUIRED_ROLES.issubset(roles):
        raise ValueError("Missing required recovery components")
    names = [row["name"] for row in components]
    if len(names) != len(set(names)) or any(not re.fullmatch(r"[a-zA-Z0-9_-]+", name) for name in names):
        raise ValueError("Unique safe component names are required")
    if (
        inventory
        and inventory.get("airflow_enabled")
        and not any(row["role"] == "orchestration" and row["kind"] == "postgres" for row in components)
    ):
        raise ValueError("Enabled Airflow requires its metadata database dump")
    target = Path(target)
    if target.is_symlink():
        raise ValueError("Backup target cannot be symlink")
    for row in components:
        if row["kind"] == "directory" and target.resolve().is_relative_to(Path(row["path"]).resolve()):
            raise ValueError("Backup destination cannot be inside a source")
    with _guard(guard) as state:
        _quiet(state)
        target.mkdir(parents=True, exist_ok=True)
        snapshot_at = datetime.now(timezone.utc).isoformat()
        identifier = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:12]
        stage = target / (".incomplete-" + identifier)
        stage.mkdir(mode=0o700)
        rows = []
        for component in components:
            destination = stage / "payload" / component["name"]
            kind = component["kind"]
            if kind == "directory":
                _copy_directory(component["path"], destination)
            else:
                destination.mkdir(parents=True)
                if kind == "postgres":
                    _pg_run(
                        component,
                        "pg_dump",
                        ["--format=custom", "--no-owner", "--no-acl", "--file", str(destination / "database.dump")],
                    )
                elif kind == "minio":
                    client = _minio(component)
                    for obj in client.list_objects(component["bucket"], recursive=True):
                        path = _safe(destination, obj.object_name)
                        path.parent.mkdir(parents=True, exist_ok=True)
                        client.fget_object(component["bucket"], obj.object_name, str(path))
                else:
                    raise ValueError("Unknown component kind")
            fingerprint = _pg_fingerprint(component) if kind == "postgres" else None
            files = [
                {"path": str(path.relative_to(stage).as_posix()), "size": path.stat().st_size, "sha256": _hash(path)}
                for path in sorted(destination.rglob("*"))
                if path.is_file()
            ]
            directories = [
                str(path.relative_to(stage).as_posix()) for path in sorted(destination.rglob("*")) if path.is_dir()
            ]
            rows.append(
                {
                    "name": component["name"],
                    "kind": kind,
                    "role": component["role"],
                    "source": _identity(component),
                    "sql_fingerprint": fingerprint,
                    "files": files,
                    "directories": directories,
                }
            )
        manifest = {
            "schema_version": 1,
            "status": "complete",
            "backup_id": identifier,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_at": snapshot_at,
            "components": rows,
            "inventory": inventory or {},
            "quiescence": state,
            "duration_seconds": time.monotonic() - started,
            "disaster_recovery_certified": False,
        }
        _json(stage / "manifest.json", manifest)
        verify_backup(stage)
        final = target / ("backup-" + identifier)
        stage.rename(final)
        return final


def verify_backup(backup):
    backup = Path(backup)
    if backup.is_symlink() or (backup / "manifest.json").is_symlink():
        raise ValueError("Backup manifest symlink forbidden")
    manifest = json.loads((backup / "manifest.json").read_text())
    if manifest.get("schema_version") != 1 or manifest.get("status") != "complete":
        raise ValueError("Unsupported or incomplete backup")
    if not REQUIRED_ROLES.issubset({row["role"] for row in manifest["components"]}):
        raise ValueError("Incomplete recovery inventory")
    expected = set()
    names = set()
    for component in manifest["components"]:
        name = component["name"]
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", name) or name in names:
            raise ValueError("Invalid component name")
        names.add(name)
        for row in component["files"]:
            path = _safe(backup, row["path"])
            if not row["path"].startswith("payload/" + name + "/") or row["path"] in expected:
                raise ValueError("Invalid component path")
            expected.add(row["path"])
            if not path.is_file() or path.stat().st_size != row["size"] or _hash(path) != row["sha256"]:
                raise ValueError("Backup checksum mismatch")
        for relative in component.get("directories", []):
            if not relative.startswith("payload/" + name + "/") or not _safe(backup, relative).is_dir():
                raise ValueError("Invalid directory path")
    actual = set()
    for path in (backup / "payload").rglob("*"):
        if path.is_symlink():
            raise ValueError("Backup payload symlink forbidden")
        if path.is_file():
            actual.add(path.relative_to(backup).as_posix())
    if actual != expected:
        raise ValueError("Unexpected payload paths")
    return manifest


def list_backups(target):
    result = []
    for path in sorted(Path(target).glob("backup-*")):
        try:
            manifest = verify_backup(path)
        except (ValueError, OSError, KeyError):
            continue
        result.append(
            {"path": str(path), "created_at": manifest["created_at"], "manifest_sha256": _hash(path / "manifest.json")}
        )
    return result


def _check_target(component, target):
    kind = component["kind"]
    merged = dict(target, kind=kind)
    if _identity(merged) == component["source"]:
        raise ValueError("Restore cannot replace the source namespace")
    if kind == "directory":
        path = Path(target["path"])
        if path.exists() or path.is_symlink():
            raise ValueError("Restore destination already exists")
        for parent in path.absolute().parents:
            if parent.is_symlink():
                raise ValueError("Restore ancestor symlink forbidden")
    elif kind == "postgres":
        import psycopg2

        with psycopg2.connect(**_pg(target)) as connection:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')"
            )
            if cursor.fetchone()[0]:
                raise ValueError("Restore database must be empty")
    elif kind == "minio":
        client = _minio(target)
        if (
            client.bucket_exists(target["bucket"])
            and next(client.list_objects(target["bucket"], recursive=True), None) is not None
        ):
            raise ValueError("Restore bucket must be empty")


def restore_plan(backup, targets):
    manifest = verify_backup(backup)
    if set(targets) != {row["name"] for row in manifest["components"]}:
        raise ValueError("Every component requires an explicit restore target")
    destinations = []
    for row in manifest["components"]:
        _check_target(row, targets[row["name"]])
        if row["kind"] == "directory":
            destinations.append(Path(targets[row["name"]]["path"]).resolve())
    for a in destinations:
        for b in destinations:
            if a != b and a.is_relative_to(b):
                raise ValueError("Restore destinations cannot overlap")
    if len(set(destinations)) != len(destinations):
        raise ValueError("Duplicate restore destination")
    return {
        "schema_version": 1,
        "backup": str(Path(backup).resolve()),
        "manifest_sha256": _hash(Path(backup) / "manifest.json"),
        "targets": targets,
        "target_identities": {
            row["name"]: _identity(dict(targets[row["name"]], kind=row["kind"])) for row in manifest["components"]
        },
    }


def restore_apply(backup, plan, guard=None):
    with _guard(guard) as state:
        _quiet(state)
        if plan != restore_plan(backup, plan["targets"]):
            raise ValueError("Restore plan changed or stale")
        manifest = verify_backup(backup)
        for component in manifest["components"]:
            target = plan["targets"][component["name"]]
            source = Path(backup) / "payload" / component["name"]
            if component["kind"] == "directory":
                _copy_directory(source, target["path"])
            elif component["kind"] == "postgres":
                _pg_run(
                    target,
                    "pg_restore",
                    [
                        "--exit-on-error",
                        "--single-transaction",
                        "--no-owner",
                        "--no-acl",
                        "--dbname",
                        _pg(target)["dbname"],
                        str(source / "database.dump"),
                    ],
                )
            else:
                client = _minio(target)
                if not client.bucket_exists(target["bucket"]):
                    client.make_bucket(target["bucket"])
                for row in component["files"]:
                    path = _safe(backup, row["path"])
                    client.fput_object(target["bucket"], path.relative_to(source).as_posix(), str(path))
        report = restore_verify(backup, plan["targets"])
        report["writers_resumed"] = False
        return report


def restore_verify(backup, targets):
    manifest = verify_backup(backup)
    for component in manifest["components"]:
        target = targets[component["name"]]
        prefix = "payload/" + component["name"] + "/"
        if component["kind"] == "directory":
            expected = set()
            for row in component["files"]:
                relative = row["path"][len(prefix) :]
                expected.add(relative)
                path = _safe(target["path"], relative)
                if not path.is_file() or _hash(path) != row["sha256"]:
                    raise ValueError("Restored checksum mismatch")
            actual = {p.relative_to(target["path"]).as_posix() for p in Path(target["path"]).rglob("*") if p.is_file()}
            if actual != expected:
                raise ValueError("Unexpected restored file")
        elif component["kind"] == "minio":
            client = _minio(target)
            expected = {row["path"][len(prefix) :]: row["sha256"] for row in component["files"]}
            objects = list(client.list_objects(target["bucket"], recursive=True))
            if {obj.object_name for obj in objects} != set(expected):
                raise ValueError("Restored object inventory mismatch")
            for obj in objects:
                response = client.get_object(target["bucket"], obj.object_name)
                try:
                    digest = hashlib.sha256()
                    for chunk in response.stream(1024 * 1024):
                        digest.update(chunk)
                finally:
                    response.close()
                    response.release_conn()
                if digest.hexdigest() != expected[obj.object_name]:
                    raise ValueError("Restored object checksum mismatch")
        else:
            if _pg_fingerprint(target) != component.get("sql_fingerprint"):
                raise ValueError("Restored PostgreSQL business fingerprint mismatch")
    return {"verified": True, "backup_id": manifest["backup_id"], "components": len(manifest["components"])}


def retention_plan(backups):
    ordered = sorted(backups, key=lambda row: row["created_at"], reverse=True)
    keep = set()
    days = set()
    weeks = set()
    for row in ordered:
        date = datetime.fromisoformat(row["created_at"])
        day = date.date()
        week = date.isocalendar()[:2]
        if day not in days and len(days) < 7:
            keep.add(row["path"])
            days.add(day)
        if week not in weeks and len(weeks) < 4:
            keep.add(row["path"])
            weeks.add(week)
    if ordered:
        keep.add(ordered[0]["path"])
    return [row["path"] for row in ordered if row["path"] not in keep]


def kafka_drain_state(bootstrap_servers, groups, topics):
    """Check committed offsets only after admission is fenced and writers stopped."""
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition

    if not groups or not topics:
        raise ValueError("Explicit Kafka groups and topics are required for drain verification")
    offsets = {}
    for group in groups:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=group, enable_auto_commit=False)
        try:
            positions = []
            for topic in topics:
                partitions = consumer.partitions_for_topic(topic)
                if partitions is None:
                    raise ValueError("Drain topic does not exist")
                positions.extend(TopicPartition(topic, partition) for partition in partitions)
            ends = consumer.end_offsets(positions)
            for position, end in ends.items():
                committed = consumer.committed(position)
                if (committed is None and end != 0) or (committed is not None and committed < end):
                    raise ValueError("Kafka has unarchived or uncommitted records; drain before backup")
                offsets[f"{group}/{position.topic}/{position.partition}"] = {"committed": committed, "end": end}
        finally:
            consumer.close()
    return offsets


def retention_apply(target, plan):
    """Apply an explicit plan only if every source manifest still matches."""
    target = Path(target).resolve()
    inventory = list_backups(target)
    if plan.get("backup_target") != str(target) or plan.get("inventory") != inventory:
        raise ValueError("Stale retention inventory")
    eligible = set(retention_plan(inventory))
    requested = plan.get("remove", [])
    if len(requested) != len(set(requested)) or not set(requested).issubset(eligible):
        raise ValueError("Unsafe retention deletion request")
    for value in requested:
        path = Path(value)
        if path.parent.resolve() != target or path.is_symlink() or not path.name.startswith("backup-"):
            raise ValueError("Unsafe retention path")
        verify_backup(path)
    for value in requested:
        shutil.rmtree(value)
    return {"deleted": requested, "kept": len(inventory) - len(requested)}
