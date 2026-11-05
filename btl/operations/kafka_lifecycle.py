"""Durable ownership and fail-closed cleanup for run-scoped article topics."""

from functools import wraps
import hashlib
import json
import time
from config.settings import settings
from database.warehouse import WarehouseManager


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def admitted(function):
    @wraps(function)
    def wrapped(self, *args, **kwargs):
        from operations.backup import writer_admission

        with writer_admission(self.mode):
            return function(self, *args, **kwargs)

    return wrapped


class TopicRegistry:
    def __init__(self, mode=None):
        self.mode = mode
        self.wh = WarehouseManager(mode=mode)
        self.wh.execute(
            "CREATE TABLE IF NOT EXISTS ops_topic_registry(topic TEXT PRIMARY KEY,payload_json TEXT NOT NULL)"
        )
        self.param = "%s" if self.wh._engine_type == "postgres" else "?"

    def close(self):
        self.wh.close()

    def entries(self):
        rows = self.wh.query("SELECT payload_json FROM ops_topic_registry")
        return [json.loads(value) for value in rows["payload_json"]]

    def get(self, topic):
        return next((row for row in self.entries() if row["topic"] == topic), None)

    def _put(self, row):
        cur = self.wh._conn.cursor()
        cur.execute(
            f"INSERT INTO ops_topic_registry VALUES({self.param},{self.param}) ON CONFLICT(topic) DO UPDATE SET payload_json=excluded.payload_json",
            (row["topic"], json.dumps(row, allow_nan=False)),
        )

    def _lock(self):
        if self.wh._engine_type == "postgres":
            self.wh._conn.cursor().execute("SELECT pg_advisory_xact_lock(1397048149)")

    @admitted
    def register(self, topic, run_id, source_group, group, now=None):
        if (
            not topic.startswith(settings.kafka.topic_articles + ".")
            or not topic[len(settings.kafka.topic_articles) + 1 :]
        ):
            raise ValueError("Only owned run-scoped article topics can be registered")
        if not run_id or not group or source_group != "web_content":
            raise ValueError("Invalid topic ownership")
        with self.wh.transaction():
            self._lock()
            if self.get(topic):
                raise ValueError("Topic already owned; resume requires explicit recovery")
            clock = time.time() if now is None else now
            self._put(
                dict(
                    topic=topic,
                    run_id=run_id,
                    source_group=source_group,
                    group=group,
                    state="running",
                    created_at=clock,
                    updated_at=clock,
                    bronze=[],
                )
            )

    @admitted
    def finish(self, topic, bronze, received, accepted, quarantined, now=None):
        with self.wh.transaction():
            self._lock()
            row = self.get(topic)
            if not row or row["state"] != "running":
                raise ValueError("No active topic run")
            if not bronze or received != accepted or quarantined or received <= 0:
                raise ValueError("Transport not reconciled")
            row.update(
                state="complete",
                updated_at=time.time() if now is None else now,
                bronze=bronze,
                received=received,
                accepted=accepted,
                quarantined=quarantined,
            )
            self._put(row)

    @admitted
    def archive_failure(self, topic, proofs, review_ref, verify, now=None):
        if (
            not review_ref
            or {p.get("kind") for p in proofs} != {"bronze", "quality", "quarantine"}
            or not verify(proofs)
        ):
            raise ValueError("Reviewed durable archive required")
        with self.wh.transaction():
            self._lock()
            row = self.get(topic)
            if not row or row["state"] != "failed":
                raise ValueError("Only failed runs need archive review")
            row.update(archive_review=review_ref, bronze=proofs, updated_at=time.time() if now is None else now)
            self._put(row)

    @admitted
    def fail(self, topic):
        with self.wh.transaction():
            self._lock()
            row = self.get(topic)
            if row and row["state"] == "running":
                row.update(state="failed", updated_at=time.time())
                self._put(row)


def eligible(row, broker, verify, now):
    complete = row["state"] == "complete"
    reviewed = row["state"] == "failed" and bool(row.get("archive_review"))
    age = 48 * 3600 if complete else 7 * 86400
    return (
        (complete or reviewed)
        and now - row["updated_at"] >= age
        and row.get("bronze")
        and verify(row["bronze"])
        and broker.safe(row["topic"], row["group"])
    )


def cleanup_plan(registry, broker, verify, now=None):
    now = time.time() if now is None else now
    candidates = [
        dict(topic=r["topic"], fingerprint=digest(r)) for r in registry.entries() if eligible(r, broker, verify, now)
    ]
    plan = dict(version=1, created_at=now, candidates=candidates)
    plan["sha256"] = digest(plan)
    return plan


def _apply_cleanup(registry, broker, verify, plan, now=None):
    now = time.time() if now is None else now
    unsigned = {k: v for k, v in plan.items() if k != "sha256"}
    if plan.get("version") != 1 or plan.get("sha256") != digest(unsigned) or not 0 <= now - plan["created_at"] <= 3600:
        raise ValueError("Invalid or expired cleanup plan")
    deleted = []
    for item in plan["candidates"]:
        with registry.wh.transaction():
            registry._lock()
            row = registry.get(item["topic"])
            if row and row["state"] == "deleted":
                continue
            if not row or digest(row) != item["fingerprint"] or not eligible(row, broker, verify, now):
                raise ValueError("Cleanup eligibility changed")
            broker.delete(row["topic"], row["group"])
            row.update(state="deleted", updated_at=now)
            registry._put(row)
            deleted.append(row["topic"])
    return deleted


class KafkaMaintenance:
    def __init__(self, servers=None):
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient

        self.admin = KafkaAdminClient(bootstrap_servers=servers or settings.kafka.bootstrap_servers)
        self.consumer = KafkaConsumer(
            bootstrap_servers=servers or settings.kafka.bootstrap_servers, enable_auto_commit=False
        )

    def safe(self, topic, group):
        from kafka.structs import TopicPartition

        groups = self.admin.describe_groups([group])
        info = groups.get(group, {})
        if info.get("group_state") not in ("Empty", "Dead") or info.get("members"):
            return False
        parts = self.consumer.partitions_for_topic(topic)
        if parts is None:
            return topic not in self.admin.list_topics()
        partitions = [TopicPartition(topic, p) for p in parts]
        ends = self.consumer.end_offsets(partitions)
        offsets = self.admin.list_group_offsets(group)[group]
        return all(ends[p] == 0 or (p in offsets and offsets[p].offset >= ends[p]) for p in partitions)

    def delete(self, topic, group):
        if topic in self.admin.list_topics():
            self.admin.delete_topics([topic])
        result = self.admin.delete_groups([group])
        items = result.items() if isinstance(result, dict) else result
        for _, error in items:
            if error not in (None, "OK", "GroupIdNotFoundError"):
                raise RuntimeError("Consumer group deletion failed: " + str(error))

    def close(self):
        self.consumer.close()
        self.admin.close()


def verify_bronze(proofs):
    from storage.storage_manager import storage

    return all(digest(storage.read_json(item["path"])) == item["sha256"] for item in proofs)


def apply_cleanup(registry, broker, verify, plan, now=None):
    from operations.backup import writer_admission

    with writer_admission(registry.mode):
        return _apply_cleanup(registry, broker, verify, plan, now)
