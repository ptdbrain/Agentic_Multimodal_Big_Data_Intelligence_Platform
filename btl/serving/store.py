"""Transactional serving windows with durable, content-verified micro-batch receipts."""

from datetime import datetime, timezone, timedelta
from collections.abc import Iterable, Mapping
import hashlib, json, math, re, sqlite3
from serving.admission import writer_transaction

SCHEMA = """
CREATE TABLE IF NOT EXISTS publication_lineage (
 dataset_id TEXT NOT NULL,generation TEXT NOT NULL,source_group TEXT NOT NULL,
 run_id TEXT NOT NULL,receipt_json TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,created_at TEXT NOT NULL,
 PRIMARY KEY(dataset_id,generation));

CREATE TABLE IF NOT EXISTS serving_alerts (
 dataset_id TEXT NOT NULL,event_id TEXT NOT NULL,generation TEXT NOT NULL,source_group TEXT NOT NULL,
 product_id TEXT NOT NULL,event_type TEXT NOT NULL,rule TEXT NOT NULL,state TEXT NOT NULL,
 window_start TEXT NOT NULL,window_end TEXT NOT NULL,score DOUBLE PRECISION,
 evidence_json TEXT NOT NULL,payload_sha256 TEXT NOT NULL,revision BIGINT NOT NULL,
 detected_at TEXT NOT NULL,updated_at TEXT NOT NULL,PRIMARY KEY(dataset_id,event_id));
CREATE INDEX IF NOT EXISTS serving_alerts_by_state ON serving_alerts(dataset_id,state,window_start);
CREATE TABLE IF NOT EXISTS serving_alert_history (
 dataset_id TEXT NOT NULL,event_id TEXT NOT NULL,revision BIGINT NOT NULL,generation TEXT NOT NULL,
 payload_json TEXT NOT NULL,observed_at TEXT NOT NULL,PRIMARY KEY(dataset_id,event_id,revision));
CREATE TABLE IF NOT EXISTS alert_notifications (
 notification_id TEXT PRIMARY KEY,dataset_id TEXT NOT NULL,event_id TEXT NOT NULL,revision BIGINT NOT NULL,
 state TEXT NOT NULL,payload_json TEXT NOT NULL,created_at TEXT NOT NULL,delivered_at TEXT,attempts INTEGER NOT NULL DEFAULT 0,error_kind TEXT);

CREATE TABLE IF NOT EXISTS search_jobs (
 dataset_id TEXT NOT NULL,generation TEXT NOT NULL,entity_kind TEXT NOT NULL,
 state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,error_kind TEXT,
 index_name TEXT,document_count BIGINT,content_sha256 TEXT,created_at TEXT NOT NULL,completed_at TEXT,
 PRIMARY KEY(dataset_id,generation,entity_kind));

CREATE TABLE IF NOT EXISTS stream_deployments (dataset_id TEXT NOT NULL,generation TEXT NOT NULL,configuration TEXT NOT NULL,state TEXT NOT NULL,started_at TEXT NOT NULL,heartbeat_at TEXT,watermark TEXT,last_batch BIGINT,PRIMARY KEY(dataset_id,generation));
CREATE UNIQUE INDEX IF NOT EXISTS stream_one_current_generation ON stream_deployments(dataset_id) WHERE state!='retired';
CREATE TABLE IF NOT EXISTS serving_generation (
 dataset_id TEXT PRIMARY KEY, generation TEXT NOT NULL, run_id TEXT NOT NULL,
 source_group TEXT NOT NULL, published_at TEXT NOT NULL, data_as_of TEXT, kind TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS stream_batches (
 dataset_id TEXT NOT NULL,generation TEXT NOT NULL,batch_id BIGINT NOT NULL,
 sha256 TEXT NOT NULL,row_count BIGINT NOT NULL,committed_at TEXT NOT NULL,
 PRIMARY KEY(dataset_id,generation,batch_id));
CREATE TABLE IF NOT EXISTS stream_windows (
 dataset_id TEXT NOT NULL,generation TEXT NOT NULL,window_seconds BIGINT NOT NULL,
 product_id TEXT NOT NULL,source_group TEXT NOT NULL,window_start TEXT NOT NULL,window_end TEXT NOT NULL,
 review_count BIGINT NOT NULL,avg_rating DOUBLE PRECISION NOT NULL,negative_count BIGINT NOT NULL,
 batch_id BIGINT NOT NULL,updated_at TEXT NOT NULL,
 PRIMARY KEY(dataset_id,generation,window_seconds,product_id,window_start));
"""


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", value):
        raise ValueError("Explicit safe serving identity required")
    return value


def instant(value) -> datetime:
    point = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if point.tzinfo is None:
        raise ValueError("Serving clock requires explicit timezone")
    return point.astimezone(timezone.utc)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_dataset(binding):
    if binding.sqlite_path is not None:
        connection = sqlite3.connect(binding.sqlite_path, timeout=10)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection
    import psycopg2

    return psycopg2.connect(**dict(binding.connection))


class ServingStore:
    def __init__(self, connection, dataset_id: str, source_group: str):
        self.conn = connection
        self.dataset_id = identifier(dataset_id)
        self.source_group = source_group
        if source_group not in {"marketplace", "simulation", "unverified", "web_content"}:
            raise ValueError("Invalid source group")
        self.sqlite = connection.__class__.__module__.startswith("sqlite3")
        self.param = "?" if self.sqlite else "%s"

    def execute(self, sql: str, values=()):
        cursor = self.conn.cursor()
        cursor.execute(sql.replace("?", self.param), values)
        return cursor

    def initialize(self) -> None:
        for statement in SCHEMA.split(";"):
            if statement.strip():
                self.execute(statement)

    def stage_evaluations(self, generation: str, evaluations: Iterable[Mapping]) -> int:
        """Persist evaluations/history/delivery outbox within caller transaction."""
        generation = identifier(generation)
        changed = 0
        for value in evaluations:
            value = dict(value)
            event = identifier(value["event_id"])
            state = value["state"]
            if state not in {"firing", "clear", "insufficient_data"}:
                raise ValueError("Invalid statistical evaluation state")
            evidence = value["evidence"]
            if state == "firing" and evidence.get("quality_status") != "valid":
                raise ValueError("Invalid quality cannot emit a firing alert")
            start = instant(value["window_start"])
            end = instant(value["window_end"])
            if end <= start:
                raise ValueError("Invalid alert window")
            payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
            digest = hashlib.sha256(payload.encode()).hexdigest()
            old = self.execute(
                "SELECT payload_sha256,revision,state,event_type FROM serving_alerts WHERE dataset_id=? AND event_id=?",
                (self.dataset_id, event),
            ).fetchone()
            if old and old[0] == digest:
                self.execute(
                    "UPDATE serving_alerts SET generation=? WHERE dataset_id=? AND event_id=?",
                    (generation, self.dataset_id, event),
                )
                continue
            revision = (old[1] if old else 0) + 1
            clock = now()
            self.execute(
                """INSERT INTO serving_alerts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(dataset_id,event_id) DO UPDATE SET
                generation=excluded.generation,event_type=excluded.event_type,state=excluded.state,score=excluded.score,
                evidence_json=excluded.evidence_json,payload_sha256=excluded.payload_sha256,revision=excluded.revision,updated_at=excluded.updated_at""",
                (
                    self.dataset_id,
                    event,
                    generation,
                    self.source_group,
                    value["product_id"],
                    value["event_type"],
                    value["rule"],
                    state,
                    start.isoformat(),
                    end.isoformat(),
                    value.get("score"),
                    json.dumps(evidence, sort_keys=True, allow_nan=False),
                    digest,
                    revision,
                    clock,
                    clock,
                ),
            )
            self.execute(
                "INSERT INTO serving_alert_history VALUES(?,?,?,?,?,?)",
                (self.dataset_id, event, revision, generation, payload, clock),
            )
            changed += 1
            notify = (
                state == "firing"
                and (not old or old[2] != "firing" or old[3] != value["event_type"])
                or state == "clear"
                and old
                and old[2] == "firing"
            )
            if notify:
                status = "firing" if state == "firing" else "resolved"
                notification_id = hashlib.sha256(
                    (self.dataset_id + ":" + event + ":" + str(revision) + ":" + status).encode()
                ).hexdigest()
                notification = {
                    "status": status,
                    "alerts": [
                        {
                            "status": status,
                            "labels": {
                                "alertname": value["event_type"],
                                "dataset_id": self.dataset_id,
                                "source": self.source_group,
                                "event_id": event,
                            },
                            "annotations": {
                                "summary": value["event_type"],
                                "product_id": value["product_id"],
                                "configuration_sha256": evidence.get("configuration_sha256", ""),
                            },
                            "startsAt": start.isoformat(),
                            "endsAt": end.isoformat() if status == "resolved" else None,
                        }
                    ],
                }
                self.execute(
                    "INSERT INTO alert_notifications(notification_id,dataset_id,event_id,revision,state,payload_json,created_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        notification_id,
                        self.dataset_id,
                        event,
                        revision,
                        status,
                        json.dumps(notification, sort_keys=True, allow_nan=False),
                        clock,
                    ),
                )
        return changed

    def stage_publication(self, generation: str, run_id: str, kind: str, *, data_as_of: str | None = None) -> None:
        """Stage inside the caller's warehouse transaction; NEVER commit here."""
        generation = identifier(generation)
        run_id = identifier(run_id)
        if kind not in {"commerce", "web"} or (kind == "web") != (self.source_group == "web_content"):
            raise ValueError("Publication kind/source mismatch")
        if data_as_of is not None:
            data_as_of = instant(data_as_of).isoformat()
        current = self.execute(
            "SELECT generation,run_id,source_group,kind FROM serving_generation WHERE dataset_id=?", (self.dataset_id,)
        ).fetchone()
        known = self.execute(
            "SELECT 1 FROM search_jobs WHERE dataset_id=? AND generation=?", (self.dataset_id, generation)
        ).fetchone()
        if current and current[0] != generation and known:
            raise ValueError("Older publication generation cannot replace current snapshot")
        if current and current[0] == generation:
            if tuple(current[1:]) != (run_id, self.source_group, kind):
                raise ValueError("Publication generation identity changed")
        else:
            self.execute(
                """INSERT INTO serving_generation VALUES(?,?,?,?,?,?,?) ON CONFLICT(dataset_id)
                DO UPDATE SET generation=excluded.generation,run_id=excluded.run_id,source_group=excluded.source_group,
                published_at=excluded.published_at,data_as_of=excluded.data_as_of,kind=excluded.kind""",
                (self.dataset_id, generation, run_id, self.source_group, now(), data_as_of, kind),
            )
        for entity in ("articles",) if kind == "web" else ("products", "reviews"):
            self.execute(
                "INSERT INTO search_jobs(dataset_id,generation,entity_kind,state,created_at) VALUES(?,?,?,'queued',?) ON CONFLICT(dataset_id,generation,entity_kind) DO NOTHING",
                (self.dataset_id, generation, entity, now()),
            )

    def register_stream(self, generation: str, configuration: dict) -> None:
        generation = identifier(generation)
        encoded = json.dumps(configuration, sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self.conn:
            existing = self.execute(
                "SELECT configuration,state FROM stream_deployments WHERE dataset_id=? AND generation=?",
                (self.dataset_id, generation),
            ).fetchone()
            if existing and (existing[0] != encoded or existing[1] == "retired"):
                raise ValueError("Stream configuration changed or generation retired")
            active = self.execute(
                "SELECT generation,configuration FROM stream_deployments WHERE dataset_id=? AND state!='retired'",
                (self.dataset_id,),
            ).fetchall()
            for old, config in active:
                if old != generation:
                    raise ValueError("Retire the previous generation with confirmed stopped ownership first")
                if config != encoded:
                    raise ValueError("Stream configuration changed; explicit generation migration required")
            self.execute(
                "INSERT INTO stream_deployments(dataset_id,generation,configuration,state,started_at) VALUES(?,?,?,'active',?) ON CONFLICT(dataset_id,generation) DO UPDATE SET state='active'",
                (self.dataset_id, generation, encoded, now()),
            )

    def stream_progress(self, generation: str, progress: dict) -> bool:
        generation = identifier(generation)
        batch = progress.get("batchId")
        if type(batch) is not int or batch < 0:
            raise ValueError("Committed stream batch required")
        clock = instant(progress["timestamp"]).isoformat()
        watermark = progress.get("eventTime", {}).get("watermark")
        if watermark:
            watermark = instant(watermark).isoformat()
        with writer_transaction(self.conn):
            receipt = self.execute(
                "SELECT 1 FROM stream_batches WHERE dataset_id=? AND generation=? AND batch_id=?",
                (self.dataset_id, generation, batch),
            ).fetchone()
            if not receipt:
                return False
            previous = self.execute(
                "SELECT watermark,last_batch FROM stream_deployments WHERE dataset_id=? AND generation=?",
                (self.dataset_id, generation),
            ).fetchone()
            if previous and previous[0] and watermark and instant(watermark) < instant(previous[0]):
                raise ValueError("Stream watermark cannot regress")
            cursor = self.execute(
                "UPDATE stream_deployments SET heartbeat_at=?,watermark=?,last_batch=? WHERE dataset_id=? AND generation=? AND (last_batch IS NULL OR last_batch<=?)",
                (clock, watermark, batch, self.dataset_id, generation, batch),
            )
            accepted = cursor.rowcount == 1
            if accepted and watermark:
                self._stage_stream_alerts(
                    generation,
                    batch,
                    previous[1] if previous and previous[1] is not None else -1,
                    watermark,
                    previous[0] if previous else None,
                )
            return accepted

    def _stage_stream_alerts(
        self, generation: str, batch: int, previous_batch: int, watermark: str, previous_watermark: str | None
    ) -> None:
        from analytics.causal import StatisticalConfig, stream_evaluations

        config = StatisticalConfig.from_settings()
        changed = self.execute(
            "SELECT MIN(window_start) FROM stream_windows WHERE dataset_id=? AND generation=? AND batch_id>? AND batch_id<=?",
            (self.dataset_id, generation, previous_batch, batch),
        ).fetchone()[0]
        finalized = self.execute(
            "SELECT MIN(window_start) FROM stream_windows WHERE dataset_id=? AND generation=? "
            "AND window_end>? AND window_end<=? AND batch_id<=?",
            (self.dataset_id, generation, previous_watermark or "", watermark, batch),
        ).fetchone()[0]
        changed = min((value for value in (changed, finalized) if value), default=None)
        if changed is None:
            return
        lower = (instant(changed) - timedelta(hours=config.history_hours)).isoformat()
        cursor = self.execute(
            "SELECT product_id,source_group,window_seconds,window_start,window_end,review_count,avg_rating "
            "FROM stream_windows WHERE dataset_id=? AND generation=? AND window_start>=? AND batch_id<=? ORDER BY window_start",
            (self.dataset_id, generation, lower, batch),
        )
        columns = [item[0] for item in cursor.description]
        evaluations = stream_evaluations(
            (dict(zip(columns, row)) for row in cursor),
            dataset_id=self.dataset_id,
            generation=generation,
            watermark=watermark,
            config=config,
        )
        # Earlier rows supply the baseline; do not replace their historical
        # evidence with the intentionally truncated baseline of this query.
        self.stage_evaluations(
            generation, (value for value in evaluations if instant(value["window_start"]) >= instant(changed))
        )

    def write_windows(self, generation: str, batch_id: int, rows: Iterable[Mapping], *, window_seconds: int) -> int:
        generation = identifier(generation)
        if type(batch_id) is not int or batch_id < 0:
            raise ValueError("Nonnegative integer batch ID required")
        if type(window_seconds) is not int or window_seconds < 1:
            raise ValueError("Positive window duration required")
        row_hashes = []
        count = 0
        seen = set()
        clock = now()
        with writer_transaction(self.conn):
            if not self.sqlite:
                self.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(?))", ("stream:" + self.dataset_id + ":" + generation,)
                )
            previous = self.execute(
                "SELECT sha256 FROM stream_batches WHERE dataset_id=? AND generation=? AND batch_id=?",
                (self.dataset_id, generation, batch_id),
            ).fetchone()
            for raw in rows:
                value = self._window(raw, window_seconds)
                key = (value["product_id"], value["window_start"])
                if key in seen:
                    raise ValueError("Duplicate window identity within batch")
                seen.add(key)
                count += 1
                row_hashes.append(
                    hashlib.sha256(
                        json.dumps(
                            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
                        ).encode()
                        + b"\n"
                    ).digest()
                )
                if previous:
                    continue
                self.execute(
                    """INSERT INTO stream_windows VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(dataset_id,generation,window_seconds,product_id,window_start) DO UPDATE SET
                    window_end=excluded.window_end,review_count=excluded.review_count,avg_rating=excluded.avg_rating,
                    negative_count=excluded.negative_count,batch_id=excluded.batch_id,updated_at=excluded.updated_at
                    WHERE stream_windows.batch_id < excluded.batch_id""",
                    (
                        self.dataset_id,
                        generation,
                        window_seconds,
                        value["product_id"],
                        self.source_group,
                        value["window_start"],
                        value["window_end"],
                        value["review_count"],
                        value["avg_rating"],
                        value["negative_count"],
                        batch_id,
                        clock,
                    ),
                )
            fingerprint = hashlib.sha256(b"".join(sorted(row_hashes))).hexdigest()
            if previous:
                if previous[0] != fingerprint:
                    raise ValueError("Committed stream batch identity changed")
                return 0
            self.execute(
                "INSERT INTO stream_batches VALUES(?,?,?,?,?,?)",
                (self.dataset_id, generation, batch_id, fingerprint, count, clock),
            )
        return count

    def _window(self, raw: Mapping, seconds: int) -> dict:
        if raw.get("source_group") != self.source_group:
            raise ValueError("Window source differs from dataset")
        product = identifier(raw["product_id"])
        start = instant(raw["window_start"])
        end = instant(raw["window_end"])
        if (end - start).total_seconds() != seconds:
            raise ValueError("Window clock/duration mismatch")
        count = raw["review_count"]
        negative = raw["negative_count"]
        rating = float(raw["avg_rating"])
        if type(count) is not int or count < 1 or type(negative) is not int or not 0 <= negative <= count:
            raise ValueError("Invalid window counts")
        if not math.isfinite(rating) or not 1 <= rating <= 5:
            raise ValueError("Invalid window rating")
        return dict(
            product_id=product,
            source_group=self.source_group,
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            review_count=count,
            negative_count=negative,
            avg_rating=rating,
        )
