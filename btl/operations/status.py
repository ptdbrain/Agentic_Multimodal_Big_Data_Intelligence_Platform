"""Durable operational state. Run IDs stay in SQL, never in metric labels."""

from functools import wraps
import os
import math
import time

PIPELINES = frozenset({"commerce", "articles", "streaming", "backup", "migration", "platform"})
SOURCES = frozenset({"marketplace", "web_content", "simulation", "unverified", "system"})
GAUGES = frozenset(
    {
        "backup_last_complete_timestamp",
        "restore_last_success_timestamp",
        "restore_duration_seconds",
        "checkpoint_errors",
        "consumer_lag",
        "under_replicated_partitions",
        "disk_free_ratio",
        "stream_progress_timestamp",
    }
)


def _labels(pipeline, source):
    if pipeline not in PIPELINES or source not in SOURCES:
        raise ValueError("Unsupported operational labels")


def _number(value, *, nonnegative=True):
    value = float(value)
    if not math.isfinite(value) or (nonnegative and value < 0):
        raise ValueError("Measurement must be finite and nonnegative")
    return value


def admitted(function):
    @wraps(function)
    def wrapped(self, *args, **kwargs):
        from operations.backup import writer_admission

        with writer_admission(self.mode):
            return function(self, *args, **kwargs)

    return wrapped


class OperationStatus:
    def __init__(self, mode=None, connection=None):
        self.mode = mode
        self._warehouse = None
        if connection is None:
            from database.warehouse import WarehouseManager

            self._warehouse = WarehouseManager(mode=mode)
            self.conn = self._warehouse._conn
            self.pg = self._warehouse._engine_type == "postgres"
        else:
            self.conn = connection
            self.pg = not connection.__class__.__module__.startswith("sqlite3")
        self.param = "%s" if self.pg else "?"
        from operations.backup import writer_admission

        with writer_admission(mode), self.conn:
            cur = self.conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS operations_runs (
                run_id TEXT PRIMARY KEY, pipeline TEXT NOT NULL, source TEXT NOT NULL,
                state TEXT NOT NULL, started DOUBLE PRECISION NOT NULL, ended DOUBLE PRECISION,
                expected_min BIGINT NOT NULL, received BIGINT, valid BIGINT, quarantined BIGINT,
                data_as_of DOUBLE PRECISION)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS operations_gauges (
                name TEXT NOT NULL, pipeline TEXT NOT NULL, source TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL, observed DOUBLE PRECISION NOT NULL,
                PRIMARY KEY(name,pipeline,source))""")

            cur.execute(
                "CREATE TABLE IF NOT EXISTS operations_expectations(pipeline TEXT NOT NULL,source TEXT NOT NULL,registered DOUBLE PRECISION NOT NULL,PRIMARY KEY(pipeline,source))"
            )
            self.expected = set()
            for item in os.environ.get("OPS_EXPECTED_PIPELINES", "commerce:unverified,articles:web_content").split(","):
                if not item.strip():
                    continue
                pair = tuple(item.strip().split(":"))
                if len(pair) != 2:
                    raise ValueError("Expected pipeline format is pipeline:source")
                _labels(*pair)
                self.expected.add(pair)
                self._execute(
                    "INSERT INTO operations_expectations VALUES(?,?,?) ON CONFLICT(pipeline,source) DO NOTHING",
                    (*pair, time.time()),
                )

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        if self._warehouse:
            self._warehouse.close()

    def _execute(self, sql, values=()):
        cur = self.conn.cursor()
        cur.execute(sql.replace("?", self.param), values)
        return cur

    @admitted
    def start(self, run_id, pipeline, source, expected_min=1, now=None):
        _labels(pipeline, source)
        if not isinstance(run_id, str) or not run_id or len(run_id) > 256:
            raise ValueError("Invalid operational run ID")
        if isinstance(expected_min, bool) or not isinstance(expected_min, int) or expected_min < 0:
            raise ValueError("Invalid expected input count")
        now = _number(time.time() if now is None else now)
        with self.conn:
            existing = self._execute(
                "SELECT pipeline,source,state,expected_min FROM operations_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if existing:
                if tuple(existing) != (pipeline, source, "running", expected_min):
                    raise ValueError("Run already exists with a different identity or terminal state")
                return
            self._execute(
                "INSERT INTO operations_runs(run_id,pipeline,source,state,started,expected_min) VALUES (?,?,?,?,?,?)",
                (run_id, pipeline, source, "running", now, expected_min),
            )

    @admitted
    def finish(self, run_id, received, valid, quarantined, data_as_of=None, now=None):
        for count in (received, valid, quarantined):
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError("Counts must be nonnegative integers")
        if valid + quarantined > received:
            raise ValueError("Accounted outputs exceed inputs")
        if data_as_of is not None:
            data_as_of = _number(data_as_of)
        now = _number(time.time() if now is None else now)
        with self.conn:
            cur = self._execute(
                """UPDATE operations_runs SET state=?,ended=?,received=?,valid=?,quarantined=?,data_as_of=?
                WHERE run_id=? AND state=? AND started<=?""",
                ("success", now, received, valid, quarantined, data_as_of, run_id, "running", now),
            )
            if cur.rowcount != 1:
                raise ValueError("Run must be active and cannot finish before it started")

    @admitted
    def fail(self, run_id, now=None):
        now = _number(time.time() if now is None else now)
        with self.conn:
            cur = self._execute(
                "UPDATE operations_runs SET state=?,ended=? WHERE run_id=? AND state=? AND started<=?",
                ("failed", now, run_id, "running", now),
            )
            if cur.rowcount != 1:
                raise ValueError("Run must be active to fail")

    @admitted
    def set_gauge(self, name, value, pipeline="platform", source="system", now=None):
        _labels(pipeline, source)
        if name not in GAUGES:
            raise ValueError("Unsupported operational gauge")
        value = _number(value)
        now = _number(time.time() if now is None else now)
        with self.conn:
            self._execute(
                """INSERT INTO operations_gauges(name,pipeline,source,value,observed) VALUES(?,?,?,?,?)
                ON CONFLICT(name,pipeline,source) DO UPDATE SET value=excluded.value,observed=excluded.observed""",
                (name, pipeline, source, value, now),
            )

    def metrics(self):
        lines = []

        def emit(name, value, pipeline, source, extra=""):
            value = "NaN" if value is None else format(float(value), ".15g")
            lines.append(f'sentinel_ops_{name}{{pipeline="{pipeline}",source="{source}"{extra}}} {value}')

        # SQL groups stay bounded: arbitrary run identifiers never become series.
        actual_pairs = set(self._execute("SELECT DISTINCT pipeline,source FROM operations_runs").fetchall())
        for pipeline, source, registered in self._execute(
            "SELECT pipeline,source,registered FROM operations_expectations"
        ).fetchall():
            if (pipeline, source) not in self.expected:
                continue
            emit("expected_since_timestamp", registered, pipeline, source)
            if (pipeline, source) not in actual_pairs:
                emit("last_success_timestamp", None, pipeline, source)
                emit("last_started_timestamp", None, pipeline, source)
        for pipeline, source in sorted(actual_pairs):
            _labels(pipeline, source)
            latest = self._execute(
                """SELECT state,started,ended,expected_min,received,valid,quarantined,data_as_of
                FROM operations_runs WHERE pipeline=? AND source=? ORDER BY started DESC,run_id DESC LIMIT 1""",
                (pipeline, source),
            ).fetchone()
            success, failures = self._execute(
                """SELECT MAX(CASE WHEN state='success' THEN ended END),
                SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END) FROM operations_runs WHERE pipeline=? AND source=?""",
                (pipeline, source),
            ).fetchone()
            for name, value in [
                ("last_success_timestamp", success),
                ("failure_total", failures),
                ("last_started_timestamp", latest[1]),
                ("last_ended_timestamp", latest[2]),
                ("expected_min", latest[3]),
                ("received", latest[4]),
                ("valid", latest[5]),
                ("quarantined", latest[6]),
                ("data_as_of_timestamp", latest[7]),
            ]:
                emit(name, value, pipeline, source)
            for state in ("running", "success", "failed"):
                emit("run_state", int(state == latest[0]), pipeline, source, f',state="{state}"')
        known = set()
        for name, pipeline, source, value, observed in self._execute(
            "SELECT name,pipeline,source,value,observed FROM operations_gauges"
        ).fetchall():
            _labels(pipeline, source)
            if name not in GAUGES:
                raise ValueError("Unregistered gauge in storage")
            emit(name, value, pipeline, source)
            emit(name + "_observed_timestamp", observed, pipeline, source)
            known.add((name, pipeline, source))
        for name in sorted(GAUGES):
            if (name, "platform", "system") not in known:
                emit(name, None, "platform", "system")
        return "\n".join(lines) + "\n"
