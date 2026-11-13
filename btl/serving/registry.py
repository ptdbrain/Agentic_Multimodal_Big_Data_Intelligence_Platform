"""Immutable per-session bindings; source secrets never enter process environment."""

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
import re
import yaml
from scripts.run_scheduled_backup import environment

WEB_TABLES = frozenset({"articles", "article_versions", "article_daily_stats", "article_pipeline_runs"})
GROUPS = {
    "real": {"marketplace", "web_content"},
    "simulation": {"simulation"},
    "unverified": {"unverified"},
    "all": {"marketplace", "web_content", "simulation", "unverified"},
}


@dataclass(frozen=True)
class DatasetRef:
    id: str
    label: str
    kind: str
    source_group: str
    schema: str
    bucket: str
    mode: str
    connection: Mapping[str, object] = field(repr=False)
    sqlite_path: Path | None = None
    environment_file: Path | None = field(default=None, repr=False)

    def __post_init__(self):
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,100}", self.id):
            raise ValueError("Unsafe dataset identity / ID")
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", self.schema):
            raise ValueError("Unsafe dataset schema")
        if self.kind not in {"commerce", "web"} or self.source_group not in GROUPS["all"]:
            raise ValueError("Unsupported dataset kind/provenance")
        if (self.kind == "web") != (self.source_group == "web_content"):
            raise ValueError("Dataset kind/source mismatch")
        object.__setattr__(self, "connection", MappingProxyType(dict(self.connection)))


class DatasetRegistry:
    def __init__(self, datasets: list[DatasetRef]):
        self.datasets = MappingProxyType({item.id: item for item in datasets})
        if len(self.datasets) != len(datasets):
            raise ValueError("Duplicate dataset ID")
        targets = set()
        for item in datasets:
            target = (
                ("sqlite", str(item.sqlite_path.resolve()))
                if item.sqlite_path
                else (item.connection["host"], item.connection["port"], item.connection["dbname"], item.schema)
            )
            if target in targets:
                raise ValueError("Shared physical dataset target")
            targets.add(target)

    @classmethod
    def load(cls, path: Path | str) -> "DatasetRegistry":
        config = yaml.safe_load(Path(path).read_text())
        if config.get("version") != 1 or not config.get("datasets"):
            raise ValueError("Dataset registry version and entries required")
        items = []
        for row in config["datasets"]:
            key = row["id"]
            schema = row["postgres_schema"]
            kind = row["kind"]
            group = row["source_group"]
            if not re.fullmatch(r"[a-z][a-z0-9_-]{0,60}", key):
                raise ValueError("Unsafe dataset ID")
            if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema):
                raise ValueError("Unsafe dataset schema")
            if kind not in {"commerce", "web"} or group not in GROUPS["all"]:
                raise ValueError("Unsupported dataset kind/provenance")
            if (kind == "web") != (group == "web_content"):
                raise ValueError("Dataset kind/source mismatch")
            env_path = Path(row["environment_file"])
            if not env_path.is_absolute():
                raise ValueError("Absolute private environment required")
            env = environment(env_path)
            mode = env.get("RUNTIME_MODE", "integration")
            if mode not in {"local", "test", "integration", "production"}:
                raise ValueError("Invalid dataset mode")
            local = mode in {"local", "test"}
            connection = (
                {}
                if local
                else {
                    "host": env["POSTGRES_HOST"],
                    "port": int(env.get("POSTGRES_PORT", "5432")),
                    "user": env["POSTGRES_USER"],
                    "password": env["POSTGRES_PASSWORD"],
                    "dbname": env["POSTGRES_DB"],
                    "options": "-c search_path=" + schema,
                    "connect_timeout": 10,
                }
            )
            sqlite = Path(env["SQLITE_DB_PATH"]) if local else None
            if sqlite is not None and not sqlite.is_absolute():
                raise ValueError("Absolute explicitly local database required")
            items.append(
                DatasetRef(
                    key, row.get("label", key), kind, group, schema, row["bucket"], mode, connection, sqlite, env_path
                )
            )
        return cls(items)

    def choices(self, kind: str, scope: str) -> list[DatasetRef]:
        if scope not in GROUPS:
            raise ValueError("Unknown data scope")
        return [item for item in self.datasets.values() if item.kind == kind and item.source_group in GROUPS[scope]]


@dataclass(frozen=True)
class ServingContext:
    registry: DatasetRegistry
    commerce: str | None
    articles: str | None
    scope: str = "real"

    def __post_init__(self):
        if self.scope not in GROUPS:
            raise ValueError("Unknown data scope")
        for kind, key in [("commerce", self.commerce), ("web", self.articles)]:
            if key is None:
                continue
            item = self.registry.datasets[key]
            if item.kind != kind or item.source_group not in GROUPS[self.scope]:
                raise ValueError("Dataset conflicts with requested kind/scope")

    def resolve(self, table: str) -> DatasetRef | None:
        key = self.articles if table in WEB_TABLES else self.commerce
        return self.registry.datasets[key] if key else None


class DashboardSources:
    """One repeatable-read connection per selected dataset, never a global cross-DB snapshot."""

    def __init__(self, context: ServingContext):
        self.context = context
        self.connections = {}

    @property
    def data_scope(self):
        return self.context.scope

    @property
    def is_integration(self):
        return all(
            ref.sqlite_path is None
            for ref in self.context.registry.datasets.values()
            if ref.id in (self.context.commerce, self.context.articles)
        )

    def _db(self, table):
        from dashboard.db_connector import DashboardDB

        ref = self.context.resolve(table)
        if ref is None:
            return None
        if ref.id not in self.connections:
            self.connections[ref.id] = DashboardDB(binding=ref, data_scope=self.data_scope)
        return self.connections[ref.id]

    def read_table(self, table, **kwargs):
        import pandas as pd

        db = self._db(table)
        if db is None:
            return pd.DataFrame()
        result = db.read_table(table, **kwargs)
        result.attrs.update(dataset_id=db.binding.id, data_source=db.get_active_source_label())
        return result

    def publication_metadata(self):
        records = []
        for table in ("products", "articles"):
            ref = self.context.resolve(table)
            if ref is None:
                continue
            db = self._db(table)
            frame = db.read_table("serving_generation") if db._columns("serving_generation") else None
            record = {
                "dataset_id": ref.id,
                "label": ref.label,
                "generation": None,
                "published_at": None,
                "data_as_of": None,
            }
            if frame is not None and not frame.empty:
                record.update(frame.iloc[0].to_dict())
            records.append(record)
        return records

    def get_active_source_label(self):
        return " | ".join(
            self.context.registry.datasets[key].label + " · " + self.context.registry.datasets[key].schema
            for key in (self.context.commerce, self.context.articles)
            if key
        )

    def __getattr__(self, name):
        if not name.startswith("get_"):
            raise AttributeError(name)
        db = self._db("products")
        if db is None:
            raise ConnectionError("No commerce dataset selected for this scope")
        return getattr(db, name)

    def close(self):
        for db in self.connections.values():
            db.close()
