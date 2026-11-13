"""Read-only per-dataset lake access. Never creates buckets or changes global settings."""

from pathlib import Path, PurePosixPath
import json
from scripts.run_scheduled_backup import environment


class DatasetLake:
    def __init__(self, binding):
        self.binding = binding
        self.env = environment(binding.environment_file)
        self.root = None
        self.client = None
        if binding.sqlite_path is not None:
            if not self.env.get("LOCAL_DATA_DIR"):
                raise ConnectionError("Dataset has no explicit local lake")
            self.root = Path(self.env["LOCAL_DATA_DIR"]).resolve()
        else:
            from minio import Minio

            self.client = Minio(
                self.env["MINIO_ENDPOINT"],
                access_key=self.env["MINIO_ACCESS_KEY"],
                secret_key=self.env["MINIO_SECRET_KEY"],
                secure=self.env.get("MINIO_SECURE", "false").lower() == "true",
            )

    def _key(self, key):
        path = PurePosixPath(key)
        if not key or path.is_absolute() or ".." in path.parts or "\\" in key:
            raise ValueError("Unsafe dataset object reference")
        if self.root:
            target = self.root / path
            if target.is_symlink() or not target.resolve().is_relative_to(self.root):
                raise ValueError("Dataset object escapes lake")
        return str(path)

    def objects(self, prefix):
        trailing = prefix.endswith("/")
        prefix = self._key(prefix) + ("/" if trailing else "")
        if self.root:
            for path in sorted((self.root / prefix).rglob("*")):
                if path.is_file() and not path.is_symlink():
                    yield str(path.relative_to(self.root)), path.stat().st_size
        else:
            for item in self.client.list_objects(self.binding.bucket, prefix=prefix, recursive=True):
                yield item.object_name, item.size

    def read_bytes(self, key, max_bytes=20 * 1024 * 1024):
        key = self._key(key)
        if self.root:
            path = self.root / key
            if path.stat().st_size > max_bytes:
                raise ValueError("Evidence object exceeds read limit")
            return path.read_bytes()
        response = self.client.get_object(self.binding.bucket, key)
        try:
            raw = response.read(max_bytes + 1)
            if len(raw) > max_bytes:
                raise ValueError("Evidence object exceeds read limit")
            return raw
        finally:
            response.close()
            response.release_conn()

    def metrics(self):
        result = {}
        for layer in ("bronze", "silver", "gold"):
            count = size = 0
            for _, length in self.objects(layer + "/"):
                count += 1
                size += length
            result[layer] = {"files": count, "size_bytes": size}
        return result

    def bronze_reviews(self, limit=1000):
        topic = self.env.get("KAFKA_TOPIC_REVIEWS")
        if not topic:
            raise ValueError("Dataset review topic is not configured")
        rows = []
        for key, _ in self.objects("bronze/" + topic + "/"):
            if not key.endswith(".json"):
                continue
            for item in json.loads(self.read_bytes(key)):
                rows.append(item.get("payload", item))
                if len(rows) >= limit:
                    return rows
        return rows
