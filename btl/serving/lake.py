"""Read-only per-dataset lake access. Never creates buckets or changes global settings."""

import hashlib
import io
import json
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from scripts.run_scheduled_backup import environment
from serving.registry import DatasetRef


def bronze_records(stream: BinaryIO) -> Iterator[object]:
    """Decode a UTF-8 JSON array one record at a time, preserving JSON numeric types."""
    text = io.TextIOWrapper(stream, encoding="utf-8")
    decoder = json.JSONDecoder()
    buffer = ""
    eof = False

    def read_more() -> None:
        nonlocal buffer, eof
        chunk = text.read(65536)
        buffer += chunk
        eof = not chunk

    def token() -> str:
        nonlocal buffer
        buffer = buffer.lstrip(" \t\n\r")
        while not buffer and not eof:
            read_more()
            buffer = buffer.lstrip(" \t\n\r")
        return buffer[:1]

    try:
        if token() != "[":
            raise ValueError("Expected Bronze record array")
        buffer = buffer[1:]
        if token() != "]":
            while True:
                while True:
                    try:
                        record, end = decoder.raw_decode(buffer)
                        if end < len(buffer) and buffer[end] in ",] \t\n\r":
                            break
                        if eof:
                            break
                    except json.JSONDecodeError:
                        if eof:
                            raise ValueError("Invalid Bronze record") from None
                    if len(buffer) > 20 * 1024 * 1024:
                        raise ValueError("Oversized or invalid Bronze record")
                    read_more()
                buffer = buffer[end:]
                separator = token()
                if separator not in (",", "]"):
                    raise ValueError("Missing Bronze record separator")
                yield record
                if separator == "]":
                    break
                buffer = buffer[1:]
                token()
        buffer = buffer[1:]
        if token():
            raise ValueError("Unexpected content after Bronze array")
    finally:
        text.detach()


class DatasetLake:
    def __init__(self, binding: DatasetRef) -> None:
        self.binding = binding
        self.env = environment(binding.environment_file)
        self.root = None
        self.client = None
        if binding.sqlite_path is not None:
            if not self.env.get("LOCAL_DATA_DIR"):
                raise ConnectionError("Dataset has no explicit local lake")
            self.root = Path(self.env["LOCAL_DATA_DIR"]).resolve()
        else:
            import urllib3
            from minio import Minio

            self.client = Minio(
                self.env["MINIO_ENDPOINT"],
                access_key=self.env["MINIO_ACCESS_KEY"],
                secret_key=self.env["MINIO_SECRET_KEY"],
                secure=self.env.get("MINIO_SECURE", "false").lower() == "true",
                http_client=urllib3.PoolManager(
                    timeout=urllib3.Timeout(connect=1, read=2), retries=urllib3.Retry(total=1, connect=1, read=0)
                ),
            )

    def _key(self, key: str) -> str:
        path = PurePosixPath(key)
        if not key or path.is_absolute() or ".." in path.parts or "\\" in key:
            raise ValueError("Unsafe dataset object reference")
        if self.root:
            target = self.root / path
            if target.is_symlink() or not target.resolve().is_relative_to(self.root):
                raise ValueError("Dataset object escapes lake")
        return str(path)

    def objects(self, prefix: str) -> Iterator[tuple[str, int]]:
        trailing = prefix.endswith("/")
        prefix = self._key(prefix) + ("/" if trailing else "")
        if self.root:
            for path in sorted((self.root / prefix).rglob("*")):
                if path.is_file() and not path.is_symlink():
                    yield str(path.relative_to(self.root)), path.stat().st_size
        else:
            for item in self.client.list_objects(self.binding.bucket, prefix=prefix, recursive=True):
                yield item.object_name, item.size

    @contextmanager
    def _open_object(self, key: str) -> Iterator[BinaryIO]:
        key = self._key(key)
        stream = (self.root / key).open("rb") if self.root else self.client.get_object(self.binding.bucket, key)
        try:
            yield stream
        finally:
            stream.close()
            if not self.root:
                stream.release_conn()

    def read_bytes(self, key: str, max_bytes: int = 20 * 1024 * 1024) -> bytes:
        with self._open_object(key) as stream:
            raw = stream.read(max_bytes + 1)
            if len(raw) > max_bytes:
                raise ValueError("Evidence object exceeds read limit")
            return raw

    @contextmanager
    def verified_file(self, reference: dict, *, max_bytes: int = 512 * 1024 * 1024) -> Iterator[BinaryIO]:
        """Freeze verified evidence in a bounded private temporary file before parsing."""
        key = self._key(reference["path"])
        if type(reference["size"]) is not int or not 0 <= reference["size"] <= max_bytes:
            raise ValueError("Evidence object exceeds verified read limit")
        with self._open_object(key) as stream:
            with tempfile.TemporaryFile(mode="w+b") as target:
                digest, size = hashlib.sha256(), 0
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > min(max_bytes, reference["size"]):
                        raise ValueError("Evidence bytes exceed pinned size")
                    digest.update(chunk)
                    target.write(chunk)
                if size != reference["size"] or digest.hexdigest() != reference["sha256"]:
                    raise ValueError("Evidence bytes/checksum mismatch")
                target.seek(0)
                yield target

    def metrics(self) -> dict[str, dict[str, int]]:
        result = {}
        for layer in ("bronze", "silver", "gold"):
            count = size = 0
            for _, length in self.objects(layer + "/"):
                count += 1
                size += length
            result[layer] = {"files": count, "size_bytes": size}
        return result

    def bronze_reviews(self, limit: int = 1000) -> list[dict[str, object]]:
        topic = self.env.get("KAFKA_TOPIC_REVIEWS")
        if not topic:
            raise ValueError("Dataset review topic is not configured")
        rows = []
        if limit <= 0:
            return rows
        for key, _ in self.objects("bronze/" + topic + "/"):
            if not key.endswith(".json"):
                continue
            with self._open_object(key) as stream:
                for item in bronze_records(stream):
                    if isinstance(item, dict):
                        rows.append(item.get("payload", item))
                        if len(rows) >= limit:
                            return rows
        return rows
