"""Bind metadata writers to the existing maintenance fence on their actual database."""

from __future__ import annotations

import fcntl
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from operations.backup import LOCK_KEY

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection


@contextmanager
def writer_transaction(connection: sqlite3.Connection | PgConnection) -> Iterator[None]:
    handle = None
    try:
        if connection.__class__.__module__.startswith("sqlite3"):
            path = connection.execute("PRAGMA database_list").fetchone()[2]
            if path:
                handle = Path(path + ".maintenance.lock").open("a")
                try:
                    fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise RuntimeError("Dataset maintenance admission is busy")
        with connection:
            if handle is None and not connection.__class__.__module__.startswith("sqlite3"):
                cursor = connection.cursor()
                cursor.execute("SELECT pg_try_advisory_xact_lock_shared(%s)", (LOCK_KEY,))
                if not cursor.fetchone()[0]:
                    raise RuntimeError("Dataset maintenance admission is busy")
            yield
    finally:
        if handle:
            fcntl.flock(handle, fcntl.LOCK_UN)
            handle.close()
