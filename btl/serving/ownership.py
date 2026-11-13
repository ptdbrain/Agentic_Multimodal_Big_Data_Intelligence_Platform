"""Session/process-scoped stream ownership; stale age never grants a second driver."""

from contextlib import contextmanager
from pathlib import Path
import fcntl, hashlib


@contextmanager
def acquire_stream_owner(connection, binding):
    handle = None
    if binding.sqlite_path is not None:
        path = Path(str(binding.sqlite_path) + ".owner-" + hashlib.sha256(binding.id.encode()).hexdigest()[:24])
        handle = path.open("a")
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise RuntimeError("Dataset already has a live stream owner")
    else:
        cursor = connection.cursor()
        cursor.execute("SELECT pg_try_advisory_lock(hashtext(%s))", ("stream-owner:" + binding.id,))
        if not cursor.fetchone()[0]:
            raise RuntimeError("Dataset already has a live stream owner")
        connection.commit()
    try:
        yield
    finally:
        if handle:
            fcntl.flock(handle, fcntl.LOCK_UN)
            handle.close()
        else:
            cursor = connection.cursor()
            cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", ("stream-owner:" + binding.id,))
            connection.commit()
