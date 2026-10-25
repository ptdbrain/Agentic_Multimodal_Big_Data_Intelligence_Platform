"""Reentrant per-path writer serialization for read/merge/publish transactions."""
from contextlib import contextmanager
import functools
import hashlib
import inspect
import os
from pathlib import Path
import tempfile
import threading

_registry={}
_registry_guard=threading.Lock()
_held=threading.local()


@contextmanager
def write_lock(path):
    key=str(path)
    if not key.startswith(('s3a://','hdfs://')):
        key=str(Path(key.removeprefix('file://')).resolve())
    held=getattr(_held,'paths',set())
    if key in held:
        yield
        return
    with _registry_guard:
        mutex=_registry.setdefault(key,threading.RLock())
    with mutex:
        remote=key.startswith(('s3a://','hdfs://'))
        conn=None;stream=None
        try:
            if remote:
                from database.warehouse import WarehouseManager
                conn=WarehouseManager(mode='integration')._conn
                conn.cursor().execute('SELECT pg_advisory_lock(hashtext(%s))',(key,))
            else:
                folder=Path(tempfile.gettempdir())/'sentinel-write-locks';folder.mkdir(exist_ok=True)
                stream=open(folder/hashlib.sha256(key.encode()).hexdigest(),'a+b')
                if os.name=='nt':
                    import msvcrt
                    if stream.tell()==0:stream.write(b'0');stream.flush()
                    stream.seek(0);msvcrt.locking(stream.fileno(),msvcrt.LK_LOCK,1)
                else:
                    import fcntl
                    fcntl.flock(stream.fileno(),fcntl.LOCK_EX)
            _held.paths=held | {key}
            yield
        finally:
            _held.paths=held
            if conn is not None:conn.close()  # Session close releases advisory lock.
            if stream is not None:
                if os.name=='nt':
                    import msvcrt
                    stream.seek(0);msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
                else:
                    import fcntl
                    fcntl.flock(stream.fileno(),fcntl.LOCK_UN)
                stream.close()


def synchronized_path(argument):
    def decorate(function):
        @functools.wraps(function)
        def wrapped(*args,**kwargs):
            bound=inspect.signature(function).bind(*args,**kwargs)
            path=bound.arguments.get(argument)
            if path is None:
                path=getattr(args[0],argument)
            with write_lock(path): return function(*args,**kwargs)
        return wrapped
    return decorate
