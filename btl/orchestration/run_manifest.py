"""Durable JSON handoff between workers; no process-global execution state."""
from dataclasses import asdict, dataclass, field
import hashlib
import json
from storage.storage_manager import storage


@dataclass
class RunManifest:
    run_id: str
    mode: str
    source_type: str = 'dataset'
    batches_by_topic: dict = field(default_factory=dict)
    required_datasets: list = field(default_factory=lambda: ['products','reviews'])
    silver_paths: dict = field(default_factory=dict)
    dq_report_ids: list = field(default_factory=list)
    dq_metrics: list = field(default_factory=list)
    status: str = "running"


def save_manifest(manifest: RunManifest) -> str:
    payload=asdict(manifest)
    json.dumps(payload,allow_nan=False)  # Refuse DataFrames/NaN before persistence.
    digest=hashlib.sha256(manifest.run_id.encode()).hexdigest()
    path=f'runs/{digest}/manifest.json'
    from storage.local_backend import LocalBackend
    if isinstance(storage.backend, LocalBackend):
        import os, tempfile
        target=storage.backend._get_full_path(path)
        target.parent.mkdir(parents=True,exist_ok=True)
        fd,temp=tempfile.mkstemp(dir=target.parent,prefix=".manifest-")
        try:
            with os.fdopen(fd,"w",encoding="utf-8") as stream:
                json.dump(payload,stream,ensure_ascii=False,allow_nan=False)
                stream.flush();os.fsync(stream.fileno())
            os.replace(temp,target)
        finally:
            if os.path.exists(temp): os.unlink(temp)
    else:
        # One complete object PUT; never a partially overwritten local JSON file.
        storage.write_json(path,payload)
    return path


def load_manifest(path: str) -> RunManifest:
    if not path or not path.startswith('runs/') or '..' in path.split('/'):
        raise ValueError('A valid run manifest path is required')
    return RunManifest(**storage.read_json(path))


class RunLease:
    """Durable single-run ownership of classic canonical Parquet outputs.

    A crashed run retains ownership until an explicit recovery releases it.
    This favors integrity over automatically stealing an expired lease.
    """
    def __init__(self, mode):
        from database.warehouse import WarehouseManager
        self.wh=WarehouseManager(mode=mode)
        from operations.backup import writer_admission
        try:
            with writer_admission(mode):
                self.wh.execute('CREATE TABLE IF NOT EXISTS pipeline_run_lease (resource TEXT PRIMARY KEY, run_id TEXT NOT NULL)')
        except Exception:
            self.wh.close()
            raise

    def acquire(self,run_id):
        with self.wh._conn:
            cur=self.wh._conn.cursor()
            param='%s' if self.wh._engine_type == 'postgres' else '?'
            cur.execute(f'INSERT INTO pipeline_run_lease (resource,run_id) VALUES ({param},{param}) ON CONFLICT (resource) DO NOTHING',('canonical',run_id))
            cur.execute('SELECT run_id FROM pipeline_run_lease WHERE resource = '+param,('canonical',))
            if cur.fetchone()[0] != run_id:
                raise RuntimeError('Another run owns canonical outputs; recover/release its lease before retrying')

    def assert_owner(self,run_id):
        cur=self.wh._conn.cursor()
        cur.execute('SELECT run_id FROM pipeline_run_lease WHERE resource='+('%s' if self.wh._engine_type=='postgres' else '?'),('canonical',))
        row=cur.fetchone()
        if row is None or row[0] != run_id:
            raise RuntimeError('Run is not the current canonical-output owner')

    def release(self,run_id):
        with self.wh._conn:
            cur=self.wh._conn.cursor()
            param='%s' if self.wh._engine_type == 'postgres' else '?'
            cur.execute(f'DELETE FROM pipeline_run_lease WHERE resource={param} AND run_id={param}',('canonical',run_id))

    def close(self): self.wh.close()


def canonical_writer(function):
    """Share whole-pipeline ownership between CLI and scheduled DAG entrypoints."""
    import functools,inspect,uuid
    from config.settings import settings
    @functools.wraps(function)
    def wrapped(*args,**kwargs):
        bound=inspect.signature(function).bind_partial(*args,**kwargs)
        mode=bound.arguments.get('mode') or settings.runtime_mode
        settings.set_runtime_mode(mode)
        storage.set_mode(mode)
        run_id=bound.arguments.get('run_id') or str(uuid.uuid4())
        lease=RunLease(mode)
        acquired=False
        try:
            from operations.backup import writer_admission
            with writer_admission(mode):
                lease.acquire(run_id);acquired=True
                return function(*args,**kwargs)
        finally:
            if acquired: lease.release(run_id)
            lease.close()
    return wrapped
