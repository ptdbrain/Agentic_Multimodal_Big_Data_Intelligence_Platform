"""Transactional snapshot catalog. This is not the Delta transaction protocol."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


class ConcurrentCommitError(RuntimeError):
    pass


class MetadataStore:
    def __init__(self, sqlite_path=None):
        self.sqlite = sqlite_path is not None
        if self.sqlite:
            Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn=sqlite3.connect(str(sqlite_path),timeout=30)
        else:
            from database.warehouse import WarehouseManager
            self.conn=WarehouseManager(mode='integration')._conn
        with self.conn:
            self.conn.cursor().execute('''CREATE TABLE IF NOT EXISTS lakehouse_commits (
                table_key TEXT NOT NULL, version INTEGER NOT NULL,
                operation_id TEXT NOT NULL, commit_json TEXT NOT NULL,
                PRIMARY KEY (table_key,version), UNIQUE(table_key,operation_id))''')
        self.param='?' if self.sqlite else '%s'

    def get_commit(self,table,version=None):
        cur=self.conn.cursor()
        if version is None:
            cur.execute(f'SELECT commit_json FROM lakehouse_commits WHERE table_key={self.param} ORDER BY version DESC LIMIT 1',(table,))
        else:
            cur.execute(f'SELECT commit_json FROM lakehouse_commits WHERE table_key={self.param} AND version={self.param}',(table,version))
        row=cur.fetchone()
        return json.loads(row[0]) if row else None

    def history(self,table):
        cur=self.conn.cursor()
        cur.execute(f'SELECT commit_json FROM lakehouse_commits WHERE table_key={self.param} ORDER BY version',(table,))
        return [json.loads(row[0]) for row in cur.fetchall()]

    def operation(self,table,operation_id):
        cur=self.conn.cursor()
        cur.execute(f'SELECT commit_json FROM lakehouse_commits WHERE table_key={self.param} AND operation_id={self.param}',(table,operation_id))
        row=cur.fetchone()
        return json.loads(row[0]) if row else None

    def commit(self,table,expected_version,snapshot_path,schema_json,operation_id,**extra):
        try:
            cur=self.conn.cursor()
            if self.sqlite:
                cur.execute('BEGIN IMMEDIATE')
            else:
                cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))',(table,))
            previous_op=self.operation(table,operation_id)
            if previous_op is not None:
                for key in ("payload_hash", "operation", "mode"):
                    if previous_op.get(key) != extra.get(key):
                        raise ValueError("Operation ID was already committed with a different intent")
                if previous_op["schema"] != json.loads(schema_json):
                    raise ValueError("Operation ID was already committed with a different schema")
                self.conn.commit()
                return previous_op['version']
            latest=self.get_commit(table)
            actual=latest['version'] if latest else -1
            if actual != expected_version:
                raise ConcurrentCommitError(f'Expected version {expected_version}, found {actual}; retry from the latest snapshot')
            info=dict(extra,version=actual+1,data_file=snapshot_path,schema=json.loads(schema_json),
                      operation_id=operation_id,timestamp=datetime.now(timezone.utc).isoformat())
            cur.execute(f'INSERT INTO lakehouse_commits (table_key,version,operation_id,commit_json) VALUES ({",".join([self.param]*4)})',
                        (table,info['version'],operation_id,json.dumps(info,allow_nan=False)))
            self.conn.commit()
            return info['version']
        except Exception:
            self.conn.rollback()
            raise

    def close(self): self.conn.close()
