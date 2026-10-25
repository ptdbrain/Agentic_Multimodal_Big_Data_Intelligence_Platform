"""Immutable Parquet snapshots with a transactional SQL metadata catalog.

Readers see only committed snapshots. Concurrent writers use compare-and-swap;
conflicts require an explicit retry. Legacy JSON logs require explicit import.
"""
import hashlib
import json
import uuid
from typing import Optional, List
import pandas as pd
import pyarrow as pa
from storage.storage_manager import storage
from storage.local_backend import LocalBackend
from storage.lakehouse_metadata import MetadataStore


class LakehouseTable:
    def __init__(self,table_name: str,layer: str='silver',storage_backend=None):
        if not table_name or '/' in table_name or '..' in table_name or layer not in ('bronze','silver','gold'):
            raise ValueError('Invalid lakehouse table or layer')
        self.table_name=table_name
        self.layer=layer
        self.storage=storage_backend or storage
        self.table_prefix=f'{layer}/{table_name}'
        self.log_prefix=f'{self.table_prefix}/_delta_log'
        local=isinstance(self.storage.backend,LocalBackend)
        self.metadata=MetadataStore(sqlite_path=self.storage.base_dir/'.lakehouse.sqlite' if local else None)
        namespace=str(self.storage.base_dir.resolve()) if local else f'{self.storage.backend.endpoint}/{self.storage.backend.bucket}'
        self.table_key=f'{namespace}:{self.table_prefix}'

    def _get_latest_version(self):
        info=self.metadata.get_commit(self.table_key)
        return info['version'] if info else -1

    def get_history(self): return self.metadata.history(self.table_key)

    @staticmethod
    def _schema(df):
        schema=pa.Schema.from_pandas(df,preserve_index=False)
        return {f.name: {'type':str(f.type),'nullable':f.nullable} for f in schema}

    def _write(self,df,mode,operation,operation_id,merge_keys=None):
        if mode not in ('append','overwrite','upsert'):
            raise ValueError('Invalid write mode')
        if df.empty: return self._get_latest_version()
        op=operation_id or str(uuid.uuid4())
        previous=self.metadata.operation(self.table_key,op)
        payload_hash=hashlib.sha256(pd.util.hash_pandas_object(df,index=False).values.tobytes()+json.dumps(self._schema(df),sort_keys=True).encode()+str((mode,merge_keys)).encode()).hexdigest()
        if previous:
            if previous.get('payload_hash') != payload_hash:
                raise ValueError('Operation ID was already used with different data')
            return previous['version']
        current=self.metadata.get_commit(self.table_key)
        version=current['version'] if current else -1
        if current is None and self.storage.list_objects(self.log_prefix):
            raise RuntimeError('Legacy lakehouse log requires explicit metadata migration')
        schema=self._schema(df)
        if current and current['schema'] != schema:
            raise ValueError('Lakehouse schema mismatch; schema evolution is not enabled')
        write_df=df.copy()
        if current and mode in ('append','upsert'):
            existing=self.read(version)
            write_df=pd.concat([existing,df],ignore_index=True)
        if mode == 'upsert':
            if not merge_keys or any(k not in write_df.columns for k in merge_keys):
                raise ValueError('Valid merge keys are required')
            if write_df[merge_keys].isna().any().any():
                raise ValueError('Merge keys cannot be null')
            write_df=write_df.drop_duplicates(subset=merge_keys,keep='last')
        snapshot=f'{self.table_prefix}/_snapshots/_snapshot_{uuid.uuid4().hex}.parquet'
        self.storage.backend.write_parquet(snapshot,write_df)
        staged=self.storage.backend.read_parquet(snapshot)
        if len(staged) != len(write_df) or self._schema(staged) != schema:
            raise RuntimeError('Snapshot validation failed before metadata commit')
        return self.metadata.commit(self.table_key,version,snapshot,json.dumps(schema,sort_keys=True),op,
                                    operation=operation,mode=mode,num_records=len(write_df),payload_hash=payload_hash)

    def write(self,df: pd.DataFrame,mode: str='append',operation: str='WRITE',operation_id=None) -> int:
        return self._write(df,mode,operation,operation_id)

    def upsert(self,df: pd.DataFrame,merge_keys: List[str],operation_id=None) -> int:
        return self._write(df,'upsert','MERGE_UPSERT',operation_id,merge_keys)

    def read(self,version: Optional[int]=None) -> pd.DataFrame:
        info=self.metadata.get_commit(self.table_key,version)
        if info is None:
            if version is not None:
                raise KeyError(f'Unknown lakehouse version {version}')
            return pd.DataFrame()
        return self.storage.backend.read_parquet(info['data_file'])

    def close(self): self.metadata.close()
