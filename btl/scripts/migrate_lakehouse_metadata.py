"""Explicit legacy-log import; dry-run by default, raw logs are never deleted."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from storage.lakehouse import LakehouseTable


def migrate(table,apply=False):
    logs=sorted(table.storage.list_objects(table.log_prefix))
    result=[]
    expected=-1
    for path in logs:
        if not path.endswith('.json'): continue
        info=table.storage.backend.read_json(path)
        if info['version'] != expected+1:
            raise ValueError('Legacy versions must be contiguous')
        df=table.storage.backend.read_parquet(info['data_file'])
        if len(df) != info['num_records']:
            raise ValueError('Legacy snapshot row count mismatch')
        schema=table._schema(df)
        result.append({'version':info['version'],'data_file':info['data_file'],'num_records':len(df),'schema':schema})
        if apply:
            table.metadata.commit(table.table_key,expected,info['data_file'],json.dumps(schema),
                                  f'legacy:{info["version"]}',operation=info.get('operation','LEGACY_IMPORT'),
                                  num_records=len(df),legacy_import=True)
        expected=info['version']
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('table');p.add_argument('--layer',default='silver');p.add_argument('--apply',action='store_true')
    a=p.parse_args();t=LakehouseTable(a.table,a.layer)
    try: print(json.dumps(migrate(t,a.apply),indent=2))
    finally: t.close()
