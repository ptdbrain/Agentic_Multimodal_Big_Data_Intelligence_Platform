"""Verify artifact hashes separately from evidence about their origin."""
import argparse
import hashlib
import json
from pathlib import Path


def verify_manifest(path) -> dict:
    path=Path(path)
    manifest=json.loads(path.read_text())
    root=(path.parent/manifest.get('root','.')).resolve()
    results=[]
    for dataset in manifest['datasets']:
        verified=True
        for entry in dataset['files']:
            file=(root/entry['path']).resolve()
            if not file.is_relative_to(root):
                raise ValueError('Manifest file escapes dataset root')
            verified &= file.is_file() and hashlib.sha256(file.read_bytes()).hexdigest() == entry['sha256']
        # A label or URL is never proof of a live capture.
        kind=dataset.get('source_kind','unverified')
        status='synthetic' if kind == 'synthetic' else 'unverified'
        results.append({'dataset_id':dataset['dataset_id'],'content_verified':bool(verified),'provenance_status':status})
    return {'content_verified':all(r['content_verified'] for r in results),'datasets':results}


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('manifest',nargs='?',default='data/manifest.json')
    result=verify_manifest(parser.parse_args().manifest)
    print(json.dumps(result,indent=2))
    raise SystemExit(0 if result['content_verified'] else 1)
