"""Private raw HTTP evidence; source metadata never contains cookies or headers."""
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re
import uuid


class ResponseEvidence:
    def __init__(self, source_name, parser_version, directory=None, run_id=None):
        self.run_id = run_id or uuid.uuid4().hex
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',self.run_id):
            raise ValueError('Invalid collection run identifier')
        self.source_name = source_name
        self.parser_version = parser_version
        self.directory = (Path(directory) if directory is not None else
                          Path(__file__).resolve().parents[2] / '.runtime' / 'source-evidence') / self.run_id

    def stamp(self, rows, response, fallback_url=None):
        if response.status_code != 200:
            raise ValueError('HTTP evidence requires a successful observed response')
        content = response.content
        if not isinstance(content,bytes) or not content or len(content) > 20*1024*1024:
            raise ValueError('Missing or oversized HTTP response evidence')
        digest = hashlib.sha256(content).hexdigest()
        self.directory.mkdir(parents=True,exist_ok=True,mode=0o700)
        path = self.directory / (digest + '.http')
        try:
            with path.open('xb') as stream:
                path.chmod(0o600)
                stream.write(content)
        except FileExistsError:
            if path.read_bytes() != content:
                raise RuntimeError('Existing HTTP evidence does not match its content hash')
        proof = {'source_group':'marketplace','source_name':self.source_name,
                 'source_url':response.url or fallback_url,'acquisition_method':'live',
                 'collected_at':datetime.now(timezone.utc).isoformat(),
                 'collection_run_id':self.run_id,'parser_version':self.parser_version,'schema_version':'1.0',
                 'evidence_sha256':digest,'evidence_ref':str(path.resolve())}
        return [dict(row,_source_provenance=proof.copy()) for row in rows]
