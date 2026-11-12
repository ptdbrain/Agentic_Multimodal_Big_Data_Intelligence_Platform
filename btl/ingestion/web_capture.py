"""Durable, bounded web capture state; SQL publication is a separate operation."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from ingestion.web_content import collect_articles
from storage.write_lock import write_lock

MAX_FAILURES = 3


def _now():
    return datetime.now(timezone.utc).isoformat()


def _fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _encoded(payload):
    return (json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()


def _write_new(path, payload):
    """Write immutable capture metadata before a state reference may advance."""
    content = _encoded(payload)
    with Path(path).open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    Path(path).chmod(0o400)
    _fsync_directory(Path(path).parent)
    return hashlib.sha256(content).hexdigest()


def _save_state(root, state):
    descriptor, temporary = tempfile.mkstemp(prefix=".state-", dir=root)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_encoded(state))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, root / "state.json")
        _fsync_directory(root)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _url(value):
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
        raise ValueError("Public absolute HTTP seed URL required")
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path, "", ""))


def _page(root, attempt, index, url, run_id, request_interval):
    folder = attempt / "pages" / f"{index:06d}"
    evidence_dir = folder / "evidence"
    evidence_dir.mkdir(parents=True, mode=0o700)
    records, collection = collect_articles(
        [url],
        limit=1,
        max_pages=1,
        evidence_dir=evidence_dir,
        request_interval=request_interval,
        collection_run_id=run_id,
    )
    requests = collection["requests"]
    error = next((row for row in requests if row.get("error")), None)
    status = (
        "blocked"
        if error and (error.get("error") == "robots_disallowed" or error.get("http_status") in (401, 403, 429))
        else "failed"
        if error
        else "complete"
    )
    evidence = []
    for path in sorted(evidence_dir.iterdir()):
        # The existing collector creates response files; flush them and their
        # parent directory before publishing a durable page manifest.
        with path.open("rb") as stream:
            os.fsync(stream.fileno())
        path.chmod(0o400)
        evidence.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    _fsync_directory(evidence_dir)
    records_path = folder / "capture.json"
    capture_sha = _write_new(records_path, records)
    manifest = {
        "url": url,
        "status": status,
        "collection_run_id": run_id,
        "finished_at": _now(),
        "collection": collection,
        "evidence": evidence,
        "records_path": str(records_path),
        "capture_sha256": capture_sha,
    }
    manifest_path = folder / "manifest.json"
    manifest_sha = _write_new(manifest_path, manifest)
    _fsync_directory(folder.parent)
    return records, collection, status, {"manifest_path": str(manifest_path), "manifest_sha256": manifest_sha}


def capture_web(root, seed_urls, limit=50, max_pages=150, request_interval=0.5, run_id=None):
    """Capture pending URLs first, then revisit known URLs with fresh HTTP clocks.

    Each URL is attempted at most once per call and at most three consecutive
    failing calls. A successful capture resets its failure budget. Exhausted
    failures remain explicit in state and need operator review before retry.
    All paths returned are absolute. Existing attempt IDs are never reused.
    """
    if limit < 1 or max_pages < 1 or request_interval < 0:
        raise ValueError("Invalid web capture bounds")
    seeds = list(dict.fromkeys(_url(value) for value in seed_urls))
    if not seeds:
        raise ValueError("At least one HTTP seed URL required")
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_id = run_id or uuid.uuid4().hex
    with write_lock(root):
        state_path = root / "state.json"
        state = (
            json.loads(state_path.read_text())
            if state_path.exists()
            else {"schema_version": 1, "pending_urls": [], "urls": {}}
        )
        attempt = root / "attempts" / hashlib.sha256(run_id.encode()).hexdigest()
        try:
            attempt.mkdir(parents=True, exist_ok=False, mode=0o700)
        except FileExistsError as error:
            raise ValueError("Web capture run_id already exists; use a new run_id") from error
        _fsync_directory(attempt.parent)
        started = _now()
        urls = state["urls"]
        # Resume unfinished work before revisiting earlier successful pages.
        candidates = state["pending_urls"] + seeds + list(urls)
        queue = list(
            dict.fromkeys(url for url in candidates if urls.get(url, {}).get("failure_count", 0) < MAX_FAILURES)
        )
        records, pages, changed, retry_urls = [], [], [], []
        processed = set()
        while queue and len(pages) < max_pages and len(records) < limit:
            url = queue.pop(0)
            if url in processed:
                continue
            captured, collection, status, page = _page(root, attempt, len(pages), url, run_id, request_interval)
            pages.append(page)
            processed.add(url)
            prior = urls.get(url, {})
            entry = dict(
                prior,
                status=status,
                attempts=prior.get("attempts", 0) + 1,
                failure_count=0 if status == "complete" else prior.get("failure_count", 0) + 1,
                last_attempt_run_id=run_id,
                last_attempt_at=_now(),
                **page,
            )
            if captured:
                fingerprint = captured[0]["content_sha256"]
                if prior.get("content_sha256") is not None and prior["content_sha256"] != fingerprint:
                    changed.append(url)
                entry["content_sha256"] = fingerprint
                entry["collected_at"] = captured[0]["collected_at"]
                records.extend(captured)
            if status != "complete" and entry["failure_count"] < MAX_FAILURES:
                retry_urls.append(url)
            urls[url] = entry
            for discovered in collection["pending_urls"]:
                if discovered not in processed and discovered not in queue and discovered not in urls:
                    queue.append(discovered)
            # Advancing the cursor is legal only after the page records,
            # response evidence and manifest above are durably committed.
            state["pending_urls"] = list(dict.fromkeys(queue + retry_urls))
            _save_state(root, state)
        pending = state["pending_urls"] = list(dict.fromkeys(queue + retry_urls))
        problems = [entry["status"] for entry in urls.values() if entry["status"] in ("failed", "blocked")]
        if records:
            status = "partial" if pending or problems or len(records) < limit else "complete"
        elif problems:
            status = "blocked" if all(problem == "blocked" for problem in problems) else "failed"
        else:
            status = "partial"
        records_path = attempt / "capture.json"
        capture_sha = _write_new(records_path, records)
        manifest = {
            "source_group": "web_content",
            "acquisition_method": "live",
            "collection_run_id": run_id,
            "status": status,
            "started_at": started,
            "finished_at": _now(),
            "requested_articles": limit,
            "max_pages": max_pages,
            "request_interval": request_interval,
            "max_consecutive_failures": MAX_FAILURES,
            "articles_collected": len(records),
            "pages_attempted": len(pages),
            "pending_urls": pending,
            "changed_urls": changed,
            "pages": pages,
            "records_path": str(records_path),
            "capture_sha256": capture_sha,
            "retry_exhausted_urls": [url for url, row in urls.items() if row.get("failure_count", 0) >= MAX_FAILURES],
        }
        manifest_path = attempt / "manifest.json"
        manifest_sha = _write_new(manifest_path, manifest)
        _save_state(root, state)
        return dict(manifest, manifest_path=str(manifest_path), manifest_sha256=manifest_sha, records=records)
