"""Persistent bounded web captures with fixture HTTP, never live evidence."""

import hashlib
import json
from pathlib import Path

import pytest
import requests

pytestmark = pytest.mark.unit


class FixtureResponse:
    def __init__(self, url, body, status=200):
        self.url, self.text, self.status_code = url, body, status
        self.content = body.encode()

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"fixture HTTP {self.status_code}")


@pytest.fixture
def fixture_http(monkeypatch):
    routes, calls = {}, []

    class FixtureSession:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, timeout):
            calls.append(url)
            if url.endswith("/robots.txt"):
                return FixtureResponse(url, "User-agent: *\nAllow: /\n")
            item = routes[url]
            if isinstance(item, Exception):
                raise item
            body, status = item
            return FixtureResponse(url, body, status)

    monkeypatch.setattr(requests, "Session", FixtureSession)
    return routes, calls


def html(body="Initial body", link=None):
    return (
        "<article><h1>Observed title</h1><p>" + body + "</p></article>" + (f'<a href="{link}">Next</a>' if link else "")
    )


def capture(root, seeds, **kwargs):
    from ingestion.web_capture import capture_web

    return capture_web(root, seeds, request_interval=0, **kwargs)


def read(path):
    return json.loads(Path(path).read_text())


def test_page_limit_preserves_discovered_unvisited_urls_and_continues(fixture_http, tmp_path):
    routes, calls = fixture_http
    one, two = "https://tinhte.vn/thread/one/", "https://tinhte.vn/thread/two/"
    routes[one], routes[two] = (html(link=two), 200), (html("Second body"), 200)
    first = capture(tmp_path, [one], limit=5, max_pages=1, run_id="one")
    assert first["status"] == "partial"
    assert first["pending_urls"] == [two]
    assert read(tmp_path / "state.json")["pending_urls"] == [two]
    second = capture(tmp_path, [one], limit=5, max_pages=1, run_id="two")
    assert second["records"][0]["source_url"] == two
    assert calls.count(two) == 1
    assert first["records"][0]["acquisition_method"] == "live"
    assert first["records"][0]["collected_at"] != second["records"][0]["collected_at"]


def test_failed_url_retries_in_new_immutable_attempt_with_actual_clock(fixture_http, tmp_path):
    routes, _ = fixture_http
    url = "https://tinhte.vn/thread/one/"
    routes[url] = requests.Timeout("fixture timeout")
    first = capture(tmp_path, [url], limit=1, max_pages=1, run_id="failed")
    assert first["status"] == "failed"
    assert first["records"] == []
    assert first["pending_urls"] == [url]
    original_manifest = Path(first["manifest_path"]).read_bytes()
    routes[url] = html(), 200
    second = capture(tmp_path, [url], limit=1, max_pages=1, run_id="retry")
    assert second["status"] == "complete"
    assert len(second["records"]) == 1
    assert second["records"][0]["collection_run_id"] == "retry"
    assert second["records"][0]["collected_at"] >= first["finished_at"]
    assert Path(first["manifest_path"]).read_bytes() == original_manifest
    state = read(tmp_path / "state.json")["urls"][url]
    assert state["attempts"] == 2
    assert state["failure_count"] == 0


def test_revisit_detects_body_change_and_preserves_unchanged_fingerprint(fixture_http, tmp_path):
    routes, _ = fixture_http
    url = "https://tinhte.vn/thread/one/"
    routes[url] = html(), 200
    first = capture(tmp_path, [url], limit=1, max_pages=1, run_id="first")
    same = capture(tmp_path, [url], limit=1, max_pages=1, run_id="same")
    assert same["changed_urls"] == []
    routes[url] = html("Changed body"), 200
    changed = capture(tmp_path, [url], limit=1, max_pages=1, run_id="changed")
    assert changed["changed_urls"] == [url]
    assert changed["records"][0]["content_sha256"] != first["records"][0]["content_sha256"]
    assert read(tmp_path / "state.json")["urls"][url]["content_sha256"] == changed["records"][0]["content_sha256"]
    assert Path(first["records_path"]).read_bytes() != Path(changed["records_path"]).read_bytes()


@pytest.mark.parametrize("status", [403, 429])
def test_blocked_response_is_honest_and_retry_budget_persists(status, fixture_http, tmp_path):
    routes, calls = fixture_http
    url = "https://tinhte.vn/thread/one/"
    routes[url] = "Access denied", status
    for index in range(3):
        result = capture(tmp_path, [url], limit=1, max_pages=1, run_id=f"blocked-{index}")
        assert result["status"] == "blocked"
        assert result["records"] == []
    result = capture(tmp_path, [url], limit=1, max_pages=1, run_id="exhausted")
    assert result["status"] == "blocked"
    assert calls.count(url) == 3
    assert read(tmp_path / "state.json")["urls"][url]["failure_count"] == 3
    assert result["pending_urls"] == []


def test_capture_manifest_and_payload_are_hashed_and_run_id_cannot_overwrite(fixture_http, tmp_path):
    routes, _ = fixture_http
    url = "https://tinhte.vn/thread/one/"
    routes[url] = html(), 200
    result = capture(tmp_path, [url], limit=1, max_pages=1, run_id="immutable")
    manifest_bytes = Path(result["manifest_path"]).read_bytes()
    assert result["manifest_sha256"] == hashlib.sha256(manifest_bytes).hexdigest()
    manifest = json.loads(manifest_bytes)
    assert manifest["capture_sha256"] == hashlib.sha256(Path(result["records_path"]).read_bytes()).hexdigest()
    for item in manifest["pages"]:
        page = read(item["manifest_path"])
        assert item["manifest_sha256"] == hashlib.sha256(Path(item["manifest_path"]).read_bytes()).hexdigest()
        for evidence in page["evidence"]:
            assert evidence["sha256"] == hashlib.sha256(Path(evidence["path"]).read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="already exists"):
        capture(tmp_path, [url], limit=1, max_pages=1, run_id="immutable")
    assert Path(result["manifest_path"]).read_bytes() == manifest_bytes


def test_failed_durable_page_write_never_advances_url_state(fixture_http, tmp_path, monkeypatch):
    from ingestion import web_capture

    routes, _ = fixture_http
    url = "https://tinhte.vn/thread/one/"
    routes[url] = html(), 200
    write = web_capture._write_new

    def disk_failure(path, payload):
        if Path(path).name == "manifest.json":
            raise OSError("fixture full disk")
        return write(path, payload)

    monkeypatch.setattr(web_capture, "_write_new", disk_failure)
    with pytest.raises(OSError, match="full disk"):
        capture(tmp_path, [url], limit=1, max_pages=1, run_id="disk-failure")
    state_path = tmp_path / "state.json"
    assert not state_path.exists() or read(state_path)["urls"].get(url, {}).get("status") != "complete"


def test_robots_denial_records_blocked_without_fetching_page(monkeypatch, tmp_path):
    url = "https://tinhte.vn/thread/one/"
    calls = []

    class DeniedSession:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, target, timeout):
            calls.append(target)
            return FixtureResponse(target, "Robots access denied", 403)

    monkeypatch.setattr(requests, "Session", DeniedSession)
    result = capture(tmp_path, [url], limit=1, max_pages=1, run_id="robots-blocked")
    assert result["status"] == "blocked"
    assert calls == ["https://tinhte.vn/robots.txt"]
    assert result["records"] == []
