"""Cumulative article serving and observed, immutable version history."""

import hashlib
import sqlite3

import pandas as pd
import pytest

from config.settings import settings
from database.warehouse import WarehouseManager
from ingestion.web_content import parse_article
from spark.batch.article_etl import publish_articles
from storage.storage_manager import storage

pytestmark = pytest.mark.unit


def observed(
    name="one", body="Original text", published="2026-11-01T08:00:00+07:00", collected="2026-11-02T08:00:00+07:00"
):
    row = parse_article(
        f"<article><h1>Observed title</h1><p>{body}</p></article>", f"https://tinhte.vn/thread/{name}/", collected
    )
    return dict(
        row, published_at=published, evidence_ref=f"private/{name}/{body}", collection_run_id=f"collection-{collected}"
    )


def query(sql):
    wh = WarehouseManager(mode="test")
    try:
        return wh.query(sql)
    finally:
        wh.close()


def test_new_batch_keeps_prior_articles_and_rebuilds_daily_from_cumulative_state():
    one = publish_articles([observed()], mode="test", run_id="first")
    publish_articles([observed("two", "Second text")], mode="test", run_id="second")
    assert len(query("SELECT * FROM articles")) == 2
    daily = query("SELECT * FROM article_daily_stats")
    assert daily["article_count"].tolist() == [2]
    assert daily["total_text_characters"].tolist() == [54]
    # Lake snapshots continue to describe their own run, not cumulative SQL.
    assert storage.read_parquet(one["silver_path"])["article_id"].nunique() == 1
    assert storage.read_parquet(one["gold_path"])["article_count"].tolist() == [1]


def test_same_content_new_run_keeps_one_immutable_version_and_original_clocks():
    original = observed()
    publish_articles([original], mode="test", run_id="first")
    publish_articles(
        [
            dict(
                original,
                collected_at="2026-11-05T00:00:00Z",
                evidence_ref="private/later",
                collection_run_id="later-collection",
            )
        ],
        mode="test",
        run_id="reobserved",
    )
    versions = query("SELECT * FROM article_versions")
    assert len(versions) == 1
    assert versions.iloc[0]["run_id"] == "first"
    assert versions.iloc[0]["collected_at_raw"] == "2026-11-02T08:00:00+07:00"
    assert versions.iloc[0]["published_at_raw"] == "2026-11-01T08:00:00+07:00"
    assert versions.iloc[0]["evidence_ref"] == original["evidence_ref"]
    current = query("SELECT * FROM articles").iloc[0]
    assert current["run_id"] == "reobserved"
    assert current["first_seen_run_id"] == "first"
    assert current["last_seen_run_id"] == "reobserved"
    assert current["current_version"] == 1
    assert query("SELECT article_count FROM article_daily_stats")["article_count"].tolist() == [1]


def test_changed_content_appends_version_without_editing_prior_snapshot():
    original = observed()
    publish_articles([original], mode="test", run_id="first")
    before = query("SELECT * FROM article_versions")
    updated = observed(body="Changed text", collected="2026-11-03T00:00:00Z")
    publish_articles([updated], mode="test", run_id="changed")
    versions = query("SELECT * FROM article_versions ORDER BY version_number")
    assert versions["version_number"].tolist() == [1, 2]
    pd.testing.assert_frame_equal(before, versions.iloc[:1].reset_index(drop=True))
    assert versions.iloc[1]["body"] == "Observed title Changed text"
    assert versions.iloc[1]["collected_at_raw"] == "2026-11-03T00:00:00Z"
    assert versions.iloc[1]["content_sha256"] == hashlib.sha256(b"Observed title Changed text").hexdigest()
    current = query("SELECT * FROM articles").iloc[0]
    assert current["body"] == "Observed title Changed text"
    assert current["current_version"] == 2
    assert query("SELECT total_text_characters FROM article_daily_stats")["total_text_characters"].tolist() == [27]


def test_publication_correction_rebuilds_both_dates_without_losing_other_articles():
    first = observed()
    publish_articles(
        [first, observed("two"), observed("three", published="2026-11-04T00:00:00Z")], mode="test", run_id="first"
    )
    publish_articles(
        [dict(first, published_at="2026-11-02T00:00:00Z", collected_at="2026-11-03T00:00:00Z")],
        mode="test",
        run_id="corrected",
    )
    daily = query("SELECT * FROM article_daily_stats ORDER BY date")
    assert daily["date"].astype(str).tolist() == ["2026-11-01", "2026-11-02", "2026-11-04"]
    assert daily["article_count"].tolist() == [1, 1, 1]
    assert len(query("SELECT * FROM article_versions")) == 4
    publish_articles(
        [dict(first, published_at=None, collected_at="2026-11-04T00:00:00Z")], mode="test", run_id="untimed"
    )
    daily = query("SELECT * FROM article_daily_stats ORDER BY date")
    assert daily["date"].astype(str).tolist() == ["2026-11-01", "2026-11-04"]
    assert daily["article_count"].tolist() == [1, 1]


def test_legacy_migration_keeps_unknown_history_without_inventing_versions():
    connection = sqlite3.connect(settings.database.sqlite_path)
    connection.execute("""CREATE TABLE articles (
        article_id TEXT PRIMARY KEY, source_group TEXT NOT NULL, source_name TEXT NOT NULL,
        source_url TEXT NOT NULL, acquisition_method TEXT NOT NULL, title TEXT NOT NULL,
        body TEXT NOT NULL, published_at TIMESTAMP, collected_at TIMESTAMP NOT NULL,
        content_sha256 TEXT NOT NULL, run_id TEXT NOT NULL)""")
    connection.execute(
        "INSERT INTO articles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "legacy",
            "web_content",
            "tinhte.vn",
            "https://tinhte.vn/thread/legacy/",
            "live",
            "Legacy",
            "Stored text",
            None,
            "2026-01-01",
            "unknown-fingerprint",
            "legacy-run",
        ),
    )
    connection.commit()
    connection.close()
    wh = WarehouseManager(mode="test")
    try:
        wh.initialize_schema()
        wh.initialize_schema()
        legacy = wh.query("SELECT * FROM articles").iloc[0]
        assert legacy["body"] == "Stored text"
        assert legacy["collected_at"] == "2026-01-01"
        assert legacy["collected_at_raw"] is None
        assert legacy["current_version"] is None
        assert legacy["first_seen_run_id"] is None
        assert wh.query("SELECT * FROM article_versions").empty
    finally:
        wh.close()
    publish_articles([observed()], mode="test", run_id="new-observation")
    assert len(query("SELECT * FROM articles")) == 2
    assert len(query("SELECT * FROM article_versions")) == 1


def test_sql_failure_rolls_back_articles_versions_daily_and_manifest():
    original = observed()
    publish_articles([original], mode="test", run_id="first")
    tables = ["articles", "article_versions", "article_daily_stats", "article_pipeline_runs"]
    before = {table: query(f"SELECT * FROM {table}") for table in tables}
    connection = sqlite3.connect(settings.database.sqlite_path)
    connection.execute("""CREATE TRIGGER reject_article_run BEFORE INSERT ON article_pipeline_runs
        WHEN NEW.run_id = 'fail' BEGIN SELECT RAISE(ABORT, 'injected manifest failure'); END""")
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match="injected manifest failure"):
        publish_articles([observed(body="New version", collected="2026-11-03T00:00:00Z")], mode="test", run_id="fail")
    for table in tables:
        pd.testing.assert_frame_equal(query(f"SELECT * FROM {table}"), before[table])


def test_late_replay_records_history_without_reverting_newer_current_observation():
    original = observed(body="Newer current", collected="2026-11-05T00:00:00Z")
    publish_articles([original], mode="test", run_id="newer")
    publish_articles(
        [observed(body="Older recovered text", collected="2026-11-03T00:00:00Z")], mode="test", run_id="older-replay"
    )
    current = query("SELECT * FROM articles").iloc[0]
    assert current["body"] == "Observed title Newer current"
    assert current["last_seen_run_id"] == "newer" and current["current_version"] == 1
    assert len(query("SELECT * FROM article_versions")) == 2
    publish_articles([observed(body="Newest text", collected="2026-11-06T00:00:00Z")], mode="test", run_id="newest")
    assert query("SELECT current_version FROM articles").iloc[0, 0] == 3
    assert query("SELECT total_text_characters FROM article_daily_stats").iloc[0, 0] == 26


def test_conflicting_content_at_same_observation_clock_across_runs_is_refused():
    publish_articles([observed()], mode="test", run_id="one")
    before = query("SELECT * FROM articles")
    with pytest.raises(ValueError, match="Conflicting article observation"):
        publish_articles([observed(body="Different text")], mode="test", run_id="conflicting")
    pd.testing.assert_frame_equal(before, query("SELECT * FROM articles"))
    assert len(query("SELECT * FROM article_versions")) == 1


def test_repeated_older_observation_does_not_duplicate_a_historical_version():
    publish_articles([observed(body="Current", collected="2026-11-05T00:00:00Z")], mode="test", run_id="current")
    older = observed(body="Older", collected="2026-11-03T00:00:00Z")
    publish_articles([older], mode="test", run_id="late-one")
    publish_articles([older], mode="test", run_id="late-two")
    assert len(query("SELECT * FROM article_versions")) == 2
    with pytest.raises(ValueError, match="Conflicting article observation"):
        publish_articles(
            [dict(older, body="Conflicting older", content_sha256=hashlib.sha256(b"Conflicting older").hexdigest())],
            mode="test",
            run_id="late-conflict",
        )


def test_serving_lineage_snapshot_includes_cumulative_sql_gold_and_latest_articles():
    publish_articles([observed()], mode='test', run_id='lineage-first')
    latest = publish_articles([observed('two', 'Second text')], mode='test', run_id='lineage-second')
    assert len(storage.read_parquet(latest['serving_silver_path'])) == 2
    assert storage.read_parquet(latest['serving_gold_path'])['article_count'].tolist() == [2]
    assert storage.read_parquet(latest['gold_path'])['article_count'].tolist() == [1]
