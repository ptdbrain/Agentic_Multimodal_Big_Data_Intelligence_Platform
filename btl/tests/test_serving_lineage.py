"""Evidence is returned only after checking the pinned bytes and exact record ID."""

import hashlib, json, sqlite3
from types import SimpleNamespace
import pytest

pytestmark = pytest.mark.unit


def fixture(tmp_path):
    from serving.store import ServingStore
    from serving.lineage import stage_lineage
    from serving.lake import DatasetLake

    raw = json.dumps(
        [
            {
                "payload": {
                    "review_id": "r",
                    "product_id": "p",
                    "rating": 4,
                    "review_date": "2026-11-13",
                    "email": "private@example.test",
                }
            }
        ]
    ).encode()
    key = "bronze/reviews/batch.json"
    path = tmp_path / key
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    ref = {"path": key, "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}
    conn = sqlite3.connect(tmp_path / "lineage.sqlite")
    store = ServingStore(conn, "fixture", "simulation")
    with conn:
        store.initialize()
        stage_lineage(store, "g", {"run_id": "run", "status": "complete", "bronze": [ref], "silver": [], "gold": []})
    lake = object.__new__(DatasetLake)
    lake.root = tmp_path
    lake.binding = SimpleNamespace(id="fixture", bucket="fixture")
    return store, lake, path


def test_trace_verifies_bytes_and_record_identity_and_does_not_invent_legacy_links(tmp_path):
    from serving.lineage import trace_record

    store, lake, path = fixture(tmp_path)
    result = trace_record(store, lake, "g", "reviews", "r")
    assert result["status"] == "verified_input"
    assert result["records"][0]["record_index"] == 0
    assert result["records"][0]["payload"]["rating"] == 4
    assert "email" not in result["records"][0]["payload"]
    assert trace_record(store, lake, "g", "reviews", "missing")["status"] == "not_found"
    assert trace_record(store, lake, "legacy", "reviews", "r")["status"] == "unknown"
    path.write_text("[]")
    with pytest.raises(ValueError, match="checksum|bytes"):
        trace_record(store, lake, "g", "reviews", "r")
    store.conn.close()


def test_lineage_receipt_cannot_be_replaced_or_escape_bronze(tmp_path):
    from serving.lineage import stage_lineage

    store, lake, path = fixture(tmp_path)
    for key in ("../secret", "/etc/passwd", "gold/pretend.json"):
        with pytest.raises(ValueError):
            with store.conn:
                stage_lineage(
                    store,
                    "other",
                    {
                        "run_id": "run",
                        "bronze": [{"path": key, "sha256": "a" * 64, "size": 1}],
                        "silver": [],
                        "gold": [],
                    },
                )
    with pytest.raises(ValueError, match="changed|immutable"):
        with store.conn:
            stage_lineage(store, "g", {"run_id": "different", "bronze": [], "silver": [], "gold": []})
    store.conn.close()


@pytest.mark.parametrize(
    "selected_event,expected_status,expected_records", [("selected", "verified_winner", 1), (None, "verified_input", 2)]
)
def test_trace_selects_actual_silver_winner_instead_of_every_raw_candidate(
    tmp_path, selected_event, expected_status, expected_records
):
    import pandas as pd
    from serving.lineage import stage_lineage, trace_record

    store, lake, path = fixture(tmp_path)
    records = [
        {"event_id": "old", "payload": {"review_id": "r", "product_id": "p", "rating": 4}},
        {"event_id": "selected", "payload": {"review_id": "r", "product_id": "p", "rating": 1}},
    ]
    raw = json.dumps(records).encode()
    path.write_bytes(raw)
    silver = tmp_path / "silver/serving_snapshots/new/reviews/data.parquet"
    silver.parent.mkdir(parents=True)
    pd.DataFrame([{"event_id": selected_event, "review_id": "r", "product_id": "p", "rating": 1}]).to_parquet(silver)

    def reference(path):
        value = path.read_bytes()
        return {
            "path": path.relative_to(tmp_path).as_posix(),
            "sha256": hashlib.sha256(value).hexdigest(),
            "size": len(value),
        }

    receipt = {
        "run_id": "new",
        "bronze": [reference(path)],
        "silver": [reference(silver)],
        "gold": [],
        "tables": {"reviews": [silver.relative_to(tmp_path).as_posix()]},
    }
    with store.conn:
        stage_lineage(store, "new", receipt)
    result = trace_record(store, lake, "new", "reviews", "r")
    assert result["status"] == expected_status
    assert len(result["records"]) == expected_records
    if selected_event:
        assert result["records"][0]["payload"]["rating"] == 1
        assert result["silver_record"]["event_id"] == "selected"
    store.conn.close()


def test_publication_archives_parquet_and_later_canonical_updates_do_not_change_evidence(tmp_path):
    import pandas as pd
    from serving.lineage import commerce_receipt
    from storage.storage_manager import storage
    from orchestration.run_manifest import reference_output, _output_bytes

    storage.write_silver_parquet("reviews", pd.DataFrame([{"review_id": "r", "event_id": "selected"}]))
    paths = storage.list_objects("silver/reviews/")
    expected = {path: reference_output(path) for path in paths}
    receipt = commerce_receipt("g", tables=["reviews"], marts=[])
    assert receipt["tables"]["reviews"]
    for ref in receipt["silver"]:
        assert "/serving_snapshots/" in ref["path"]
        assert ref["sha256"] in {value["sha256"] for value in expected.values()}
    storage.write_silver_parquet("reviews", pd.DataFrame([{"review_id": "later", "event_id": "later"}]))
    for ref in receipt["silver"]:
        assert hashlib.sha256(_output_bytes(ref["path"])).hexdigest() == ref["sha256"]


def test_aggregate_recipe_checks_full_membership_and_pages_without_future_or_other_products(tmp_path):
    import pandas as pd
    from serving.lineage import stage_lineage, trace_aggregate

    store, lake, _ = fixture(tmp_path)
    silver = tmp_path / "silver/serving_snapshots/aggregate/reviews/data.parquet"
    gold = tmp_path / "gold/serving_snapshots/aggregate/product_daily_stats/data.parquet"
    silver.parent.mkdir(parents=True)
    gold.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {"review_id": "r1", "product_id": "p", "rating": 5, "review_date": "2026-11-13T01:00:00Z"},
            {"review_id": "r2", "product_id": "p", "rating": 1, "review_date": "2026-11-13T02:00:00Z"},
            {"review_id": "future", "product_id": "p", "rating": 1, "review_date": "2026-11-14T01:00:00Z"},
            {"review_id": "other", "product_id": "different", "rating": 1, "review_date": "2026-11-13T01:00:00Z"},
        ]
    ).to_parquet(silver)
    pd.DataFrame([{"date": "2026-11-13", "product_id": "p", "review_count": 2, "avg_rating": 3.0}]).to_parquet(gold)

    def ref(path):
        raw = path.read_bytes()
        return {
            "path": path.relative_to(tmp_path).as_posix(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size": len(raw),
        }

    receipt = {
        "run_id": "aggregate",
        "bronze": [],
        "silver": [ref(silver)],
        "gold": [ref(gold)],
        "tables": {"reviews": [ref(silver)["path"]]},
        "marts": {"product_daily_stats": [ref(gold)["path"]]},
        "strict_analytics_time": True,
    }
    with store.conn:
        stage_lineage(store, "aggregate", receipt)
    grain = {"date": "2026-11-13", "product_id": "p"}
    result = trace_aggregate(store, lake, "aggregate", "product_daily_stats", grain, limit=1)
    assert result["status"] == "verified_aggregate"
    assert result["total_members"] == 2 and result["next_offset"] == 1
    assert result["records"][0]["review_id"] == "r1"
    second = trace_aggregate(store, lake, "aggregate", "product_daily_stats", grain, offset=1, limit=1)
    assert second["records"][0]["review_id"] == "r2"
    assert second["checks"]["avg_rating"]["passed"]
    store.conn.close()


def test_article_winner_uses_content_and_collection_clock_without_inventing_event_ids(tmp_path):
    import pandas as pd
    from serving.lineage import stage_lineage, trace_record

    store, lake, path = fixture(tmp_path)
    body = "Actual text in an explicitly generated lineage fixture"
    digest = hashlib.sha256(body.encode()).hexdigest()
    article = {
        "article_id": "a",
        "body": body,
        "content_sha256": digest,
        "collected_at": "2026-11-13T01:00:00Z",
        "source_url": "https://example.test/a",
    }
    path.write_text(json.dumps([{"payload": article}]))
    silver = tmp_path / "silver/article_snapshots/article/articles.parquet"
    silver.parent.mkdir(parents=True)
    pd.DataFrame([article]).to_parquet(silver)

    def ref(file):
        raw = file.read_bytes()
        return {
            "path": file.relative_to(tmp_path).as_posix(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size": len(raw),
        }

    with store.conn:
        stage_lineage(
            store,
            "article",
            {
                "run_id": "article",
                "bronze": [ref(path)],
                "silver": [ref(silver)],
                "gold": [],
                "tables": {"articles": [ref(silver)["path"]]},
            },
        )
    result = trace_record(store, lake, "article", "articles", "a")
    assert result["status"] == "verified_winner"
    assert result["records"][0]["payload"]["content_sha256"] == digest
    store.conn.close()


def snapshot_file(tmp_path, layer, table, rows):
    import pandas as pd

    path = tmp_path / layer / "serving_snapshots" / "recipe" / table / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path)
    raw = path.read_bytes()
    return {"path": path.relative_to(tmp_path).as_posix(), "sha256": hashlib.sha256(raw).hexdigest(), "size": len(raw)}


@pytest.mark.parametrize("dimension", ["brand", "category"])
def test_dimension_recipe_joins_the_pinned_catalog_not_another_product(tmp_path, dimension):
    from serving.lineage import stage_lineage, trace_aggregate

    store, lake, _ = fixture(tmp_path)
    reviews = snapshot_file(
        tmp_path,
        "silver",
        "reviews",
        [
            {"review_id": "one", "product_id": "p1", "rating": 5, "review_date": "2026-11-13T00:00:00Z"},
            {"review_id": "two", "product_id": "p2", "rating": 1, "review_date": "2026-11-13T00:00:00Z"},
        ],
    )
    products = snapshot_file(
        tmp_path,
        "silver",
        "products",
        [{"product_id": "p1", "brand": "A", "category": "X"}, {"product_id": "p2", "brand": "B", "category": "Y"}],
    )
    value = "A" if dimension == "brand" else "X"
    mart = dimension + "_daily_stats"
    gold = snapshot_file(
        tmp_path, "gold", mart, [{"date": "2026-11-13", dimension: value, "review_count": 1, "avg_rating": 5}]
    )
    with store.conn:
        stage_lineage(
            store,
            "dimension",
            {
                "run_id": "dimension",
                "bronze": [],
                "silver": [reviews, products],
                "gold": [gold],
                "tables": {"reviews": [reviews["path"]], "products": [products["path"]]},
                "marts": {mart: [gold["path"]]},
            },
        )
    result = trace_aggregate(store, lake, "dimension", mart, {"date": "2026-11-13", dimension: value})
    assert result["status"] == "verified_aggregate"
    assert [member["review_id"] for member in result["records"]] == ["one"]
    assert result["recipe"]["catalog_objects"] == [products["path"]]
    store.conn.close()


@pytest.mark.parametrize("mixed_currency", [False, True])
def test_price_recipe_reconciles_original_units_and_detects_mixed_currency(tmp_path, mixed_currency):
    from serving.lineage import stage_lineage, trace_aggregate

    store, lake, _ = fixture(tmp_path)
    observations = [
        {"price_id": "one", "product_id": "p", "price": 100, "currency": "VND", "timestamp": "2026-11-13T00:00:00Z"},
        {"price_id": "two", "product_id": "p", "price": 200, "currency": "VND", "timestamp": "2026-11-13T01:00:00Z"},
    ]
    if mixed_currency:
        observations.append({**observations[0], "price_id": "foreign", "price": 900, "currency": "USD"})
    prices = snapshot_file(tmp_path, "silver", "prices", observations)
    gold = snapshot_file(
        tmp_path,
        "gold",
        "price_daily_stats",
        [
            {
                "date": "2026-11-13",
                "product_id": "p",
                "data_points": 2,
                "min_price": 100,
                "max_price": 200,
                "avg_price": 150,
                "currency": "VND",
            }
        ],
    )
    with store.conn:
        stage_lineage(
            store,
            "prices",
            {
                "run_id": "prices",
                "bronze": [],
                "silver": [prices],
                "gold": [gold],
                "tables": {"prices": [prices["path"]]},
                "marts": {"price_daily_stats": [gold["path"]]},
            },
        )
    result = trace_aggregate(store, lake, "prices", "price_daily_stats", {"date": "2026-11-13", "product_id": "p"})
    assert result["status"] == ("aggregate_mismatch" if mixed_currency else "verified_aggregate")
    assert result["checks"]["currency"]["passed"] == (not mixed_currency)
    store.conn.close()


def test_article_recipe_counts_only_known_publication_times_for_selected_source(tmp_path):
    from serving.lineage import stage_lineage, trace_aggregate

    store, lake, _ = fixture(tmp_path)
    articles = snapshot_file(
        tmp_path,
        "silver",
        "articles",
        [
            {"article_id": "one", "source_name": "example.test", "body": "abc", "published_at": "2026-11-13T00:00:00Z"},
            {"article_id": "two", "source_name": "example.test", "body": "de", "published_at": "2026-11-13T01:00:00Z"},
            {"article_id": "unknown", "source_name": "example.test", "body": "must stay untimed", "published_at": None},
            {
                "article_id": "other",
                "source_name": "different.test",
                "body": "excluded",
                "published_at": "2026-11-13T00:00:00Z",
            },
        ],
    )
    gold = snapshot_file(
        tmp_path,
        "gold",
        "article_daily_stats",
        [{"date": "2026-11-13", "source_name": "example.test", "article_count": 2, "total_text_characters": 5}],
    )
    with store.conn:
        stage_lineage(
            store,
            "articles",
            {
                "run_id": "articles",
                "bronze": [],
                "silver": [articles],
                "gold": [gold],
                "tables": {"articles": [articles["path"]]},
                "marts": {"article_daily_stats": [gold["path"]]},
            },
        )
    result = trace_aggregate(
        store, lake, "articles", "article_daily_stats", {"date": "2026-11-13", "source_name": "example.test"}
    )
    assert result["status"] == "verified_aggregate" and result["total_members"] == 2
    assert result["checks"]["total_text_characters"]["passed"]
    store.conn.close()


def test_new_publication_pins_actual_sentiment_thresholds_including_explicit_overrides():
    import pandas as pd
    from spark.etl.gold_aggregator import GoldAggregator
    from storage.storage_manager import storage

    products = pd.DataFrame([{"product_id": "p", "brand": "A", "category": "Phone", "price": 10, "currency": "VND"}])
    reviews = pd.DataFrame([{"review_id": "r", "product_id": "p", "rating": 5, "review_date": "2026-11-13T00:00:00Z"}])
    GoldAggregator.build_gold_marts(products, reviews, positive_threshold=5, negative_threshold=1, run_id="policy")
    snapshot = storage.read_json("gold/_snapshot.json")
    assert snapshot["sentiment_thresholds"] == {"positive": 5, "negative": 1}


def test_review_recipe_checks_sample_variance_thresholds_and_catalog_price_without_review_weighting(tmp_path):
    from serving.lineage import stage_lineage, trace_aggregate

    store, lake, _ = fixture(tmp_path)
    reviews = snapshot_file(
        tmp_path,
        "silver",
        "reviews",
        [
            {"review_id": "one", "product_id": "p", "rating": 5, "review_date": "2026-11-13T00:00:00Z"},
            {"review_id": "two", "product_id": "p", "rating": 1, "review_date": "2026-11-13T00:00:00Z"},
        ],
    )
    products = snapshot_file(tmp_path, "silver", "products", [{"product_id": "p", "price": 10, "currency": "VND"}])
    gold = snapshot_file(
        tmp_path,
        "gold",
        "product_daily_stats",
        [
            {
                "date": "2026-11-13",
                "product_id": "p",
                "review_count": 2,
                "rating_sample_size": 2,
                "avg_rating": 3,
                "weighted_avg_rating": 3,
                "rating_std": 2.83,
                "positive_ratio": 0.5,
                "negative_ratio": 0.5,
                "high_rating_ratio": 0.5,
                "low_rating_ratio": 0.5,
                "avg_price": 10,
                "min_price": 10,
                "max_price": 10,
                "price_source": "catalog_fallback",
                "currency": "VND",
            }
        ],
    )
    with store.conn:
        stage_lineage(
            store,
            "review-policy",
            {
                "run_id": "review-policy",
                "bronze": [],
                "silver": [reviews, products],
                "gold": [gold],
                "tables": {"reviews": [reviews["path"]], "products": [products["path"]], "prices": []},
                "marts": {"product_daily_stats": [gold["path"]]},
                "sentiment_thresholds": {"positive": 5, "negative": 1},
            },
        )
    result = trace_aggregate(
        store, lake, "review-policy", "product_daily_stats", {"date": "2026-11-13", "product_id": "p"}
    )
    assert result["status"] == "verified_aggregate"
    assert result["checks"]["weighted_avg_rating"]["passed"]
    assert result["checks"]["rating_std"]["passed"]
    assert result["checks"]["positive_ratio"]["passed"] and result["checks"]["negative_ratio"]["passed"]
    assert result["checks"]["price_source"]["passed"]
    assert result["unchecked_fields"] == []
    store.conn.close()


def test_price_recipe_variance_is_stable_for_large_original_currency_values(tmp_path):
    from serving.lineage import stage_lineage, trace_aggregate

    store, lake, _ = fixture(tmp_path)
    base = 100_000_000_000_000
    prices = snapshot_file(
        tmp_path,
        "silver",
        "prices",
        [
            {
                "price_id": "one",
                "product_id": "p",
                "price": base,
                "currency": "VND",
                "timestamp": "2026-11-13T00:00:00Z",
            },
            {
                "price_id": "two",
                "product_id": "p",
                "price": base + 2,
                "currency": "VND",
                "timestamp": "2026-11-13T00:00:00Z",
            },
        ],
    )
    gold = snapshot_file(
        tmp_path,
        "gold",
        "price_daily_stats",
        [
            {
                "date": "2026-11-13",
                "product_id": "p",
                "data_points": 2,
                "min_price": base,
                "max_price": base + 2,
                "avg_price": base + 1,
                "price_std": 1.41,
                "currency": "VND",
            }
        ],
    )
    with store.conn:
        stage_lineage(
            store,
            "large-price",
            {
                "run_id": "large-price",
                "bronze": [],
                "silver": [prices],
                "gold": [gold],
                "tables": {"prices": [prices["path"]]},
                "marts": {"price_daily_stats": [gold["path"]]},
            },
        )
    result = trace_aggregate(store, lake, "large-price", "price_daily_stats", {"date": "2026-11-13", "product_id": "p"})
    assert result["status"] == "verified_aggregate"
    assert result["checks"]["price_std"]["passed"]
    store.conn.close()


def test_winner_link_rejects_reused_event_id_with_a_different_observation_clock(tmp_path):
    from serving.lineage import stage_lineage, trace_record

    store, lake, path = fixture(tmp_path)
    rows = [
        {"event_id": "shared", "payload": {"review_id": "r", "product_id": "p", "rating": 5, "review_date": clock}}
        for clock in ("2026-11-12T00:00:00Z", "2026-11-13T00:00:00Z")
    ]
    path.write_text(json.dumps(rows))
    silver = snapshot_file(
        tmp_path,
        "silver",
        "reviews",
        [
            {
                "event_id": "shared",
                "review_id": "r",
                "product_id": "p",
                "rating": 5,
                "review_date_raw": "2026-11-13T00:00:00Z",
                "review_date": "2026-11-13T00:00:00Z",
            }
        ],
    )
    raw = path.read_bytes()
    bronze = {
        "path": path.relative_to(tmp_path).as_posix(),
        "size": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }
    with store.conn:
        stage_lineage(
            store,
            "clock",
            {
                "run_id": "clock",
                "bronze": [bronze],
                "silver": [silver],
                "gold": [],
                "tables": {"reviews": [silver["path"]]},
            },
        )
    result = trace_record(store, lake, "clock", "reviews", "r")
    assert result["status"] == "verified_winner" and len(result["records"]) == 1
    assert result["records"][0]["payload"]["review_date"] == "2026-11-13T00:00:00Z"
    store.conn.close()


@pytest.mark.parametrize("tampering", ["replace", "add"])
def test_publication_refuses_canonical_parquet_that_differs_from_run_checkpoint(tmp_path, tampering):
    import pandas as pd
    from serving.lineage import commerce_receipt
    from storage.storage_manager import storage
    from orchestration.run_manifest import manifest_path, reference_output

    storage.write_silver_parquet("reviews", pd.DataFrame([{"review_id": "original", "event_id": "original-event"}]))
    references = [reference_output(path) for path in storage.list_objects("silver/reviews/")]
    storage.write_json(manifest_path("pinned"), {"run_id": "pinned", "working_outputs": references})
    if tampering == "replace":
        storage.write_silver_parquet("reviews", pd.DataFrame([{"review_id": "changed", "event_id": "changed-event"}]))
    else:
        storage.write_parquet(
            "silver/reviews/unexpected.parquet", pd.DataFrame([{"review_id": "extra", "event_id": "extra-event"}])
        )
    with pytest.raises(ValueError, match="checkpoint|publication"):
        commerce_receipt("pinned", tables=["reviews"], marts=[])
