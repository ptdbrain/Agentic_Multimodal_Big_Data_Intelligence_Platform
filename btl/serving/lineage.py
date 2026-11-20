"""Checksum-pinned publication snapshots with explicit winner and legacy-partial evidence."""

import hashlib
import json
import math
import re
from collections.abc import Iterator, Sequence
from pathlib import PurePosixPath

from ingestion.privacy import sanitize_record_pii
from serving.lake import DatasetLake, bronze_records
from serving.store import ServingStore, identifier, instant, now

ENTITY_KEYS = {"products": "product_id", "reviews": "review_id", "prices": "price_id", "articles": "article_id"}


def _reference(value: dict, layer: str) -> dict:
    key = value["path"]
    path = PurePosixPath(key)
    if (
        not isinstance(key, str)
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in key
        or len(path.parts) < 2
        or path.parts[0] != layer
    ):
        raise ValueError("Unsafe lineage object reference")
    if not re.fullmatch(r"[0-9a-f]{64}", value["sha256"]):
        raise ValueError("Lineage checksum required")
    if type(value["size"]) is not int or value["size"] < 0:
        raise ValueError("Lineage byte size required")
    return {"path": str(path), "sha256": value["sha256"], "size": value["size"]}


def stage_lineage(store: ServingStore, generation: str, receipt: dict) -> None:
    """Stage immutable receipt within the caller's warehouse transaction."""
    generation = identifier(generation)
    value = {"run_id": identifier(receipt["run_id"])}
    for layer in ("bronze", "silver", "gold"):
        value[layer] = sorted((_reference(item, layer) for item in receipt.get(layer, [])), key=lambda x: x["path"])
        if len({item["path"] for item in value[layer]}) != len(value[layer]):
            raise ValueError("Duplicate lineage object")
    for name, layer in (("tables", "silver"), ("marts", "gold")):
        if name in receipt:
            allowed = (
                set(ENTITY_KEYS)
                if name == "tables"
                else {
                    "product_daily_stats",
                    "brand_daily_stats",
                    "category_daily_stats",
                    "price_daily_stats",
                    "article_daily_stats",
                }
            )
            paths = {item["path"] for item in value[layer]}
            mapping = receipt[name]
            if set(mapping) - allowed or any(
                not isinstance(items, list) or set(items) - paths for items in mapping.values()
            ):
                raise ValueError("Snapshot mapping is outside pinned lineage references")
            value[name] = {key: sorted(set(items)) for key, items in sorted(mapping.items())}
    if "strict_analytics_time" in receipt:
        if type(receipt["strict_analytics_time"]) is not bool:
            raise ValueError("Explicit analytics time policy required")
        value["strict_analytics_time"] = receipt["strict_analytics_time"]
    if "sentiment_thresholds" in receipt:
        thresholds = receipt["sentiment_thresholds"]
        if set(thresholds) != {"positive", "negative"} or any(
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(item)
            or not 1 <= item <= 5
            for item in thresholds.values()
        ):
            raise ValueError("Explicit finite sentiment thresholds required")
        value["sentiment_thresholds"] = thresholds
    value["status"] = "pinned_inputs" if value["bronze"] else "unknown"
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    digest = hashlib.sha256(payload.encode()).hexdigest()
    previous = store.execute(
        "SELECT receipt_sha256 FROM publication_lineage WHERE dataset_id=? AND generation=?",
        (store.dataset_id, generation),
    ).fetchone()
    if previous:
        if previous[0] != digest:
            raise ValueError("Immutable publication lineage changed")
        return
    store.execute(
        "INSERT INTO publication_lineage VALUES(?,?,?,?,?,?,?)",
        (store.dataset_id, generation, store.source_group, value["run_id"], payload, digest, now()),
    )


def _archive_reference(run_id: str, source: dict, layer: str) -> dict:
    """Copy into a generation-owned immutable object before SQL publication."""
    import os
    import shutil
    import tempfile

    from orchestration.run_manifest import reference_output
    from storage.local_backend import LocalBackend
    from storage.minio_backend import MinIOBackend
    from storage.storage_manager import storage

    source = _reference(source, layer)
    relative = PurePosixPath(source["path"]).relative_to(layer)
    if "serving_snapshots" in relative.parts:
        raise ValueError("Cannot recursively archive serving snapshots")
    destination = layer + "/serving_snapshots/" + hashlib.sha256(run_id.encode()).hexdigest() + "/" + str(relative)
    expected = {**source, "path": destination}
    if storage.object_exists(destination):
        if reference_output(destination) != expected:
            raise ValueError("Immutable serving snapshot changed")
        return expected
    backend = storage.backend
    if isinstance(backend, LocalBackend):
        root = backend.base_dir.resolve()
        target = root / destination
        current = root
        for component in PurePosixPath(destination).parts:
            current /= component
            if current.is_symlink():
                raise ValueError("Snapshot target contains symlink")
        if not target.resolve().is_relative_to(root):
            raise ValueError("Snapshot escapes lake")
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".archive-", dir=target.parent)
        try:
            with os.fdopen(descriptor, "wb") as output, (root / source["path"]).open("rb") as stream:
                shutil.copyfileobj(stream, output)
                output.flush()
                os.fsync(output.fileno())
            os.link(temporary, target)
        finally:
            os.unlink(temporary)
    elif isinstance(backend, MinIOBackend):
        response = backend.client.get_object(backend.bucket, source["path"])
        try:
            backend.client.put_object(
                backend.bucket, destination, response, length=source["size"], content_type="application/octet-stream"
            )
        finally:
            response.close()
            response.release_conn()
    else:
        raise ValueError("Unsupported archive backend")
    if reference_output(destination) != expected:
        raise ValueError("Archived Parquet checksum differs from publication input")
    return expected


def _inherit_bronze(store: ServingStore | None, bronze: list[dict]) -> list[dict]:
    if store is not None:
        prior = store.execute(
            "SELECT receipt_json,receipt_sha256 FROM publication_lineage WHERE dataset_id=? "
            "ORDER BY created_at DESC LIMIT 1",
            (store.dataset_id,),
        ).fetchone()
        if prior:
            if hashlib.sha256(prior[0].encode()).hexdigest() != prior[1]:
                raise ValueError("Previous lineage receipt checksum mismatch")
            inherited = {item["path"]: item for item in json.loads(prior[0])["bronze"]}
            for item in bronze:
                if item["path"] in inherited and inherited[item["path"]] != item:
                    raise ValueError("Immutable Bronze lineage changed")
                inherited[item["path"]] = item
            bronze = list(inherited.values())
    return bronze


def commerce_receipt(run_id: str, *, tables: Sequence[str] | None = None, marts: Sequence[str] | None = None, store: ServingStore | None = None) -> dict:
    """Pin raw checkpoints and immutable Silver/Gold versions; never infer missing raw ancestry."""
    from orchestration.run_manifest import manifest_path, reference_output
    from storage.storage_manager import storage

    run_id = identifier(run_id)
    try:
        manifest = storage.read_json(manifest_path(run_id))
    except FileNotFoundError:
        manifest = {"run_id": run_id}
    if manifest.get("run_id") != run_id:
        raise ValueError("Run manifest identity mismatch")
    references = manifest.get("checkpoints", {}).get("collect_and_publish", {}).get("outputs", [])
    bronze = [item for item in references if item.get("path", "").startswith("bronze/")]
    for item in bronze:
        _reference(item, "bronze")
        if reference_output(item["path"]) != item:
            raise ValueError("Checkpoint Bronze checksum changed before publication")
    bronze = _inherit_bronze(store, bronze)
    result = {"run_id": run_id, "bronze": bronze, "silver": [], "gold": [], "tables": {}, "marts": {}}
    for layer, names, mapping in (
        ("silver", tables if tables is not None else ["products", "reviews", "prices"], "tables"),
        (
            "gold",
            marts
            if marts is not None
            else ["product_daily_stats", "brand_daily_stats", "category_daily_stats", "price_daily_stats"],
            "marts",
        ),
    ):
        for name in names:
            if name not in (
                set(ENTITY_KEYS)
                if layer == "silver"
                else {"product_daily_stats", "brand_daily_stats", "category_daily_stats", "price_daily_stats"}
            ):
                raise ValueError("Unsupported snapshot table")
            result[mapping][name] = []
            prefix = layer + "/" + name + "/"
            paths = sorted(path for path in storage.list_objects(prefix) if path.endswith(".parquet"))
            pins = {
                item["path"]: item
                for item in manifest.get("working_outputs", [])
                if item.get("path", "").startswith(prefix) and item["path"].endswith(".parquet")
            }
            if manifest.get("working_outputs") and set(paths) != set(pins):
                raise ValueError("Publication Parquet object set differs from run checkpoint")
            for path in paths:
                source = reference_output(path)
                if path in pins and source != pins[path]:
                    raise ValueError("Publication Parquet checksum differs from run checkpoint")
                archived = _archive_reference(run_id, source, layer)
                result[layer].append(archived)
                result[mapping][name].append(archived["path"])
    return result


def article_receipt(
    run_id: str, paths: list[str], silver_path: str, gold_path: str, *, store: ServingStore | None = None
) -> dict:
    from orchestration.run_manifest import reference_canonical_output, reference_output

    return {
        "run_id": run_id,
        "bronze": _inherit_bronze(store, [reference_canonical_output(path) for path in paths]),
        "silver": [reference_output(silver_path)],
        "gold": [reference_output(gold_path)],
        "tables": {"articles": [silver_path]},
        "marts": {"article_daily_stats": [gold_path]},
    }


def pinned_rows(
    receipt: dict, lake: DatasetLake, table: str, *, layer: str = "silver", columns: Sequence[str] | None = None
) -> Iterator[dict]:
    """Stream verified Parquet batches; missing snapshots yield no fabricated rows."""
    import pyarrow.parquet as pq

    mapping = "tables" if layer == "silver" else "marts"
    refs = {item["path"]: item for item in receipt[layer]}
    if len(receipt.get(mapping, {}).get(table, [])) > 1000:
        raise ValueError("Snapshot exceeds interactive object scan limit")
    for path in receipt.get(mapping, {}).get(table, []):
        reference = _reference(refs[path], layer)
        with lake.verified_file(reference) as stream:
            parquet = pq.ParquetFile(stream)
            selected = [name for name in columns if name in parquet.schema_arrow.names] if columns else None
            for batch in parquet.iter_batches(batch_size=4096, columns=selected):
                yield from batch.to_pylist()


def _winner_matches(payload: dict, envelope: dict, silver: dict, entity: str) -> bool:
    # The event link is necessary; critical business values also guard ambiguous
    # or malformed input that reuses an event identity with different content.
    if entity == "articles":
        body = payload.get("body")
        if not isinstance(body, str) or hashlib.sha256(body.encode()).hexdigest() != silver.get("content_sha256"):
            return False
        if payload.get("source_url") != silver.get("source_url"):
            return False
        try:
            if instant(payload.get("collected_at")) != instant(silver.get("collected_at")):
                return False
        except (TypeError, ValueError):
            return False
        if silver.get("published_at_raw") is not None and str(
            payload.get("published_at_raw", payload.get("published_at"))
        ) != str(silver["published_at_raw"]):
            return False
        return True
    event = payload.get("event_id") or envelope.get("event_id")
    if not silver.get("event_id") or event != silver["event_id"]:
        return False
    for key in ("product_id", "review_id", "price_id", "article_id"):
        if silver.get(key) is not None:
            if payload.get(key) is None or str(silver[key]).strip() != str(payload[key]).strip():
                return False
    numeric = "rating" if entity == "reviews" else "price" if entity in {"products", "prices"} else None
    if numeric:
        if numeric not in silver or payload.get(numeric) is None:
            return False
        raw = silver.get(numeric + "_raw")
        if raw is not None:
            if str(raw).strip() != str(payload[numeric]).strip():
                try:
                    from decimal import Decimal

                    if Decimal(str(raw)) != Decimal(str(payload[numeric])):
                        return False
                except Exception:
                    return False
        else:
            try:
                if float(silver[numeric]) != float(payload[numeric]):
                    return False
            except (TypeError, ValueError):
                return False
    if entity in {"products", "prices"}:
        from spark.etl.normalizer import EntityNormalizer

        currency = EntityNormalizer.normalize_currency(payload.get("currency_raw", payload.get("currency")))
        if currency is None or currency != silver.get("currency"):
            return False
    for key in ("review_date", "timestamp", "created_at", "ingested_at"):
        expected = silver.get(key + "_raw")
        if expected is None:
            expected = silver.get(key)
        if expected is None:
            continue
        observed = payload.get(key)
        if observed is None and key == "ingested_at":
            observed = envelope.get(key)
        if str(expected).strip() == str(observed).strip():
            continue
        try:
            from ingestion.identity import canonical_timestamp

            equal = canonical_timestamp(str(expected)) == canonical_timestamp(str(observed))
        except (TypeError, ValueError):
            try:
                from decimal import Decimal

                equal = Decimal(str(expected)) == Decimal(str(observed))
            except Exception:
                equal = False
        if not equal:
            return False
    for key in ("review_text", "review_title", "product_name"):
        if silver.get(key) is not None:
            if payload.get(key) is None or str(silver[key]).strip() != str(payload[key]).strip():
                return False
    return True


def trace_record(
    store: ServingStore,
    lake: DatasetLake,
    generation: str,
    entity: str,
    record_id: str,
    *,
    max_objects: int = 500,
    object_offset: int = 0,
) -> dict:
    """Locate exact logical IDs in verified raw input objects, without guessing missing links."""
    identifier(generation)
    if entity not in ENTITY_KEYS or not record_id or len(record_id) > 500:
        raise ValueError("Explicit entity and record identity required")
    if lake.binding.id != store.dataset_id:
        raise ValueError("Lineage lake belongs to another dataset")
    if type(max_objects) is not int or not 1 <= max_objects <= 500:
        raise ValueError("Bounded lineage scan required")
    if type(object_offset) is not int or object_offset < 0:
        raise ValueError("Nonnegative object cursor required")
    row = store.execute(
        "SELECT receipt_json,receipt_sha256 FROM publication_lineage WHERE dataset_id=? AND generation=?",
        (store.dataset_id, generation),
    ).fetchone()
    if not row:
        return {"status": "unknown", "records": [], "reason": "No pinned publication receipt"}
    if hashlib.sha256(row[0].encode()).hexdigest() != row[1]:
        raise ValueError("Publication receipt checksum mismatch")
    receipt = json.loads(row[0])
    silver = [
        value for value in pinned_rows(receipt, lake, entity) if str(value.get(ENTITY_KEYS[entity], "")) == record_id
    ]
    if len(silver) > 1:
        raise ValueError("Pinned Silver identity is not unique")
    winner = silver[0] if silver else None
    winner_ready = winner is not None and (
        bool(winner.get("event_id"))
        if entity != "articles"
        else all(winner.get(key) for key in ("content_sha256", "collected_at", "source_url"))
    )
    result = []
    for value in receipt["bronze"][object_offset : object_offset + max_objects]:
        ref = _reference(value, "bronze")
        with lake.verified_file(ref) as stream:
            for index, envelope in enumerate(bronze_records(stream)):
                payload = envelope.get("payload", envelope) if isinstance(envelope, dict) else None
                if not isinstance(payload, dict) or str(payload.get(ENTITY_KEYS[entity], "")).strip() != record_id:
                    continue
                if winner_ready and not _winner_matches(payload, envelope, winner, entity):
                    continue
                result.append(
                    {
                        "object_path": ref["path"],
                        "object_sha256": ref["sha256"],
                        "record_index": index,
                        "record_sha256": hashlib.sha256(
                            json.dumps(envelope, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
                        ).hexdigest(),
                        "payload": sanitize_record_pii(payload),
                    }
                )
    truncated = len(receipt["bronze"]) > object_offset + max_objects
    return {
        "status": "verified_winner"
        if result and winner_ready
        else "verified_input"
        if result
        else "bounded_not_found"
        if truncated
        else "not_found"
        if receipt["bronze"]
        else "unknown",
        "records": result,
        "truncated": truncated,
        "next_object_offset": object_offset + max_objects if truncated else None,
        "physical_occurrence_unique": len(result) == 1 and not truncated and object_offset == 0,
        "receipt": receipt,
        "relationship": "selected_silver_record" if result and winner_ready else "publication_input_candidate",
        "silver_record": sanitize_record_pii(winner) if winner is not None else None,
        "reason": "Verified archived Silver event identity and business values"
        if result and winner_ready
        else "Missing winner evidence; raw input candidates alone do not establish dedup selection",
    }


MART_GRAINS = {
    "product_daily_stats": ("date", "product_id"),
    "brand_daily_stats": ("date", "brand"),
    "category_daily_stats": ("date", "category"),
    "price_daily_stats": ("date", "product_id"),
    "article_daily_stats": ("date", "source_name"),
}


def _day(value):
    from datetime import date, datetime, timezone

    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    try:
        point = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if point.tzinfo is None:
        # These values are read from normalized Silver, whose Spark session uses UTC.
        point = point.replace(tzinfo=timezone.utc)
    return point.astimezone(timezone.utc).date().isoformat()


def trace_aggregate(
    store: ServingStore, lake: DatasetLake, generation: str, mart: str, grain: dict, *, offset: int = 0, limit: int = 100
) -> dict:
    """Verify complete normalized input membership, returning a bounded page of contributing IDs."""
    from datetime import date
    from decimal import Decimal, InvalidOperation

    identifier(generation)
    if lake.binding.id != store.dataset_id:
        raise ValueError("Lineage lake belongs to another dataset")
    if mart not in MART_GRAINS or set(grain) != set(MART_GRAINS[mart]):
        raise ValueError("Exact supported aggregate grain required")
    if any(not isinstance(value, str) or not value or len(value) > 500 for value in grain.values()):
        raise ValueError("Bounded explicit grain values required")
    date.fromisoformat(grain["date"])
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Bounded member page required")
    row = store.execute(
        "SELECT receipt_json,receipt_sha256 FROM publication_lineage WHERE dataset_id=? AND generation=?",
        (store.dataset_id, generation),
    ).fetchone()
    if not row:
        return {"status": "unknown", "records": [], "reason": "No pinned publication receipt"}
    if hashlib.sha256(row[0].encode()).hexdigest() != row[1]:
        raise ValueError("Publication receipt checksum mismatch")
    receipt = json.loads(row[0])
    gold = [
        row
        for row in pinned_rows(receipt, lake, mart, layer="gold")
        if all((_day(row.get(key)) if key == "date" else str(row.get(key))) == value for key, value in grain.items())
    ]
    if len(gold) != 1:
        return {
            "status": "not_found" if not gold else "ambiguous",
            "records": [],
            "reason": "Gold grain must identify one row",
        }
    published = gold[0]
    entity = "prices" if mart == "price_daily_stats" else "articles" if mart == "article_daily_stats" else "reviews"
    if entity not in receipt.get("tables", {}):
        return {
            "status": "unknown",
            "records": [],
            "reason": "Missing archived normalized input",
            "gold_row": published,
        }
    dimension = MART_GRAINS[mart][1]
    catalog = {}
    if dimension in {"brand", "category"}:
        if "products" not in receipt.get("tables", {}):
            return {"status": "unknown", "records": [], "reason": "Missing archived catalog for dimension join"}
        for product in pinned_rows(
            receipt, lake, "products", columns=["product_id", "brand", "category", "price", "currency"]
        ):
            product_id = str(product["product_id"])
            if product_id in catalog or len(catalog) >= 100000:
                raise ValueError("Ambiguous or oversized catalog join")
            catalog[product_id] = product
    records, total, value_sum, minimum, maximum, text_characters = [], 0, Decimal(0), None, None, 0
    units, product_ids = set(), set()
    catalog_truncated = False
    used_products = set()
    running_mean, running_m2 = Decimal(0), Decimal(0)
    positive_count, negative_count = 0, 0
    thresholds = receipt.get("sentiment_thresholds")
    flags_known, all_times_available = True, True
    columns = [
        ENTITY_KEYS[entity],
        "product_id",
        "event_id",
        "source_name",
        "rating",
        "price",
        "currency",
        "review_date",
        "timestamp",
        "published_at",
        "ingested_at",
        "body",
        "time_available",
    ]
    for member in pinned_rows(receipt, lake, entity, columns=columns):
        product_id = str(member.get("product_id", ""))
        if dimension == "product_id" and product_id != grain[dimension]:
            continue
        if dimension in {"brand", "category"} and str(catalog.get(product_id, {}).get(dimension)) != grain[dimension]:
            continue
        if dimension == "source_name" and str(member.get("source_name")) != grain[dimension]:
            continue
        clock = member.get(
            "review_date" if entity == "reviews" else "timestamp" if entity == "prices" else "published_at"
        )
        if clock is None and receipt.get("strict_analytics_time") is False and entity == "reviews":
            clock = member.get("ingested_at")
        if _day(clock) != grain["date"]:
            continue
        if entity != "articles":
            try:
                value = Decimal(str(member.get("rating" if entity == "reviews" else "price")))
            except InvalidOperation as error:
                raise ValueError("Invalid normalized aggregate member value") from error
            if (
                not value.is_finite()
                or (entity == "reviews" and not 1 <= value <= 5)
                or (entity == "prices" and value <= 0)
            ):
                raise ValueError("Invalid normalized aggregate member value")
            value_sum += value
            delta = value - running_mean
            running_mean += delta / (total + 1)
            running_m2 += delta * (value - running_mean)
            if entity == "reviews" and thresholds:
                positive_count += int(value >= Decimal(str(thresholds["positive"])))
                negative_count += int(value <= Decimal(str(thresholds["negative"])))
            minimum = value if minimum is None else min(minimum, value)
            maximum = value if maximum is None else max(maximum, value)
            if entity == "prices":
                units.add(member.get("currency"))
        else:
            text_characters += len(member.get("body") or "")
        if offset <= total < offset + limit:
            records.append(
                {
                    key: member[key]
                    for key in (ENTITY_KEYS[entity], "product_id", "event_id", "source_name")
                    if key in member
                }
            )
        total += 1
        flag = member.get("time_available")
        if not isinstance(flag, bool):
            flags_known = False
        else:
            all_times_available = all_times_available and flag
        if product_id:
            used_products.add(product_id)
            if len(used_products) > 100000:
                raise ValueError("Interactive dependency set exceeds limit")
        if product_id and product_id not in product_ids:
            if len(product_ids) < 1000:
                product_ids.add(product_id)
            else:
                catalog_truncated = True
    count_field = "data_points" if entity == "prices" else "article_count" if entity == "articles" else "review_count"
    try:
        count = Decimal(str(published[count_field]))
    except (KeyError, InvalidOperation) as error:
        raise ValueError("Published aggregate has no valid sample count") from error
    checks = {count_field: {"expected": str(count), "actual": total, "passed": count == total}}

    def check_number(field, actual, *, decimals=2, exact=False):
        if field not in published:
            return
        expected_value = published[field]
        if expected_value is None:
            if actual is None:
                checks[field] = {"expected": None, "actual": None, "passed": True}
            return  # Nullable/uncomputed metric remains explicitly unchecked.
        expected = Decimal(str(expected_value))
        tolerance = Decimal(0) if exact else Decimal(10) ** -decimals / 2 + Decimal("0.000000001")
        passed = actual is not None and expected.is_finite() and abs(expected - actual) <= tolerance
        checks[field] = {
            "expected": str(expected),
            "actual": str(actual) if actual is not None else None,
            "passed": passed,
        }

    def positive_price(value):
        try:
            price = Decimal(str(value))
            return price if price.is_finite() and price > 0 else None
        except InvalidOperation:
            return None

    if entity == "articles":
        check_number("total_text_characters", Decimal(text_characters), exact=True)
    elif total:
        check_number("avg_rating" if entity == "reviews" else "avg_price", value_sum / total)
        variance = running_m2 / (total - 1) if total > 1 else None
        if variance is not None and variance < 0:
            raise ValueError("Unstable variance cannot be certified")
        standard_deviation = variance.sqrt() if variance is not None else None
        check_number("rating_std" if entity == "reviews" else "price_std", standard_deviation)
        if entity == "prices":
            check_number("min_price", minimum)
            check_number("max_price", maximum)
            checks["currency"] = {
                "expected": published.get("currency"),
                "actual": sorted(str(unit) for unit in units),
                "passed": units == {published.get("currency")} and published.get("currency") is not None,
            }
        else:
            check_number("rating_sample_size", Decimal(total), exact=True)
            check_number("weighted_avg_rating", value_sum / total)
            if flags_known and "time_available" in published:
                checks["time_available"] = {
                    "expected": published["time_available"],
                    "actual": all_times_available,
                    "passed": published["time_available"] == all_times_available,
                }
            if thresholds:
                for field, count in (
                    ("positive_ratio", positive_count),
                    ("high_rating_ratio", positive_count),
                    ("negative_ratio", negative_count),
                    ("low_rating_ratio", negative_count),
                ):
                    check_number(field, Decimal(count) / total, decimals=4)
            if dimension in {"brand", "category"}:
                check_number("product_count", Decimal(len(used_products)), exact=True)
                catalog_prices = [
                    value
                    for product_id in used_products
                    if (value := positive_price(catalog[product_id].get("price"))) is not None
                ]
                check_number(
                    "avg_price", sum(catalog_prices, Decimal(0)) / len(catalog_prices) if catalog_prices else None
                )
                catalog_units = {catalog[product_id].get("currency") for product_id in used_products}
                if published.get("currency") is not None and None not in catalog_units:
                    checks["currency"] = {
                        "expected": published["currency"],
                        "actual": sorted(catalog_units),
                        "passed": catalog_units == {published["currency"]},
                    }
            elif "products" in receipt.get("tables", {}):
                products = [
                    product
                    for product in pinned_rows(receipt, lake, "products", columns=["product_id", "price", "currency"])
                    if str(product.get("product_id")) == grain["product_id"]
                ]
                if len(products) > 1:
                    raise ValueError("Ambiguous pinned catalog identity")
                product = products[0] if products else {}
                catalog[grain["product_id"]] = product
                observed_sum, observed_min, observed_max, observed_count, observed_units = (
                    Decimal(0),
                    None,
                    None,
                    0,
                    set(),
                )
                if "prices" in receipt.get("tables", {}):
                    for observation in pinned_rows(
                        receipt, lake, "prices", columns=["product_id", "price", "currency", "timestamp", "created_at"]
                    ):
                        clock = observation.get("timestamp") or observation.get("created_at")
                        if str(observation.get("product_id")) != grain["product_id"] or _day(clock) != grain["date"]:
                            continue
                        value = positive_price(observation.get("price"))
                        if value is None:
                            continue
                        observed_count += 1
                        observed_sum += value
                        observed_units.add(observation.get("currency"))
                        observed_min = value if observed_min is None else min(observed_min, value)
                        observed_max = value if observed_max is None else max(observed_max, value)
                fallback = positive_price(product.get("price"))
                if observed_count:
                    source, average, low, high, price_units = (
                        "observed",
                        observed_sum / observed_count,
                        observed_min,
                        observed_max,
                        observed_units,
                    )
                else:
                    source, average, low, high, price_units = (
                        "catalog_fallback" if fallback is not None else "unavailable",
                        fallback,
                        fallback,
                        fallback,
                        {product.get("currency")},
                    )
                for field, actual in (("avg_price", average), ("min_price", low), ("max_price", high)):
                    check_number(field, actual)
                if "price_source" in published:
                    checks["price_source"] = {
                        "expected": published["price_source"],
                        "actual": source,
                        "passed": published["price_source"] == source,
                    }
                if published.get("currency") is not None and None not in price_units:
                    checks["currency"] = {
                        "expected": published["currency"],
                        "actual": sorted(price_units),
                        "passed": price_units == {published["currency"]},
                    }
    recipe = {
        "version": 1,
        "generation": generation,
        "mart": mart,
        "grain": grain,
        "input_table": entity,
        "input_objects": receipt.get("tables", {}).get(entity, []),
        "join": "reviews.product_id = products.product_id" if dimension in {"brand", "category"} else None,
        "catalog_objects": receipt.get("tables", {}).get("products", []) if catalog else [],
        "time_basis": "UTC normalized observation date; explicit ingestion fallback only if receipt permits",
        "strict_analytics_time": receipt.get("strict_analytics_time"),
        "sentiment_thresholds": thresholds,
        "price_rule": "positive observations on the day, else catalog fallback; dimension means use one catalog price per reviewed product",
        "currency_policy": "original units; no conversion",
        "verified_fields": list(checks),
        "verification_scope": "complete membership and listed numeric checks",
    }
    return {
        "status": "verified_aggregate" if all(value["passed"] for value in checks.values()) else "aggregate_mismatch",
        "total_members": total,
        "records": records,
        "next_offset": offset + limit if offset + limit < total else None,
        "checks": checks,
        "gold_row": published,
        "recipe": recipe,
        "recipe_sha256": hashlib.sha256(json.dumps(recipe, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
        "catalog_product_ids": sorted(product_ids),
        "catalog_dependencies_truncated": catalog_truncated,
        "unchecked_fields": sorted(
            set(published)
            - set(grain)
            - set(checks)
            - {"run_id", "source_run_id", "source_group", "data_as_of", "time_available"}
        ),
    }
