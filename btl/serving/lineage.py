"""Pinned publication inputs; verified input candidates are never labelled exact transformed winners."""

from pathlib import PurePosixPath
import hashlib
import json
import re
from serving.store import ServingStore, identifier, now
from ingestion.privacy import sanitize_record_pii

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


def commerce_receipt(run_id: str) -> dict:
    """Use checkpoint-pinned Bronze hashes; missing legacy manifests stay unknown."""
    from orchestration.run_manifest import manifest_path, reference_output
    from storage.storage_manager import storage

    try:
        manifest = storage.read_json(manifest_path(run_id))
    except FileNotFoundError:
        return {"run_id": run_id, "bronze": [], "silver": [], "gold": []}
    if manifest.get("run_id") != run_id:
        raise ValueError("Run manifest identity mismatch")
    references = manifest.get("checkpoints", {}).get("collect_and_publish", {}).get("outputs", [])
    bronze = [item for item in references if item.get("path", "").startswith("bronze/")]
    for item in bronze:
        _reference(item, "bronze")
        if reference_output(item["path"]) != item:
            raise ValueError("Checkpoint Bronze checksum changed before publication")
    result = {"run_id": run_id, "bronze": bronze}
    for layer in ("silver", "gold"):
        result[layer] = [
            item for item in manifest.get("working_outputs", []) if item.get("path", "").startswith(layer + "/")
        ]
    return result


def article_receipt(run_id: str, paths: list[str], silver_path: str, gold_path: str) -> dict:
    from orchestration.run_manifest import reference_canonical_output, reference_output

    return {
        "run_id": run_id,
        "bronze": [reference_canonical_output(path) for path in paths],
        "silver": [reference_output(silver_path)],
        "gold": [reference_output(gold_path)],
    }


def trace_record(
    store: ServingStore, lake, generation: str, entity: str, record_id: str, *, max_objects: int = 500
) -> dict:
    """Locate exact logical IDs in verified raw input objects, without guessing missing links."""
    identifier(generation)
    if entity not in ENTITY_KEYS or not record_id or len(record_id) > 500:
        raise ValueError("Explicit entity and record identity required")
    if lake.binding.id != store.dataset_id:
        raise ValueError("Lineage lake belongs to another dataset")
    if type(max_objects) is not int or not 1 <= max_objects <= 500:
        raise ValueError("Bounded lineage scan required")
    row = store.execute(
        "SELECT receipt_json,receipt_sha256 FROM publication_lineage WHERE dataset_id=? AND generation=?",
        (store.dataset_id, generation),
    ).fetchone()
    if not row:
        return {"status": "unknown", "records": [], "reason": "No pinned publication receipt"}
    if hashlib.sha256(row[0].encode()).hexdigest() != row[1]:
        raise ValueError("Publication receipt checksum mismatch")
    receipt = json.loads(row[0])
    result = []
    for value in receipt["bronze"][:max_objects]:
        ref = _reference(value, "bronze")
        raw = lake.read_bytes(ref["path"])
        if len(raw) != ref["size"] or hashlib.sha256(raw).hexdigest() != ref["sha256"]:
            raise ValueError("Bronze object bytes/checksum mismatch")
        records = json.loads(raw)
        if not isinstance(records, list):
            raise ValueError("Expected Bronze record array")
        for index, envelope in enumerate(records):
            payload = envelope.get("payload", envelope) if isinstance(envelope, dict) else None
            if not isinstance(payload, dict) or str(payload.get(ENTITY_KEYS[entity], "")) != record_id:
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
    truncated = len(receipt["bronze"]) > max_objects
    return {
        "status": "verified_input"
        if result
        else "bounded_not_found"
        if truncated
        else "not_found"
        if receipt["bronze"]
        else "unknown",
        "records": result,
        "truncated": truncated,
        "receipt": receipt,
        "relationship": "publication_input_candidate",
        "reason": "Input evidence does not by itself prove the selected Silver deduplication winner",
    }
