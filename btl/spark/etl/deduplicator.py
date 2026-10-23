import hashlib
from typing import List, Dict, Any, Tuple, Optional

class Deduplicator:
    """Deduplication engine using unique IDs and MD5 composite hashes (Section 19 in plan.md).
    Generic across reviews, products, events, and arbitrary schemas.
    """

    @staticmethod
    def compute_composite_hash(record: Dict[str, Any], hash_fields: Optional[List[str]] = None) -> str:
        if hash_fields:
            parts = [str(record.get(f, "")).strip().lower() for f in hash_fields]
        else:
            # Default review composite key
            prod_id = str(record.get("product_id", "")).strip().lower()
            user_id = str(record.get("user_id", "")).strip().lower()
            text = str(record.get("review_text", "")).strip().lower()
            date = str(record.get("review_date", ""))[:10]
            parts = [prod_id, user_id, text, date]

        content = "|".join(parts)
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    DEFAULT_TIMESTAMP_FIELDS = ("ingested_at", "updated_at", "timestamp", "review_date", "created_at")

    @staticmethod
    def _extract_sort_key(
        record: Dict[str, Any],
        timestamp_fields: Optional[List[str]] = None
    ) -> Tuple[str, str]:
        """Extracts a deterministic (normalized_timestamp, event_id) sort key for latest-wins deduplication."""
        from spark.etl.normalizer import EntityNormalizer

        ts_candidates = timestamp_fields or list(Deduplicator.DEFAULT_TIMESTAMP_FIELDS)
        best_ts = ""
        for field in ts_candidates:
            raw_val = record.get(field)
            if raw_val is not None and str(raw_val).strip() != "":
                norm = EntityNormalizer.normalize_timestamp(raw_val)
                best_ts = norm if norm is not None else str(raw_val).strip()
                break
        tie_breaker = str(record.get("event_id") or "")
        return (best_ts, tie_breaker)

    @staticmethod
    def deduplicate_records(
        records: List[Dict[str, Any]],
        id_field: Optional[str] = "id",
        hash_fields: Optional[List[str]] = None,
        timestamp_fields: Optional[List[str]] = None,
        keep_latest: bool = True
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Generic record deduplicator by unique ID and content hash.
        When keep_latest=True (default), retains the newest record according to timestamp_fields
        (with tie-breaker on event_id) when duplicate keys or composite hashes occur.
        """
        if not records:
            return [], 0

        if keep_latest:
            # Rank records so newer timestamps win while preserving original order on ties
            indexed = list(enumerate(records))
            indexed_sorted = sorted(
                indexed,
                key=lambda item: (
                    Deduplicator._extract_sort_key(item[1], timestamp_fields=timestamp_fields),
                    -item[0],
                ),
                reverse=True,
            )
        else:
            indexed_sorted = list(enumerate(records))

        seen_ids = set()
        seen_hashes = set()
        kept_with_index: List[Tuple[int, Dict[str, Any]]] = []
        dup_count = 0

        for orig_idx, r in indexed_sorted:
            rec_id = r.get(id_field) if id_field else None
            chash = Deduplicator.compute_composite_hash(r, hash_fields=hash_fields)

            if (rec_id and rec_id in seen_ids) or (chash in seen_hashes):
                dup_count += 1
                continue

            if rec_id:
                seen_ids.add(rec_id)
            seen_hashes.add(chash)
            kept_with_index.append((orig_idx, r))

        # Restore original chronological/arrival order of the surviving records
        kept_with_index.sort(key=lambda item: item[0])
        unique = [r for _, r in kept_with_index]
        return unique, dup_count

    @staticmethod
    def deduplicate_reviews(
        reviews: List[Dict[str, Any]],
        timestamp_fields: Optional[List[str]] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Backward-compatible wrapper for reviews dataset."""
        return Deduplicator.deduplicate_records(
            reviews,
            id_field="review_id",
            hash_fields=None,
            timestamp_fields=timestamp_fields
        )


