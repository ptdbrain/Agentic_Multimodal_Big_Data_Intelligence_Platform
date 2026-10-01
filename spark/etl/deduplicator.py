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

    @staticmethod
    def deduplicate_records(
        records: List[Dict[str, Any]],
        id_field: Optional[str] = "id",
        hash_fields: Optional[List[str]] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Generic record deduplicator by unique ID and content hash."""
        seen_ids = set()
        seen_hashes = set()
        unique = []
        dup_count = 0

        for r in records:
            rec_id = r.get(id_field) if id_field else None
            chash = Deduplicator.compute_composite_hash(r, hash_fields=hash_fields)

            if (rec_id and rec_id in seen_ids) or (chash in seen_hashes):
                dup_count += 1
                continue

            if rec_id:
                seen_ids.add(rec_id)
            seen_hashes.add(chash)
            unique.append(r)

        return unique, dup_count

    @staticmethod
    def deduplicate_reviews(reviews: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """Backward-compatible wrapper for reviews dataset."""
        return Deduplicator.deduplicate_records(
            reviews,
            id_field="review_id",
            hash_fields=None
        )

