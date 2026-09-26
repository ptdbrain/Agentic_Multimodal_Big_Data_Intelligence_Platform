import hashlib
from typing import List, Dict, Any, Tuple

class Deduplicator:
    """Deduplication engine using unique IDs and MD5 composite hashes (Section 19 in plan.md)."""

    @staticmethod
    def compute_composite_hash(record: Dict[str, Any]) -> str:
        prod_id = str(record.get("product_id", ""))
        user_id = str(record.get("user_id", ""))
        text = str(record.get("review_text", "")).strip().lower()
        date = str(record.get("review_date", ""))[:10] # match on date level
        content = f"{prod_id}|{user_id}|{text}|{date}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()

    @staticmethod
    def deduplicate_reviews(reviews: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        seen_ids = set()
        seen_hashes = set()
        unique = []
        dup_count = 0

        for r in reviews:
            rev_id = r.get("review_id")
            chash = Deduplicator.compute_composite_hash(r)
            
            if (rev_id and rev_id in seen_ids) or (chash in seen_hashes):
                dup_count += 1
                continue
                
            if rev_id:
                seen_ids.add(rev_id)
            seen_hashes.add(chash)
            unique.append(r)

        return unique, dup_count
