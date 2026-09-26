import time
from typing import List, Dict, Any
from spark.quality.rules import QualityRule

class DataQualityEvaluator:
    """Calculates data quality metrics and scorecards (Section 20-22 in plan.md)."""
    
    @staticmethod
    def evaluate_batch(dataset_name: str, records: List[Dict[str, Any]], rules: List[QualityRule]) -> Dict[str, Any]:
        t0 = time.time()
        received = len(records)
        valid_records = []
        invalid_records = []
        duplicate_count = 0
        missing_count = 0

        seen_ids = set()

        for rec in records:
            # Check duplication by primary id
            rec_id = rec.get("review_id") or rec.get("product_id") or rec.get("price_id")
            if rec_id and rec_id in seen_ids:
                duplicate_count += 1
                continue
            if rec_id:
                seen_ids.add(rec_id)

            # Evaluate rules
            rec_valid = True
            for rule in rules:
                is_ok, err_msg = rule.validate(rec)
                if not is_ok:
                    rec_valid = False
                    if "Missing" in err_msg:
                        missing_count += 1
                    break

            if rec_valid:
                valid_records.append(rec)
            else:
                invalid_records.append(rec)

        processing_time_ms = int((time.time() - t0) * 1000)
        valid_count = len(valid_records)
        dq_score = round((valid_count / received * 100.0), 2) if received > 0 else 0.0

        return {
            "dataset_name": dataset_name,
            "records_received": received,
            "records_valid": valid_count,
            "records_invalid": len(invalid_records),
            "records_duplicate": duplicate_count,
            "records_missing": missing_count,
            "dq_score": dq_score,
            "processing_time_ms": processing_time_ms,
            "valid_records": valid_records,
            "invalid_records": invalid_records
        }
