from typing import List, Dict, Any, Optional, Union
import time
from spark.quality.rules import QualityRule

class DataQualityEvaluator:
    """Calculates data quality metrics and scorecards (Section 20-22 in plan.md).
    Dynamically computes overall and per-rule scores without hardcoded assumptions.
    """
    
    @staticmethod
    def evaluate_batch(
        dataset_name: str,
        records: List[Dict[str, Any]],
        rules: List[QualityRule],
        primary_key: Optional[Union[str, List[str]]] = None
    ) -> Dict[str, Any]:
        t0 = time.time()
        received = len(records)
        valid_records = []
        invalid_records = []
        duplicate_count = 0
        error_counts_by_code: Dict[str, int] = {}

        # Rule tracking statistics
        rule_stats = {
            r.name: {"rule_name": r.name, "error_code": r.error_code, "evaluated": 0, "passed": 0, "failed": 0}
            for r in rules
        }

        seen_keys = set()

        for rec in records:
            # 1. Primary key deduplication
            if primary_key:
                if isinstance(primary_key, list):
                    rec_key = tuple(str(rec.get(k, "")) for k in primary_key)
                else:
                    rec_key = rec.get(primary_key)
            else:
                # Dynamic fallback to common ID fields
                rec_key = rec.get("review_id") or rec.get("product_id") or rec.get("id") or rec.get("item_id")

            if rec_key and rec_key in seen_keys:
                duplicate_count += 1
                continue
            if rec_key:
                seen_keys.add(rec_key)

            # 2. Evaluate all rules
            rec_valid = True
            for rule in rules:
                rule_stats[rule.name]["evaluated"] += 1
                is_ok, err_msg = rule.validate(rec)
                if is_ok:
                    rule_stats[rule.name]["passed"] += 1
                else:
                    rule_stats[rule.name]["failed"] += 1
                    error_counts_by_code[rule.error_code] = error_counts_by_code.get(rule.error_code, 0) + 1
                    rec_valid = False

            if rec_valid:
                valid_records.append(rec)
            else:
                invalid_records.append(rec)

        processing_time_ms = int((time.time() - t0) * 1000)
        valid_count = len(valid_records)
        dq_score = round((valid_count / received * 100.0), 2) if received > 0 else 0.0

        # Compute per-rule pass rates
        rule_evaluations = []
        for r_name, stat in rule_stats.items():
            ev = stat["evaluated"]
            p_rate = round((stat["passed"] / ev * 100.0), 1) if ev > 0 else 100.0
            rule_evaluations.append({
                "Rule Name": r_name,
                "Status": "PASSED" if stat["failed"] == 0 else "FAILURES_DETECTED",
                "Pass Rate": f"{p_rate}%",
                "Failed Count": stat["failed"]
            })

        missing_count = error_counts_by_code.get("MISSING_FIELD", 0)

        return {
            "dataset_name": dataset_name,
            "records_received": received,
            "records_valid": valid_count,
            "records_invalid": len(invalid_records),
            "records_duplicate": duplicate_count,
            "records_missing": missing_count,
            "error_breakdown": error_counts_by_code,
            "rule_evaluations": rule_evaluations,
            "dq_score": dq_score,
            "processing_time_ms": processing_time_ms,
            "valid_records": valid_records,
            "invalid_records": invalid_records
        }

