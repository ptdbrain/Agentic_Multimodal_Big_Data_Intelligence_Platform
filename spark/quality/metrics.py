from typing import List, Dict, Any, Optional, Union, Tuple
import datetime
import time
from spark.quality.rules import QualityRule, NonNullRule, RangeRule, PositiveNumberRule


class DataQualityEvaluator:
    """Calculates data quality metrics and scorecards (Section 20-22 in plan.md).
    - Separates field-level null statistics (field_null_counts, total_field_nulls)
      from row-level invalid record counts (records_invalid, records_missing).
    - Performs parse/cast on numeric and timestamp fields BEFORE rule validation.
    """

    @staticmethod
    def _is_null_or_empty(val: Any) -> bool:
        if val is None:
            return True
        if isinstance(val, str) and val.strip() == "":
            return True
        return False

    @classmethod
    def parse_and_cast_record(
        cls,
        record: Dict[str, Any],
        rules: List[QualityRule],
        type_casts: Optional[Dict[str, str]] = None,
    ) -> Tuple[Dict[str, Any], List[str]]:
        """Parses and casts typed fields in a copy of record BEFORE rule validation.
        Returns (casted_record, list_of_cast_error_fields).
        """
        casted = dict(record)
        cast_errors: List[str] = []

        inferred_casts: Dict[str, str] = {}
        for r in rules:
            if isinstance(r, (RangeRule, PositiveNumberRule)):
                inferred_casts[r.field_name] = "float"

        for k in ("rating", "price"):
            if k in casted and k not in inferred_casts:
                inferred_casts[k] = "float"
        for k in ("review_date", "timestamp", "created_at"):
            if k in casted and k not in inferred_casts:
                inferred_casts[k] = "timestamp"

        if type_casts:
            inferred_casts.update(type_casts)

        for field_name, target_type in inferred_casts.items():
            if field_name not in casted:
                continue
            raw_val = casted.get(field_name)
            if cls._is_null_or_empty(raw_val):
                casted[field_name] = None
                continue

            if target_type in ("float", "double"):
                try:
                    num = float(raw_val)
                    if num != num:  # NaN check
                        casted[field_name] = None
                        cast_errors.append(field_name)
                    else:
                        casted[field_name] = num
                except (ValueError, TypeError):
                    casted[field_name] = None
                    cast_errors.append(field_name)
            elif target_type == "int":
                try:
                    casted[field_name] = int(float(raw_val))
                except (ValueError, TypeError):
                    casted[field_name] = None
                    cast_errors.append(field_name)
            elif target_type == "timestamp":
                if isinstance(raw_val, datetime.datetime):
                    continue
                if isinstance(raw_val, (int, float)):
                    continue
                if isinstance(raw_val, str):
                    try:
                        datetime.datetime.fromisoformat(raw_val.strip().replace("Z", "+00:00"))
                    except ValueError:
                        casted[field_name] = None
                        cast_errors.append(field_name)

        return casted, cast_errors

    @classmethod
    def evaluate_batch(
        cls,
        dataset_name: str,
        records: List[Dict[str, Any]],
        rules: List[QualityRule],
        primary_key: Optional[Union[str, List[str]]] = None,
        type_casts: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        t0 = time.time()
        received = len(records)
        valid_records = []
        invalid_records = []
        duplicate_count = 0
        missing_row_count = 0
        cast_error_row_count = 0
        error_counts_by_code: Dict[str, int] = {}

        # Discover all fields across records and rules for field-level null statistics
        all_fields = set()
        for rec in records:
            if isinstance(rec, dict):
                all_fields.update(rec.keys())
        for r in rules:
            if hasattr(r, "field_name"):
                all_fields.add(getattr(r, "field_name"))
        field_null_counts: Dict[str, int] = {f: 0 for f in sorted(all_fields)}

        rule_stats = {
            r.name: {"rule_name": r.name, "error_code": r.error_code, "evaluated": 0, "passed": 0, "failed": 0}
            for r in rules
        }

        seen_keys = set()

        for rec in records:
            # 1. Field-level null statistics (counted per field BEFORE parse/cast)
            for f in field_null_counts:
                if cls._is_null_or_empty(rec.get(f)):
                    field_null_counts[f] += 1

            # 2. Primary key deduplication
            if primary_key:
                if isinstance(primary_key, list):
                    rec_key = tuple(str(rec.get(k, "")) for k in primary_key)
                else:
                    rec_key = rec.get(primary_key)
            else:
                rec_key = rec.get("review_id") or rec.get("product_id") or rec.get("id") or rec.get("item_id")

            if rec_key and rec_key in seen_keys:
                duplicate_count += 1
                invalid_records.append(rec)
                continue
            if rec_key:
                seen_keys.add(rec_key)

            # 3. Parse / Cast BEFORE validation
            casted_rec, cast_err_fields = cls.parse_and_cast_record(rec, rules, type_casts=type_casts)
            row_has_cast_err = len(cast_err_fields) > 0
            if row_has_cast_err:
                cast_error_row_count += 1
                error_counts_by_code["INVALID_TYPE_CAST"] = error_counts_by_code.get("INVALID_TYPE_CAST", 0) + 1

            # 4. Evaluate all rules on parsed/casted record
            rec_valid = not row_has_cast_err
            row_has_missing_req = False
            for rule in rules:
                rule_stats[rule.name]["evaluated"] += 1
                if isinstance(rule, NonNullRule) and getattr(rule, "field_name", None) in cast_err_fields:
                    # Field was non-null raw, but failed type cast (not a missing field)
                    rule_stats[rule.name]["passed"] += 1
                    continue

                is_ok, err_msg = rule.validate(casted_rec)
                if is_ok:
                    rule_stats[rule.name]["passed"] += 1
                else:
                    rule_stats[rule.name]["failed"] += 1
                    error_counts_by_code[rule.error_code] = error_counts_by_code.get(rule.error_code, 0) + 1
                    if rule.error_code == "MISSING_FIELD":
                        row_has_missing_req = True
                    rec_valid = False

            if row_has_missing_req:
                missing_row_count += 1

            if rec_valid:
                valid_records.append(casted_rec)
            else:
                invalid_records.append(rec)

        processing_time_ms = int((time.time() - t0) * 1000)
        valid_count = len(valid_records)
        invalid_count = len(invalid_records)
        dq_score = round((valid_count / received * 100.0), 2) if received > 0 else 0.0

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

        return {
            "dataset_name": dataset_name,
            "records_received": received,
            "records_valid": valid_count,
            "records_invalid": invalid_count,
            "records_duplicate": duplicate_count,
            "records_missing": missing_row_count,
            "cast_error_count": cast_error_row_count,
            "field_null_counts": field_null_counts,
            "total_field_nulls": sum(field_null_counts.values()),
            "error_breakdown": error_counts_by_code,
            "rule_evaluations": rule_evaluations,
            "dq_score": dq_score,
            "processing_time_ms": processing_time_ms,
            "valid_records": valid_records,
            "invalid_records": invalid_records
        }
