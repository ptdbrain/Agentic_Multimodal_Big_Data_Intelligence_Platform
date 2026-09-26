from typing import Dict, Any, List, Tuple

class QualityRule:
    """Base rule definition for record validation."""
    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        raise NotImplementedError

class NonNullRule(QualityRule):
    def __init__(self, field_name: str):
        self.field_name = field_name

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(self.field_name)
        if val is None or str(val).strip() == "":
            return False, f"Missing or empty required field: '{self.field_name}'"
        return True, ""

class RangeRule(QualityRule):
    def __init__(self, field_name: str, min_val: float, max_val: float):
        self.field_name = field_name
        self.min_val = min_val
        self.max_val = max_val

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(self.field_name)
        if val is None:
            return False, f"Field '{self.field_name}' is None"
        try:
            num = float(val)
            if not (self.min_val <= num <= self.max_val):
                return False, f"Field '{self.field_name}'={num} out of bounds [{self.min_val}, {self.max_val}]"
            return True, ""
        except (ValueError, TypeError):
            return False, f"Field '{self.field_name}' cannot be parsed as float"

class PositiveNumberRule(QualityRule):
    def __init__(self, field_name: str):
        self.field_name = field_name

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(self.field_name)
        if val is None:
            return False, f"Field '{self.field_name}' is None"
        try:
            num = float(val)
            if num <= 0:
                return False, f"Field '{self.field_name}' must be positive, got {num}"
            return True, ""
        except (ValueError, TypeError):
            return False, f"Field '{self.field_name}' is not numeric"

REVIEW_QUALITY_RULES = [
    NonNullRule("review_id"),
    NonNullRule("product_id"),
    NonNullRule("review_text"),
    RangeRule("rating", 1.0, 5.0)
]

PRODUCT_QUALITY_RULES = [
    NonNullRule("product_id"),
    NonNullRule("product_name"),
    NonNullRule("brand"),
    NonNullRule("category"),
    PositiveNumberRule("price")
]
