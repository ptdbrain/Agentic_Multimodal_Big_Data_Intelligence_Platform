from typing import Dict, Any, List, Tuple, Optional, Callable

class QualityRule:
    """Base rule definition for record validation with standardized error categorization."""
    name: str = "BaseQualityRule"
    error_code: str = "VALIDATION_ERROR"

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        raise NotImplementedError

class NonNullRule(QualityRule):
    name: str = "NonNullRule"
    error_code: str = "MISSING_FIELD"

    def __init__(self, field_name: str, allow_whitespace: bool = False):
        self.field_name = field_name
        self.allow_whitespace = allow_whitespace
        self.name = f"NonNullRule({field_name})"

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(self.field_name)
        if val is None:
            return False, f"Missing required field: '{self.field_name}'"
        if not self.allow_whitespace and isinstance(val, str) and val.strip() == "":
            return False, f"Empty string in required field: '{self.field_name}'"
        return True, ""

class RangeRule(QualityRule):
    name: str = "RangeRule"
    error_code: str = "OUT_OF_BOUNDS"

    def __init__(self, field_name: str, min_val: float, max_val: float, inclusive: bool = True):
        self.field_name = field_name
        self.min_val = min_val
        self.max_val = max_val
        self.inclusive = inclusive
        self.name = f"RangeRule({field_name} [{min_val}, {max_val}])"

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(self.field_name)
        if val is None:
            return False, f"Field '{self.field_name}' is None"
        try:
            num = float(val)
            in_range = (self.min_val <= num <= self.max_val) if self.inclusive else (self.min_val < num < self.max_val)
            if not in_range:
                return False, f"Field '{self.field_name}'={num} out of bounds [{self.min_val}, {self.max_val}]"
            return True, ""
        except (ValueError, TypeError):
            return False, f"Field '{self.field_name}' cannot be parsed as numeric"

class PositiveNumberRule(QualityRule):
    name: str = "PositiveNumberRule"
    error_code: str = "INVALID_NUMERIC"

    def __init__(self, field_name: str, allow_zero: bool = False):
        self.field_name = field_name
        self.allow_zero = allow_zero
        self.name = f"PositiveNumberRule({field_name}, allow_zero={allow_zero})"

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        val = record.get(self.field_name)
        if val is None:
            return False, f"Field '{self.field_name}' is None"
        try:
            num = float(val)
            valid = (num >= 0) if self.allow_zero else (num > 0)
            if not valid:
                desc = "non-negative" if self.allow_zero else "positive"
                return False, f"Field '{self.field_name}' must be {desc}, got {num}"
            return True, ""
        except (ValueError, TypeError):
            return False, f"Field '{self.field_name}' is not numeric"

class CustomPredicateRule(QualityRule):
    name: str = "CustomPredicateRule"
    error_code: str = "PREDICATE_FAILED"

    def __init__(self, name: str, predicate: Callable[[Dict[str, Any]], bool], error_message: str):
        self.name = name
        self.predicate = predicate
        self.error_message = error_message

    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            if not self.predicate(record):
                return False, self.error_message
            return True, ""
        except Exception as e:
            return False, f"{self.error_message}: {str(e)}"

class RuleRegistry:
    """Dynamic registry for data quality validation rules."""
    _rule_sets: Dict[str, List[QualityRule]] = {}

    @classmethod
    def register(cls, dataset_name: str, rules: List[QualityRule]):
        cls._rule_sets[dataset_name] = list(rules)

    @classmethod
    def get_rules(cls, dataset_name: str) -> List[QualityRule]:
        return cls._rule_sets.get(dataset_name, [])

# Standard built-in default rules
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
    PositiveNumberRule("price", allow_zero=False)
]

RuleRegistry.register("reviews", REVIEW_QUALITY_RULES)
RuleRegistry.register("products", PRODUCT_QUALITY_RULES)

PRICE_QUALITY_RULES = [
    NonNullRule('price_id'),
    NonNullRule('product_id'),
    PositiveNumberRule('price', allow_zero=False),
    NonNullRule('currency'),
]
RuleRegistry.register('prices', PRICE_QUALITY_RULES)

