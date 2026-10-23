import json
from pathlib import Path
from typing import Dict, Any, Tuple, List

SCHEMA_DIR = Path(__file__).resolve().parent

class SchemaRegistry:
    SUPPORTED_VERSIONS = {"1.0"}

    def __init__(self):
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._load_schemas()

    def _load_schemas(self):
        for name in ["product", "review", "price", "event"]:
            p = SCHEMA_DIR / f"{name}.json"
            if p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    self._schemas[name] = json.load(f)

    def get_schema(self, entity_name: str) -> Dict[str, Any]:
        if entity_name not in self._schemas:
            raise KeyError(f"Schema '{entity_name}' not registered.")
        return self._schemas[entity_name]

    def validate_record(
        self,
        entity_name: str,
        record: Dict[str, Any],
        expected_version: str = "1.0",
    ) -> Tuple[bool, List[str]]:
        if not isinstance(record, dict):
            return False, ["Record must be a dictionary"]

        errors: List[str] = []
        schema = self.get_schema(entity_name)

        # 1. Check schema_version if provided on record or envelope
        if "schema_version" in record:
            ver = record.get("schema_version")
            if not isinstance(ver, str) or ver != expected_version or ver not in self.SUPPORTED_VERSIONS:
                errors.append(
                    f"Unsupported schema_version: '{ver}' (expected '{expected_version}')"
                )

        # 2. Check required fields
        required = schema.get("required", [])
        for field in required:
            if field not in record or record[field] is None:
                errors.append(f"Missing required field: '{field}'")

        # 3. Check field types, ranges, minLength, and enums
        props = schema.get("properties", {})
        for k, v in record.items():
            if k in props and v is not None:
                field_spec = props[k]
                expected_type = field_spec.get("type")
                if expected_type == "number":
                    if isinstance(v, bool) or not isinstance(v, (int, float)):
                        errors.append(f"Field '{k}' expected number, got {type(v).__name__}")
                        continue
                    if "minimum" in field_spec and v < field_spec["minimum"]:
                        errors.append(f"Field '{k}' value {v} is below minimum {field_spec['minimum']}")
                    if "maximum" in field_spec and v > field_spec["maximum"]:
                        errors.append(f"Field '{k}' value {v} exceeds maximum {field_spec['maximum']}")
                elif expected_type == "integer":
                    if isinstance(v, bool) or not isinstance(v, int):
                        errors.append(f"Field '{k}' expected integer, got {type(v).__name__}")
                        continue
                    if "minimum" in field_spec and v < field_spec["minimum"]:
                        errors.append(f"Field '{k}' value {v} is below minimum {field_spec['minimum']}")
                    if "maximum" in field_spec and v > field_spec["maximum"]:
                        errors.append(f"Field '{k}' value {v} exceeds maximum {field_spec['maximum']}")
                elif expected_type == "string":
                    if not isinstance(v, str):
                        errors.append(f"Field '{k}' expected string, got {type(v).__name__}")
                        continue
                    if "minLength" in field_spec and len(v.strip()) < field_spec["minLength"]:
                        errors.append(f"Field '{k}' violates minLength {field_spec['minLength']}")
                    if "enum" in field_spec and v not in field_spec["enum"]:
                        errors.append(f"Field '{k}' value '{v}' not in allowed enum {field_spec['enum']}")
                elif expected_type == "boolean":
                    if not isinstance(v, bool):
                        errors.append(f"Field '{k}' expected boolean, got {type(v).__name__}")
                elif expected_type == "object":
                    if not isinstance(v, dict):
                        errors.append(f"Field '{k}' expected object, got {type(v).__name__}")

        return len(errors) == 0, errors

registry = SchemaRegistry()

