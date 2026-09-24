import json
from pathlib import Path
from typing import Dict, Any, Tuple, List

SCHEMA_DIR = Path(__file__).resolve().parent

class SchemaRegistry:
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

    def validate_record(self, entity_name: str, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        errors = []
        schema = self.get_schema(entity_name)
        required = schema.get("required", [])
        for field in required:
            if field not in record or record[field] is None:
                errors.append(f"Missing required field: '{field}'")

        props = schema.get("properties", {})
        for k, v in record.items():
            if k in props and v is not None:
                expected_type = props[k].get("type")
                if expected_type == "number" and not isinstance(v, (int, float)):
                    errors.append(f"Field '{k}' expected number, got {type(v).__name__}")
                elif expected_type == "integer" and not isinstance(v, int):
                    errors.append(f"Field '{k}' expected integer, got {type(v).__name__}")
                elif expected_type == "string" and not isinstance(v, str):
                    errors.append(f"Field '{k}' expected string, got {type(v).__name__}")
                elif expected_type == "boolean" and not isinstance(v, bool):
                    errors.append(f"Field '{k}' expected boolean, got {type(v).__name__}")

        return len(errors) == 0, errors

registry = SchemaRegistry()
