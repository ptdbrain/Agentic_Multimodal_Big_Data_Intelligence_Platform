import json
from pathlib import Path
from typing import Dict, List, Tuple, Any
from jsonschema import validate, ValidationError

class SchemaValidator:
    """Validates records against JSON schemas on the ingestion path."""
    
    _schemas: Dict[str, dict] = {}  # cache
    
    @classmethod
    def load_schema(cls, schema_name: str) -> dict:
        """Load schema from data/schemas/<name>.json"""
        if schema_name in cls._schemas:
            return cls._schemas[schema_name]
            
        schema_path = Path(__file__).parent / f"{schema_name}.json"
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")
            
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
            cls._schemas[schema_name] = schema
            return schema
    
    SUPPORTED_SCHEMA_VERSIONS = {"1.0"}

    @classmethod  
    def validate_record(
        cls,
        record: dict,
        schema_name: str,
        expected_version: str = "1.0",
    ) -> Tuple[bool, List[str]]:
        """Validate a single record against JSON schema and schema_version.
        Returns (is_valid, list_of_errors).
        """
        if not isinstance(record, dict):
            return False, ["Record must be a JSON object"]

        if "schema_version" in record:
            ver = record.get("schema_version")
            if not isinstance(ver, str) or ver != expected_version or ver not in cls.SUPPORTED_SCHEMA_VERSIONS:
                return False, [
                    f"Unsupported schema_version '{ver}' for '{schema_name}' (expected '{expected_version}')"
                ]

        schema = cls.load_schema(schema_name)
        try:
            validate(instance=record, schema=schema)
            return True, []
        except ValidationError as e:
            return False, [e.message]
    
    @classmethod
    def validate_batch(cls, records: List[dict], schema_name: str) -> Tuple[List[dict], List[dict]]:
        """Validate a batch. Returns (valid_records, invalid_records_with_errors)."""
        valid_records = []
        invalid_records = []
        
        for record in records:
            is_valid, errors = cls.validate_record(record, schema_name)
            if is_valid:
                valid_records.append(record)
            else:
                invalid_records.append({
                    "record": record,
                    "errors": errors
                })
                
        return valid_records, invalid_records
