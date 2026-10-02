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
    
    @classmethod  
    def validate_record(cls, record: dict, schema_name: str) -> Tuple[bool, List[str]]:
        """Validate a single record. Returns (is_valid, list_of_errors)."""
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
