"""Ingest-time PII anonymization and user ID hashing utilities.

Ensures raw user/customer identifiers and unnecessary PII fields are hashed
or stripped immediately at the ingestion boundary (Crawler, FileLoader,
Dataset Generator, and Stage 1 Ingest) before entering Kafka or Bronze storage.
"""
import hashlib
import re
from typing import Any, Dict, List, Optional

_ANON_PATTERN = re.compile(r"^anon_[0-9a-f]{16}$")
_DEFAULT_SALT = "sentinel-pii-v1"

_RAW_PII_FIELDS = (
    "customer_id",
    "customer_name",
    "user_name",
    "username",
    "author_name",
    "created_by",
    "email",
    "customer_email",
    "phone",
    "phone_number",
    "raw_user_id",
)


def is_anonymized_user_id(value: Any) -> bool:
    """Returns True if the value is already a canonical anonymized user_id or redacted token."""
    if not isinstance(value, str):
        return False
    return value == "anon_redacted" or bool(_ANON_PATTERN.match(value))


def anonymize_user_id(
    raw_uid: Optional[Any],
    salt: str = _DEFAULT_SALT,
    remove: bool = False,
) -> str:
    """Deterministically hashes a raw user/customer ID into `anon_<16-hex-sha256>`
    or returns `anon_redacted` when `remove=True` or the input is empty.

    Idempotent: already-anonymized IDs (`anon_<16-hex>`) are returned unchanged.
    """
    if remove or raw_uid is None:
        return "anon_redacted"
    uid_str = str(raw_uid).strip()
    if not uid_str or uid_str.lower() in ("none", "null", "nan", ""):
        return "anon_redacted"
    if is_anonymized_user_id(uid_str):
        return uid_str
    digest = hashlib.sha256(f"{salt}:{uid_str}".encode("utf-8")).hexdigest()[:16]
    return f"anon_{digest}"


def sanitize_record_pii(
    record: Dict[str, Any],
    remove_user_id: bool = False,
    salt: str = _DEFAULT_SALT,
) -> Dict[str, Any]:
    """Sanitizes a single ingested record by stripping unnecessary PII fields
    and hashing/redacting `user_id` at the ingestion boundary.
    """
    if not isinstance(record, dict):
        return record

    cleaned = dict(record)
    raw_uid = cleaned.get("user_id")
    if raw_uid is None and "customer_id" in cleaned:
        raw_uid = cleaned.get("customer_id")

    for field in _RAW_PII_FIELDS:
        cleaned.pop(field, None)

    is_review_record = (
        "review_id" in cleaned
        or "review_text" in cleaned
        or "user_id" in record
        or "customer_id" in record
    )
    if is_review_record:
        cleaned["user_id"] = anonymize_user_id(raw_uid, salt=salt, remove=remove_user_id)
    else:
        cleaned.pop("user_id", None)

    return cleaned


def sanitize_records_pii(
    records: List[Dict[str, Any]],
    remove_user_id: bool = False,
    salt: str = _DEFAULT_SALT,
) -> List[Dict[str, Any]]:
    """Sanitizes a batch of ingested records at the ingestion boundary."""
    return [
        sanitize_record_pii(rec, remove_user_id=remove_user_id, salt=salt)
        for rec in records
    ]
