"""Stable logical identities, independent of Kafka routing and crawl ordering."""
import hashlib
import json
from datetime import datetime, timezone

_TIME_FIELDS = {'timestamp', 'review_date', 'created_at', 'updated_at', 'observed_at'}


def canonical_timestamp(value):
    if not value:
        raise ValueError('Observation timestamp is required')
    dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _canonical(value, field=None):
    if isinstance(value, dict):
        return {k: _canonical(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_canonical(v) for v in value]
    if field in _TIME_FIELDS and isinstance(value, str):
        try:
            return canonical_timestamp(value)
        except ValueError:
            pass  # Invalid raw values must retain their identity for quarantine.
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(_canonical(value), sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':'), default=str).encode()).hexdigest()


def make_event_id(event_type: str, source: str, payload: dict) -> str:
    if not source or not event_type:
        raise ValueError('Event source and type are required')
    return 'evt_' + _digest([source, event_type, payload])[:32]


def make_price_id(source: str, product_id: str, seller: str, observed_at: str) -> str:
    return 'pr_' + _digest([source, product_id, seller, canonical_timestamp(observed_at)])[:32]
