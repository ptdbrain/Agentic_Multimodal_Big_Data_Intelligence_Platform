"""Persist confirmed Kafka prices and evaluate new observations against closed UTC days."""

from datetime import timedelta
import hashlib
import json
import math

from analytics.basic import _currency
from analytics.causal import StatisticalConfig, price_evaluations
from data.schemas.validator import SchemaValidator
from serving.store import ServingStore, instant

GENERATION = "price-stream"
ALERT_MAX_AGE = timedelta(hours=1)

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS price_stream_events (
        dataset_id TEXT NOT NULL, source_id TEXT NOT NULL, partition_id INTEGER NOT NULL,
        kafka_offset BIGINT NOT NULL, payload_sha256 TEXT NOT NULL, outcome TEXT NOT NULL,
        PRIMARY KEY (dataset_id,source_id,partition_id,kafka_offset)
    )""",
    """CREATE TABLE IF NOT EXISTS price_stream_observations (
        dataset_id TEXT NOT NULL, price_id TEXT NOT NULL, product_id TEXT NOT NULL,
        seller TEXT NOT NULL, currency TEXT NOT NULL, observed_at TEXT NOT NULL,
        price DOUBLE PRECISION NOT NULL, PRIMARY KEY (dataset_id,price_id)
    )""",
    """CREATE INDEX IF NOT EXISTS price_stream_baseline ON price_stream_observations
       (dataset_id,product_id,seller,currency,observed_at)""",
)


def initialize_price_stream(store: ServingStore) -> None:
    store.initialize()
    for statement in SCHEMA:
        store.execute(statement)


def checked_price(message, source_group):
    from kafka.consumers.raw_consumer import RawConsumer

    if not RawConsumer().validate_envelope(message)[0] or message["event_type"] != "PRICE_UPDATE":
        raise ValueError("Invalid price envelope")
    row = message["payload"]
    if not SchemaValidator.validate_record(row, "price")[0]:
        raise ValueError("Invalid price payload")
    if row.get("source_group") != source_group:
        raise ValueError("Price source differs from the selected dataset")
    methods = {"simulation": {"generated"}, "marketplace": {"live", "snapshot_replay"}}
    method = row.get("acquisition_method")
    if method not in methods.get(source_group, set()):
        raise ValueError("Invalid price acquisition method")
    if source_group == "marketplace":
        evidence = row.get("evidence_sha256", "")
        if (not isinstance(evidence, str) or len(evidence) != 64
                or any(c not in "0123456789abcdef" for c in evidence) or not row.get("evidence_ref")):
            raise ValueError("Observed marketplace price has no source evidence")
    unit = _currency(row["currency"])
    value = float(row["price"])
    if unit is None or not math.isfinite(value) or value <= 0:
        raise ValueError("Price and currency must be known")
    price_id, product = (row[key].strip() for key in ("price_id", "product_id"))
    if not price_id or not product:
        raise ValueError("Price and product identities are required")
    return price_id, product, (row.get("seller") or "").strip(), unit, instant(row["timestamp"]), value, method


def stage_observation(store, observation, checked_at, config):
    price_id, product, seller, unit, observed, price, method = observation
    day = observed.date()
    values = (product, seller, unit, observed.isoformat(), price)
    found = store.execute(
        """SELECT product_id,seller,currency,observed_at,price
           FROM price_stream_observations WHERE dataset_id=? AND price_id=?""",
        (store.dataset_id, price_id),
    ).fetchone()
    if found is not None:
        return "duplicate" if tuple(found) == values else "identity_conflict"
    if observed > checked_at:
        return "future_timestamp"
    store.execute(
        "INSERT INTO price_stream_observations VALUES(?,?,?,?,?,?,?)", (store.dataset_id, price_id, *values)
    )
    if day != checked_at.date() or checked_at - observed > ALERT_MAX_AGE or method == "snapshot_replay":
        return "historical_observation"
    history = store.execute(
        """SELECT substr(observed_at,1,10) AS day,AVG(price)
           FROM price_stream_observations WHERE dataset_id=? AND product_id=? AND seller=? AND currency=?
           AND observed_at>=? AND observed_at<? GROUP BY substr(observed_at,1,10) ORDER BY day""",
        (store.dataset_id, product, seller, unit, (day - timedelta(days=config.price_days)).isoformat(), day.isoformat()),
    ).fetchall()
    candidate = [(d, v) for d, v in history] + [(day.isoformat(), price)]
    from_day = day.isoformat()
    evaluations = price_evaluations(
        (dict(product_id=product, source_group=store.source_group, currency=unit, date=d, avg_price=v)
         for d, v in candidate),
        dataset_id=store.dataset_id, generation=GENERATION, config=config,
    )
    scoped = []
    for value in evaluations:
        if value["window_start"][:10] != from_day:
            continue
        rule = "stream-" + value["rule"]
        value.update(
            event_id=hashlib.sha256(json.dumps([store.dataset_id, price_id, rule]).encode()).hexdigest(),
            window_start=observed.isoformat(), window_end=(observed + timedelta(seconds=1)).isoformat(), rule=rule,
        )
        value["evidence"].update(
            detection_mode="causal_price_stream", price_id=price_id, seller=seller or None,
            acquisition_method=method, observed_at=observed.isoformat(),
        )
        scoped.append(value)
    store.stage_evaluations(GENERATION, scoped)
    return "evaluated"


def stage_price_event(store, source_id, partition, offset, raw, checked_at, config=None):
    """Persist the decision and outbox in the caller's transaction before offset commit."""
    config = config or StatisticalConfig.from_settings()
    payload_hash = hashlib.sha256(raw or b"").hexdigest()
    identity = (store.dataset_id, source_id, partition, offset)
    previous = store.execute(
        """SELECT payload_sha256,outcome FROM price_stream_events
           WHERE dataset_id=? AND source_id=? AND partition_id=? AND kafka_offset=?""",
        identity,
    ).fetchone()
    if previous:
        if previous[0] != payload_hash:
            raise ValueError("Kafka coordinates were reused for different price content")
        return previous[1]
    try:
        observation = checked_price(json.loads(raw), store.source_group)
    except (ValueError, TypeError, UnicodeError, OverflowError):
        outcome = "rejected"
    else:
        outcome = stage_observation(store, observation, checked_at, config)
    store.execute("INSERT INTO price_stream_events VALUES(?,?,?,?,?,?)", (*identity, payload_hash, outcome))
    return outcome
