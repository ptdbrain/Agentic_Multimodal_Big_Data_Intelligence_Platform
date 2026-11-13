"""Durable business-alert deliveries to the explicitly owned local receiver only."""

from dataclasses import replace
from urllib.parse import urlsplit
import json, os
import requests
from serving.store import ServingStore, connect_dataset, now
from serving.admission import writer_transaction
from serving.ownership import acquire_stream_owner


def deliver_notifications(binding, *, url: str | None = None, session=None) -> int:
    destination = url or os.environ.get("ALERT_WEBHOOK_URL", "http://127.0.0.1:18093/alerts")
    parsed = urlsplit(destination)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost", "receiver"}
        or parsed.username
        or parsed.password
    ):
        raise ValueError("Only the authorized local alert receiver is configured")
    connection = connect_dataset(binding)
    store = ServingStore(connection, binding.id, binding.source_group)
    sent = 0
    try:
        with writer_transaction(connection):
            store.initialize()
        with acquire_stream_owner(connection, replace(binding, id=binding.id + "-notifications")):
            rows = store.execute(
                "SELECT notification_id,payload_json FROM alert_notifications WHERE dataset_id=? AND delivered_at IS NULL ORDER BY created_at,revision",
                (binding.id,),
            ).fetchmany(100)
            connection.commit()
            for key, payload in rows:
                try:
                    response = (session or requests).post(
                        destination, json=json.loads(payload), headers={"Idempotency-Key": key}, timeout=10
                    )
                    response.raise_for_status()
                except Exception as error:
                    with writer_transaction(connection):
                        store.execute(
                            "UPDATE alert_notifications SET attempts=attempts+1,error_kind=? WHERE notification_id=?",
                            (type(error).__name__, key),
                        )
                    raise
                with writer_transaction(connection):
                    store.execute(
                        "UPDATE alert_notifications SET delivered_at=?,attempts=attempts+1,error_kind=NULL WHERE notification_id=?",
                        (now(), key),
                    )
                sent += 1
        return sent
    finally:
        connection.close()
