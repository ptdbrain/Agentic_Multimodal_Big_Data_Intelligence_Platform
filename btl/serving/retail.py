"""Publish and read the separate, versioned retail history daily mart."""

from contextlib import closing
from datetime import date
from decimal import Decimal
import re

SOURCE_URL = "https://archive.ics.uci.edu/dataset/502/online+retail+ii"


def publish_verified_mart(archive, spark_receipt):
    """Publish the daily rows from a paired Kafka and Spark reconciliation."""
    source = archive["source"]
    expected = source["counts"]["distinct"]
    generation = archive["run_id"]
    checksum = source["parquet_sha256"]
    if (source["dataset"] != "Online Retail II" or source["license"] != "CC BY 4.0"
            or spark_receipt["run_id"] != generation or spark_receipt["parquet_sha256"] != checksum
            or not spark_receipt["spark_master"].startswith("spark://")
            or not expected == archive["kafka_acks"] == archive["verified_bronze_rows"]
            == spark_receipt["silver_rows"] == spark_receipt["gold_total_transaction_lines"]):
        raise ValueError("Historical retail receipts disagree")
    rows = [(date.fromisoformat(day), unit, count, canceled, Decimal(amount))
            for day, unit, count, canceled, amount in spark_receipt["daily"]]
    publish(rows, generation, checksum, expected)


def connection():
    import psycopg2

    from config.settings import settings

    if not settings.is_integration_mode:
        raise ConnectionError("A PostgreSQL environment is required for historical retail data")
    db = settings.database
    return psycopg2.connect(
        host=db.host, port=db.port, user=db.user, password=db.password,
        dbname=db.database, connect_timeout=8,
    )


def validate_daily(rows, expected):
    dates = set()
    total = 0
    for day, unit, count, cancellations, amount in rows:
        if (type(day) is not date or day in dates or unit != "GBP" or type(count) is not int or count <= 0
                or type(cancellations) is not int or not 0 <= cancellations <= count
                or not isinstance(amount, Decimal) or not amount.is_finite()):
            raise ValueError("Invalid daily retail mart")
        dates.add(day)
        total += count
    if total != expected or not dates:
        raise ValueError("Daily retail counts do not match verified data")


def publish(rows, generation, source_checksum, expected):
    from operations.backup import LOCK_KEY

    if not generation or not re.fullmatch(r"[a-f0-9]{64}", source_checksum):
        raise ValueError("Version and source checksum required")
    validate_daily(rows, expected)
    with closing(connection()) as db, db, db.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock_shared(%s)", (LOCK_KEY,))
        if not cur.fetchone()[0]:
            raise RuntimeError("Warehouse maintenance temporarily blocks publication")
        cur.execute("SELECT pg_advisory_xact_lock(1397048151)")
        cur.execute("CREATE SCHEMA IF NOT EXISTS retail_history")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS retail_history.generation (
                id text PRIMARY KEY, checksum text NOT NULL, line_count bigint NOT NULL,
                published_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS retail_history.daily (
                generation text REFERENCES retail_history.generation(id), invoice_day date,
                currency text NOT NULL CHECK (currency = 'GBP'), transaction_lines bigint NOT NULL,
                cancellations bigint NOT NULL, line_amount numeric NOT NULL,
                PRIMARY KEY (generation, invoice_day)
            )"""
        )
        cur.execute("SELECT checksum,line_count FROM retail_history.generation WHERE id=%s", (generation,))
        existing = cur.fetchone()
        if existing is not None:
            cur.execute(
                """SELECT invoice_day,currency,transaction_lines,cancellations,line_amount
                   FROM retail_history.daily WHERE generation=%s ORDER BY invoice_day""", (generation,)
            )
            if existing != (source_checksum, expected) or cur.fetchall() != sorted(rows):
                raise ValueError("Registered retail generation already contains different data")
            return
        cur.execute("INSERT INTO retail_history.generation (id,checksum,line_count) VALUES (%s,%s,%s)",
                    (generation, source_checksum, expected))
        from psycopg2.extras import execute_values

        execute_values(cur, "INSERT INTO retail_history.daily VALUES %s", [(generation, *row) for row in rows])


def read_daily():
    import pandas as pd

    with closing(connection()) as db:
        db.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with db.cursor() as cur:
            cur.execute("SELECT to_regclass('retail_history.generation')")
            if cur.fetchone()[0] is None:
                return pd.DataFrame()
            cur.execute(
                """SELECT d.invoice_day,d.currency,d.transaction_lines,d.cancellations,d.line_amount,g.id
                   FROM retail_history.daily d
                   JOIN retail_history.generation g ON g.id=d.generation
                   WHERE g.id=(SELECT id FROM retail_history.generation ORDER BY published_at DESC,id DESC LIMIT 1)
                   ORDER BY d.invoice_day"""
            )
            return pd.DataFrame(cur.fetchall(), columns=[col.name for col in cur.description])
