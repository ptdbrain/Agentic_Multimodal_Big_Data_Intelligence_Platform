"""An imported retail line keeps the original observation clock and currency."""
from datetime import datetime
from decimal import Decimal

import pytest
from config.settings import settings
from kafka.consumers.raw_consumer import RawConsumer
from scripts.archive_online_retail_ii import batch_fingerprints, kafka_record

pytestmark = pytest.mark.unit


def test_retail_contract_rejects_overclaimed_time_and_personal_fields():
    payload = kafka_record({
        "line_fingerprint": "b" * 64, "source_sheet": "Year 2009-2010", "source_row": 2,
        "invoice": "C123", "stock_code": "001", "description": None, "quantity": -2,
        "unit_price": Decimal("14.120000"), "currency": "GBP", "country": "United Kingdom",
        "invoice_local_time": datetime(2009, 12, 1, 10, 5), "is_cancellation": True,
    })
    topic = settings.kafka.topic_events + ".retail.test-run"
    consumer = RawConsumer()
    event = dict(
        event_id="fixture-transport", schema_version="1.0", event_type="RETAIL_TRANSACTION_IMPORT",
        source="uci-online-retail-ii", ingested_at="2026-11-22T00:00:00Z", payload=payload,
    )
    assert consumer.validate_message(topic, event)[0]
    for invalid in (dict(payload, currency="VND"), dict(payload, user_id="raw-fixture"),
                    dict(payload, invoice_local_time=payload["invoice_local_time"] + "Z")):
        assert not consumer.validate_message(topic, dict(event, payload=invalid))[0]
    assert payload["unit_price"] == "14.120000" and payload["stock_code"] == "001"
    assert batch_fingerprints([payload]) == batch_fingerprints([dict(reversed(list(payload.items())))])


def test_daily_mart_enforces_original_currency_count_and_grain():
    from datetime import date
    from serving.retail import validate_daily

    rows = [(date(2010, 12, 1), "GBP", 3, 1, Decimal("11.20"))]
    validate_daily(rows, 3)
    for invalid in (rows * 2, [(date(2010, 12, 1), "VND", 3, 1, Decimal("11.20"))],
                    [(date(2010, 12, 1), "GBP", 3, 4, Decimal("11.20"))]):
        with pytest.raises(ValueError, match="Invalid daily retail mart"):
            validate_daily(invalid, 3)
    with pytest.raises(ValueError, match="Daily retail counts"):
        validate_daily(rows, 4)


def test_retail_page_filters_observations_and_stays_out_of_simulation(monkeypatch):
    from datetime import date
    from pathlib import Path

    import pandas as pd
    from streamlit.testing.v1 import AppTest
    from serving import retail

    monkeypatch.setenv("DASHBOARD_DATA_SCOPE", "real")
    fields = ["invoice_day", "currency", "transaction_lines", "cancellations", "line_amount", "id"]
    fig = pd.DataFrame([
        (date(2009, 12, 1), "GBP", 12, 1, Decimal("19.10"), "fixture"),
        (date(2010, 12, 1), "GBP", 22, 2, Decimal("29.10"), "fixture"),
    ], columns=fields)
    monkeypatch.setattr(retail, "read_daily", lambda: fig)
    root = Path(__file__).resolve().parents[1]
    app = AppTest.from_file(str(root / "dashboard/app.py")).run()
    app.switch_page("pages/13_Retail_History.py").run()
    assert not app.exception
    assert app.dataframe[0].value["transaction_lines"].sum() == 34
    app.date_input[0].set_value((date(2010, 12, 1), date(2010, 12, 1))).run()
    assert app.dataframe[0].value["transaction_lines"].tolist() == [22]
    app.selectbox(key="dashboard_data_scope").set_value("simulation").run()
    assert not app.exception and not app.dataframe


def test_csv_keeps_retail_identity_and_money_text(tmp_path):
    from ingestion.file_loader.loader import FileLoader

    path = tmp_path / "scope.csv"
    path.write_text("invoice,stock_code,unit_price\n00123,00045,0.100010\n", encoding="utf-8")
    assert FileLoader.load_records(path, delimiter=",") == [
        {"invoice": "00123", "stock_code": "00045", "unit_price": "0.100010"}
    ]
