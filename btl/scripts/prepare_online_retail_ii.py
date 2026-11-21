"""Prepare an attributed, customer-free historical dataset from UCI's workbook."""

import argparse
from collections import Counter
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq

SOURCE_PAGE = "https://archive.ics.uci.edu/dataset/502/online+retail+ii"
WORKBOOK = "online_retail_II.xlsx"
COLUMNS = ("Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price", "Customer ID", "Country")
SCHEMA = pa.schema([
    ("line_fingerprint", pa.string()), ("source_sheet", pa.string()), ("source_row", pa.int64()),
    ("invoice", pa.string()), ("stock_code", pa.string()), ("description", pa.string()), ("quantity", pa.int64()),
    ("invoice_local_time", pa.timestamp("us")), ("unit_price", pa.decimal128(18, 6)), ("currency", pa.string()),
    ("country", pa.string()), ("is_cancellation", pa.bool_()),
])


def normalize_line(fields, sheet, position):
    invoice, stock_code, description, quantity, observed, price, _, country = fields
    if invoice is None or stock_code is None or not isinstance(observed, datetime):
        raise ValueError("Missing invoice, stock code or observed local clock")
    if not isinstance(quantity, int) or not isinstance(price, (int, float)):
        raise ValueError("Invalid quantity or price")
    amount = Decimal(str(price))
    scale = Decimal("0.000001")
    if not amount.is_finite() or amount.quantize(scale) != amount:
        raise ValueError("Invalid unit price precision")
    invoice = str(invoice)
    stock_code = str(stock_code)
    description = str(description) if description is not None else None
    country = str(country) if country is not None else None
    values = [invoice, stock_code, description, quantity, observed.isoformat(), str(amount.normalize()), country]
    fingerprint = hashlib.sha256(json.dumps(values, ensure_ascii=False).encode()).hexdigest()
    return {
        "line_fingerprint": fingerprint, "source_sheet": sheet, "source_row": position,
        "invoice": invoice, "stock_code": stock_code, "description": description, "quantity": quantity,
        "invoice_local_time": observed, "unit_price": amount, "currency": "GBP", "country": country,
        "is_cancellation": invoice.upper().startswith("C"),
    }


def prepare(archive, destination):
    from openpyxl import load_workbook

    if destination.exists():
        raise FileExistsError(destination)
    counts = Counter(received=0, distinct=0, duplicate=0, rejected=0, cancellation=0)
    by_sheet = {}
    with TemporaryDirectory(prefix=".uci-retail-", dir=destination.parent) as stage:
        stage = Path(stage)
        with ZipFile(archive) as source:
            names = [item for item in source.infolist() if Path(item.filename).name == WORKBOOK and not item.is_dir()]
            if len(names) != 1:
                raise ValueError("Expected one Online Retail II workbook")
            workbook_file = stage / WORKBOOK
            with source.open(names[0]) as workbook, workbook_file.open("xb") as target:
                shutil.copyfileobj(workbook, target)
        workbook = load_workbook(workbook_file, read_only=True, data_only=True)
        with sqlite3.connect(stage / "fingerprints.sqlite") as seen, pq.ParquetWriter(
            stage / "transaction_lines.parquet", SCHEMA, compression="zstd"
        ) as writer:
            seen.execute("CREATE TABLE seen (hash TEXT PRIMARY KEY) WITHOUT ROWID")
            pending = []
            for sheet in workbook.worksheets:
                rows = sheet.values
                if tuple(next(rows)) != COLUMNS:
                    raise ValueError(f"Unexpected Online Retail II columns in {sheet.title}")
                by_sheet[sheet.title] = 0
                for pos, row in enumerate(rows, start=2):
                    if not any(value is not None for value in row):
                        continue
                    counts["received"] += 1
                    by_sheet[sheet.title] += 1
                    try:
                        event = normalize_line(row, sheet.title, pos)
                    except ValueError:
                        counts["rejected"] += 1
                        continue
                    if seen.execute("INSERT OR IGNORE INTO seen VALUES (?)", (event["line_fingerprint"],)).rowcount == 0:
                        counts["duplicate"] += 1
                        continue
                    pending.append(event)
                    counts["distinct"] += 1
                    counts["cancellation"] += int(event["is_cancellation"])
                    if len(pending) == 10_000:
                        writer.write_table(pa.Table.from_pylist(pending, schema=SCHEMA))
                        pending.clear()
            if pending:
                writer.write_table(pa.Table.from_pylist(pending, schema=SCHEMA))
        workbook.close()
        parquet = stage / "transaction_lines.parquet"
        assert pq.ParquetFile(parquet).metadata.num_rows == counts["distinct"]
        assert counts["received"] == counts["distinct"] + counts["duplicate"] + counts["rejected"]
        with archive.open("rb") as stream:
            source_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
        with parquet.open("rb") as stream:
            parquet_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
        manifest = {
            "dataset": "Online Retail II", "author": "Daqing Chen", "year": 2012, "publisher": "UCI Machine Learning Repository",
            "url": SOURCE_PAGE, "doi": "10.24432/C5CG6D", "license": "CC BY 4.0",
            "source_sha256": source_sha256, "parquet_sha256": parquet_sha256, "sheets": by_sheet, "counts": dict(counts),
            "method": "Observed UK online retail lines. Customer ID excluded. Duplicate fingerprint uses all published business fields, excluding row location and Customer ID. Invoice clock retained as provided, with no inferred timezone. Cancellations and original quantity/unit price retained.",
        }
        output = stage / "ready"
        output.mkdir()
        parquet.rename(output / parquet.name)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        output.rename(destination)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare(args.archive, args.destination), indent=2))
