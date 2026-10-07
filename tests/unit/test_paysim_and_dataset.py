import csv
import json
from datetime import UTC, datetime, timedelta

import pytest

from fraudx.ingestion import paysim
from fraudx.ingestion.__main__ import main
from fraudx.ingestion.dataset import (
    EVENTS_FILE,
    GROUND_TRUTH_FILE,
    SUMMARY_FILE,
    read_events,
)

HEADER = list(paysim.PAYSIM_COLUMNS)
ROWS = [
    ["1", "PAYMENT", "9839.64", "C1231006815", "170136.0", "160296.36", "M1979787155",
     "0.0", "0.0", "0", "0"],
    ["1", "TRANSFER", "181.0", "C1305486145", "181.0", "0.0", "C553264065",
     "0.0", "0.0", "1", "0"],
    ["2", "CASH_OUT", "181.0", "C840083671", "181.0", "0.0", "C38997010",
     "21182.0", "0.0", "1", "0"],
    ["2", "WIRE", "10.0", "C1", "10.0", "0.0", "C2", "0.0", "0.0", "0", "0"],  # bad type
    ["3", "PAYMENT", "-5", "C3", "10.0", "0.0", "M4", "0.0", "0.0", "0", "0"],  # negative
]  # fmt: skip


@pytest.fixture
def paysim_csv(tmp_path):
    path = tmp_path / "paysim.csv"
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        writer.writerows(ROWS)
    return path


def test_paysim_rows_map_to_events(paysim_csv):
    report = paysim.LoadReport()
    events = list(paysim.load(paysim_csv, report))
    assert (report.rows, report.valid, report.invalid) == (5, 3, 2)
    assert len(report.sample_errors) == 2

    first, fraud = events[0], events[1]
    assert first.customer_id == first.source_account == "C1231006815"
    assert first.destination_account == "M1979787155"
    assert first.balances.source_after == first.balances.source_before - first.amount
    assert not first.is_fraud_label and fraud.is_fraud_label
    # step 1 -> first hour after start; step 2 -> second hour
    assert paysim.DEFAULT_START <= first.event_time < paysim.DEFAULT_START + timedelta(hours=1)
    assert events[2].event_time >= paysim.DEFAULT_START + timedelta(hours=1)


def test_paysim_enrichment_is_stable_and_label_independent():
    assert paysim.enrich("C1", "s") == paysim.enrich("C1", "s")
    assert paysim.enrich("C1", "s") != paysim.enrich("C1", "other-salt")
    # Same customer, fraud or not, gets the same device/IP/location.
    row = dict(zip(HEADER, ROWS[1], strict=True))
    start = datetime(2026, 1, 1, tzinfo=UTC)
    legit = paysim.parse_row({**row, "isFraud": "0"}, 1, start, "s")
    fraud = paysim.parse_row({**row, "isFraud": "1"}, 1, start, "s")
    assert (legit.device_id, legit.ip_address, legit.location) == (
        fraud.device_id,
        fraud.ip_address,
        fraud.location,
    )


def test_paysim_rejects_non_paysim_file(tmp_path):
    path = tmp_path / "x.csv"
    path.write_text("a,b\n1,2\n")
    with pytest.raises(ValueError, match="not a PaySim file"):
        list(paysim.load(path, paysim.LoadReport()))


def test_paysim_limit(paysim_csv):
    report = paysim.LoadReport()
    assert len(list(paysim.load(paysim_csv, report, limit=2))) == 2
    assert report.rows == 2


def test_cli_generate_writes_complete_dataset(tmp_path):
    out = tmp_path / "gen"
    assert main(["generate", "--out", str(out), "--customers", "100", "--days", "2"]) == 0

    summary = json.loads((out / SUMMARY_FILE).read_text())
    events = list(read_events(out / EVENTS_FILE))
    with (out / GROUND_TRUTH_FILE).open() as fh:
        truth = list(csv.DictReader(fh))

    assert summary["events"] == len(events) == len(truth) > 0
    assert summary["source"] == "synthetic"
    assert summary["config"]["customers"] == 100
    assert [t["transaction_id"] for t in truth] == [e.transaction_id for e in events]
    assert sum(int(t["is_fraud"]) for t in truth) == summary["fraud_events"]
    assert not list(out.glob("*.tmp"))


def test_cli_paysim(paysim_csv, tmp_path):
    out = tmp_path / "ps"
    assert main(["paysim", "--input", str(paysim_csv), "--out", str(out)]) == 0
    summary = json.loads((out / SUMMARY_FILE).read_text())
    assert (summary["events"], summary["invalid_rows"], summary["fraud_events"]) == (3, 2, 2)


def test_cli_paysim_missing_file(tmp_path):
    assert main(["paysim", "--input", str(tmp_path / "nope.csv")]) == 2
