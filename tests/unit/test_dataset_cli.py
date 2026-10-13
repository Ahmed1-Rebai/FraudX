import csv
import json

from fraudx.ingestion.__main__ import main
from fraudx.ingestion.dataset import (
    EVENTS_FILE,
    GROUND_TRUTH_FILE,
    SUMMARY_FILE,
    read_events,
)


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


def test_cli_replay_without_dataset_fails_cleanly(tmp_path):
    assert main(["replay", "--dataset", str(tmp_path / "missing")]) == 2
