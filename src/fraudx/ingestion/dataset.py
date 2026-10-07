"""Writes a transaction dataset: events, ground truth and a summary.

Layout of an output directory::

    enriched_transactions.jsonl   one TransactionEvent per line, event-time order
    ground_truth.csv              transaction_id,is_fraud,scenario (evaluation only)
    summary.json                  counts, fraud rate, time range, generation parameters

Files are written to a temporary name and renamed on success, so a crashed
run never leaves a half-written dataset that looks complete.
"""

from __future__ import annotations

import csv
import json
import os
from collections import Counter
from collections.abc import Callable, Iterable
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from fraudx.core.schemas import TransactionEvent

EVENTS_FILE = "enriched_transactions.jsonl"
GROUND_TRUTH_FILE = "ground_truth.csv"
SUMMARY_FILE = "summary.json"


@dataclass(slots=True)
class DatasetStats:
    events: int = 0
    fraud: int = 0
    by_type: Counter[str] = field(default_factory=Counter)
    by_scenario: Counter[str] = field(default_factory=Counter)
    customers: set[str] = field(default_factory=set)
    first_event: datetime | None = None
    last_event: datetime | None = None

    def add(self, event: TransactionEvent, scenario: str) -> None:
        self.events += 1
        self.fraud += bool(event.is_fraud_label)
        self.by_type[event.transaction_type.value] += 1
        self.by_scenario[scenario] += 1
        self.customers.add(event.customer_id)
        if self.first_event is None or event.event_time < self.first_event:
            self.first_event = event.event_time
        if self.last_event is None or event.event_time > self.last_event:
            self.last_event = event.event_time

    def to_dict(self) -> dict[str, Any]:
        return {
            "events": self.events,
            "fraud_events": self.fraud,
            "fraud_rate": round(self.fraud / self.events, 6) if self.events else 0.0,
            "customers": len(self.customers),
            "first_event": self.first_event.isoformat() if self.first_event else None,
            "last_event": self.last_event.isoformat() if self.last_event else None,
            "by_type": dict(sorted(self.by_type.items())),
            "by_scenario": dict(sorted(self.by_scenario.items())),
        }


def write_dataset(
    rows: Iterable[tuple[TransactionEvent, str]],
    out_dir: Path,
    metadata: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Stream ``(event, scenario)`` pairs to ``out_dir``; returns the summary.

    ``metadata`` is called after the stream is exhausted, so it can report
    counters (e.g. invalid rows) that are only known once every row was read.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    stats = DatasetStats()
    targets = [out_dir / EVENTS_FILE, out_dir / GROUND_TRUTH_FILE]
    temps = [p.with_name(p.name + ".tmp") for p in targets]
    try:
        with ExitStack() as stack:
            events_fh = stack.enter_context(temps[0].open("w", encoding="utf-8"))
            truth_fh = stack.enter_context(temps[1].open("w", encoding="utf-8", newline=""))
            truth = csv.writer(truth_fh)
            truth.writerow(["transaction_id", "is_fraud", "scenario"])
            for event, scenario in rows:
                events_fh.write(event.model_dump_json(exclude_none=True))
                events_fh.write("\n")
                truth.writerow([event.transaction_id, int(bool(event.is_fraud_label)), scenario])
                stats.add(event, scenario)
        for tmp, target in zip(temps, targets, strict=True):
            os.replace(tmp, target)
    finally:
        for tmp in temps:
            tmp.unlink(missing_ok=True)

    summary = {**stats.to_dict(), **metadata()}
    _write_json(out_dir / SUMMARY_FILE, summary)
    return summary


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def read_events(path: Path) -> Iterable[TransactionEvent]:
    """Read back an events file, validating every line."""
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield TransactionEvent.model_validate_json(line)
