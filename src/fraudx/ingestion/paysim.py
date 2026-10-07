"""PaySim CSV loader.

Maps PaySim rows (https://www.kaggle.com/datasets/ealaxi/paysim1) onto
``TransactionEvent``:

* ``step`` (hour index since simulation start) -> ``event_time`` = start +
  step hours + a deterministic offset within the hour.
* ``nameOrig`` -> ``customer_id`` and ``source_account``; ``nameDest`` ->
  ``destination_account``; the four balance columns -> ``balances``.
* ``isFraud`` -> ``is_fraud_label``.

PaySim has no device, IP or location data. They are derived from a hash of
the customer id, so they're stable per customer and **independent of the
fraud label**: on PaySim, device/graph features carry no injected signal, and
model metrics reflect PaySim's own columns only (see memory.md, ADR-013).
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import itertools
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import ValidationError

from fraudx.core.schemas import Balances, Location, TransactionEvent
from fraudx.ingestion import geo

log = logging.getLogger(__name__)

PAYSIM_COLUMNS = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
)
DEFAULT_START = datetime(2026, 1, 1, tzinfo=UTC)
_HOMES = tuple(geo.HOME_WEIGHTS)
_HOME_CDF = tuple(itertools.accumulate(geo.HOME_WEIGHTS.values()))


@dataclass(slots=True)
class LoadReport:
    rows: int = 0
    valid: int = 0
    invalid: int = 0
    sample_errors: list[str] = field(default_factory=list)


def _digest(salt: str, value: str) -> bytes:
    return hashlib.blake2b(f"{salt}:{value}".encode(), digest_size=16).digest()


def _unit(digest: bytes, offset: int) -> float:
    """A float in [0, 1) taken from 4 bytes of the digest."""
    return int.from_bytes(digest[offset : offset + 4], "big") / 2**32


def enrich(customer_id: str, salt: str) -> tuple[str, str, Location]:
    """Stable, label-independent device / IP / location for a customer."""
    d = _digest(salt, customer_id)
    home = _HOMES[bisect.bisect_right(_HOME_CDF, _unit(d, 0) * _HOME_CDF[-1])]
    country = geo.COUNTRIES[home]
    prefix = country.ip_prefixes[d[4] % len(country.ip_prefixes)]
    ip = f"{prefix}.{d[5] % 254 + 1}.{d[6] % 254 + 1}"
    city = country.cities[d[7] % len(country.cities)]
    return f"DEV-{d[8:13].hex()}", ip, Location(country=home, city=city)


def parse_row(
    row: dict[str, str], line: int, start: datetime, salt: str, currency: str = "TND"
) -> TransactionEvent:
    customer = row["nameOrig"]
    step = int(row["step"])
    # Spread events inside their hour deterministically (PaySim has hour resolution).
    jitter = int.from_bytes(_digest(salt, f"t{line}")[:4], "big") % 3600
    device, ip, location = enrich(customer, salt)
    return TransactionEvent(
        transaction_id=f"PS-{line:09d}",
        event_time=start + timedelta(hours=step - 1, seconds=jitter),
        customer_id=customer,
        source_account=customer,
        destination_account=row["nameDest"],
        amount=row["amount"],
        currency=currency,
        transaction_type=row["type"],
        device_id=device,
        ip_address=ip,
        location=location,
        balances=Balances(
            source_before=row["oldbalanceOrg"],
            source_after=row["newbalanceOrig"],
            destination_before=row["oldbalanceDest"],
            destination_after=row["newbalanceDest"],
        ),
        is_fraud_label=row["isFraud"] == "1",
    )


def load(
    path: Path,
    report: LoadReport,
    start: datetime = DEFAULT_START,
    salt: str = "fraudx",
    limit: int | None = None,
) -> Iterator[TransactionEvent]:
    """Stream valid events from a PaySim CSV; invalid rows are counted in ``report``.

    PaySim is ordered by ``step``, so output is in event-time order at hour
    granularity (minute jitter can reorder events within the same hour).
    """
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        missing = set(PAYSIM_COLUMNS) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path} is not a PaySim file; missing columns: {sorted(missing)}")
        for line, row in enumerate(reader, start=1):
            if limit is not None and report.rows >= limit:
                break
            report.rows += 1
            try:
                event = parse_row(row, line, start, salt)
            except (ValidationError, ValueError) as exc:
                report.invalid += 1
                if len(report.sample_errors) < 10:
                    report.sample_errors.append(f"line {line}: {exc}".splitlines()[0])
                continue
            report.valid += 1
            yield event
