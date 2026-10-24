import json
from decimal import Decimal

import pytest
from pydantic import ValidationError

from fraudx.core.schemas import SCHEMA_VERSION, TransactionEvent, TransactionType
from fraudx.storage.history_writer import to_values

SPEC_EXAMPLE = {
    "transaction_id": "TX-100023",
    "event_time": "2026-10-07T10:15:32Z",
    "customer_id": "C-1002",
    "source_account": "A-1002",
    "destination_account": "A-9821",
    "amount": 8500.50,
    "transaction_type": "TRANSFER",
    "device_id": "DEV-88",
    "ip_address": "192.168.1.20",
    "location": {"country": "TN", "city": "Sfax"},
}


def test_spec_example_round_trips_through_json():
    event = TransactionEvent.model_validate(SPEC_EXAMPLE)
    assert event.amount == Decimal("8500.50")
    assert event.transaction_type is TransactionType.TRANSFER
    assert event.schema_version == SCHEMA_VERSION

    wire = json.loads(event.model_dump_json())
    assert wire["amount"] == 8500.5  # JSON number, not a string
    assert TransactionEvent.model_validate(wire) == event


def test_partition_key_is_customer():
    assert TransactionEvent.model_validate(SPEC_EXAMPLE).partition_key == b"C-1002"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_time", "2026-10-07T10:15:32"),  # naive timestamp: ambiguous
        ("amount", -1),
        ("amount", 1.234),  # sub-cent precision
        ("transaction_type", "WIRE"),
        ("ip_address", "999.1.1.1"),
        ("currency", "dinar"),
        ("unexpected_field", "x"),
    ],
)
def test_invalid_events_rejected(field, value):
    with pytest.raises(ValidationError):
        TransactionEvent.model_validate({**SPEC_EXAMPLE, field: value})


def test_location_country_must_be_iso2():
    with pytest.raises(ValidationError):
        TransactionEvent.model_validate({**SPEC_EXAMPLE, "location": {"country": "Tunisia"}})


def test_event_time_is_normalised_to_utc():
    """A producer's offset must not change the hour feature or the day partition."""
    event = TransactionEvent.model_validate({
        "transaction_id": "TX-1", "event_time": "2026-03-01T00:30:00+01:00",
        "customer_id": "C-1", "source_account": "A-1", "destination_account": "M-1",
        "amount": 10, "transaction_type": "PAYMENT",
    })  # fmt: skip
    assert event.event_time.utcoffset().total_seconds() == 0
    assert (event.event_time.hour, event.event_time.day) == (23, 28)
    assert str(to_values(event)["event_date"]) == "2026-02-28"


def test_fraud_decision_event():
    from datetime import UTC, datetime

    from fraudx.core.schemas import FraudDecisionEvent, InvestigationStatus

    now = datetime(2026, 10, 24, 21, 0, tzinfo=UTC)
    event = FraudDecisionEvent(
        transaction_id="TX-99",
        status=InvestigationStatus.CONFIRMED_FRAUD,
        customer_id="C-99",
        source_account="A-1",
        destination_account="A-2",
        transaction_type="TRANSFER",
        amount=150.0,
        currency="TND",
        note="confirmed mule",
        decided_at=now,
    )
    assert event.partition_key == b"C-99"
    dumped = json.loads(event.model_dump_json())
    assert dumped["status"] == "CONFIRMED_FRAUD"
    assert FraudDecisionEvent.model_validate(dumped) == event
