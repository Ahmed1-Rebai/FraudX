from datetime import UTC, datetime, timedelta

from fraudx.core.schemas import TransactionEvent
from fraudx.ml.graph_dataset import time_groups
from fraudx.storage.graph_features import _flow

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _event(n, seconds, **kw):
    return TransactionEvent.model_validate({
        "transaction_id": f"TX-{n}", "event_time": T0 + timedelta(seconds=seconds),
        "customer_id": "C-1", "source_account": "A-1", "destination_account": "M-1",
        "amount": 10, "transaction_type": "PAYMENT", **kw,
    })  # fmt: skip


def test_time_groups_span_at_most_a_minute():
    events = [_event(1, 0), _event(2, 30), _event(3, 59), _event(4, 60), _event(5, 200)]
    groups = [[e.transaction_id for e in g] for g in time_groups(events)]
    assert groups == [["TX-1", "TX-2", "TX-3"], ["TX-4"], ["TX-5"]]


def test_flow_follows_the_money():
    assert (_flow(_event(1, 0))["payer"], _flow(_event(1, 0))["payee"]) == ("A-1", "M-1")
    cash_in = _flow(_event(2, 0, transaction_type="CASH_IN"))
    assert (cash_in["payer"], cash_in["payee"]) == ("M-1", "A-1")  # the agent pays
