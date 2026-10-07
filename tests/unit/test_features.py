import json
from datetime import UTC, datetime

import pytest

from fraudx.core.config import KafkaSettings
from fraudx.core.schemas import FeaturedTransaction, TransactionEvent, TransactionFeatures
from fraudx.messaging.consumer import PermanentError
from fraudx.messaging.producer import EventProducer
from fraudx.streaming.features import EntityStats, build_features, customer_keys, entity_keys
from fraudx.streaming.processor import FeatureProcessor

BASE = {
    "transaction_id": "TX-9",
    "event_time": "2026-01-02T03:04:05Z",
    "customer_id": "C-1",
    "source_account": "A-1",
    "destination_account": "M-1",
    "amount": 300,
    "transaction_type": "TRANSFER",
    "device_id": "DEV-1",
    "ip_address": "41.224.1.1",
    "location": {"country": "TN", "city": "Sfax"},
    "balances": {"source_before": 400, "source_after": 100,
                 "destination_before": 0, "destination_after": 300},
}  # fmt: skip


def _event(**overrides):
    return TransactionEvent.model_validate({**BASE, **overrides})


def _raw(**overrides):
    raw = {
        "prior_n": 4,
        "prior_sum": 400.0,  # mean 100
        "prior_sumsq": 4 * 100.0**2 + 4 * 20.0**2,  # std 20
        "last_ts": datetime(2026, 1, 2, 3, 0, 5, tzinfo=UTC).timestamp() * 1000,
        "known_device": True,
        "known_ip": False,
        "known_country": None,
        "windows": [[2, 350.0], [3, 400.0], [5, 700.0]],
    }
    raw.update(overrides)
    return raw


def test_profile_statistics_and_flags():
    f = build_features(_event(), _raw(), EntityStats(4, 3), None)
    assert (f.cust_txn_count_5m, f.cust_txn_count_1h, f.cust_txn_count_24h) == (2, 3, 5)
    assert f.cust_amount_sum_24h == 700.0
    assert f.cust_history_count == 4
    assert f.cust_amount_mean == 100.0
    assert f.cust_amount_std == pytest.approx(20.0)
    assert f.amount_zscore == pytest.approx(10.0)  # (300 - 100) / 20
    assert f.amount_to_mean_ratio == 3.0
    assert f.seconds_since_last_txn == 240.0
    assert (f.is_new_device, f.is_new_ip, f.is_new_country) == (False, True, False)
    assert (f.device_txn_count_1h, f.device_customer_count) == (4, 3)
    assert (f.ip_txn_count_1h, f.ip_customer_count) == (0, 0)
    assert f.balance_drain_ratio == 0.75
    assert f.hour_of_day == 3


def test_first_transaction_has_no_profile_statistics():
    raw = _raw(prior_n=0, prior_sum=0, prior_sumsq=0, last_ts=None, windows=[[1, 300.0]] * 3)
    f = build_features(_event(), raw, None, None)
    assert f.cust_history_count == 0
    assert f.cust_amount_mean is f.cust_amount_std is f.amount_zscore is None
    assert f.seconds_since_last_txn is None


def test_constant_history_has_no_zscore():
    raw = _raw(prior_n=3, prior_sum=300.0, prior_sumsq=3 * 100.0**2)
    f = build_features(_event(), raw, None, None)
    assert f.cust_amount_std == 0.0
    assert f.amount_zscore is None


def test_drain_ratio_only_for_debits_with_balance():
    assert build_features(_event(transaction_type="CASH_IN"), _raw(), None, None) \
        .balance_drain_ratio is None  # fmt: skip
    no_balance = _event(balances=None)
    assert build_features(no_balance, _raw(), None, None).balance_drain_ratio is None


def test_out_of_order_event_has_no_negative_gap():
    late = _raw(last_ts=datetime(2026, 1, 3, tzinfo=UTC).timestamp() * 1000)
    assert build_features(_event(), late, None, None).seconds_since_last_txn is None


def test_keys_share_one_hash_tag_per_entity():
    keys = customer_keys("C-1", "TX-1")
    assert all("{C-1}" in k for k in keys)
    assert entity_keys("dev", "DEV-1") == ["fx:dev:{DEV-1}:txw", "fx:dev:{DEV-1}:custs"]


class FakeStore:
    def __init__(self):
        self.events = []

    def compute(self, event):
        self.events.append(event)
        return build_features(event, _raw(), None, None)


class FakeKafka:
    def __init__(self):
        self.produced = []

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.produced.append((topic, value, key))

    def poll(self, timeout):
        return 0


class Msg:
    def __init__(self, payload):
        self._value = json.dumps(payload).encode()

    def value(self):
        return self._value


def _processor():
    store, kafka = FakeStore(), FakeKafka()
    fixed = datetime(2026, 10, 7, tzinfo=UTC)
    proc = FeatureProcessor(store, EventProducer(KafkaSettings(), "t", producer=kafka),
                            now=lambda: fixed)  # fmt: skip
    return proc, store, kafka


def test_processor_publishes_featured_transaction_keyed_by_customer():
    proc, _, kafka = _processor()
    proc(Msg(BASE))
    topic, value, key = kafka.produced[0]
    assert (topic, key) == ("transactions.features", b"C-1")
    out = FeaturedTransaction.model_validate_json(value)
    assert out.event.transaction_id == "TX-9"
    assert isinstance(out.features, TransactionFeatures)


def test_processor_strips_labels_before_features_and_output():
    proc, store, kafka = _processor()
    proc(Msg({**BASE, "is_fraud_label": True}))
    assert store.events[0].is_fraud_label is None
    assert b"is_fraud_label" not in kafka.produced[0][1]


@pytest.mark.parametrize(
    "payload",
    [{**BASE, "amount": -5}, {**BASE, "destination_account": "A-1"}, {"junk": 1}],
)
def test_processor_rejects_invalid_events_permanently(payload):
    proc, store, kafka = _processor()
    with pytest.raises(PermanentError):
        proc(Msg(payload))
    assert not store.events and not kafka.produced
