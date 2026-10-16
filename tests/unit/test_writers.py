import json
from datetime import UTC, date, datetime

import pytest
from cassandra.query import UNSET_VALUE

from fraudx.core.config import KafkaSettings
from fraudx.core.schemas import TransactionEvent
from fraudx.messaging.consumer import PermanentError, StreamWorker
from fraudx.messaging.producer import EventProducer
from fraudx.storage.graph_writer import account_kind, to_row
from fraudx.storage.history_writer import to_values
from fraudx.storage.profile_writer import MAX_RECENT, customer_update
from fraudx.storage.sink import SinkHandler
from tests.unit.test_messaging import FakeConsumer, FakeKafkaProducer, FakeMessage

EVENT = {
    "transaction_id": "TX-1",
    "event_time": "2026-01-05T23:30:00Z",
    "customer_id": "C-1",
    "source_account": "A-1",
    "destination_account": "M-7",
    "amount": 12.5,
    "transaction_type": "PAYMENT",
    "device_id": "DEV-1",
    "ip_address": "41.224.1.2",
    "location": {"country": "TN", "city": "Sfax"},
}


def _event(**overrides):
    return TransactionEvent.model_validate({**EVENT, **overrides})


def _featured(event_overrides=None):
    features = {
        "cust_txn_count_5m": 1, "cust_txn_count_1h": 1, "cust_txn_count_24h": 1,
        "cust_amount_sum_5m": 1, "cust_amount_sum_1h": 1, "cust_amount_sum_24h": 1,
        "cust_history_count": 0, "is_new_device": True, "is_new_ip": True,
        "is_new_country": True, "hour_of_day": 23,
    }  # fmt: skip
    return {
        "event": {**EVENT, **(event_overrides or {})},
        "features": features,
        "processed_at": "2026-10-07T00:00:00Z",
    }


# --- batch mode of StreamWorker ----------------------------------------------------


def _batch_worker(batch_handler):
    consumer = FakeConsumer()
    producer = EventProducer(KafkaSettings(), "t", producer=FakeKafkaProducer(log=consumer.log))
    worker = StreamWorker("t", ["t"], None, KafkaSettings(), producer, consumer=consumer,
                          batch_handler=batch_handler, max_backoff_s=0)  # fmt: skip
    return worker, consumer, producer


def test_worker_requires_exactly_one_handler():
    with pytest.raises(ValueError, match="exactly one"):
        StreamWorker("t", ["t"], None, KafkaSettings(), None, consumer=FakeConsumer())


def test_batch_success_commits_highest_offset_per_partition():
    worker, consumer, _ = _batch_worker(lambda msgs: [])
    batch = [FakeMessage(b"x", partition=p, offset=o) for p, o in [(0, 5), (1, 2), (0, 6)]]
    worker.process_batch(batch)
    worker.commit()
    assert consumer.commits == [{("t", 0): 7, ("t", 1): 3}]


def test_batch_rejects_are_dead_lettered_and_committed():
    batch = [FakeMessage(b"ok", offset=0), FakeMessage(b"bad", offset=1)]
    worker, consumer, producer = _batch_worker(lambda msgs: [(msgs[1], PermanentError("nope"))])
    worker.process_batch(batch)
    worker.commit()
    assert producer._producer.produced[0][1] == b"bad"
    assert consumer.commits == [{("t", 0): 2}]
    assert worker.dead_lettered == 1


def test_batch_failure_rewinds_each_partition_to_first_offset():
    def boom(msgs):
        raise ConnectionError("down")

    worker, consumer, _ = _batch_worker(boom)
    batch = [FakeMessage(b"x", partition=p, offset=o) for p, o in [(0, 5), (1, 9), (0, 6)]]
    worker.process_batch(batch)
    assert sorted(consumer.seeks) == [("t", 0, 5), ("t", 1, 9)]
    worker.commit()
    assert consumer.commits == []


# --- sink handler ------------------------------------------------------------------


class RecordingWriter:
    def __init__(self):
        self.batches = []

    def write(self, events):
        self.batches.append(events)


def test_sink_writes_valid_and_rejects_invalid():
    writer = RecordingWriter()
    good = FakeMessage(json.dumps(_featured()).encode())
    bad = FakeMessage(b'{"event": {}}')
    rejects = SinkHandler(writer)([good, bad])
    assert [m for m, _ in rejects] == [bad]
    assert [e.transaction_id for e in writer.batches[0]] == ["TX-1"]


def test_sink_skips_write_when_nothing_valid():
    writer = RecordingWriter()
    SinkHandler(writer)([FakeMessage(b"junk")])
    assert writer.batches == []


# --- row mapping ---------------------------------------------------------------


def test_graph_row_direction_and_kinds():
    row = to_row(_event())
    assert row["tx"] == _event().transaction_id  # tie-breaker of the counter guard
    assert row["outgoing"] is True
    assert (row["source_kind"], row["destination_kind"]) == ("CUSTOMER", "MERCHANT")
    assert row["location_id"] == "TN|Sfax"
    assert to_row(_event(transaction_type="CASH_IN"))["outgoing"] is False
    assert account_kind("M1979787155") == "MERCHANT"  # PaySim merchant id
    assert account_kind("C553264065") == "CUSTOMER"


def test_history_values_never_bind_null():
    values = to_values(_event(device_id=None, ip_address=None, location=None))
    assert values["event_date"] == date(2026, 1, 5)
    for column in ("device_id", "ip_address", "country", "risk_score", "ml_score"):
        assert values[column] is UNSET_VALUE
    assert None not in values.values()


def test_profile_update_is_bounded_pipeline_upsert():
    op = customer_update(_event(), datetime(2026, 10, 7, tzinfo=UTC))
    doc = op._doc
    assert op._upsert is True
    assert isinstance(doc, list)  # aggregation pipeline update
    devices = doc[0]["$set"]["devices"]
    assert devices["$slice"][1] == -MAX_RECENT
