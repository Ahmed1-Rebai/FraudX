from datetime import UTC, datetime, timedelta

import pytest

from fraudx.core.config import KafkaSettings
from fraudx.core.schemas import TransactionEvent
from fraudx.ingestion.replay import Pacer, prepare, replay
from fraudx.messaging.consumer import PermanentError, StreamWorker, decode, lag_from_stats
from fraudx.messaging.heartbeat import is_fresh
from fraudx.messaging.producer import DeliveryError, EventProducer

EVENT = {
    "transaction_id": "TX-1",
    "event_time": "2026-01-01T10:00:00Z",
    "customer_id": "C-1",
    "source_account": "A-1",
    "destination_account": "M-1",
    "amount": 10.5,
    "transaction_type": "PAYMENT",
    "is_fraud_label": True,
}


class FakeMessage:
    def __init__(self, value, topic="t", partition=0, offset=0, key=b"k", headers=None):
        self._value, self._topic, self._partition = value, topic, partition
        self._offset, self._key, self._headers = offset, key, headers

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def headers(self):
        return self._headers

    def timestamp(self):
        return (1, 0)

    def error(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.commits, self.seeks, self.log = [], [], []

    def commit(self, offsets, asynchronous):
        assert asynchronous is False
        self.log.append("commit")
        self.commits.append({(o.topic, o.partition): o.offset for o in offsets})

    def seek(self, tp):
        self.seeks.append((tp.topic, tp.partition, tp.offset))


class FakeKafkaProducer:
    def __init__(self, pending_after_flush=0, log=None):
        self.produced, self.pending_after_flush = [], pending_after_flush
        self.log = log if log is not None else []

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.produced.append((topic, value, key, dict(headers or [])))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        self.log.append("flush")
        return self.pending_after_flush


def _worker(handler, **kwargs):
    consumer = FakeConsumer()
    kafka = FakeKafkaProducer(log=consumer.log)
    producer = EventProducer(KafkaSettings(), "test", producer=kafka)
    worker = StreamWorker(
        "test", ["t"], handler, KafkaSettings(), producer, consumer=consumer,
        max_backoff_s=0, **kwargs,
    )  # fmt: skip
    return worker, consumer, kafka


def test_success_records_offset_and_commit_flushes_first():
    worker, consumer, _ = _worker(lambda m: None)
    worker.process(FakeMessage(b"x", partition=2, offset=41))
    worker.commit()
    assert consumer.commits == [{("t", 2): 42}]
    assert consumer.log == ["flush", "commit"]  # outputs durable before input is acked


def test_permanent_error_goes_to_dlq_and_is_committed():
    def handler(_):
        raise PermanentError("bad payload")

    worker, consumer, kafka = _worker(handler)
    worker.process(FakeMessage(b"garbage", offset=7, headers=[("trace", b"abc")]))
    worker.commit()
    topic, value, key, headers = kafka.produced[0]
    assert (topic, value, key) == ("transactions.dlq", b"garbage", b"k")
    assert headers["dlq.error"] == "bad payload"
    assert headers["dlq.source_offset"] == "7"
    assert headers["trace"] == b"abc"  # original headers kept
    assert consumer.commits == [{("t", 0): 8}]
    assert worker.dead_lettered == 1


def test_transient_error_rewinds_and_does_not_commit():
    calls = []

    def flaky(msg):
        calls.append(msg.offset())
        if len(calls) == 1:
            raise ConnectionError("db down")

    worker, consumer, _ = _worker(flaky)
    msg = FakeMessage(b"x", offset=5)
    worker.process(msg)
    assert consumer.seeks == [("t", 0, 5)]
    worker.commit()
    assert consumer.commits == []  # nothing processed yet
    worker.process(msg)  # redelivered after seek
    worker.commit()
    assert consumer.commits == [{("t", 0): 6}]


def test_commit_only_revoked_partitions_on_rebalance():
    from confluent_kafka import TopicPartition

    worker, consumer, _ = _worker(lambda m: None)
    worker.process(FakeMessage(b"x", partition=0, offset=1))
    worker.process(FakeMessage(b"x", partition=1, offset=9))
    worker._on_revoke(None, [TopicPartition("t", 1)])
    assert consumer.commits == [{("t", 1): 10}]
    worker.commit()
    assert consumer.commits[-1] == {("t", 0): 2}


def test_commit_every_n_messages():
    worker, consumer, _ = _worker(
        lambda m: None, commit_every=3, commit_interval_s=1e9, clock=lambda: 0
    )
    for offset in range(3):
        worker.process(FakeMessage(b"x", offset=offset))
        worker._maybe_commit()
    assert consumer.commits == [{("t", 0): 3}]


def test_decode_valid_and_invalid():
    import json

    event = decode(FakeMessage(json.dumps(EVENT).encode()), TransactionEvent)
    assert event.transaction_id == "TX-1"
    with pytest.raises(PermanentError, match="amount"):
        decode(FakeMessage(json.dumps({**EVENT, "amount": -1}).encode()), TransactionEvent)
    with pytest.raises(PermanentError):
        decode(FakeMessage(b"{not json"), TransactionEvent)
    with pytest.raises(PermanentError, match="empty"):
        decode(FakeMessage(None), TransactionEvent)


def test_lag_from_stats_skips_unknown():
    stats = {"topics": {"t": {"partitions": {
        "0": {"consumer_lag": 12}, "1": {"consumer_lag": -1}, "-1": {"consumer_lag": 5},
    }}}}  # fmt: skip
    assert lag_from_stats(stats) == {("t", 0): 12}


def test_producer_flush_raises_when_messages_pending():
    producer = EventProducer(
        KafkaSettings(), "x", producer=FakeKafkaProducer(pending_after_flush=3)
    )
    with pytest.raises(DeliveryError, match="3 message"):
        producer.flush(0.1)


def test_send_model_sets_key_and_schema_header():
    kafka = FakeKafkaProducer()
    producer = EventProducer(KafkaSettings(), "x", producer=kafka)
    producer.send_model("raw", TransactionEvent.model_validate(EVENT), key=b"C-1")
    _, value, key, headers = kafka.produced[0]
    assert key == b"C-1"
    assert headers["schema_version"] == "1"
    assert b'"amount":10.5' in value


def test_pacer_spaces_calls_and_does_not_burst_after_stall():
    now = [0.0]
    sleeps = []

    def sleep(s):
        sleeps.append(round(s, 3))
        now[0] += s

    pacer = Pacer(10, clock=lambda: now[0], sleep=sleep)
    for _ in range(3):
        pacer.wait()
    assert sleeps == [0.1, 0.1]
    now[0] += 5  # stall
    sleeps.clear()
    pacer.wait()
    pacer.wait()
    assert sleeps == [0.1]  # no catch-up burst


def test_prepare_strips_labels_and_rebases_time():
    base = TransactionEvent.model_validate(EVENT)
    later = base.model_copy(update={"event_time": base.event_time + timedelta(minutes=5)})
    fixed_now = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)
    out = list(prepare([base, later], rebase_time=True, now=lambda: fixed_now))
    assert all(e.is_fraud_label is None for e in out)
    assert out[0].event_time == fixed_now
    assert out[1].event_time - out[0].event_time == timedelta(minutes=5)
    assert next(iter(prepare([base], with_labels=True))).is_fraud_label is True
    resent = next(iter(prepare([base], id_prefix="LIVE1-")))
    assert resent.transaction_id == f"LIVE1-{base.transaction_id}"
    assert resent.customer_id == base.customer_id


def test_replay_keys_by_customer_and_respects_limit():
    kafka = FakeKafkaProducer()
    producer = EventProducer(KafkaSettings(), "x", producer=kafka)
    events = [TransactionEvent.model_validate({**EVENT, "transaction_id": f"TX-{i}"})
              for i in range(5)]  # fmt: skip
    report = replay(events, producer, limit=3)
    assert report.sent == 3 and len(kafka.produced) == 3
    assert {p[0] for p in kafka.produced} == {"transactions.raw"}
    assert {p[2] for p in kafka.produced} == {b"C-1"}


def test_heartbeat_freshness(tmp_path):
    beat = tmp_path / "hb"
    assert not is_fresh(beat, 30)
    beat.touch()
    assert is_fresh(beat, 30)
