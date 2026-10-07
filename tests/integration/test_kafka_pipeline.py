"""Producer → Kafka → StreamWorker round trip on throwaway topics."""

import threading
import time
import uuid

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import NewTopic

from fraudx.core.config import KafkaSettings
from fraudx.core.schemas import TransactionEvent
from fraudx.ingestion.generator import GeneratorConfig, TransactionGenerator
from fraudx.ingestion.replay import prepare, replay
from fraudx.messaging.consumer import StreamWorker, decode
from fraudx.messaging.producer import EventProducer
from fraudx.storage import clients

pytestmark = pytest.mark.integration


@pytest.fixture
def topics():
    admin = clients.kafka_admin(KafkaSettings())
    suffix = uuid.uuid4().hex[:8]
    names = (f"it.raw.{suffix}", f"it.dlq.{suffix}")
    for future in admin.create_topics([NewTopic(n, 3, 1) for n in names]).values():
        future.result(10)
    yield names
    for future in admin.delete_topics(list(names)).values():
        future.result(10)


def _run_until(worker, predicate, timeout=30.0):
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.2)
    worker.stop()
    thread.join(15)
    assert not thread.is_alive()


def _dlq_messages(topic):
    consumer = Consumer(
        {
            "bootstrap.servers": KafkaSettings().bootstrap_servers,
            "group.id": f"it-dlq-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    found, deadline = [], time.monotonic() + 15
    while not found and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            found.append(msg)
    consumer.close()
    return found


def test_replay_consume_dead_letter_and_resume(topics):
    raw_topic, dlq_topic = topics
    settings = KafkaSettings()
    config = GeneratorConfig(customers=60, days=3, victim_share=0.05, rings=2, ring_size_max=3)
    events = list(TransactionGenerator(config).generate())[:200]

    producer = EventProducer(settings, "it-replay")
    report = replay(prepare(e.event for e in events), producer, topic=raw_topic)
    producer.send(raw_topic, b'{"transaction_id": "broken"}', key=b"C-bad")
    producer.flush()
    assert report.sent == 200

    received: list[TransactionEvent] = []
    group = f"it-group-{uuid.uuid4().hex[:6]}"

    def handler(msg):
        received.append(decode(msg, TransactionEvent))

    worker = StreamWorker(
        "it", [raw_topic], handler, settings, EventProducer(settings, "it-worker"),
        group_id=group, dlq_topic=dlq_topic, commit_interval_s=0.5,
    )  # fmt: skip
    _run_until(worker, lambda: worker.processed >= 201)

    assert len(received) == 200
    assert {e.transaction_id for e in received} == {g.event.transaction_id for g in events}
    assert all(e.is_fraud_label is None for e in received)  # labels stripped on the wire
    # Per-customer order is preserved (same key -> same partition).
    by_customer: dict[str, list] = {}
    for e in received:
        by_customer.setdefault(e.customer_id, []).append(e.event_time)
    assert all(times == sorted(times) for times in by_customer.values())

    dlq = _dlq_messages(dlq_topic)
    assert len(dlq) == 1
    headers = {k: v.decode() for k, v in dlq[0].headers()}
    assert headers["dlq.source_topic"] == raw_topic
    assert "TransactionEvent invalid" in headers["dlq.error"]
    assert dlq[0].key() == b"C-bad"

    # Same group again with one new event: only the new one is delivered,
    # proving the first worker's offsets were committed.
    producer.send_model(raw_topic, events[0].event, key=b"C-new")
    producer.flush()
    resumed: list[bytes] = []
    again = StreamWorker(
        "it", [raw_topic], lambda m: resumed.append(m.key()), settings,
        EventProducer(settings, "it-worker2"), group_id=group, dlq_topic=dlq_topic,
    )  # fmt: skip
    _run_until(again, lambda: again.processed >= 1)
    time.sleep(1)
    assert resumed == [b"C-new"]
