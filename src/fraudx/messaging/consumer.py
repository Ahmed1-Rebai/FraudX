"""Consumer-group worker: the runtime shared by every pipeline stage.

Delivery semantics: **at-least-once**, with no silent loss.

* A message's offset is recorded only after its handler returns.
* Offsets are committed every ``commit_interval_s`` / ``commit_every``
  messages, on partition revocation and on shutdown, and always **after**
  flushing the producer, so anything a handler produced is durable before the
  input is marked consumed. Handlers must therefore be idempotent.

Error policy:

* ``PermanentError`` (undecodable / invalid input): the original message goes
  to the dead-letter topic with diagnostic headers, then processing moves on.
  A poison message never blocks its partition.
* Any other exception is treated as transient (e.g. a database is down): the
  partition is rewound to the failed message and retried with exponential
  backoff. Polling continues, so the worker stays in its consumer group.

Batch mode (``batch_handler`` + ``batch_size``) serves store writers: the
handler receives up to ``batch_size`` messages, writes them together and
returns the messages it rejects as permanent (→ DLQ). A batch waits at most
``batch_wait_s`` (50 ms) to fill: under load batches fill at once from the
local queue, and at low traffic a message never waits for others. Raising any exception
rewinds every partition in the batch and retries the whole batch, so batch
writes must be idempotent.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel, ValidationError

from fraudx.core.config import KafkaSettings
from fraudx.core.topics import TRANSACTIONS_DLQ
from fraudx.messaging.producer import EventProducer

log = logging.getLogger(__name__)

MESSAGES = Counter(
    "fraudx_consumer_messages_total", "Consumed messages by outcome", ["worker", "topic", "outcome"]
)
PROCESSING = Histogram(
    "fraudx_consumer_processing_seconds",
    "Handler time per message",
    ["worker"],
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
MESSAGE_AGE = Histogram(
    "fraudx_consumer_message_age_seconds",
    "Time from produce to processed (pipeline latency up to this stage)",
    ["worker"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
LAG = Gauge("fraudx_consumer_lag", "Messages behind the log end", ["group", "topic", "partition"])


class PermanentError(Exception):
    """The message can never be processed; send it to the dead-letter topic."""


class MessageHandler(Protocol):
    def __call__(self, message: Message) -> None: ...


class BatchHandler(Protocol):
    def __call__(self, messages: list[Message]) -> list[tuple[Message, PermanentError]]:
        """Process a batch; return the (message, error) pairs to dead-letter."""
        ...


def decode[M: BaseModel](message: Message, model: type[M]) -> M:
    """Parse a JSON message value into ``model`` or raise ``PermanentError``."""
    value = message.value()
    if value is None:
        raise PermanentError("empty message value")
    try:
        return model.model_validate_json(value)
    except ValidationError as exc:
        first = exc.errors()[0]
        loc = ".".join(str(p) for p in first["loc"]) or "<root>"
        raise PermanentError(f"{model.__name__} invalid: {loc}: {first['msg']}") from exc


def consumer_config(settings: KafkaSettings, group_id: str, name: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": f"{settings.client_id}-{name}",
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "statistics.interval.ms": 10_000,
    }


def lag_from_stats(stats: dict[str, Any]) -> dict[tuple[str, int], int]:
    """Extract consumer lag per partition from librdkafka statistics."""
    lags = {}
    for topic, tdata in stats.get("topics", {}).items():
        for pid, pdata in tdata.get("partitions", {}).items():
            lag = pdata.get("consumer_lag", -1)
            if int(pid) >= 0 and lag >= 0:
                lags[(topic, int(pid))] = lag
    return lags


class StreamWorker:
    def __init__(
        self,
        name: str,
        topics: list[str],
        handler: MessageHandler | None,
        settings: KafkaSettings,
        producer: EventProducer,
        group_id: str | None = None,
        dlq_topic: str = TRANSACTIONS_DLQ,
        commit_interval_s: float = 2.0,
        commit_every: int = 500,
        heartbeat_path: Path | None = None,
        max_backoff_s: float = 30.0,
        consumer: Any = None,
        clock: Callable[[], float] = time.monotonic,
        batch_handler: BatchHandler | None = None,
        batch_size: int = 500,
        batch_wait_s: float = 0.05,
    ) -> None:
        if (handler is None) == (batch_handler is None):
            raise ValueError("provide exactly one of handler / batch_handler")
        self.name = name
        self.batch_handler = batch_handler
        self.batch_size = batch_size
        self.batch_wait_s = batch_wait_s
        self.topics = topics
        self.group_id = group_id or f"fraudx-{name}"
        self.handler = handler
        self.producer = producer
        self.dlq_topic = dlq_topic
        self.commit_interval_s = commit_interval_s
        self.commit_every = commit_every
        self.heartbeat_path = heartbeat_path
        self.max_backoff_s = max_backoff_s
        self._clock = clock
        self._stop = threading.Event()
        self._pending: dict[tuple[str, int], int] = {}  # next offset to commit
        self._uncommitted = 0
        self._last_commit = clock()
        self._last_beat = 0.0
        self._attempts: dict[tuple[str, int, int], int] = {}
        self.processed = 0
        self.dead_lettered = 0
        if consumer is None:
            config = consumer_config(settings, self.group_id, name)
            config["stats_cb"] = self._on_stats
            config["error_cb"] = self._on_error
            consumer = Consumer(config)
        self._consumer = consumer

    # --- lifecycle -----------------------------------------------------------------

    def stop(self, *_: Any) -> None:
        self._stop.set()

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

    def run(self) -> None:
        self._consumer.subscribe(self.topics, on_revoke=self._on_revoke)
        log.info(
            "worker started",
            extra={"worker": self.name, "topics": self.topics, "group": self.group_id},
        )
        try:
            while not self._stop.is_set():
                if self.batch_handler is not None:
                    batch = self._consumer.consume(self.batch_size, self.batch_wait_s)
                    self._heartbeat()
                    usable = [m for m in batch if self._usable(m)]
                    if usable:
                        self.process_batch(usable)
                else:
                    msg = self._consumer.poll(1.0)
                    self._heartbeat()
                    if msg is not None and self._usable(msg):
                        self.process(msg)
                self._maybe_commit()
        finally:
            try:
                self.commit()
            finally:
                self._consumer.close()
                log.info(
                    "worker stopped",
                    extra={
                        "worker": self.name,
                        "processed": self.processed,
                        "dead_lettered": self.dead_lettered,
                    },
                )

    # --- processing ----------------------------------------------------------------

    def _usable(self, msg: Message) -> bool:
        err = msg.error()
        if err is None:
            return True
        if err.code() == KafkaError._PARTITION_EOF:
            return False
        if err.fatal():
            raise KafkaException(err)
        log.warning("consumer error", extra={"worker": self.name, "error": str(err)})
        return False

    def process(self, msg: Message) -> None:
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        key = (topic, partition, offset)
        started = time.perf_counter()
        try:
            self.handler(msg)
            outcome = "ok"
        except PermanentError as exc:
            self._dead_letter(msg, exc)
            outcome = "dlq"
        except Exception as exc:
            self._retry_later([msg], exc)
            return

        self._attempts.pop(key, None)
        PROCESSING.labels(self.name).observe(time.perf_counter() - started)
        self._mark_done(msg, outcome)

    def process_batch(self, messages: list[Message]) -> None:
        assert self.batch_handler is not None
        first = messages[0]
        key = (first.topic(), first.partition(), first.offset())
        started = time.perf_counter()
        try:
            rejects = self.batch_handler(messages)
        except Exception as exc:
            self._retry_later(messages, exc)
            return
        self._attempts.pop(key, None)
        rejected = {id(m) for m, _ in rejects}
        for msg, exc in rejects:
            self._dead_letter(msg, exc)
        per_message = (time.perf_counter() - started) / len(messages)
        for msg in messages:
            PROCESSING.labels(self.name).observe(per_message)
            self._mark_done(msg, "dlq" if id(msg) in rejected else "ok")

    def _mark_done(self, msg: Message, outcome: str) -> None:
        _, ts_ms = msg.timestamp()
        if ts_ms > 0:
            MESSAGE_AGE.labels(self.name).observe(max(time.time() - ts_ms / 1000, 0))
        MESSAGES.labels(self.name, msg.topic(), outcome).inc()
        self.processed += 1
        tp = (msg.topic(), msg.partition())
        self._pending[tp] = max(self._pending.get(tp, 0), msg.offset() + 1)
        self._uncommitted += 1

    def _retry_later(self, messages: list[Message], exc: Exception) -> None:
        """Rewind every partition in ``messages`` to its first offset and back off."""
        first = messages[0]
        key = (first.topic(), first.partition(), first.offset())
        attempt = self._attempts.get(key, 0) + 1
        self._attempts[key] = attempt
        backoff = min(self.max_backoff_s, 0.5 * 2 ** (attempt - 1))
        log.warning(
            "handler failed, will retry",
            exc_info=attempt == 1,
            extra={
                "worker": self.name,
                "topic": first.topic(),
                "partition": first.partition(),
                "offset": first.offset(),
                "messages": len(messages),
                "attempt": attempt,
                "backoff_s": backoff,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        rewind: dict[tuple[str, int], int] = {}
        for msg in messages:
            tp = (msg.topic(), msg.partition())
            rewind[tp] = min(rewind.get(tp, msg.offset()), msg.offset())
            MESSAGES.labels(self.name, msg.topic(), "retry").inc()
        for (topic, partition), offset in rewind.items():
            self._consumer.seek(TopicPartition(topic, partition, offset))
        self._stop.wait(backoff)

    def _dead_letter(self, msg: Message, exc: Exception) -> None:
        headers: dict[str, str | bytes] = {k: v for k, v in (msg.headers() or [])}
        headers.update(
            {
                "dlq.error": str(exc)[:1000],
                "dlq.error_type": type(exc).__name__,
                "dlq.source_topic": msg.topic(),
                "dlq.source_partition": str(msg.partition()),
                "dlq.source_offset": str(msg.offset()),
                "dlq.worker": self.name,
                "dlq.failed_at": datetime.now(UTC).isoformat(),
            }
        )
        self.producer.send(self.dlq_topic, msg.value() or b"", msg.key(), headers)
        self.dead_lettered += 1
        log.warning(
            "message dead-lettered",
            extra={
                "worker": self.name,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "error": str(exc),
            },
        )

    # --- offsets -------------------------------------------------------------------

    def _maybe_commit(self) -> None:
        due = self._clock() - self._last_commit >= self.commit_interval_s
        if self._uncommitted >= self.commit_every or (due and self._pending):
            self.commit()

    def commit(self, partitions: list[TopicPartition] | None = None) -> None:
        """Flush produced output, then synchronously commit processed offsets."""
        if partitions is None:
            pending = dict(self._pending)
        else:
            wanted = {(p.topic, p.partition) for p in partitions}
            pending = {k: v for k, v in self._pending.items() if k in wanted}
        if not pending:
            self._last_commit = self._clock()
            return
        self.producer.flush()
        offsets = [TopicPartition(t, p, o) for (t, p), o in pending.items()]
        self._consumer.commit(offsets=offsets, asynchronous=False)
        for k in pending:
            self._pending.pop(k, None)
        if partitions is None:
            self._uncommitted = 0
        self._last_commit = self._clock()

    def _on_revoke(self, consumer: Any, partitions: list[TopicPartition]) -> None:
        try:
            self.commit(partitions)
        except Exception:
            log.exception("commit on revoke failed", extra={"worker": self.name})
        revoked = {(p.topic, p.partition) for p in partitions}
        self._attempts = {k: v for k, v in self._attempts.items() if k[:2] not in revoked}

    # --- observability -------------------------------------------------------------

    def _heartbeat(self) -> None:
        if self.heartbeat_path is None:
            return
        now = self._clock()
        if now - self._last_beat >= 5:
            self.heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            self.heartbeat_path.touch()
            self._last_beat = now

    def _on_stats(self, raw: str) -> None:
        for (topic, partition), lag in lag_from_stats(json.loads(raw)).items():
            LAG.labels(self.group_id, topic, str(partition)).set(lag)

    def _on_error(self, err: KafkaError) -> None:
        log.warning("kafka client error", extra={"worker": self.name, "error": str(err)})
