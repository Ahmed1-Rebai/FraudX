"""Kafka producer wrapper.

Idempotent (no duplicates from producer retries), ``acks=all`` (durable),
lz4-compressed, small linger for batching. ``send`` never drops a message on
a full local queue: it serves delivery callbacks until there is room.
Delivery failures are counted and logged; ``flush`` reports what's still pending.
"""

from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, Self

from confluent_kafka import KafkaError, Message, Producer
from prometheus_client import Counter
from pydantic import BaseModel

from fraudx.core.config import KafkaSettings

log = logging.getLogger(__name__)

PRODUCED = Counter(
    "fraudx_kafka_produced_total", "Messages delivered (or failed) by topic", ["topic", "outcome"]
)


class DeliveryError(RuntimeError):
    pass


def producer_config(settings: KafkaSettings, name: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": f"{settings.client_id}-{name}",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 10,
        "delivery.timeout.ms": 120_000,
    }


class EventProducer:
    def __init__(self, settings: KafkaSettings, name: str, producer: Any = None) -> None:
        self._producer = producer or Producer(producer_config(settings, name))
        self.delivered = 0
        self.failed = 0

    def send(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
        headers: dict[str, str | bytes] | None = None,
    ) -> None:
        kwargs: dict[str, Any] = {"value": value, "key": key, "on_delivery": self._on_delivery}
        if headers:
            kwargs["headers"] = list(headers.items())
        while True:
            try:
                self._producer.produce(topic, **kwargs)
                break
            except BufferError:
                # Local queue full: let librdkafka drain it, then retry.
                self._producer.poll(0.5)
        self._producer.poll(0)

    def send_model(self, topic: str, model: BaseModel, key: bytes | None = None) -> None:
        headers: dict[str, str | bytes] = {"content-type": "application/json"}
        version = getattr(model, "schema_version", None)
        if version is not None:
            headers["schema_version"] = str(version)
        self.send(topic, model.model_dump_json(exclude_none=True).encode(), key, headers)

    def flush(self, timeout: float = 30.0) -> None:
        """Block until every queued message is delivered or ``timeout`` expires."""
        remaining = self._producer.flush(timeout)
        if remaining:
            raise DeliveryError(f"{remaining} message(s) still undelivered after {timeout}s")

    def poll(self, timeout: float = 0) -> None:
        self._producer.poll(timeout)

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            PRODUCED.labels(msg.topic(), "failed").inc()
            log.error("delivery failed", extra={"topic": msg.topic(), "error": str(err)})
        else:
            self.delivered += 1
            PRODUCED.labels(msg.topic(), "ok").inc()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.flush()
