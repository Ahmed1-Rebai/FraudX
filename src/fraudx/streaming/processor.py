"""Feature processor: transactions.raw -> validate -> online features -> transactions.features."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from confluent_kafka import Message

from fraudx.core.schemas import FeaturedTransaction, TransactionEvent, TransactionFeatures
from fraudx.core.topics import TRANSACTIONS_FEATURES
from fraudx.messaging.consumer import PermanentError, decode
from fraudx.messaging.producer import EventProducer


class FeatureStore(Protocol):
    def compute_many(self, events: list[TransactionEvent]) -> list[TransactionFeatures]: ...


def validate(event: TransactionEvent) -> None:
    """Business rules beyond the schema. Violations are permanent (→ DLQ)."""
    if event.source_account == event.destination_account:
        raise PermanentError("source and destination accounts are identical")


class FeatureProcessor:
    """Batch handler: one Redis round trip per batch, results in message order.

    Messages arrive in partition order and Redis runs pipelined scripts in
    order, so per-customer results equal one-at-a-time processing.
    """

    def __init__(
        self,
        store: FeatureStore,
        producer: EventProducer,
        topic: str = TRANSACTIONS_FEATURES,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.store, self.producer, self.topic, self._now = store, producer, topic, now

    def __call__(self, messages: list[Message]) -> list[tuple[Message, PermanentError]]:
        events: list[TransactionEvent] = []
        rejects: list[tuple[Message, PermanentError]] = []
        for message in messages:
            try:
                event = decode(message, TransactionEvent)
                validate(event)
            except PermanentError as exc:
                rejects.append((message, exc))
                continue
            if event.is_fraud_label is not None:
                # Defence in depth (ADR-019): ground truth never travels downstream.
                event = event.model_copy(update={"is_fraud_label": None})
            events.append(event)
        if events:
            processed_at = self._now()
            for event, features in zip(events, self.store.compute_many(events), strict=True):
                out = FeaturedTransaction(event=event, features=features, processed_at=processed_at)
                self.producer.send_model(self.topic, out, key=out.partition_key)
        return rejects
