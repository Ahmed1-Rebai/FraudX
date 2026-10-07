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
    def compute(self, event: TransactionEvent) -> TransactionFeatures: ...


def validate(event: TransactionEvent) -> None:
    """Business rules beyond the schema. Violations are permanent (→ DLQ)."""
    if event.source_account == event.destination_account:
        raise PermanentError("source and destination accounts are identical")


class FeatureProcessor:
    def __init__(
        self,
        store: FeatureStore,
        producer: EventProducer,
        topic: str = TRANSACTIONS_FEATURES,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.store, self.producer, self.topic, self._now = store, producer, topic, now

    def __call__(self, message: Message) -> None:
        event = decode(message, TransactionEvent)
        validate(event)
        if event.is_fraud_label is not None:
            # Defence in depth (ADR-019): ground truth never travels downstream.
            event = event.model_copy(update={"is_fraud_label": None})
        features = self.store.compute(event)
        out = FeaturedTransaction(event=event, features=features, processed_at=self._now())
        self.producer.send_model(self.topic, out, key=out.partition_key)
