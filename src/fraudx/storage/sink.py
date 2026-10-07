"""Batch handler that decodes pipeline messages and hands them to a store writer."""

from __future__ import annotations

from typing import Protocol

from confluent_kafka import Message

from fraudx.core.schemas import FeaturedTransaction, TransactionEvent
from fraudx.messaging.consumer import PermanentError, decode


class Writer(Protocol):
    def write(self, events: list[TransactionEvent]) -> None: ...


class SinkHandler:
    """Decode a batch; undecodable messages are rejected (DLQ), the rest are written.

    Input is ``transactions.features`` for now; Phase 8 switches the history
    and profile writers to ``transactions.scored`` to add risk columns.
    """

    def __init__(self, writer: Writer) -> None:
        self.writer = writer

    def __call__(self, messages: list[Message]) -> list[tuple[Message, PermanentError]]:
        events: list[TransactionEvent] = []
        rejects: list[tuple[Message, PermanentError]] = []
        for message in messages:
            try:
                events.append(decode(message, FeaturedTransaction).event)
            except PermanentError as exc:
                rejects.append((message, exc))
        if events:
            self.writer.write(events)
        return rejects
