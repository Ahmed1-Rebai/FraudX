"""Batch handler that decodes pipeline messages and hands them to a store writer."""

from __future__ import annotations

from typing import Protocol

from confluent_kafka import Message

from fraudx.core.schemas import FeaturedTransaction, ScoredTransaction, TransactionEvent
from fraudx.messaging.consumer import PermanentError, decode


class Writer(Protocol):
    def write(self, events: list[TransactionEvent]) -> None: ...


class ScoredWriter(Protocol):
    def write_scored(self, items: list[ScoredTransaction]) -> None: ...


class SinkHandler:
    """Decode a batch of ``transactions.features``; undecodable messages are
    rejected (DLQ), the rest are written (graph writer)."""

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


class ScoredSinkHandler:
    """Same for ``transactions.scored`` (history and profile writers, with risk)."""

    def __init__(self, writer: ScoredWriter) -> None:
        self.writer = writer

    def __call__(self, messages: list[Message]) -> list[tuple[Message, PermanentError]]:
        items: list[ScoredTransaction] = []
        rejects: list[tuple[Message, PermanentError]] = []
        for message in messages:
            try:
                items.append(decode(message, ScoredTransaction))
            except PermanentError as exc:
                rejects.append((message, exc))
        if items:
            self.writer.write_scored(items)
        return rejects
