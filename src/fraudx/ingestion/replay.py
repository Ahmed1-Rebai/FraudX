"""Replays a dataset into ``transactions.raw``.

Events keep their dataset order (event time). By default the ground-truth
label is stripped: a real transaction stream carries no fraud label, and a
label on the wire would be an easy leak into online features.
``rebase_time`` shifts all event times by one constant so the first event is
"now"; intervals between events (and so velocity patterns) are preserved.
``id_prefix`` makes a re-send of a dataset a new set of transactions (same
customers): without it the pipeline rightly treats them as redeliveries.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from fraudx.core.schemas import TransactionEvent
from fraudx.core.topics import TRANSACTIONS_RAW
from fraudx.messaging.producer import EventProducer

log = logging.getLogger(__name__)


class Pacer:
    """Spaces calls evenly at ``rate`` per second (0 = unthrottled)."""

    def __init__(
        self,
        rate: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.interval = 1.0 / rate if rate > 0 else 0.0
        self._clock, self._sleep = clock, sleep
        self._next: float | None = None

    def wait(self) -> None:
        if not self.interval:
            return
        now = self._clock()
        if self._next is None:
            self._next = now
        ahead = self._next - now
        if ahead > 0.002:
            self._sleep(ahead)
        # After a stall, restart the schedule from now instead of bursting to catch up.
        self._next = max(self._next, now) + self.interval


@dataclass(slots=True)
class ReplayReport:
    sent: int = 0
    seconds: float = 0.0

    @property
    def rate(self) -> float:
        return self.sent / self.seconds if self.seconds else 0.0


def prepare(
    events: Iterable[TransactionEvent],
    with_labels: bool = False,
    rebase_time: bool = False,
    id_prefix: str = "",
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Iterable[TransactionEvent]:
    offset: timedelta | None = None
    for event in events:
        update: dict[str, object] = {}
        if not with_labels:
            update["is_fraud_label"] = None
        if rebase_time:
            if offset is None:
                offset = now() - event.event_time
            update["event_time"] = event.event_time + offset
        if id_prefix:
            update["transaction_id"] = id_prefix + event.transaction_id
        yield event.model_copy(update=update) if update else event


def replay(
    events: Iterable[TransactionEvent],
    producer: EventProducer,
    rate: float = 0.0,
    limit: int | None = None,
    topic: str = TRANSACTIONS_RAW,
    pacer: Pacer | None = None,
    progress_every: int = 10_000,
) -> ReplayReport:
    pacer = pacer or Pacer(rate)
    report = ReplayReport()
    started = time.perf_counter()
    for event in events:
        if limit is not None and report.sent >= limit:
            break
        pacer.wait()
        producer.send_model(topic, event, key=event.partition_key)
        report.sent += 1
        if report.sent % progress_every == 0:
            log.info("replay progress: %d events", report.sent)
    producer.flush()
    report.seconds = time.perf_counter() - started
    return report
