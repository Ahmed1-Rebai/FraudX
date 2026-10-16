"""Cassandra writer: append-only transaction history (ADR-005).

Scored transactions (``transactions.scored``) are upserted into
``transactions_by_customer`` and ``transactions_by_id`` with their risk
columns; those at MEDIUM or HIGH also go to the alert feed
``risk_events_by_day`` (bucket = crc32(transaction id) % 8). Concurrent async
upserts are idempotent, so redelivery is harmless.

Columns without a value are bound as ``UNSET_VALUE``, never ``None``: a null
write creates a tombstone, and it would also erase a column written earlier.
``rule_hits`` is a frozen list for the same reason (a non-frozen collection
write first deletes the old value), and an empty list is left unset.
"""

from __future__ import annotations

import zlib
from typing import Any

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import UNSET_VALUE, PreparedStatement

from fraudx.core.schemas import RiskAssessment, RiskLevel, ScoredTransaction, TransactionEvent

BUCKETS = 8

_HISTORY = (
    "customer_id", "event_date", "event_time", "transaction_id", "source_account",
    "destination_account", "transaction_type", "amount", "currency", "device_id",
    "ip_address", "country", "city", "risk_score", "risk_level", "ml_score",
    "anomaly_score", "graph_score",
)  # fmt: skip
_BY_ID = (*_HISTORY, "rule_hits")
_RISK_EVENTS = (
    "event_date", "bucket", "event_time", "transaction_id", "customer_id", "amount",
    "transaction_type", "risk_score", "risk_level", "rule_hits",
)  # fmt: skip


def _insert(table: str, columns: tuple[str, ...]) -> str:
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) "  # noqa: S608 - fixed identifiers
        f"VALUES ({', '.join('?' for _ in columns)})"
    )


def bucket(transaction_id: str) -> int:
    """Stable across processes (unlike ``hash()``), so readers can find the row."""
    return zlib.crc32(transaction_id.encode()) % BUCKETS


def to_values(event: TransactionEvent, risk: RiskAssessment | None = None) -> dict[str, Any]:
    location = event.location
    values: dict[str, Any] = {
        "customer_id": event.customer_id,
        "event_date": event.event_time.date(),
        "event_time": event.event_time,
        "transaction_id": event.transaction_id,
        "bucket": bucket(event.transaction_id),
        "source_account": event.source_account,
        "destination_account": event.destination_account,
        "transaction_type": event.transaction_type.value,
        "amount": event.amount,
        "currency": event.currency,
        "device_id": event.device_id,
        "ip_address": str(event.ip_address) if event.ip_address else None,
        "country": location.country if location else None,
        "city": location.city if location else None,
        "risk_score": risk.score if risk else None,
        "risk_level": risk.level.value if risk else None,
        "ml_score": risk.components.get("ml") if risk else None,
        "anomaly_score": risk.components.get("anomaly") if risk else None,
        "graph_score": risk.components.get("graph") if risk else None,
        "rule_hits": list(risk.rule_hits) if risk and risk.rule_hits else None,
    }
    return {k: UNSET_VALUE if v is None else v for k, v in values.items()}


class HistoryWriter:
    def __init__(self, session: Session, keyspace: str, concurrency: int = 64) -> None:
        self.session = session
        self.concurrency = concurrency
        self._by_customer = self._prepare(f"{keyspace}.transactions_by_customer", _HISTORY)
        self._by_id = self._prepare(f"{keyspace}.transactions_by_id", _BY_ID)
        self._risk_events = self._prepare(f"{keyspace}.risk_events_by_day", _RISK_EVENTS)

    def _prepare(self, table: str, columns: tuple[str, ...]) -> tuple[PreparedStatement, tuple]:
        statement = self.session.prepare(_insert(table, columns))
        statement.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        statement.is_idempotent = True  # safe for the driver to retry
        return statement, columns

    def write(self, events: list[TransactionEvent]) -> None:
        """History rows without risk (risk columns stay unset)."""
        self._execute([to_values(e) for e in events], (self._by_customer, self._by_id))

    def write_scored(self, items: list[ScoredTransaction]) -> None:
        rows = [to_values(i.event, i.risk) for i in items]
        self._execute(rows, (self._by_customer, self._by_id))
        risky = [row for row, i in zip(rows, items, strict=True) if i.risk.level != RiskLevel.LOW]
        if risky:
            self._execute(risky, (self._risk_events,))

    def _execute(
        self, rows: list[dict[str, Any]], statements: tuple[tuple[PreparedStatement, tuple], ...]
    ) -> None:
        for statement, columns in statements:
            params = [tuple(row[c] for c in columns) for row in rows]
            execute_concurrent_with_args(
                self.session, statement, params, concurrency=self.concurrency,
                raise_on_first_error=True,
            )  # fmt: skip
