"""Cassandra writer: append-only transaction history (ADR-005).

Each transaction is upserted into ``transactions_by_customer`` and
``transactions_by_id`` with concurrent async writes. Upserts are idempotent,
so redelivery is harmless.

Columns without a value are bound as ``UNSET_VALUE``, never ``None``: a null
write creates a tombstone, and it would also erase risk columns written by a
later (or earlier) stage. When scored transactions arrive (Phase 8), the same
rows are enriched with risk columns and ``risk_events_by_day`` is filled.
"""

from __future__ import annotations

from typing import Any

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import UNSET_VALUE, PreparedStatement

from fraudx.core.schemas import RiskAssessment, TransactionEvent

_COLUMNS = (
    "customer_id", "event_date", "event_time", "transaction_id", "source_account",
    "destination_account", "transaction_type", "amount", "currency", "device_id",
    "ip_address", "country", "city", "risk_score", "risk_level", "ml_score",
    "anomaly_score", "graph_score",
)  # fmt: skip


def _insert(table: str, columns: tuple[str, ...]) -> str:
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) "  # noqa: S608 - fixed identifiers
        f"VALUES ({', '.join('?' for _ in columns)})"
    )


def to_values(event: TransactionEvent, risk: RiskAssessment | None = None) -> dict[str, Any]:
    location = event.location
    values: dict[str, Any] = {
        "customer_id": event.customer_id,
        "event_date": event.event_time.date(),
        "event_time": event.event_time,
        "transaction_id": event.transaction_id,
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
    }
    return {k: UNSET_VALUE if v is None else v for k, v in values.items()}


class HistoryWriter:
    def __init__(self, session: Session, keyspace: str, concurrency: int = 64) -> None:
        self.session = session
        self.concurrency = concurrency
        self._by_customer = self._prepare(_insert(f"{keyspace}.transactions_by_customer", _COLUMNS))
        self._by_id = self._prepare(_insert(f"{keyspace}.transactions_by_id", _COLUMNS))

    def _prepare(self, cql: str) -> PreparedStatement:
        statement = self.session.prepare(cql)
        statement.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        statement.is_idempotent = True  # safe for the driver to retry
        return statement

    def write(self, events: list[TransactionEvent]) -> None:
        params = [tuple(to_values(e).values()) for e in events]
        for statement in (self._by_customer, self._by_id):
            execute_concurrent_with_args(
                self.session, statement, params, concurrency=self.concurrency,
                raise_on_first_error=True,
            )  # fmt: skip
