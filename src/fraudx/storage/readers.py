"""Read side of the stores: the queries the API serves.

Each read is shaped by its store's data model, so its cost doesn't grow with
the data volume:

* Cassandra: single-partition reads (``transactions_by_id``, one
  customer-day of ``transactions_by_customer``; the alert feed reads the 8
  bucket partitions of one day concurrently and merges them), ADR-005.
* Redis: the customer's latest risk (``fx:cust:{C}:risk``, set by the scorer).
* MongoDB: a primary-key lookup of the customer profile, ADR-006.
* Neo4j: patterns anchored on one customer node (devices it shares, accounts
  it exchanges money with), ADR-004.

Values are converted to plain Python types (aware UTC datetimes, ``date``).
"""

from __future__ import annotations

from datetime import UTC, date
from typing import Any

from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from neo4j import Driver, ManagedTransaction
from pymongo.database import Database
from redis import Redis

from fraudx.storage.history_writer import BUCKETS


def _transaction(row: Any) -> dict[str, Any]:
    data = row._asdict()
    if "event_date" in data:
        data["event_date"] = data["event_date"].date()  # cassandra.util.Date
    data["event_time"] = data["event_time"].replace(tzinfo=UTC)  # driver returns naive UTC
    return data


class HistoryReader:
    """Cassandra transaction history."""

    def __init__(self, session: Session, keyspace: str) -> None:
        self.session = session
        self._by_id = session.prepare(
            f"SELECT * FROM {keyspace}.transactions_by_id WHERE transaction_id = ?"  # noqa: S608
        )
        self._by_customer = session.prepare(
            f"SELECT * FROM {keyspace}.transactions_by_customer "  # noqa: S608
            "WHERE customer_id = ? AND event_date = ? LIMIT ?"
        )
        self._risk_events = session.prepare(
            f"SELECT * FROM {keyspace}.risk_events_by_day "  # noqa: S608
            "WHERE event_date = ? AND bucket = ? LIMIT ?"
        )

    def transaction(self, transaction_id: str) -> dict[str, Any] | None:
        row = self.session.execute(self._by_id, (transaction_id,)).one()
        return _transaction(row) if row else None

    def customer_day(self, customer_id: str, day: date, limit: int) -> list[dict[str, Any]]:
        """A customer's transactions on one day, newest first (one partition)."""
        rows = self.session.execute(self._by_customer, (customer_id, day, limit))
        return [_transaction(row) for row in rows]

    def risk_events(self, day: date, limit: int) -> list[dict[str, Any]]:
        """MEDIUM/HIGH transactions of one day, newest first (8 bucket partitions)."""
        results = execute_concurrent_with_args(
            self.session, self._risk_events, [(day, b, limit) for b in range(BUCKETS)],
            raise_on_first_error=True,
        )  # fmt: skip
        rows = [_transaction(row) for result in results for row in result.result_or_exc]
        rows.sort(key=lambda r: (r["event_time"], r["transaction_id"]), reverse=True)
        return rows[:limit]


class ProfileReader:
    """MongoDB customer profiles."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def customer(self, customer_id: str) -> dict[str, Any] | None:
        doc = self.db.customers.find_one({"_id": customer_id})
        if doc is None:
            return None
        doc["customer_id"] = doc.pop("_id")
        return doc


class RiskReader:
    """Redis: the latest risk of a customer, written by the scorer."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    def latest(self, customer_id: str) -> dict[str, Any] | None:
        data = self.redis.hgetall(f"fx:cust:{{{customer_id}}}:risk")
        return {**data, "score": int(data["score"])} if data else None


_CUSTOMER_EXISTS = "MATCH (c:Customer {id: $id}) RETURN count(c) > 0 AS found"

# Devices the customer uses, with the other customers seen on each one.
_DEVICES = """
MATCH (c:Customer {id: $id})-[u:USES]->(d:Device)
OPTIONAL MATCH (d)<-[:USES]-(other:Customer)
WHERE other <> c
WITH d, u, collect(other.id) AS others
RETURN d.id AS device_id, u.count AS uses, u.first_seen AS first_seen, u.last_seen AS last_seen,
       size(others) AS shared_with_count, others[..$limit] AS shared_with
ORDER BY shared_with_count DESC, uses DESC
LIMIT $limit
"""

# Money flows between the customer's accounts and other accounts, both ways.
_COUNTERPARTIES = """
MATCH (:Customer {id: $id})-[:OWNS]->(a:Account)-[t:TRANSFERRED_TO]-(p:Account)
RETURN a.id AS account, p.id AS counterparty, p.kind AS kind,
       CASE WHEN startNode(t) = a THEN 'out' ELSE 'in' END AS direction,
       t.count AS count, round(t.total_amount, 2) AS total_amount, t.last_seen AS last_seen
ORDER BY total_amount DESC
LIMIT $limit
"""


def _native(record: dict[str, Any]) -> dict[str, Any]:
    """neo4j.time.DateTime -> datetime."""
    return {k: v.to_native() if hasattr(v, "to_native") else v for k, v in record.items()}


class GraphReader:
    """Neo4j relationship graph around one customer."""

    def __init__(self, driver: Driver, database: str | None = None) -> None:
        self.driver = driver
        self.database = database

    def customer_network(self, customer_id: str, limit: int) -> dict[str, Any] | None:
        with self.driver.session(database=self.database) as session:
            return session.execute_read(self._network, customer_id, limit)

    @staticmethod
    def _network(tx: ManagedTransaction, customer_id: str, limit: int) -> dict[str, Any] | None:
        if not tx.run(_CUSTOMER_EXISTS, id=customer_id).single()["found"]:
            return None
        params = {"id": customer_id, "limit": limit}
        return {
            "customer_id": customer_id,
            "devices": [_native(r.data()) for r in tx.run(_DEVICES, params)],
            "counterparties": [_native(r.data()) for r in tx.run(_COUNTERPARTIES, params)],
        }
