"""Neo4j writer: entities and aggregated relationships (ADR-004).

Per batch, one write transaction runs three ``UNWIND`` statements:

1. ``(:Customer)-[:OWNS]->(:Account)`` and the money flow
   ``(payer)-[:TRANSFERRED_TO {count, total_amount, first_seen, last_seen, last_tx}]->(payee)``.
2. ``(:Customer)-[:USES {count, first_seen, last_seen, last_tx}]->(:Device)``.
3. ``(:Device)-[:CONNECTED_FROM {first_seen, last_seen}]->(:IP)-[:LOCATED_IN]->(:Location)``.

Idempotency: ``MERGE`` makes nodes and edges idempotent, and ``first_seen`` /
``last_seen`` are min/max. Counters are only incremented when the event comes
after the last counted one in ``(event time, transaction id)`` order (stored
as ``last_seen`` / ``last_tx``). Every counted edge in (1) and (2) includes
the event's own customer/source account, and that account's events arrive in
order on a single partition (keyed by customer). So a redelivered event is
never counted twice, while two different transactions with the same
timestamp both are (IBM AML has minute resolution). Device→IP edges are
written from many customers' partitions, so they carry no counter.
"""

from __future__ import annotations

from typing import Any

from neo4j import Driver, ManagedTransaction

from fraudx.core.schemas import DEBIT_TYPES, TransactionEvent

_TRANSFERS = """
UNWIND $rows AS row
WITH row, datetime({epochMillis: row.ts}) AS ts
MERGE (c:Customer {id: row.customer_id})
MERGE (src:Account {id: row.source_account})
  ON CREATE SET src.kind = row.source_kind
MERGE (c)-[:OWNS]->(src)
MERGE (dst:Account {id: row.destination_account})
  ON CREATE SET dst.kind = row.destination_kind
WITH row, ts,
     CASE WHEN row.outgoing THEN src ELSE dst END AS payer,
     CASE WHEN row.outgoing THEN dst ELSE src END AS payee
MERGE (payer)-[t:TRANSFERRED_TO]->(payee)
  ON CREATE SET t.count = 0, t.total_amount = 0.0, t.first_seen = ts
FOREACH (_ IN CASE WHEN t.last_seen IS NULL OR ts > t.last_seen
                      OR (ts = t.last_seen AND row.tx > coalesce(t.last_tx, ''))
                 THEN [1] ELSE [] END |
  SET t.count = t.count + 1,
      t.total_amount = t.total_amount + row.amount,
      t.last_seen = ts, t.last_tx = row.tx)
SET t.first_seen = CASE WHEN ts < t.first_seen THEN ts ELSE t.first_seen END
"""

_DEVICES = """
UNWIND $rows AS row
WITH row, datetime({epochMillis: row.ts}) AS ts
MERGE (c:Customer {id: row.customer_id})
MERGE (d:Device {id: row.device_id})
MERGE (c)-[u:USES]->(d)
  ON CREATE SET u.count = 0, u.first_seen = ts
FOREACH (_ IN CASE WHEN u.last_seen IS NULL OR ts > u.last_seen
                      OR (ts = u.last_seen AND row.tx > coalesce(u.last_tx, ''))
                 THEN [1] ELSE [] END |
  SET u.count = u.count + 1, u.last_seen = ts, u.last_tx = row.tx)
SET u.first_seen = CASE WHEN ts < u.first_seen THEN ts ELSE u.first_seen END
"""

_NETWORK = """
UNWIND $rows AS row
WITH row, datetime({epochMillis: row.ts}) AS ts
MERGE (d:Device {id: row.device_id})
MERGE (ip:IP {address: row.ip})
MERGE (d)-[r:CONNECTED_FROM]->(ip)
  ON CREATE SET r.first_seen = ts, r.last_seen = ts
SET r.first_seen = CASE WHEN ts < r.first_seen THEN ts ELSE r.first_seen END,
    r.last_seen = CASE WHEN ts > r.last_seen THEN ts ELSE r.last_seen END
WITH row, ip
WHERE row.location_id IS NOT NULL
MERGE (loc:Location {id: row.location_id})
  ON CREATE SET loc.country = row.country, loc.city = row.city
MERGE (ip)-[:LOCATED_IN]->(loc)
"""


def account_kind(account_id: str) -> str:
    """PaySim and the generator both prefix merchant accounts with ``M``."""
    return "MERCHANT" if account_id.startswith("M") else "CUSTOMER"


def to_row(event: TransactionEvent) -> dict[str, Any]:
    location = event.location
    location_id = None
    if location is not None:
        location_id = f"{location.country}|{location.city or ''}"
    return {
        "tx": event.transaction_id,
        "customer_id": event.customer_id,
        "source_account": event.source_account,
        "source_kind": account_kind(event.source_account),
        "destination_account": event.destination_account,
        "destination_kind": account_kind(event.destination_account),
        # CASH_IN moves money from the agent to the customer.
        "outgoing": event.transaction_type in DEBIT_TYPES,
        "amount": float(event.amount),
        "ts": int(event.event_time.timestamp() * 1000),
        "device_id": event.device_id,
        "ip": str(event.ip_address) if event.ip_address else None,
        "location_id": location_id,
        "country": location.country if location else None,
        "city": location.city if location else None,
    }


class GraphWriter:
    def __init__(self, driver: Driver, database: str | None = None) -> None:
        self.driver = driver
        self.database = database

    def write(self, events: list[TransactionEvent]) -> None:
        rows = sorted((to_row(e) for e in events), key=lambda r: (r["ts"], r["tx"]))
        with self.driver.session(database=self.database) as session:
            session.execute_write(self._write, rows)

    @staticmethod
    def _write(tx: ManagedTransaction, rows: list[dict[str, Any]]) -> None:
        tx.run(_TRANSFERS, rows=rows).consume()
        with_device = [r for r in rows if r["device_id"]]
        if with_device:
            tx.run(_DEVICES, rows=with_device).consume()
        with_network = [r for r in with_device if r["ip"]]
        if with_network:
            tx.run(_NETWORK, rows=with_network).consume()
