"""Graph features of a transaction, read from Neo4j (the risk engine's ``graph`` input).

Defined in money-flow terms (payer → payee, as stored by ``graph_writer``)
and **as of the transaction's event time**: only edges first seen before it
count. The transaction never sees itself, whether or not the graph writer
has already stored it, and an offline replay gives exactly the live values.

| feature | meaning | pattern it exposes |
|---|---|---|
| ``payee_payer_count`` | other accounts that paid the payee | mule / fan-in |
| ``payee_payee_count`` | accounts the payee paid (except the payer) | layering, pass-through |
| ``payer_payee_count`` | other accounts the payer paid | fan-out |
| ``is_new_payee`` | the payer never paid this payee before | first contact |
| ``customer_ring_size`` | other customers sharing a device with this customer | device rings |
| ``payee_owner_ring_size`` | customers sharing a device with the payee's owner | ring mule |

One ``UNWIND`` query per batch: every pattern is anchored on the indexed
ids of the transaction's own customer and accounts.
"""

from __future__ import annotations

from typing import Any

from neo4j import Driver

from fraudx.core.schemas import TransactionEvent
from fraudx.storage.graph_writer import to_row

GRAPH_FEATURE_VERSION = 1
GRAPH_FEATURE_NAMES: tuple[str, ...] = (
    "payee_payer_count",
    "payee_payee_count",
    "payer_payee_count",
    "is_new_payee",
    "customer_ring_size",
    "payee_owner_ring_size",
)

_QUERY = """
UNWIND $rows AS row
WITH row, datetime({epochMillis: row.ts}) AS ts
RETURN row.i AS i,
  COUNT { MATCH (p:Account)-[t:TRANSFERRED_TO]->(:Account {id: row.payee})
          WHERE t.first_seen < ts AND p.id <> row.payer } AS payee_payer_count,
  COUNT { MATCH (:Account {id: row.payee})-[t:TRANSFERRED_TO]->(q:Account)
          WHERE t.first_seen < ts AND q.id <> row.payer } AS payee_payee_count,
  COUNT { MATCH (:Account {id: row.payer})-[t:TRANSFERRED_TO]->(q:Account)
          WHERE t.first_seen < ts AND q.id <> row.payee } AS payer_payee_count,
  NOT EXISTS { MATCH (:Account {id: row.payer})-[t:TRANSFERRED_TO]->(:Account {id: row.payee})
               WHERE t.first_seen < ts } AS is_new_payee,
  COUNT { MATCH (:Customer {id: row.customer_id})-[u:USES]->(:Device)<-[v:USES]-(o:Customer)
          WHERE u.first_seen < ts AND v.first_seen < ts AND o.id <> row.customer_id
          RETURN DISTINCT o } AS customer_ring_size,
  COUNT { MATCH (owner:Customer)-[:OWNS]->(:Account {id: row.payee}),
                (owner)-[u:USES]->(:Device)<-[v:USES]-(o:Customer)
          WHERE owner.id <> row.customer_id AND u.first_seen < ts AND v.first_seen < ts
            AND o.id <> owner.id
          RETURN DISTINCT o } AS payee_owner_ring_size
"""


def _flow(event: TransactionEvent) -> dict[str, Any]:
    row = to_row(event)
    payer, payee = row["source_account"], row["destination_account"]
    if not row["outgoing"]:  # CASH_IN: the agent pays the customer
        payer, payee = payee, payer
    return {"customer_id": event.customer_id, "payer": payer, "payee": payee, "ts": row["ts"]}


def graph_features(
    driver: Driver, events: list[TransactionEvent], database: str | None = None
) -> list[list[float]]:
    """Feature vectors (order of ``GRAPH_FEATURE_NAMES``) for a batch, in event order."""
    if not events:
        return []
    rows = [{**_flow(e), "i": i} for i, e in enumerate(events)]
    records, _, _ = driver.execute_query(_QUERY, rows=rows, database_=database, routing_="r")
    out: list[list[float]] = [[] for _ in events]
    for record in records:
        out[record["i"]] = [float(record[name]) for name in GRAPH_FEATURE_NAMES]
    return out


# --- Known fraud (analyst feedback, fraudx.storage.feedback) -----------------------
#
# Not model features: confirmations exist only after analysts work, so no
# training set holds them. They feed the deterministic rules and the
# explanation instead (spec §11.3: distance_to_known_fraud, fraud_neighbor_count).

_ANY_CONFIRMED = "MATCH (f:Account {fraud_flag: true}) RETURN count(f) > 0 AS any"

_KNOWN_FRAUD = """
UNWIND $rows AS row
OPTIONAL MATCH (payee:Account {id: row.payee})
OPTIONAL MATCH (payer:Account {id: row.payer})
WITH row, payee, payer,
     payee IS NOT NULL AND coalesce(payee.kind, '') <> 'MERCHANT' AS customer_payee
RETURN row.i AS i,
  coalesce(payer.fraud_flag, false) AS payer_confirmed_fraud,
  CASE
    WHEN NOT customer_payee THEN null
    WHEN coalesce(payee.fraud_flag, false) THEN 0
    WHEN EXISTS { MATCH (payee)-[:TRANSFERRED_TO]-(:Account {fraud_flag: true}) } THEN 1
    WHEN EXISTS { MATCH (payee)-[:TRANSFERRED_TO]-(m:Account)
                        -[:TRANSFERRED_TO]-(:Account {fraud_flag: true})
                  WHERE coalesce(m.kind, '') <> 'MERCHANT' } THEN 2
  END AS distance_to_known_fraud,
  CASE WHEN customer_payee
       THEN COUNT { MATCH (payee)-[:TRANSFERRED_TO]-(n:Account {fraud_flag: true})
                    RETURN DISTINCT n }
       ELSE 0 END AS fraud_neighbor_count
"""


def known_fraud(
    driver: Driver, events: list[TransactionEvent], database: str | None = None
) -> list[dict[str, float]]:
    """Proximity of each transaction to accounts confirmed as fraud, in event order.

    Per event, only meaningful values are present: ``distance_to_known_fraud``
    (0 = the payee itself was confirmed, 1 or 2 hops through customer
    accounts), ``fraud_neighbor_count`` (confirmed accounts the payee traded
    with) and ``payer_confirmed_fraud`` (the paying account was confirmed: a
    mule moving money on). Merchant payees are skipped (hubs). While no
    account has been confirmed, one indexed lookup answers for the batch.
    """
    if not events:
        return []
    records, _, _ = driver.execute_query(_ANY_CONFIRMED, database_=database, routing_="r")
    if not records[0]["any"]:
        return [{} for _ in events]
    rows = [{**_flow(e), "i": i} for i, e in enumerate(events)]
    records, _, _ = driver.execute_query(_KNOWN_FRAUD, rows=rows, database_=database, routing_="r")
    out: list[dict[str, float]] = [{} for _ in events]
    for record in records:
        values: dict[str, float] = {}
        if record["distance_to_known_fraud"] is not None:
            values["distance_to_known_fraud"] = float(record["distance_to_known_fraud"])
        if record["fraud_neighbor_count"]:
            values["fraud_neighbor_count"] = float(record["fraud_neighbor_count"])
        if record["payer_confirmed_fraud"]:
            values["payer_confirmed_fraud"] = 1.0
        out[record["i"]] = values
    return out
