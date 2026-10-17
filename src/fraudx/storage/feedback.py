"""Analyst feedback in the graph: accounts confirmed as fraud recipients.

When an analyst confirms a transaction as fraud, its payee account (the one
the money went to) is marked in Neo4j. The confirming transaction ids are
kept on the account (``fraud_sources``), and ``fraud_flag`` is true while at
least one remains, so a reversed decision (reopened, false positive) removes
exactly its own evidence even when several cases confirmed the same account.
Merchant accounts are never marked: they receive from everyone by design.

The scorer reads these marks (``fraudx.storage.graph_features.known_fraud``):
this is how ``distance_to_known_fraud`` and ``fraud_neighbor_count`` (spec
§11.3) get their meaning.
"""

from __future__ import annotations

from typing import Any

from neo4j import Driver

from fraudx.core.schemas import TransactionType
from fraudx.storage.graph_writer import account_kind

_RECORD = """
MERGE (a:Account {id: $account})
WITH a, [x IN coalesce(a.fraud_sources, []) WHERE x <> $tx]
        + CASE WHEN $confirmed THEN [$tx] ELSE [] END AS sources
SET a.fraud_sources = sources, a.fraud_flag = size(sources) > 0
RETURN a.fraud_flag AS flagged, size(sources) AS sources
"""


def payee_account(transaction: dict[str, Any]) -> str:
    """The account the money went to (a CASH_IN pays the customer's own account)."""
    if transaction["transaction_type"] == TransactionType.CASH_IN.value:
        return transaction["source_account"]
    return transaction["destination_account"]


class GraphFeedback:
    def __init__(self, driver: Driver, database: str | None = None) -> None:
        self.driver, self.database = driver, database

    def record(self, transaction: dict[str, Any], confirmed: bool) -> dict[str, Any] | None:
        """Mark (or unmark) the payee of ``transaction``; None for a merchant payee."""
        account = payee_account(transaction)
        if account_kind(account) == "MERCHANT":
            return None
        records, _, _ = self.driver.execute_query(
            _RECORD, account=account, tx=transaction["transaction_id"], confirmed=confirmed,
            database_=self.database,
        )  # fmt: skip
        row = records[0]
        return {"account": account, "flagged": row["flagged"], "confirmations": row["sources"]}
