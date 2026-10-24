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

from typing import TYPE_CHECKING, Any

from neo4j import Driver
from prometheus_client import Counter

from fraudx.core.schemas import FraudDecisionEvent, InvestigationStatus, TransactionType
from fraudx.messaging.consumer import PermanentError, decode
from fraudx.storage.graph_writer import account_kind

if TYPE_CHECKING:
    from confluent_kafka import Message

    from fraudx.storage.investigations import Investigations

DECISIONS_PROCESSED = Counter(
    "fraudx_decisions_processed_total",
    "Analyst decisions processed by the feedback worker",
    ["status"],
)

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


class FeedbackHandler:
    """Batch consumer for ``fraud.decisions``."""

    def __init__(
        self, feedback: GraphFeedback, investigations: Investigations | None = None
    ) -> None:
        self.feedback = feedback
        self.investigations = investigations

    def __call__(self, messages: list[Message]) -> list[tuple[Message, PermanentError]]:
        rejects: list[tuple[Message, PermanentError]] = []
        for message in messages:
            try:
                event = decode(message, FraudDecisionEvent)
            except PermanentError as exc:
                rejects.append((message, exc))
                continue

            dest = event.destination_account
            src = event.source_account
            tx_type = event.transaction_type or TransactionType.TRANSFER.value

            if not dest and self.investigations is not None:
                doc = self.investigations.get(event.transaction_id)
                if doc and "evidence" in doc:
                    dest = doc["evidence"].get("destination_account")
                    src = doc["evidence"].get("source_account")

            if dest:
                tx_data = {
                    "transaction_id": event.transaction_id,
                    "transaction_type": tx_type,
                    "source_account": src or "",
                    "destination_account": dest,
                }
                confirmed = event.status == InvestigationStatus.CONFIRMED_FRAUD.value
                fb = self.feedback.record(tx_data, confirmed=confirmed)
                if self.investigations is not None and fb is not None:
                    self.investigations.set_feedback(event.transaction_id, fb)

            DECISIONS_PROCESSED.labels(status=event.status).inc()
        return rejects
