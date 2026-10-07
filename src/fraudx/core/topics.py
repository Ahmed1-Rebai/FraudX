"""Kafka topic names - the single source of truth for Python code.

``infra/kafka/create-topics.sh`` creates the same topics; the integration
suite checks the two stay in sync.
"""

from typing import Final

TRANSACTIONS_RAW: Final = "transactions.raw"
TRANSACTIONS_FEATURES: Final = "transactions.features"
TRANSACTIONS_SCORED: Final = "transactions.scored"
TRANSACTIONS_DLQ: Final = "transactions.dlq"
FRAUD_ALERTS: Final = "fraud.alerts"

ALL_TOPICS: Final = (
    TRANSACTIONS_RAW,
    TRANSACTIONS_FEATURES,
    TRANSACTIONS_SCORED,
    TRANSACTIONS_DLQ,
    FRAUD_ALERTS,
)
