"""Event contracts shared by every service.

Messages on Kafka are JSON documents validated by these models. Each payload
carries ``schema_version`` so consumers can evolve without breaking replay of
older events. Changes must be backward compatible (add optional fields only);
anything else requires a new version and a migration note in ``memory.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from ipaddress import IPv4Address, IPv6Address
from typing import Annotated

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, PlainSerializer

SCHEMA_VERSION = 1

# Money is a Decimal normalised to 2 decimal places (100 -> 100.00, so every
# store holds the same value) and a JSON number on the wire (not a string),
# so non-Python consumers see the natural type.
Money = Annotated[
    Decimal,
    Field(ge=0, max_digits=18, decimal_places=2),
    AfterValidator(lambda v: v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
    PlainSerializer(float, return_type=float, when_used="json"),
]
# Timestamps are normalised to UTC on validation, so every consumer (window
# features, hour of day, Cassandra day partitions) sees the same instant
# whatever offset the producer used.
UtcDatetime = Annotated[AwareDatetime, AfterValidator(lambda v: v.astimezone(UTC))]
# An amount in its original currency, without rounding (e.g. 0.000084 BTC).
ExactAmount = Annotated[
    Decimal,
    Field(ge=0, max_digits=30),
    PlainSerializer(float, return_type=float, when_used="json"),
]


class TransactionType(StrEnum):
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"
    CASH_OUT = "CASH_OUT"
    CASH_IN = "CASH_IN"
    DEBIT = "DEBIT"


class PaymentMethod(StrEnum):
    """Payment rail / instrument, when the source provides it (e.g. IBM AML)."""

    ACH = "ACH"
    WIRE = "WIRE"
    CHEQUE = "CHEQUE"
    CREDIT_CARD = "CREDIT_CARD"
    CASH = "CASH"
    BITCOIN = "BITCOIN"
    REINVESTMENT = "REINVESTMENT"


# Types that move money out of the source account.
DEBIT_TYPES = frozenset(
    {
        TransactionType.PAYMENT,
        TransactionType.TRANSFER,
        TransactionType.CASH_OUT,
        TransactionType.DEBIT,
    }
)


class RiskLevel(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class _Contract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)


class Location(_Contract):
    country: str = Field(pattern=r"^[A-Z]{2}$", description="ISO 3166-1 alpha-2")
    city: str | None = None


class Balances(_Contract):
    """Account balances before/after the transaction (available in PaySim)."""

    source_before: Money
    source_after: Money
    destination_before: Money
    destination_after: Money


class TransactionEvent(_Contract):
    """A financial transaction as it enters the platform (``transactions.raw``)."""

    schema_version: int = SCHEMA_VERSION
    transaction_id: str = Field(min_length=1, max_length=64)
    event_time: UtcDatetime = Field(description="When the transaction happened (event time)")
    customer_id: str = Field(min_length=1, max_length=64)
    source_account: str = Field(min_length=1, max_length=64)
    destination_account: str = Field(min_length=1, max_length=64)
    amount: Money
    currency: str = Field(default="TND", pattern=r"^[A-Z]{3}$")
    transaction_type: TransactionType
    device_id: str | None = Field(default=None, max_length=64)
    ip_address: IPv4Address | IPv6Address | None = None
    location: Location | None = None
    session_id: str | None = Field(default=None, max_length=64)
    balances: Balances | None = None
    # Additive in v1 (all optional): payment rail, and the amount as paid when
    # ``amount``/``currency`` were normalised to the platform's base currency.
    payment_method: PaymentMethod | None = None
    original_amount: ExactAmount | None = None
    original_currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    # Ground truth from the dataset. Used for offline training and evaluation
    # only - online scoring must never read it.
    is_fraud_label: bool | None = None

    @property
    def partition_key(self) -> bytes:
        """Kafka key: per-customer ordering keeps velocity windows consistent."""
        return self.customer_id.encode()


FEATURE_VERSION = 1


class TransactionFeatures(_Contract):
    """Online features computed as of the transaction's event time.

    Window counts/sums include the current transaction. ``cust_*`` profile
    values (history count, mean, std, known devices...) describe the customer
    *before* this transaction. Bump ``FEATURE_VERSION`` when a definition changes.
    """

    cust_txn_count_5m: int = Field(ge=1)
    cust_txn_count_1h: int = Field(ge=1)
    cust_txn_count_24h: int = Field(ge=1)
    cust_amount_sum_5m: float = Field(ge=0)
    cust_amount_sum_1h: float = Field(ge=0)
    cust_amount_sum_24h: float = Field(ge=0)
    cust_history_count: int = Field(ge=0)
    cust_amount_mean: float | None = None
    cust_amount_std: float | None = None
    amount_zscore: float | None = None
    amount_to_mean_ratio: float | None = None
    seconds_since_last_txn: float | None = None
    is_new_device: bool
    is_new_ip: bool
    is_new_country: bool
    device_txn_count_1h: int = Field(default=0, ge=0)
    device_customer_count: int = Field(default=0, ge=0)
    ip_txn_count_1h: int = Field(default=0, ge=0)
    ip_customer_count: int = Field(default=0, ge=0)
    balance_drain_ratio: float | None = Field(default=None, ge=0)
    hour_of_day: int = Field(ge=0, le=23)


class FeaturedTransaction(_Contract):
    """A validated transaction with its online features (``transactions.features``)."""

    schema_version: int = SCHEMA_VERSION
    feature_version: int = FEATURE_VERSION
    event: TransactionEvent
    features: TransactionFeatures
    processed_at: UtcDatetime

    @property
    def partition_key(self) -> bytes:
        return self.event.partition_key


class RiskAssessment(_Contract):
    """Output of the risk engine for one transaction."""

    score: int = Field(ge=0, le=100)
    level: RiskLevel
    components: dict[str, float] = Field(
        description="Signals that contributed, each in [0, 1]",
    )
    missing_signals: tuple[str, ...] = ()
    rule_hits: tuple[str, ...] = ()
    assessed_at: datetime


class ScoredTransaction(_Contract):
    """A transaction with its risk assessment (``transactions.scored``, ``fraud.alerts``).

    Carries the inputs of the decision (features, graph features, model
    versions) so that stores, alerts and explanations need no second lookup.
    """

    schema_version: int = SCHEMA_VERSION
    event: TransactionEvent
    features: TransactionFeatures
    graph_features: dict[str, float] | None = None
    risk: RiskAssessment
    models: dict[str, str] = Field(default_factory=dict, description="signal -> name:version")
    scored_at: UtcDatetime

    @property
    def partition_key(self) -> bytes:
        return self.event.partition_key


class InvestigationStatus(StrEnum):
    OPEN = "OPEN"
    CONFIRMED_FRAUD = "CONFIRMED_FRAUD"
    FALSE_POSITIVE = "FALSE_POSITIVE"
    DISMISSED = "DISMISSED"


class FraudDecisionEvent(_Contract):
    """An analyst's decision on an investigation (``fraud.decisions``).

    Published when an analyst updates an investigation status, decoupling
    the synchronous REST write from graph feedback, retrain pipelines, and metrics.
    """

    schema_version: int = SCHEMA_VERSION
    transaction_id: str
    status: InvestigationStatus
    customer_id: str | None = None
    source_account: str | None = None
    destination_account: str | None = None
    transaction_type: str | None = None
    amount: float | None = None
    currency: str | None = None
    note: str | None = None
    decided_at: UtcDatetime

    @property
    def partition_key(self) -> bytes:
        return (self.customer_id or self.transaction_id).encode("utf-8")

