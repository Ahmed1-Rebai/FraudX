"""Event contracts shared by every service.

Messages on Kafka are JSON documents validated by these models. Each payload
carries ``schema_version`` so consumers can evolve without breaking replay of
older events. Changes must be backward compatible (add optional fields only);
anything else requires a new version and a migration note in ``memory.md``.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from ipaddress import IPv4Address, IPv6Address
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, PlainSerializer

SCHEMA_VERSION = 1

# Money is a Decimal internally and a JSON number on the wire (not a string),
# so non-Python consumers see the natural type.
Money = Annotated[
    Decimal,
    Field(ge=0, max_digits=18, decimal_places=2),
    PlainSerializer(float, return_type=float, when_used="json"),
]


class TransactionType(StrEnum):
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"
    CASH_OUT = "CASH_OUT"
    CASH_IN = "CASH_IN"
    DEBIT = "DEBIT"


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
    event_time: AwareDatetime = Field(description="When the transaction happened (event time)")
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
    processed_at: AwareDatetime

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
