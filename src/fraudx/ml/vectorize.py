"""Model input: the single definition shared by training and online inference.

``vectorize`` turns a transaction and its online features into a fixed-order
float vector (``None`` → NaN, which LightGBM treats as missing). Training
and the scorer both call it, so there is no second implementation that
could drift.
"""

from __future__ import annotations

import math

from fraudx.core.schemas import TransactionEvent, TransactionFeatures, TransactionType

# Stable integer codes for the categorical transaction type.
TYPE_CODES: dict[TransactionType, int] = {t: i for i, t in enumerate(TransactionType)}

FEATURE_NAMES: tuple[str, ...] = (
    "amount_log",
    "transaction_type",
    "hour_of_day",
    "cust_txn_count_5m",
    "cust_txn_count_1h",
    "cust_txn_count_24h",
    "cust_amount_sum_5m",
    "cust_amount_sum_1h",
    "cust_amount_sum_24h",
    "cust_history_count",
    "cust_amount_mean",
    "cust_amount_std",
    "amount_zscore",
    "amount_to_mean_ratio",
    "seconds_since_last_txn",
    "is_new_device",
    "is_new_ip",
    "is_new_country",
    "device_txn_count_1h",
    "device_customer_count",
    "ip_txn_count_1h",
    "ip_customer_count",
    "balance_drain_ratio",
)
CATEGORICAL: tuple[str, ...] = ("transaction_type",)

# Groups used for ablation studies (e.g. PaySim balance leakage, ADR-016).
FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "balance": ("balance_drain_ratio",),
    "network": (
        "device_txn_count_1h",
        "device_customer_count",
        "ip_txn_count_1h",
        "ip_customer_count",
    ),
}


def _num(value: float | int | bool | None) -> float:
    return math.nan if value is None else float(value)


def vectorize(event: TransactionEvent, features: TransactionFeatures) -> list[float]:
    values = {
        "amount_log": math.log1p(float(event.amount)),
        "transaction_type": float(TYPE_CODES[event.transaction_type]),
    }
    raw = features.model_dump()
    return [values[name] if name in values else _num(raw[name]) for name in FEATURE_NAMES]
