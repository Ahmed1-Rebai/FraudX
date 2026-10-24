"""Deterministic rules: a minimum risk level for known-bad patterns (ADR-008).

Each rule was kept only if it is precise on the synthetic training data and
the holdout, so a rule hit is a strong, explainable reason on its own:

* ``takeover_pattern`` (HIGH): a new device drains ≥ 50 % of the balance,
  from abroad or ≥ 90 % of it. Precision 0.91 train / 0.86 holdout.
* ``burst_on_shared_device`` (MEDIUM): ≥ 5 transactions in 5 min on a device
  other customers also use. Precision 0.73 train / 0.92 holdout.

Known fraud (analyst feedback, ``fraudx.storage.feedback``) reads the live
graph's confirmations, which no training set holds. Measured by replaying the
synthetic datasets with every earlier fraud confirmed (perfect feedback):

* ``payee_confirmed_fraud`` (MEDIUM): a first TRANSFER from this payer to an
  account an analyst confirmed as a fraud recipient. Precision 0.58 demo /
  0.58 training; it catches 63 % of the fraud sent to customer accounts,
  including 12 of 18 authorised-push-payment scams, which the models miss.
  Any payment to a confirmed account is only 0.20 precise (mules also
  receive their contacts' money), hence the first-transfer condition.
* Not rules, measured too weak: the payer itself confirmed (0.47 / 0.36) and
  a payee one hop from a confirmed account (0.00 / 0.00). Both stay in the
  evidence (``graph.payer_confirmed_fraud``, ``graph.distance_to_known_fraud``).

Rules read the model's feature vector (``fraudx.ml.vectorize``, missing =
NaN, so a missing value never fires a rule). The same expressions evaluate
one transaction (floats) or a whole matrix (numpy columns).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np

from fraudx.core.schemas import RiskLevel, TransactionType
from fraudx.ml.vectorize import FEATURE_NAMES, TYPE_CODES

Values = Mapping[str, Any]  # feature name -> float or numpy column


def _takeover_pattern(f: Values) -> Any:
    drain = f["balance_drain_ratio"]
    return (
        (f["is_new_device"] == 1) & (drain >= 0.5) & ((f["is_new_country"] == 1) | (drain >= 0.9))
    )


def _burst_on_shared_device(f: Values) -> Any:
    return (f["cust_txn_count_5m"] >= 5) & (f["device_customer_count"] >= 2)


_TRANSFER = TYPE_CODES[TransactionType.TRANSFER]


# Known-fraud values exist only live, once an analyst has confirmed an account;
# is_new_payee is a graph feature. Absent values never fire the rule.
def _payee_confirmed_fraud(f: Values) -> Any:
    return (
        (f.get("distance_to_known_fraud") == 0)
        & (f.get("is_new_payee") == 1)
        & (f["transaction_type"] == _TRANSFER)
    )


def _circular_transfer_pattern(f: Values) -> Any:
    """Directed money routing cycle (A -> B -> ... -> A) indicating circular layering."""
    return (f.get("has_transfer_cycle") == 1) & (f["transaction_type"] == _TRANSFER)


RULES: dict[str, tuple[RiskLevel, Any]] = {
    "takeover_pattern": (RiskLevel.HIGH, _takeover_pattern),
    "burst_on_shared_device": (RiskLevel.MEDIUM, _burst_on_shared_device),
    "payee_confirmed_fraud": (RiskLevel.MEDIUM, _payee_confirmed_fraud),
    "circular_transfer_pattern": (RiskLevel.MEDIUM, _circular_transfer_pattern),
}


def as_values(vector: list[float] | np.ndarray) -> dict[str, Any]:
    """Feature vector(s) -> name -> value (rows: one vector; 2-D: columns)."""
    array = np.asarray(vector, dtype=np.float64)
    if array.ndim == 1:
        return {name: float(v) for name, v in zip(FEATURE_NAMES, array, strict=True)}
    return {name: array[:, i] for i, name in enumerate(FEATURE_NAMES)}


def evaluate(values: Values) -> list[tuple[str, RiskLevel]]:
    """Rules that fire for one transaction."""
    return [(name, level) for name, (level, rule) in RULES.items() if bool(rule(values))]
