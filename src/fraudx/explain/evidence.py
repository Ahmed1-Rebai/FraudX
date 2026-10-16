"""Evidence for one risk decision: flat, citable facts (ADR-009).

Every value comes from the scored transaction (which carries all the inputs
of the decision) or the customer's stored profile; nothing is inferred. Keys
are what explanations cite, e.g. ``graph.payee_owner_ring_size``. Missing
values are left out, so a reason can only cite a fact that exists.
"""

from __future__ import annotations

from typing import Any

from fraudx.core.config import RiskConfig
from fraudx.core.schemas import ScoredTransaction
from fraudx.storage.graph_writer import account_kind

FEATURES = (
    "amount_to_mean_ratio", "cust_amount_mean", "cust_history_count", "cust_txn_count_5m",
    "cust_txn_count_1h", "seconds_since_last_txn", "is_new_device", "is_new_ip",
    "is_new_country", "device_customer_count", "ip_customer_count", "balance_drain_ratio",
    "hour_of_day",
)  # fmt: skip

# Never sent to an external model: identifiers (pseudonymisation, ADR-009).
IDENTIFYING = ("transaction.id", "customer.id")


def build(
    scored: ScoredTransaction, config: RiskConfig, profile: dict[str, Any] | None = None
) -> dict[str, Any]:
    event, risk = scored.event, scored.risk
    evidence: dict[str, Any] = {
        "transaction.id": event.transaction_id,
        "customer.id": event.customer_id,
        "transaction.amount": float(event.amount),
        "transaction.currency": event.currency,
        "transaction.type": event.transaction_type.value,
        "transaction.time": event.event_time.isoformat(),
        "risk.score": risk.score,
        "risk.level": risk.level.value,
        "risk.alert_threshold": config.alert_threshold,
    }
    if event.location is not None:
        evidence["transaction.country"] = event.location.country
    # Merchants and cash-out agents are hubs: high fan-in is normal for them.
    evidence["payee.kind"] = account_kind(event.destination_account)
    weights = config.weights.model_dump()
    total = sum(weights[k] * v for k, v in risk.components.items()) or 1.0
    for signal, value in risk.components.items():
        evidence[f"signals.{signal}"] = round(value, 4)
        evidence[f"contribution.{signal}"] = round(weights[signal] * value / total, 4)
    for signal in risk.missing_signals:
        evidence[f"signals.{signal}.missing"] = True
    for rule in risk.rule_hits:
        evidence[f"rules.{rule}"] = True
    features = scored.features.model_dump()
    for name in FEATURES:
        if features.get(name) is not None:
            evidence[f"features.{name}"] = features[name]
    for name, value in (scored.graph_features or {}).items():
        evidence[f"graph.{name}"] = value
    if profile:
        if profile.get("country"):
            evidence["profile.home_country"] = profile["country"]
        evidence["profile.known_devices"] = len(profile.get("devices") or [])
    return evidence


def pseudonymise(evidence: dict[str, Any]) -> dict[str, Any]:
    """Evidence for an external model: no identifiers (IPs and balances are never in it)."""
    return {k: v for k, v in evidence.items() if k not in IDENTIFYING}
