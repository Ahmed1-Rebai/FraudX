"""Deterministic, evidence-grounded explanations (the default generator, ADR-009).

Each reason is produced by a rule over the evidence and cites the keys it
used; ``grounded`` drops any reason citing a key the evidence doesn't hold
(it also guards LLM output). Reasons are ordered strongest first: rule hits,
graph patterns, then transaction and behaviour facts, and at most five are kept.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

MAX_REASONS = 5
SIGNAL_NAMES = {"ml": "the supervised model", "anomaly": "the anomaly detector",
                "graph": "the graph model"}  # fmt: skip


@dataclass(frozen=True)
class Reason:
    text: str
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class Explanation:
    summary: str
    reasons: tuple[Reason, ...]
    generator: str
    dropped: int = 0  # reasons removed for citing missing evidence
    notes: dict[str, Any] = field(default_factory=dict)


Rule = Callable[[dict[str, Any]], Reason | None]


def _known_fraud(e: dict[str, Any]) -> Reason | None:
    """Analyst-confirmed accounts (spec §13). Proximity of one hop or more stays
    evidence only: it was not indicative when measured (precision 0.00)."""
    if e.get("graph.distance_to_known_fraud") == 0:
        first = " This is the payer's first transfer to it." if e.get("graph.is_new_payee") else ""
        return Reason(
            f"The payee account was confirmed as a fraud recipient by an analyst.{first}",
            ("graph.distance_to_known_fraud",) + (("graph.is_new_payee",) if first else ()),
        )
    if e.get("graph.payer_confirmed_fraud"):
        return Reason(
            "The paying account was itself confirmed by an analyst as a fraud recipient "
            "(money moving on from a known mule).",
            ("graph.payer_confirmed_fraud",),
        )
    return None


def _takeover(e: dict[str, Any]) -> Reason | None:
    if not e.get("rules.takeover_pattern"):
        return None
    abroad = " from a country new for this customer" if e.get("features.is_new_country") else ""
    return Reason(
        f"Account-takeover pattern: a new device moved "
        f"{e['features.balance_drain_ratio']:.0%} of the balance{abroad}.",
        ("rules.takeover_pattern", "features.is_new_device", "features.balance_drain_ratio",
         *(("features.is_new_country",) if abroad else ())),
    )  # fmt: skip


def _burst(e: dict[str, Any]) -> Reason | None:
    if not e.get("rules.burst_on_shared_device"):
        return None
    return Reason(
        f"Burst of {e['features.cust_txn_count_5m']} transactions in 5 minutes on a device "
        f"used by {e['features.device_customer_count']} customers (card-testing pattern).",
        ("rules.burst_on_shared_device", "features.cust_txn_count_5m",
         "features.device_customer_count"),
    )  # fmt: skip


def _precedent(e: dict[str, Any]) -> Reason | None:
    """Historical case-law precedent match with high behavioral similarity."""
    top_id = e.get("precedent.top_id")
    sim = e.get("precedent.top_similarity")
    status = e.get("precedent.top_status")
    if not top_id or sim is None or sim < 0.75 or not status:
        return None
    label = "confirmed fraud" if status == "CONFIRMED_FRAUD" else "false positive"
    pct = round(sim * 100)
    note = f" (analyst: '{e['precedent.top_note']}')" if e.get("precedent.top_note") else ""
    return Reason(
        f"Behavioral similarity ({pct}%) to {label} case {top_id}{note}.",
        ("precedent.top_id", "precedent.top_similarity", "precedent.top_status"),
    )


def _payee_ring(e: dict[str, Any]) -> Reason | None:
    n = e.get("graph.payee_owner_ring_size", 0)
    if n < 1:
        return None
    return Reason(
        f"The payee's owner shares a device with {n:.0f} other customer(s), a sign of a mule ring.",
        ("graph.payee_owner_ring_size",),
    )


def _customer_ring(e: dict[str, Any]) -> Reason | None:
    n = e.get("graph.customer_ring_size", 0)
    if n < 1:
        return None
    return Reason(f"This customer shares a device with {n:.0f} other customer(s).",
                  ("graph.customer_ring_size",))  # fmt: skip


def _fan_in(e: dict[str, Any]) -> Reason | None:
    n = e.get("graph.payee_payer_count", 0)
    if e.get("payee.kind") != "CUSTOMER" or n < 5:
        return None  # merchants and agents receive from many accounts by design
    return Reason(f"The payee account has already received money from {n:.0f} other accounts.",
                  ("graph.payee_payer_count", "payee.kind"))  # fmt: skip


def _new_payee(e: dict[str, Any]) -> Reason | None:
    if e.get("transaction.type") != "TRANSFER" or not e.get("graph.is_new_payee"):
        return None
    return Reason("First transfer from this account to this payee.",
                  ("graph.is_new_payee", "transaction.type"))  # fmt: skip


def _amount(e: dict[str, Any]) -> Reason | None:
    ratio = e.get("features.amount_to_mean_ratio")
    if (
        ratio is None
        or ratio < 3
        or "features.cust_amount_mean" not in e
        or "transaction.currency" not in e
    ):
        return None
    return Reason(
        f"The amount is {ratio:.1f}x the customer's average "
        f"({e['features.cust_amount_mean']:,.2f} {e['transaction.currency']}).",
        ("features.amount_to_mean_ratio", "features.cust_amount_mean", "transaction.currency"),
    )


def _drain(e: dict[str, Any]) -> Reason | None:
    drain = e.get("features.balance_drain_ratio")
    if drain is None or drain < 0.5 or e.get("rules.takeover_pattern"):
        return None
    return Reason(f"It moves {drain:.0%} of the available balance.",
                  ("features.balance_drain_ratio",))  # fmt: skip


def _velocity(e: dict[str, Any]) -> Reason | None:
    n = e.get("features.cust_txn_count_5m", 0)
    if n < 3 or e.get("rules.burst_on_shared_device"):
        return None
    return Reason(f"{n} transactions by this customer in the last 5 minutes.",
                  ("features.cust_txn_count_5m",))  # fmt: skip


def _new_context(e: dict[str, Any]) -> Reason | None:
    if e.get("rules.takeover_pattern"):
        return None
    labels = {"features.is_new_device": "device", "features.is_new_country": "country",
              "features.is_new_ip": "IP address"}  # fmt: skip
    new = [(k, label) for k, label in labels.items() if e.get(k)]
    if not new:
        return None
    return Reason(f"First time this customer uses this {' and '.join(lbl for _, lbl in new)}.",
                  tuple(k for k, _ in new))  # fmt: skip


def _shared_device(e: dict[str, Any]) -> Reason | None:
    n = e.get("features.device_customer_count", 0)
    if n < 3 or e.get("rules.burst_on_shared_device") or e.get("graph.customer_ring_size", 0) >= 1:
        return None  # already said by the rule or the customer's ring
    return Reason(f"The device has been used by {n} different customers.",
                  ("features.device_customer_count",))  # fmt: skip


def _signal(e: dict[str, Any]) -> Reason | None:
    """The strongest model signal, when no fact above already explains it."""
    signals = {k.split(".")[1]: v for k, v in e.items()
               if k.startswith("signals.") and isinstance(v, float)}  # fmt: skip
    if not signals:
        return None
    name, value = max(signals.items(), key=lambda kv: e.get(f"contribution.{kv[0]}", 0))
    if value < 0.2:
        return None
    return Reason(f"{SIGNAL_NAMES[name].capitalize()} estimates a {value:.0%} fraud probability.",
                  (f"signals.{name}", f"contribution.{name}"))  # fmt: skip


RULES: tuple[Rule, ...] = (
    _known_fraud,
    _takeover, _burst, _precedent, _payee_ring, _customer_ring, _fan_in, _amount, _drain, _velocity,
    _new_payee, _new_context, _shared_device, _signal,
)  # fmt: skip


def grounded(reasons: list[Reason], evidence: dict[str, Any]) -> tuple[list[Reason], int]:
    """Keep reasons whose citations are all present in the evidence."""
    kept = [r for r in reasons if r.evidence and all(k in evidence for k in r.evidence)]
    return kept, len(reasons) - len(kept)


def summary(evidence: dict[str, Any]) -> str:
    contributions = {
        k.split(".")[1]: v for k, v in evidence.items() if k.startswith("contribution.")
    }
    driver = ""
    if contributions:
        name, share = max(contributions.items(), key=lambda kv: kv[1])
        driver = f"; main driver: {SIGNAL_NAMES[name]} ({share:.0%} of the score)"
    rules = [k.split(".", 1)[1] for k in evidence if k.startswith("rules.")]
    rule_text = f"; rule hit: {', '.join(rules)}" if rules else ""
    return (
        f"{evidence['transaction.type']} of {evidence['transaction.amount']:,.2f} "
        f"{evidence['transaction.currency']} scored {evidence['risk.score']}/100 "
        f"({evidence['risk.level']}){driver}{rule_text}."
    )


def explain(evidence: dict[str, Any]) -> Explanation:
    reasons = [r for rule in RULES if (r := rule(evidence)) is not None]
    kept, dropped = grounded(reasons, evidence)
    return Explanation(summary(evidence), tuple(kept[:MAX_REASONS]), "template", dropped)


class TemplateExplainer:
    """The default generator, as an object with the same interface as ``ClaudeExplainer``."""

    def explain(self, evidence: dict[str, Any]) -> Explanation:
        return explain(evidence)
