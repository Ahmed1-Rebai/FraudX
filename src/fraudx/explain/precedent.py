"""Historical precedent retrieval (case-law RAG) for fraud investigations.

Computes a compact behavioral fingerprint for transactions/alerts and retrieves
the most similar historical resolved cases (confirmed fraud or false positives)
to ground explanations in compliance precedent.
"""

from __future__ import annotations

import math
from typing import Any

SIGNATURE_KEYS = (
    "amount_to_mean_ratio",
    "balance_drain_ratio",
    "cust_txn_count_5m",
    "is_new_device",
    "is_new_country",
    "is_new_ip",
    "customer_ring_size",
    "payee_owner_ring_size",
    "payee_payer_count",
    "signals.ml",
    "signals.anomaly",
    "signals.graph",
    "rules.takeover_pattern",
    "rules.burst_on_shared_device",
)


def build_signature(evidence: dict[str, Any]) -> list[float]:
    """Extract and normalize behavioral features into a fixed-dimension vector."""
    vec = [
        min(float(evidence.get("features.amount_to_mean_ratio", 1.0)) / 10.0, 2.0),
        min(float(evidence.get("features.balance_drain_ratio", 0.0)), 1.0),
        min(float(evidence.get("features.cust_txn_count_5m", 0.0)) / 10.0, 2.0),
        1.0 if evidence.get("features.is_new_device") else 0.0,
        1.0 if evidence.get("features.is_new_country") else 0.0,
        1.0 if evidence.get("features.is_new_ip") else 0.0,
        min(float(evidence.get("graph.customer_ring_size", 0.0)) / 5.0, 2.0),
        min(float(evidence.get("graph.payee_owner_ring_size", 0.0)) / 5.0, 2.0),
        min(float(evidence.get("graph.payee_payer_count", 0.0)) / 20.0, 2.0),
        float(evidence.get("signals.ml", 0.0)),
        float(evidence.get("signals.anomaly", 0.0)),
        float(evidence.get("signals.graph", 0.0)),
        1.0 if evidence.get("rules.takeover_pattern") else 0.0,
        1.0 if evidence.get("rules.burst_on_shared_device") else 0.0,
    ]
    return vec


def cosine_similarity(v1: list[float], v2: list[float]) -> float:
    """Cosine similarity between two normalized feature vectors."""
    dot = sum(a * b for a, b in zip(v1, v2, strict=False))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0
    return max(0.0, min(1.0, dot / (norm1 * norm2)))


def find_precedents(
    current_evidence: dict[str, Any],
    candidates: list[dict[str, Any]],
    limit: int = 3,
    min_similarity: float = 0.70,
) -> list[dict[str, Any]]:
    """Find the top-k most similar historical cases to the current evidence."""
    target_sig = build_signature(current_evidence)
    current_id = current_evidence.get("transaction.id")

    matches = []
    for cand in candidates:
        cand_id = cand.get("_id") or cand.get("transaction_id")
        if cand_id == current_id:
            continue
        cand_ev = cand.get("evidence")
        if not cand_ev or not isinstance(cand_ev, dict):
            continue
        cand_sig = build_signature(cand_ev)
        sim = cosine_similarity(target_sig, cand_sig)
        if sim >= min_similarity:
            expl = cand.get("explanation")
            summary = expl.get("summary") if isinstance(expl, dict) else None
            matches.append(
                {
                    "transaction_id": cand_id,
                    "status": cand.get("status", "CONFIRMED_FRAUD"),
                    "similarity": round(sim, 3),
                    "risk_score": cand.get("risk", {}).get("score", 0),
                    "summary": summary,
                    "analyst_note": cand.get("analyst_note"),
                }
            )

    matches.sort(key=lambda m: m["similarity"], reverse=True)
    return matches[:limit]


def enrich_evidence_with_precedent(
    evidence: dict[str, Any], precedents: list[dict[str, Any]]
) -> dict[str, Any]:
    """Attach the top precedent citations into evidence for grounded explanations."""
    if not precedents:
        return evidence
    top = precedents[0]
    evidence["precedent.top_id"] = top["transaction_id"]
    evidence["precedent.top_similarity"] = top["similarity"]
    evidence["precedent.top_status"] = top["status"]
    if top.get("analyst_note"):
        evidence["precedent.top_note"] = top["analyst_note"]
    evidence["precedent.match_count"] = len(precedents)
    return evidence
