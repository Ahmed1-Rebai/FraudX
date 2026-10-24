from fraudx.explain.precedent import (
    build_signature,
    cosine_similarity,
    enrich_evidence_with_precedent,
    find_precedents,
)
from fraudx.explain.template import explain


def test_signature_and_cosine_similarity():
    ev1 = {
        "features.amount_to_mean_ratio": 9.0,
        "features.balance_drain_ratio": 0.95,
        "features.is_new_device": True,
        "rules.takeover_pattern": True,
        "signals.ml": 0.9,
    }
    ev2 = {
        "features.amount_to_mean_ratio": 8.5,
        "features.balance_drain_ratio": 0.90,
        "features.is_new_device": True,
        "rules.takeover_pattern": True,
        "signals.ml": 0.85,
    }
    ev3 = {
        "features.amount_to_mean_ratio": 0.5,
        "features.balance_drain_ratio": 0.05,
        "features.is_new_device": False,
        "signals.ml": 0.05,
    }

    sig1 = build_signature(ev1)
    sig2 = build_signature(ev2)
    sig3 = build_signature(ev3)

    sim_1_2 = cosine_similarity(sig1, sig2)
    sim_1_3 = cosine_similarity(sig1, sig3)

    assert sim_1_2 > 0.95  # highly similar takeover pattern
    assert sim_1_2 > sim_1_3 + 0.20  # clearly more similar than distinct profile


def test_find_precedents_and_enrichment():
    current_ev = {
        "transaction.id": "TX-NEW",
        "transaction.type": "TRANSFER",
        "transaction.amount": 1000.0,
        "transaction.currency": "TND",
        "risk.score": 85,
        "risk.level": "HIGH",
        "features.amount_to_mean_ratio": 10.0,
        "features.cust_amount_mean": 100.0,
        "features.balance_drain_ratio": 0.95,
        "features.is_new_device": True,
        "rules.takeover_pattern": True,
    }

    candidates = [
        {
            "_id": "TX-PAST-1",
            "status": "CONFIRMED_FRAUD",
            "evidence": {
                "features.amount_to_mean_ratio": 9.5,
                "features.balance_drain_ratio": 0.92,
                "features.is_new_device": True,
                "rules.takeover_pattern": True,
            },
            "risk": {"score": 85},
            "explanation": {"summary": "Account takeover to new device"},
            "analyst_note": "Called customer; SIM swap confirmed",
        },
        {
            "_id": "TX-PAST-2",
            "status": "FALSE_POSITIVE",
            "evidence": {
                "features.amount_to_mean_ratio": 1.0,
                "features.balance_drain_ratio": 0.1,
                "features.is_new_device": False,
            },
            "risk": {"score": 25},
            "explanation": {"summary": "Normal purchase"},
        },
    ]

    matches = find_precedents(current_ev, candidates, limit=2, min_similarity=0.70)
    assert len(matches) == 1
    assert matches[0]["transaction_id"] == "TX-PAST-1"
    assert matches[0]["status"] == "CONFIRMED_FRAUD"
    assert matches[0]["similarity"] > 0.95
    assert matches[0]["analyst_note"] == "Called customer; SIM swap confirmed"

    enriched = enrich_evidence_with_precedent(current_ev.copy(), matches)
    assert enriched["precedent.top_id"] == "TX-PAST-1"
    assert enriched["precedent.top_status"] == "CONFIRMED_FRAUD"
    assert enriched["precedent.top_similarity"] > 0.95

    # Check that template explanation cites this precedent
    out = explain(enriched)
    reasons_text = [r.text for r in out.reasons]
    assert any("Behavioral similarity" in t and "TX-PAST-1" in t for t in reasons_text)
    assert any("SIM swap confirmed" in t for t in reasons_text)
