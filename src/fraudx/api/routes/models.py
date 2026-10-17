"""Model prediction: the live scorer's decision for one transaction, with no side effects."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any

import numpy as np
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from fraudx.api.routes.transactions import SimulatedTransaction, build_event
from fraudx.api.stores import Stores, get_stores
from fraudx.core.schemas import ScoredTransaction, TransactionFeatures
from fraudx.explain import evidence as evidence_mod
from fraudx.explain import template
from fraudx.ml.vectorize import vectorize
from fraudx.scoring.engine import decide
from fraudx.storage.graph_features import GRAPH_FEATURE_NAMES

router = APIRouter(prefix="/models", tags=["models"])

EXAMPLE = {
    "transaction": {
        "customer_id": "C-000322", "source_account": "A-000322",
        "destination_account": "A-000777", "amount": 9200, "transaction_type": "TRANSFER",
        "device_id": "DEV-unknown", "location": {"country": "AE"},
        "balances": {"source_before": 9500, "source_after": 300, "destination_before": 0,
                     "destination_after": 9200},
    },
    "features": {
        "cust_txn_count_5m": 1, "cust_txn_count_1h": 1, "cust_txn_count_24h": 2,
        "cust_amount_sum_5m": 9200, "cust_amount_sum_1h": 9200, "cust_amount_sum_24h": 9300,
        "cust_history_count": 40, "cust_amount_mean": 180.5, "cust_amount_std": 95.2,
        "amount_zscore": 94.7, "amount_to_mean_ratio": 51.0, "seconds_since_last_txn": 7200,
        "is_new_device": True, "is_new_ip": True, "is_new_country": True,
        "device_txn_count_1h": 1, "device_customer_count": 1, "ip_txn_count_1h": 1,
        "ip_customer_count": 1, "balance_drain_ratio": 0.968, "hour_of_day": 3,
    },
    "graph_features": {
        "payee_payer_count": 6, "payee_payee_count": 2, "payer_payee_count": 9,
        "is_new_payee": 1, "customer_ring_size": 0, "payee_owner_ring_size": 5,
    },
}  # fmt: skip


class PredictRequest(BaseModel):
    """A transaction with its features, as the pipeline would have computed them."""

    model_config = {"json_schema_extra": {"examples": [EXAMPLE]}}

    transaction: SimulatedTransaction
    features: TransactionFeatures
    graph_features: dict[str, float] | None = Field(
        default=None,
        description="The six graph features (omit to score without the graph signal): "
        + ", ".join(GRAPH_FEATURE_NAMES),
    )


class Reason(BaseModel):
    text: str
    evidence: dict[str, Any]


class Prediction(BaseModel):
    score: int
    level: str
    components: dict[str, float] = Field(description="calibrated fraud probability per signal")
    missing_signals: list[str]
    rule_hits: list[str]
    models: dict[str, str]
    summary: str
    reasons: list[Reason]


@router.post("/predict", response_model=Prediction)
def predict(body: PredictRequest, stores: Annotated[Stores, Depends(get_stores)]):
    """Score one transaction with the active models, exactly as the live scorer would.

    No side effect: nothing is published or stored. Features are supplied by
    the caller, because computing them would update the live feature store;
    known-fraud rules (analyst confirmations) are not evaluated here.
    """
    detectors = stores.detectors()
    if not detectors:
        raise HTTPException(status_code=503, detail="no active model in the registry")
    event = build_event(body.transaction, "PREDICT")
    G = None
    if body.graph_features is not None:
        missing = sorted(set(GRAPH_FEATURE_NAMES) - set(body.graph_features))
        if missing:
            raise HTTPException(status_code=422, detail=f"missing graph features: {missing}")
        G = np.asarray([[body.graph_features[n] for n in GRAPH_FEATURE_NAMES]], dtype=np.float32)
    X = np.asarray([vectorize(event, body.features)], dtype=np.float32)
    config = stores.config()
    decision = decide(detectors, X, G, None, config)[0]
    scored = ScoredTransaction(
        event=event, features=body.features, graph_features=decision.graph_features,
        risk=decision.risk, models=decision.models, scored_at=datetime.now(UTC),
    )  # fmt: skip
    evidence = evidence_mod.build(scored, config)
    explanation = template.explain(evidence)
    risk = decision.risk
    return {
        "score": risk.score,
        "level": risk.level.value,
        "components": risk.components,
        "missing_signals": list(risk.missing_signals),
        "rule_hits": list(risk.rule_hits),
        "models": decision.models,
        "summary": explanation.summary,
        "reasons": [
            {"text": r.text, "evidence": {k: evidence[k] for k in r.evidence}}
            for r in explanation.reasons
        ],
    }
