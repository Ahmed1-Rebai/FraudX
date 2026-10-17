"""Fraud views: the alert feed (Cassandra) and investigations (MongoDB)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from fraudx.api.stores import Stores, get_stores

router = APIRouter(prefix="/fraud", tags=["fraud"])


class RiskEvent(BaseModel):
    transaction_id: str
    customer_id: str
    event_time: datetime
    amount: float
    transaction_type: str
    risk_score: int
    risk_level: str
    rule_hits: list[str] | None = None


Status = Literal["OPEN", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "DISMISSED"]
StoresDep = Annotated[Stores, Depends(get_stores)]


class Risk(BaseModel):
    score: int
    level: str
    components: dict[str, float]
    rule_hits: list[str] = []
    missing_signals: list[str] = []


class ExplanationOut(BaseModel):
    summary: str
    reasons: list[str]
    reason_evidence: list[list[str]] = []
    generator: str
    created_at: datetime


class Feedback(BaseModel):
    """What the decision changed in the graph: the payee account's confirmation mark."""

    account: str
    flagged: bool
    confirmations: int


class InvestigationSummary(BaseModel):
    transaction_id: str
    customer_id: str
    event_time: datetime
    risk: Risk
    status: Status
    explanation: ExplanationOut
    created_at: datetime
    updated_at: datetime
    analyst_note: str | None = None
    feedback: Feedback | None = None


class Investigation(InvestigationSummary):
    models: dict[str, str] = {}
    evidence: dict[str, Any]


class CitedReason(BaseModel):
    text: str
    evidence: dict[str, Any] = Field(description="the cited evidence keys and their values")


class ExplanationView(BaseModel):
    transaction_id: str
    summary: str
    generator: str
    reasons: list[CitedReason]


class Decision(BaseModel):
    status: Status
    note: str | None = Field(default=None, max_length=2000)


class AlertFeed(BaseModel):
    day: date
    events: list[RiskEvent]


@router.get("/alerts", response_model=AlertFeed)
def get_alerts(
    stores: Annotated[Stores, Depends(get_stores)],
    day: date | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    level: Annotated[str | None, Query(pattern="^(MEDIUM|HIGH)$")] = None,
):
    """MEDIUM and HIGH transactions of one day (default: today, UTC), newest first.

    Reads the 8 bucket partitions of ``risk_events_by_day`` and merges them.
    """
    day = day or datetime.now(UTC).date()
    events = stores.history().risk_events(day, 500 if level else limit)
    if level:
        events = [e for e in events if e["risk_level"] == level][:limit]
    return {"day": day, "events": events}


@router.get("/investigations/counts", response_model=dict[str, int])
def investigation_counts(stores: StoresDep):
    """Number of investigations per status (MongoDB aggregation)."""
    return stores.investigations().counts()


@router.get("/investigations", response_model=list[InvestigationSummary])
def list_investigations(
    stores: StoresDep,
    status: Status = "OPEN",
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    """The analyst queue: investigations with a status, newest first (MongoDB)."""
    return stores.investigations().queue(status, limit)


def _investigation(stores: Stores, transaction_id: str) -> dict[str, Any]:
    doc = stores.investigations().get(transaction_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="no investigation for this transaction")
    return doc


@router.get("/{transaction_id}", response_model=Investigation)
def get_investigation(transaction_id: str, stores: StoresDep):
    """One investigation: risk breakdown, evidence, explanation and status."""
    return _investigation(stores, transaction_id)


@router.get("/{transaction_id}/explanation", response_model=ExplanationView)
def get_explanation(transaction_id: str, stores: StoresDep):
    """The explanation, each reason with the evidence values it cites."""
    doc = _investigation(stores, transaction_id)
    explanation, evidence = doc["explanation"], doc["evidence"]
    reasons = [
        {"text": text, "evidence": {k: evidence.get(k) for k in keys}}
        for text, keys in zip(
            explanation["reasons"], explanation.get("reason_evidence", []), strict=False
        )
    ]
    return {
        "transaction_id": transaction_id,
        "summary": explanation["summary"],
        "generator": explanation["generator"],
        "reasons": reasons,
    }


@router.patch("/{transaction_id}", response_model=Investigation)
def decide(transaction_id: str, body: Decision, stores: StoresDep):
    """Record the analyst's decision (confirmed fraud, false positive, dismissed, reopened).

    The decision also feeds the graph: confirming fraud marks the payee account
    as a confirmed fraud recipient, and any other decision withdraws this
    case's confirmation. The scorer's known-fraud rules use these marks.
    Idempotent: send the same decision again if it failed half-way (503).
    """
    cases = stores.investigations()
    doc = cases.decide(transaction_id, body.status, body.note)
    if doc is None:
        raise HTTPException(status_code=404, detail="no investigation for this transaction")
    transaction = stores.history().transaction(transaction_id)
    if transaction is not None:
        feedback = stores.feedback().record(transaction, confirmed=body.status == "CONFIRMED_FRAUD")
        doc = cases.set_feedback(transaction_id, feedback) or doc
    return doc
