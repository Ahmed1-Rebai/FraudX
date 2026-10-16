"""Fraud views: the alert feed (Cassandra)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

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
