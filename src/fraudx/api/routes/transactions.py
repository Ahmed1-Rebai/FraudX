"""Transaction history, served from Cassandra."""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from fraudx.api.stores import Stores, get_stores

router = APIRouter(prefix="/transactions", tags=["transactions"])


class Transaction(BaseModel):
    """A stored transaction. The ground-truth label is never exposed (ADR-019)."""

    transaction_id: str
    customer_id: str
    event_date: date
    event_time: datetime
    source_account: str
    destination_account: str
    transaction_type: str
    amount: float
    currency: str
    device_id: str | None = None
    ip_address: str | None = None
    country: str | None = None
    city: str | None = None
    risk_score: int | None = None
    risk_level: str | None = None
    ml_score: float | None = None
    anomaly_score: float | None = None
    graph_score: float | None = None


@router.get("/{transaction_id}", response_model=Transaction)
def get_transaction(transaction_id: str, stores: Annotated[Stores, Depends(get_stores)]):
    """One transaction (``transactions_by_id``: a single-partition read)."""
    record = stores.history().transaction(transaction_id)
    if record is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return record
