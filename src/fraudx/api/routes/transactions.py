"""Transaction history, served from Cassandra."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, ValidationError

from fraudx.api.stores import Stores, get_stores
from fraudx.core.schemas import Balances, Location, PaymentMethod, TransactionEvent, TransactionType
from fraudx.core.topics import TRANSACTIONS_RAW
from fraudx.messaging.consumer import PermanentError
from fraudx.streaming.processor import validate

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
    rule_hits: list[str] | None = None


class SimulatedTransaction(BaseModel):
    """A transaction for the live pipeline or a prediction (id and time are set by the API)."""

    customer_id: str = Field(min_length=1, max_length=64, examples=["C-000322"])
    source_account: str = Field(min_length=1, max_length=64, examples=["A-000322"])
    destination_account: str = Field(min_length=1, max_length=64, examples=["A-000777"])
    amount: Decimal = Field(gt=0, examples=[2500])
    transaction_type: TransactionType = TransactionType.TRANSFER
    device_id: str | None = Field(default=None, max_length=64, examples=["DEV-0199a04abb"])
    ip_address: str | None = None
    location: Location | None = None
    balances: Balances | None = None
    payment_method: PaymentMethod | None = None


def build_event(body: SimulatedTransaction, id_prefix: str) -> TransactionEvent:
    """The event contract and the pipeline's own business rules, or 422."""
    try:
        event = TransactionEvent(
            transaction_id=f"{id_prefix}-{uuid.uuid4().hex[:12]}",
            event_time=datetime.now(UTC),
            **body.model_dump(exclude_none=True),
        )
        validate(event)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=exc.errors(include_url=False, include_input=False, include_context=False),
        ) from exc
    except PermanentError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return event


class Accepted(BaseModel):
    transaction_id: str
    topic: str


@router.get("/{transaction_id}", response_model=Transaction)
def get_transaction(transaction_id: str, stores: Annotated[Stores, Depends(get_stores)]):
    """One transaction (``transactions_by_id``: a single-partition read)."""
    record = stores.history().transaction(transaction_id)
    if record is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return record


@router.post("/simulate", response_model=Accepted, status_code=status.HTTP_202_ACCEPTED)
def simulate(body: SimulatedTransaction, stores: Annotated[Stores, Depends(get_stores)]):
    """Publish a transaction to ``transactions.raw``; it is scored like any other.

    Read the outcome a moment later with ``GET /transactions/{id}``.
    """
    event = build_event(body, "SIM")
    producer = stores.producer()
    producer.send_model(TRANSACTIONS_RAW, event, key=event.partition_key)
    producer.flush(5)
    return {"transaction_id": event.transaction_id, "topic": TRANSACTIONS_RAW}
