"""Customer views: profile (MongoDB), daily history (Cassandra), network (Neo4j)."""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from fraudx.api.routes.transactions import Transaction
from fraudx.api.stores import Stores, get_stores

router = APIRouter(prefix="/customers", tags=["customers"])

StoresDep = Annotated[Stores, Depends(get_stores)]


class CustomerProfile(BaseModel):
    customer_id: str
    country: str | None = None
    accounts: list[str] = []
    devices: list[str] = []
    known_ips: list[str] = []
    countries: list[str] = []
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    risk_profile: dict[str, Any] | None = None


class CustomerRisk(BaseModel):
    """Latest risk, set by the scorer for the customer's most recent transaction."""

    customer_id: str
    score: int
    level: str
    transaction_id: str
    event_time: datetime
    scored_at: datetime


class CustomerDay(BaseModel):
    customer_id: str
    day: date
    transactions: list[Transaction]


class Device(BaseModel):
    device_id: str
    uses: int
    first_seen: datetime
    last_seen: datetime
    shared_with_count: int
    shared_with: list[str]


class Counterparty(BaseModel):
    account: str
    counterparty: str
    kind: str | None = None
    confirmed_fraud: bool = False
    direction: str
    count: int
    total_amount: float
    last_seen: datetime


class CustomerNetwork(BaseModel):
    customer_id: str
    devices: list[Device]
    counterparties: list[Counterparty]


def _profile(stores: Stores, customer_id: str) -> dict[str, Any]:
    profile = stores.profiles().customer(customer_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="customer not found")
    return profile


@router.get("/{customer_id}", response_model=CustomerProfile)
def get_customer(customer_id: str, stores: StoresDep):
    """Customer profile (MongoDB primary-key lookup)."""
    return _profile(stores, customer_id)


@router.get("/{customer_id}/risk", response_model=CustomerRisk)
def get_customer_risk(customer_id: str, stores: StoresDep):
    """The customer's latest risk score (Redis, updated by the scorer in real time)."""
    risk = stores.risk().latest(customer_id)
    if risk is None:
        raise HTTPException(status_code=404, detail="no risk scored for this customer")
    return {"customer_id": customer_id, **risk}


@router.get("/{customer_id}/transactions", response_model=CustomerDay)
def get_customer_transactions(
    customer_id: str,
    stores: StoresDep,
    day: date | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    """A customer's transactions on one day, newest first (one Cassandra partition).

    Without ``day``, the customer's last active day from the MongoDB profile is used.
    """
    if day is None:
        day = _profile(stores, customer_id)["last_seen"].date()
    rows = stores.history().customer_day(customer_id, day, limit)
    return {"customer_id": customer_id, "day": day, "transactions": rows}


@router.get("/{customer_id}/network", response_model=CustomerNetwork)
def get_customer_network(
    customer_id: str, stores: StoresDep, limit: Annotated[int, Query(ge=1, le=100)] = 20
):
    """Devices the customer shares with others and accounts it exchanges money with (Neo4j)."""
    network = stores.graph().customer_network(customer_id, limit)
    if network is None:
        raise HTTPException(status_code=404, detail="customer not found")
    return network
