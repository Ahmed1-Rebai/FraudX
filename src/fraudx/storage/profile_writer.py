"""MongoDB writer: customer, account and device profiles (ADR-006).

One unordered ``bulk_write`` per batch. All updates are idempotent and
order-insensitive, so redelivery and concurrent writers are safe:

* timestamps use min/max (``first_seen`` / ``last_seen``);
* ``devices`` / ``known_ips`` / ``countries`` hold the 50 most recently seen
  distinct values (aggregation-pipeline update: remove, append, slice), which
  keeps documents bounded and inside the collection validator;
* no counters: transaction counts and amounts live in Redis and Cassandra;
* ``risk_profile`` is the assessment of the customer's latest transaction by
  event time, so a redelivered older message never overwrites a newer one.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pymongo import UpdateOne
from pymongo.database import Database

from fraudx.core.schemas import RiskAssessment, ScoredTransaction, TransactionEvent

MAX_RECENT = 50


def _recent(field: str, value: str) -> dict[str, Any]:
    """Pipeline expression: move ``value`` to the end of ``field``, keep the last 50."""
    existing = {"$ifNull": [f"${field}", []]}
    others = {"$filter": {"input": existing, "cond": {"$ne": ["$$this", value]}}}
    return {"$slice": [{"$concatArrays": [others, [value]]}, -MAX_RECENT]}


def _first(field: str, ts: datetime) -> dict[str, Any]:
    return {"$min": [{"$ifNull": [f"${field}", ts]}, ts]}


def _last(field: str, ts: datetime) -> dict[str, Any]:
    return {"$max": [{"$ifNull": [f"${field}", ts]}, ts]}


def _risk_profile(event: TransactionEvent, risk: RiskAssessment, now: datetime) -> dict[str, Any]:
    """Pipeline expression: this assessment if it is the latest by event time."""
    latest = {
        "score": risk.score,
        "level": risk.level.value,
        "transaction_id": event.transaction_id,
        "event_time": event.event_time,
        "updated_at": now,
    }
    previous = {"$ifNull": ["$risk_profile.event_time", datetime.min.replace(tzinfo=UTC)]}
    return {"$cond": [{"$gte": [event.event_time, previous]}, latest, "$risk_profile"]}


def customer_update(
    event: TransactionEvent, now: datetime, risk: RiskAssessment | None = None
) -> UpdateOne:
    ts = event.event_time
    fields: dict[str, Any] = {
        "created_at": {"$ifNull": ["$created_at", now]},
        "updated_at": now,
        "first_seen": _first("first_seen", ts),
        "last_seen": _last("last_seen", ts),
        "accounts": _recent("accounts", event.source_account),
    }
    if event.device_id:
        fields["devices"] = _recent("devices", event.device_id)
    if event.ip_address:
        fields["known_ips"] = _recent("known_ips", str(event.ip_address))
    if event.location:
        # Home country = first country seen for the customer.
        fields["country"] = {"$ifNull": ["$country", event.location.country]}
        fields["countries"] = _recent("countries", event.location.country)
    if risk is not None:
        fields["risk_profile"] = _risk_profile(event, risk, now)
    return UpdateOne({"_id": event.customer_id}, [{"$set": fields}], upsert=True)


def account_update(event: TransactionEvent, now: datetime) -> UpdateOne:
    ts = event.event_time
    return UpdateOne(
        {"_id": event.source_account},
        [{"$set": {
            "customer_id": event.customer_id,
            "kind": "CUSTOMER",
            "opened_at": _first("opened_at", ts),
            "updated_at": now,
        }}],
        upsert=True,
    )  # fmt: skip


def device_update(device_id: str, ts: datetime) -> UpdateOne:
    return UpdateOne(
        {"_id": device_id},
        [{"$set": {"first_seen": _first("first_seen", ts), "last_seen": _last("last_seen", ts)}}],
        upsert=True,
    )


class ProfileWriter:
    def __init__(self, db: Database) -> None:
        self.db = db

    def write(self, events: list[TransactionEvent]) -> None:
        self._write(events, [None] * len(events))

    def write_scored(self, items: list[ScoredTransaction]) -> None:
        self._write([i.event for i in items], [i.risk for i in items])

    def _write(self, events: list[TransactionEvent], risks: list[RiskAssessment | None]) -> None:
        now = datetime.now(UTC)
        customers = [customer_update(e, now, r) for e, r in zip(events, risks, strict=True)]
        accounts = [account_update(e, now) for e in events]
        devices = [device_update(e.device_id, e.event_time) for e in events if e.device_id]
        for collection, ops in (
            (self.db.customers, customers),
            (self.db.accounts, accounts),
            (self.db.devices, devices),
        ):
            if ops:
                collection.bulk_write(ops, ordered=False)
