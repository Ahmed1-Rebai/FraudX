"""MongoDB investigations: one document per alert (created by the alert handler).

Queries use the collection's indexes: ``_id`` (transaction id) and
``(status, created_at)`` for the analyst queue.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pymongo import DESCENDING, ReturnDocument
from pymongo.database import Database

STATUSES = ("OPEN", "CONFIRMED_FRAUD", "FALSE_POSITIVE", "DISMISSED")


def _out(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    if doc is None:
        return None
    doc["transaction_id"] = doc.pop("_id")
    return doc


class Investigations:
    def __init__(self, db: Database) -> None:
        self.collection = db.investigations

    def queue(self, status: str, limit: int) -> list[dict[str, Any]]:
        """Newest first; the evidence is left out of the list view."""
        cursor = (
            self.collection.find({"status": status}, {"evidence": 0})
            .sort("created_at", DESCENDING)
            .limit(limit)
        )
        return [_out(d) for d in cursor]

    def get(self, transaction_id: str) -> dict[str, Any] | None:
        return _out(self.collection.find_one({"_id": transaction_id}))

    def decide(self, transaction_id: str, status: str, note: str | None) -> dict[str, Any] | None:
        """Record the analyst's decision; returns the updated investigation."""
        update: dict[str, Any] = {"status": status, "updated_at": datetime.now(UTC)}
        if note is not None:
            update["analyst_note"] = note
        doc = self.collection.find_one_and_update(
            {"_id": transaction_id}, {"$set": update}, return_document=ReturnDocument.AFTER
        )
        return _out(doc)
