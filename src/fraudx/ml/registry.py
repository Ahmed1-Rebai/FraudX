"""Model registry in MongoDB (``model_registry`` collection).

Artifacts stay on disk (``models/<name>/<version>/``); the registry stores
metadata, metrics and lineage, and marks exactly one version per model as
active (partial unique index on ``{name, active: true}``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pymongo.database import Database

COLLECTION = "model_registry"


def register(db: Database, metadata: dict[str, Any], artifact_path: str, activate: bool) -> str:
    doc_id = f"{metadata['name']}:{metadata['version']}"
    doc = {
        "_id": doc_id,
        "name": metadata["name"],
        "version": metadata["version"],
        "artifact_path": artifact_path,
        "metrics": metadata["metrics"],
        "params": metadata["params"],
        "feature_names": metadata["feature_names"],
        "feature_version": metadata["feature_version"],
        "threshold": metadata["threshold"],
        "data": metadata["data"],
        "trained_at": datetime.fromisoformat(metadata["trained_at"]),
        "active": False,
    }
    db[COLLECTION].replace_one({"_id": doc_id}, doc, upsert=True)
    if activate:
        activate_version(db, metadata["name"], metadata["version"])
    return doc_id


def activate_version(db: Database, name: str, version: str) -> None:
    """Deactivate the current version, then activate ``version`` (no replica set,
    so there is a brief moment with no active version; the scorer keeps its
    loaded model until it sees the new one)."""
    collection = db[COLLECTION]
    if collection.count_documents({"_id": f"{name}:{version}"}) == 0:
        raise KeyError(f"{name}:{version} is not registered")
    collection.update_many({"name": name, "active": True}, {"$set": {"active": False}})
    collection.update_one({"_id": f"{name}:{version}"}, {"$set": {"active": True}})


def active(db: Database, name: str) -> dict[str, Any] | None:
    return db[COLLECTION].find_one({"name": name, "active": True})
