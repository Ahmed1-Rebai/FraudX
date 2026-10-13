"""Builds a training matrix with the *online* feature code.

Events of a dataset directory are replayed in event-time order through
``OnlineFeatureStore`` against the disposable offline Redis (compose service
``redis-offline``), never the live one. The features are exactly what the
feature processor would compute: no training/serving skew. The
per-transaction result cache is disabled (nothing is redelivered offline,
and it would double memory).

Output ``features_v<FEATURE_VERSION>.npz`` (next to the dataset):
``X`` (float32, NaN = missing), ``y`` (0/1), ``event_ms`` (int64),
``transaction_id``, ``scenario``, ``feature_names``.
"""

from __future__ import annotations

import csv
import hashlib
import logging
from dataclasses import dataclass
from itertools import batched
from pathlib import Path

import numpy as np
from redis import Redis

from fraudx.core.schemas import FEATURE_VERSION
from fraudx.ingestion.dataset import EVENTS_FILE, GROUND_TRUTH_FILE, read_events
from fraudx.ml.vectorize import FEATURE_NAMES, vectorize
from fraudx.streaming.features import OnlineFeatureStore

log = logging.getLogger(__name__)


def matrix_path(dataset_dir: Path) -> Path:
    return dataset_dir / f"features_v{FEATURE_VERSION}.npz"


@dataclass(frozen=True)
class FeatureMatrix:
    X: np.ndarray
    y: np.ndarray
    event_ms: np.ndarray
    transaction_id: np.ndarray
    scenario: np.ndarray
    feature_names: tuple[str, ...]

    @classmethod
    def load(cls, path: Path) -> FeatureMatrix:
        with np.load(path, allow_pickle=False) as data:
            names = tuple(str(n) for n in data["feature_names"])
            if names != FEATURE_NAMES:
                raise ValueError(f"{path} was built with different features; rebuild it")
            return cls(
                X=data["X"],
                y=data["y"],
                event_ms=data["event_ms"],
                transaction_id=data["transaction_id"],
                scenario=data["scenario"],
                feature_names=names,
            )

    def save(self, path: Path) -> None:
        tmp = path.with_name(path.stem + ".tmp.npz")
        np.savez_compressed(
            tmp,
            X=self.X,
            y=self.y,
            event_ms=self.event_ms,
            transaction_id=self.transaction_id,
            scenario=self.scenario,
            feature_names=np.array(self.feature_names),
        )
        tmp.replace(path)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build(dataset_dir: Path, redis: Redis, batch_size: int = 500) -> FeatureMatrix:
    """Compute features for every event of ``dataset_dir`` (Redis db is flushed)."""
    truth: dict[str, tuple[int, str]] = {}
    with (dataset_dir / GROUND_TRUTH_FILE).open(newline="") as fh:
        for row in csv.DictReader(fh):
            truth[row["transaction_id"]] = (int(row["is_fraud"]), row["scenario"])

    redis.flushdb()
    store = OnlineFeatureStore(redis, cache_results=False)
    rows: list[list[float]] = []
    labels, times, ids, scenarios = [], [], [], []
    try:
        for chunk in batched(read_events(dataset_dir / EVENTS_FILE), batch_size):
            events = list(chunk)
            for event, features in zip(events, store.compute_many(events), strict=True):
                label, scenario = truth[event.transaction_id]
                rows.append(vectorize(event, features))
                labels.append(label)
                times.append(int(event.event_time.timestamp() * 1000))
                ids.append(event.transaction_id)
                scenarios.append(scenario)
            if len(rows) % 250_000 < batch_size:
                log.info("features built for %d events", len(rows))
    finally:
        redis.flushdb()  # offline state is disposable; free the memory

    return FeatureMatrix(
        X=np.asarray(rows, dtype=np.float32),
        y=np.asarray(labels, dtype=np.int8),
        event_ms=np.asarray(times, dtype=np.int64),
        transaction_id=np.asarray(ids),
        scenario=np.asarray(scenarios),
        feature_names=FEATURE_NAMES,
    )
