"""Builds the graph-feature matrix by replaying a dataset into the offline Neo4j.

Events are replayed in event-time order into an empty graph (compose service
``neo4j-offline``) with the production ``GraphWriter``. Before each group of
events is written, ``graph_features`` (the same query the scorer runs) reads
their features, so every transaction sees the graph of its past only.
A group spans at most one minute of event time (and 500 events), close to
the live graph writer's lag: a transaction may miss others from its own minute.

Output ``graph_features_v<GRAPH_FEATURE_VERSION>.npz`` next to the dataset,
row-aligned with ``features_v<FEATURE_VERSION>.npz`` (same event order).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from pathlib import Path

import numpy as np
from neo4j import Driver

from fraudx.core.schemas import TransactionEvent
from fraudx.ingestion.dataset import EVENTS_FILE, read_events
from fraudx.ml.dataset import FeatureMatrix, read_ground_truth
from fraudx.storage.graph_features import (
    GRAPH_FEATURE_NAMES,
    GRAPH_FEATURE_VERSION,
    graph_features,
)
from fraudx.storage.graph_writer import GraphWriter

log = logging.getLogger(__name__)

SCHEMA = Path("infra/neo4j/schema.cypher")
WINDOW_MS = 60_000
MAX_GROUP = 500


def graph_matrix_path(dataset_dir: Path) -> Path:
    return dataset_dir / f"graph_features_v{GRAPH_FEATURE_VERSION}.npz"


def _ms(event: TransactionEvent) -> int:
    return int(event.event_time.timestamp() * 1000)


def time_groups(events: Iterable[TransactionEvent]) -> Iterator[list[TransactionEvent]]:
    """Consecutive events within one minute of the group's first event (max 500)."""
    group: list[TransactionEvent] = []
    start = 0
    for event in events:
        if group and (_ms(event) - start >= WINDOW_MS or len(group) >= MAX_GROUP):
            yield group
            group = []
        if not group:
            start = _ms(event)
        group.append(event)
    if group:
        yield group


def reset(driver: Driver) -> None:
    """Empty the offline graph and apply the production schema."""
    with driver.session() as session:
        session.run(
            "MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"
        ).consume()
    lines = [ln for ln in SCHEMA.read_text().splitlines() if not ln.lstrip().startswith("//")]
    for statement in "\n".join(lines).split(";"):
        if statement.strip():
            driver.execute_query(statement)


def build(dataset_dir: Path, driver: Driver) -> FeatureMatrix:
    truth = read_ground_truth(dataset_dir)
    reset(driver)
    writer = GraphWriter(driver)
    rows: list[list[float]] = []
    labels, times, ids, scenarios = [], [], [], []
    for group in time_groups(read_events(dataset_dir / EVENTS_FILE)):
        for event, features in zip(group, graph_features(driver, group), strict=True):
            label, scenario = truth[event.transaction_id]
            rows.append(features)
            labels.append(label)
            times.append(_ms(event))
            ids.append(event.transaction_id)
            scenarios.append(scenario)
        writer.write(group)
        if len(rows) % 250_000 < len(group):
            log.info("graph features built for %d events", len(rows))
    return FeatureMatrix(
        X=np.asarray(rows, dtype=np.float32).reshape(-1, len(GRAPH_FEATURE_NAMES)),
        y=np.asarray(labels, dtype=np.int8),
        event_ms=np.asarray(times, dtype=np.int64),
        transaction_id=np.asarray(ids),
        scenario=np.asarray(scenarios),
        feature_names=GRAPH_FEATURE_NAMES,
    )
