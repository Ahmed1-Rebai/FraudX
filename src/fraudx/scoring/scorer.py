"""Scorer: transactions.features -> risk engine -> transactions.scored (+ fraud.alerts).

Per batch (messages in partition order):

1. decode ``FeaturedTransaction`` (invalid → dead-letter queue);
2. transaction feature vectors (``fraudx.ml.vectorize``); graph features and
   known-fraud proximity from Neo4j, one query each
   (``fraudx.storage.graph_features``);
3. one decision per transaction (``scoring.engine.decide``): calibrated
   signals, rules, risk engine;
4. ``ScoredTransaction`` on ``transactions.scored``, and on ``fraud.alerts``
   when the score reaches the alert threshold;
5. the customer's latest risk in Redis (``fx:cust:{C}:risk``).

Degradation (ADR-008): if Neo4j fails, the batch is scored without the graph
signal and without known-fraud rules (the engine reweights and lists the
missing signal) instead of blocking. Models are reloaded when the registry's
active versions change; ``config/risk.yaml`` when the file changes.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import numpy as np
from confluent_kafka import Message
from neo4j.exceptions import DriverError, Neo4jError
from prometheus_client import Counter, Histogram
from redis import Redis

from fraudx.core.config import RiskConfig
from fraudx.core.schemas import FeaturedTransaction, ScoredTransaction, TransactionEvent
from fraudx.core.topics import FRAUD_ALERTS, TRANSACTIONS_SCORED
from fraudx.messaging.consumer import PermanentError, decode
from fraudx.messaging.producer import EventProducer
from fraudx.ml.vectorize import vectorize
from fraudx.scoring.detectors import ActiveModels
from fraudx.scoring.engine import decide
from fraudx.scoring.risk import should_alert

log = logging.getLogger(__name__)

SCORED = Counter("fraudx_scored_total", "Scored transactions by risk level", ["level"])
ALERTS = Counter("fraudx_alerts_total", "Transactions at or above the alert threshold")
RULE_HITS = Counter("fraudx_rule_hits_total", "Rule hits", ["rule"])
MISSING = Counter("fraudx_signal_missing_total", "Transactions scored without a signal", ["signal"])
SCORE = Histogram(
    "fraudx_risk_score",
    "Risk score distribution",
    buckets=(5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
)
STAGE = Histogram(
    "fraudx_scorer_stage_seconds",
    "Scorer time per batch by stage (graph features, known fraud, models + rules, publish)",
    ["stage"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
RISK_TTL_S = 90 * 86_400

BatchQuery = Callable[[list[TransactionEvent]], Any]


@contextmanager
def _timed(stage: str) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        STAGE.labels(stage).observe(time.perf_counter() - started)


class Scorer:
    """Batch handler for ``transactions.features``."""

    def __init__(
        self,
        models: ActiveModels,
        graph_features: BatchQuery | None,
        config: Callable[[], RiskConfig],
        producer: EventProducer,
        redis: Redis | None,
        known_fraud: BatchQuery | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.models, self.graph_features, self.known_fraud = models, graph_features, known_fraud
        self.config, self.producer, self.redis, self._now = config, producer, redis, now

    def __call__(self, messages: list[Message]) -> list[tuple[Message, PermanentError]]:
        items: list[FeaturedTransaction] = []
        rejects: list[tuple[Message, PermanentError]] = []
        for message in messages:
            try:
                items.append(decode(message, FeaturedTransaction))
            except PermanentError as exc:
                rejects.append((message, exc))
        if items:
            self._score(items)
        return rejects

    def _neo4j(self, query: BatchQuery | None, events: list[TransactionEvent], what: str) -> Any:
        """Run a Neo4j batch query; None when unavailable (the batch is scored without it)."""
        if query is None:
            return None
        try:
            with _timed(what):
                return query(events)
        except (Neo4jError, DriverError, OSError) as exc:
            log.warning("%s unavailable; scoring without it", what.replace("_", " "),
                        extra={"error": f"{type(exc).__name__}: {exc}"})  # fmt: skip
            return None

    def _score(self, items: list[FeaturedTransaction]) -> None:
        detectors = self.models.get()
        config = self.config()
        events = [i.event for i in items]
        X = np.asarray([vectorize(i.event, i.features) for i in items], dtype=np.float32)
        rows = (
            self._neo4j(self.graph_features, events, "graph_features")
            if "graph" in detectors
            else None
        )
        G = np.asarray(rows, dtype=np.float32) if rows is not None else None
        known = self._neo4j(self.known_fraud, events, "known_fraud")
        with _timed("models"):
            decisions = decide(detectors, X, G, known, config)

        scored_at = self._now()
        pipe = self.redis.pipeline(transaction=False) if self.redis is not None else None
        with _timed("publish"):
            for item, decision in zip(items, decisions, strict=True):
                risk = decision.risk
                out = ScoredTransaction(
                    event=item.event, features=item.features,
                    graph_features=decision.graph_features, risk=risk, models=decision.models,
                    scored_at=scored_at,
                )  # fmt: skip
                self.producer.send_model(TRANSACTIONS_SCORED, out, key=out.partition_key)
                if should_alert(risk, config):
                    self.producer.send_model(FRAUD_ALERTS, out, key=out.partition_key)
                    ALERTS.inc()
                SCORED.labels(risk.level.value).inc()
                SCORE.observe(risk.score)
                for name in risk.rule_hits:
                    RULE_HITS.labels(name).inc()
                for name in risk.missing_signals:
                    MISSING.labels(name).inc()
                if pipe is not None:
                    key = f"fx:cust:{{{item.event.customer_id}}}:risk"
                    pipe.hset(key, mapping={
                        "score": risk.score, "level": risk.level.value,
                        "transaction_id": item.event.transaction_id,
                        "event_time": item.event.event_time.isoformat(),
                        "scored_at": scored_at.isoformat(),
                    })  # fmt: skip
                    pipe.expire(key, RISK_TTL_S)
            if pipe is not None:
                pipe.execute()
