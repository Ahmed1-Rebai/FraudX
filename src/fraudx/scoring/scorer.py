"""Scorer: transactions.features -> risk engine -> transactions.scored (+ fraud.alerts).

Per batch (messages in partition order):

1. decode ``FeaturedTransaction`` (invalid → dead-letter queue);
2. transaction feature vectors (``fraudx.ml.vectorize``) and graph features
   from Neo4j in one query (``fraudx.storage.graph_features``);
3. calibrated signals from the active detectors, and rule hits;
4. ``scoring.risk.assess`` → ``ScoredTransaction`` on ``transactions.scored``,
   and on ``fraud.alerts`` when the score reaches the alert threshold;
5. the customer's latest risk in Redis (``fx:cust:{C}:risk``).

Degradation (ADR-008): if Neo4j fails, the batch is scored without the graph
signal (the engine reweights and lists it in ``missing_signals``) instead of
blocking. Models are reloaded when the registry's active versions change.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime

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
from fraudx.scoring import rules
from fraudx.scoring.detectors import Detector
from fraudx.scoring.risk import RiskSignals, RuleHit, assess, floor, should_alert
from fraudx.storage.graph_features import GRAPH_FEATURE_NAMES

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
RISK_TTL_S = 90 * 86_400

GraphFeatures = Callable[[list[TransactionEvent]], list[list[float]]]


class ActiveModels:
    """The active detectors, reloaded when the registry's active versions change."""

    def __init__(
        self,
        active_ids: Callable[[], dict[str, str]],
        load: Callable[[], dict[str, Detector]],
        check_every_s: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._active_ids, self._load, self._every, self._clock = (
            active_ids,
            load,
            check_every_s,
            clock,
        )
        self._ids: dict[str, str] | None = None
        self._detectors: dict[str, Detector] = {}
        self._checked = -float("inf")

    def get(self) -> dict[str, Detector]:
        if self._clock() - self._checked >= self._every:
            self._checked = self._clock()
            try:
                ids = self._active_ids()
                if ids != self._ids:
                    self._detectors, self._ids = self._load(), ids
                    log.info("models loaded", extra={"models": ids})
            except Exception:
                log.exception("model reload failed; keeping the loaded models")
        return self._detectors


class Scorer:
    """Batch handler for ``transactions.features``."""

    def __init__(
        self,
        models: ActiveModels,
        graph_features: GraphFeatures | None,
        config: RiskConfig,
        producer: EventProducer,
        redis: Redis | None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.models, self.graph_features, self.config = models, graph_features, config
        self.producer, self.redis, self._now = producer, redis, now

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

    def _graph(self, events: list[TransactionEvent]) -> np.ndarray | None:
        if self.graph_features is None:
            return None
        try:
            return np.asarray(self.graph_features(events), dtype=np.float32)
        except (Neo4jError, DriverError, OSError) as exc:
            log.warning("graph features unavailable; scoring without the graph signal",
                        extra={"error": f"{type(exc).__name__}: {exc}"})  # fmt: skip
            return None

    def _score(self, items: list[FeaturedTransaction]) -> None:
        detectors = self.models.get()
        events = [i.event for i in items]
        X = np.asarray([vectorize(i.event, i.features) for i in items], dtype=np.float32)
        G = self._graph(events) if "graph" in detectors else None
        signals: dict[str, np.ndarray] = {}
        for name, det in detectors.items():
            if det.inputs == "graph":
                if G is not None:
                    signals[name] = det.score(G)
            else:
                signals[name] = det.score(X)
        models = {name: det.model for name, det in detectors.items() if name in signals}
        scored_at = self._now()
        pipe = self.redis.pipeline(transaction=False) if self.redis is not None else None

        for i, item in enumerate(items):
            hits = tuple(
                RuleHit(name, floor(level, self.config))
                for name, level in rules.evaluate(rules.as_values(X[i]))
            )
            risk = assess(
                RiskSignals(**{k: float(v[i]) for k, v in signals.items()}), self.config, hits
            )
            graph = (
                dict(zip(GRAPH_FEATURE_NAMES, G[i].tolist(), strict=True))
                if G is not None
                else None
            )
            out = ScoredTransaction(
                event=item.event, features=item.features, graph_features=graph, risk=risk,
                models=models, scored_at=scored_at,
            )  # fmt: skip
            self.producer.send_model(TRANSACTIONS_SCORED, out, key=out.partition_key)
            if should_alert(risk, self.config):
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
