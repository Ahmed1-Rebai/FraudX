from datetime import UTC, datetime

import numpy as np
from neo4j.exceptions import ServiceUnavailable

from fraudx.core.config import KafkaSettings, RiskConfig
from fraudx.core.schemas import (
    FeaturedTransaction,
    RiskLevel,
    ScoredTransaction,
    TransactionEvent,
    TransactionFeatures,
)
from fraudx.messaging.consumer import PermanentError
from fraudx.messaging.producer import EventProducer
from fraudx.ml.vectorize import FEATURE_NAMES
from fraudx.scoring.detectors import ActiveModels, Detector
from fraudx.scoring.scorer import Scorer

CONFIG = RiskConfig.model_validate({
    "weights": {"ml": 0.5, "anomaly": 0.1, "graph": 0.4},
    "thresholds": {"medium": 8, "high": 23}, "alert_threshold": 23,
})  # fmt: skip
T = datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
AMOUNT = FEATURE_NAMES.index("amount_log")


def _featured(n, amount=10, tx_type="PAYMENT", dest="M-1", **features):
    event = TransactionEvent.model_validate({
        "transaction_id": f"TX-{n}", "event_time": T, "customer_id": "C-1",
        "source_account": "A-1", "destination_account": dest, "amount": amount,
        "transaction_type": tx_type, "device_id": "D-1",
    })  # fmt: skip
    base = {
        "cust_txn_count_5m": 1, "cust_txn_count_1h": 1, "cust_txn_count_24h": 1,
        "cust_amount_sum_5m": amount, "cust_amount_sum_1h": amount, "cust_amount_sum_24h": amount,
        "cust_history_count": 3, "is_new_device": False, "is_new_ip": False,
        "is_new_country": False, "hour_of_day": 3,
    }  # fmt: skip
    features = TransactionFeatures.model_validate({**base, **features})
    return FeaturedTransaction(event=event, features=features, processed_at=T)


class Msg:
    def __init__(self, model):
        self._value = model.model_dump_json().encode() if model else b"junk"

    def value(self):
        return self._value


class FakeKafka:
    def __init__(self):
        self.produced = []

    def produce(self, topic, value, key=None, headers=None, on_delivery=None):
        self.produced.append((topic, ScoredTransaction.model_validate_json(value)))

    def poll(self, timeout):
        return 0


class FakeRedis:
    def __init__(self):
        self.data = {}

    def pipeline(self, transaction=False):
        return self

    def hset(self, key, mapping):
        self.data[key] = mapping

    def expire(self, key, ttl):
        pass

    def execute(self):
        pass


def _detector(signal, inputs, fn):
    return Detector(signal, f"{signal}_model:v1", inputs, fn, {"x": [0.0, 1.0], "y": [0.0, 1.0]})


# ml: big amounts are fraud; anomaly: always low; graph: first graph feature / 10.
DETECTORS = {
    "ml": _detector("ml", "transaction", lambda X: (X[:, AMOUNT] > np.log1p(1000)).astype(float)),
    "anomaly": _detector("anomaly", "transaction", lambda X: np.full(len(X), 0.01)),
    "graph": _detector("graph", "graph", lambda G: G[:, 0] / 10),
}


def _scorer(graph=lambda events: [[0.0] * 6 for _ in events], known=None):
    kafka, redis = FakeKafka(), FakeRedis()
    models = ActiveModels(lambda: {"v": "1"}, lambda: DETECTORS)
    producer = EventProducer(KafkaSettings(), "t", producer=kafka)
    scorer = Scorer(models, graph, lambda: CONFIG, producer, redis, known_fraud=known,
                    now=lambda: T)  # fmt: skip
    return scorer, kafka, redis


def test_scores_every_transaction_and_alerts_only_high_risk():
    scorer, kafka, redis = _scorer()
    bad = Msg(None)
    rejects = scorer([Msg(_featured(1)), Msg(_featured(2, amount=5000)), bad])
    assert [m for m, _ in rejects] == [bad] and isinstance(rejects[0][1], PermanentError)
    scored = [o for t, o in kafka.produced if t == "transactions.scored"]
    alerts = [o for t, o in kafka.produced if t == "fraud.alerts"]
    assert [o.risk.level for o in scored] == [RiskLevel.LOW, RiskLevel.HIGH]
    assert [o.event.transaction_id for o in alerts] == ["TX-2"]
    assert alerts[0].risk.score == 50  # (0.5 * 1 + 0.1 * 0.01 + 0.4 * 0) / 1.0
    assert alerts[0].models["ml"] == "ml_model:v1" and alerts[0].graph_features is not None
    assert redis.data["fx:cust:{C-1}:risk"]["transaction_id"] == "TX-2"  # latest risk


def test_graph_outage_scores_without_the_graph_signal():
    def down(events):
        raise ServiceUnavailable("neo4j down")

    scorer, kafka, _ = _scorer(graph=down)
    scorer([Msg(_featured(1, amount=5000))])
    out = kafka.produced[0][1]
    assert out.risk.missing_signals == ("graph",) and out.graph_features is None
    assert out.risk.score == 84  # (0.5 * 1 + 0.1 * 0.01) / 0.6: weights renormalised
    assert "graph" not in out.models


def test_rule_floor_raises_a_low_model_score():
    scorer, kafka, _ = _scorer()
    scorer([Msg(_featured(1, cust_txn_count_5m=6, device_customer_count=3))])
    out = kafka.produced[0][1]
    assert out.risk.rule_hits == ("burst_on_shared_device",)
    assert (out.risk.score, out.risk.level) == (8, RiskLevel.MEDIUM)


def test_models_reload_only_when_active_versions_change():
    ids, loads, now = {"fraud_lgbm": "v1"}, [], [0.0]

    def load():
        loads.append(dict(ids))
        return {}

    models = ActiveModels(lambda: dict(ids), load, check_every_s=60, clock=lambda: now[0])
    models.get()
    now[0] = 30
    ids["fraud_lgbm"] = "v2"
    models.get()  # not due yet
    now[0] = 61
    models.get()
    models.get()
    assert loads == [{"fraud_lgbm": "v1"}, {"fraud_lgbm": "v2"}]


def test_first_transfer_to_a_confirmed_payee_goes_to_review_with_its_evidence():
    def known(events):
        return [{"distance_to_known_fraud": 0.0, "fraud_neighbor_count": 2.0} for _ in events]

    first_payment = [[0.0, 0.0, 0.0, 1.0, 0.0, 0.0]]  # is_new_payee = 1
    scorer, kafka, _ = _scorer(graph=lambda events: first_payment * len(events), known=known)
    scorer([Msg(_featured(1, tx_type="TRANSFER", dest="A-9"))])  # harmless for the models
    out = kafka.produced[0][1]
    assert out.risk.rule_hits == ("payee_confirmed_fraud",)
    assert (out.risk.score, out.risk.level) == (8, RiskLevel.MEDIUM)  # floor = review threshold
    assert out.graph_features["distance_to_known_fraud"] == 0.0
    assert out.graph_features["fraud_neighbor_count"] == 2.0
    assert out.graph_features["is_new_payee"] == 1.0


def test_known_fraud_outage_only_disables_its_rules():
    def down(events):
        raise ServiceUnavailable("neo4j down")

    scorer, kafka, _ = _scorer(known=down)
    scorer([Msg(_featured(1))])
    out = kafka.produced[0][1]
    assert out.risk.rule_hits == () and out.risk.missing_signals == ()
