import json
from datetime import UTC, date, datetime

from fastapi.testclient import TestClient

from fraudx.api.app import create_app
from fraudx.api.routes.models import EXAMPLE
from fraudx.core.schemas import RiskAssessment, ScoredTransaction

from .test_scorer import CONFIG, DETECTORS, _featured

T = datetime(2026, 2, 1, 9, 30, tzinfo=UTC)
TX = {
    "transaction_id": "TX-1", "customer_id": "C-1", "event_date": date(2026, 2, 1),
    "event_time": T, "source_account": "A-1", "destination_account": "M-1",
    "transaction_type": "PAYMENT", "amount": 12.5, "currency": "TND",
    "is_fraud_label": True,
}  # fmt: skip


class FakeReader:
    def __init__(self):
        self.calls = []

    def transaction(self, transaction_id):
        return TX if transaction_id == "TX-1" else None

    def customer_day(self, customer_id, day, limit):
        self.calls.append((customer_id, day, limit))
        return [TX]

    def customer(self, customer_id):
        if customer_id != "C-1":
            return None
        return {"customer_id": "C-1", "devices": ["D-1"], "last_seen": T}

    def risk_events(self, day, limit):
        self.calls.append(("feed", day, limit))
        return [
            {**TX, "risk_score": 40, "risk_level": "HIGH", "rule_hits": ["takeover_pattern"]},
            {**TX, "transaction_id": "TX-2", "risk_score": 9, "risk_level": "MEDIUM"},
        ]

    def latest(self, customer_id):
        if customer_id != "C-1":
            return None
        return {"score": 40, "level": "HIGH", "transaction_id": "TX-1",
                "event_time": T.isoformat(), "scored_at": T.isoformat()}  # fmt: skip

    def customer_network(self, customer_id, limit):
        if customer_id != "C-1":
            return None
        flow = {"account": "A-1", "counterparty": "A-9", "kind": "CUSTOMER",
                "confirmed_fraud": True, "direction": "out", "count": 2,
                "total_amount": 900.0, "last_seen": T}  # fmt: skip
        return {"customer_id": "C-1", "devices": [], "counterparties": [flow]}


INVESTIGATION = {
    "transaction_id": "TX-1", "customer_id": "C-1", "event_time": T, "status": "OPEN",
    "risk": {"score": 61, "level": "HIGH", "components": {"ml": 0.9},
             "rule_hits": ["takeover_pattern"]},
    "explanation": {"summary": "TRANSFER scored 61/100", "reasons": ["Takeover rule fired."],
                    "reason_evidence": [["rules.takeover_pattern", "features.balance_drain_ratio"]],
                    "generator": "template", "created_at": T},
    "evidence": {"rules.takeover_pattern": True, "features.balance_drain_ratio": 0.95},
    "created_at": T, "updated_at": T,
}  # fmt: skip


class FakeInvestigations:
    def __init__(self):
        self.decisions = []

    def queue(self, status, limit):
        return [INVESTIGATION] if status == "OPEN" else []

    def get(self, tx):
        return dict(INVESTIGATION) if tx == "TX-1" else None

    def counts(self):
        return {"OPEN": 5, "CONFIRMED_FRAUD": 3, "FALSE_POSITIVE": 1, "DISMISSED": 0}

    def set_feedback(self, tx, feedback):
        self.current = {**self.current, "feedback": feedback}
        return self.current

    def decide(self, tx, status, note):
        self.decisions.append((tx, status, note))
        if tx != "TX-1":
            return None
        self.current = {**INVESTIGATION, "status": status, "analyst_note": note}
        return self.current


class FakePrometheus:
    def __init__(self, up=True):
        self.up = up

    def by_label(self, promql, label):
        if not self.up:
            raise OSError("connection refused")
        return {"LOW": 900.0, "MEDIUM": 70.0, "HIGH": 30.0}

    def scalar(self, promql):
        return 12.5 if "risk_score" in promql else 30.0 if "alerts" in promql else 4.2

    def series(self, promql, start, end, step):
        return [(1.0, 3.0), (31.0, 4.0)]


class FakeKafkaMessage:
    def __init__(self, value):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value


class FakeConsumer:
    def __init__(self, payloads):
        self.batches, self.closed = [payloads], False

    def consume(self, n, timeout):
        return [FakeKafkaMessage(v) for v in self.batches.pop(0)] if self.batches else []

    def close(self):
        self.closed = True


class FakeFeedback:
    def __init__(self):
        self.calls = []

    def record(self, transaction, confirmed):
        self.calls.append((transaction["transaction_id"], confirmed))
        return {"account": "A-9", "flagged": confirmed, "confirmations": int(confirmed)}


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send_model(self, topic, model, key=None):
        self.sent.append((topic, model, key))

    def flush(self, timeout=None):
        pass


class FakeStores:
    def __init__(self):
        self.reader = FakeReader()
        self.kafka = FakeProducer()
        self.cases = FakeInvestigations()
        self.graph_feedback = FakeFeedback()
        self.models = {}
        self.prom = FakePrometheus()
        self.consumer = FakeConsumer([])
        self.closed = False

    def history(self):
        return self.reader

    profiles = graph = risk = history

    def producer(self):
        return self.kafka

    def investigations(self):
        return self.cases

    def feedback(self):
        return self.graph_feedback

    def detectors(self):
        return self.models

    def config(self):
        return CONFIG

    def prometheus(self):
        return self.prom

    def stream_consumer(self):
        return self.consumer

    def close(self):
        self.closed = True


class FakeProbe:
    async def check_all(self):
        return []

    def close(self):
        pass


def _client():
    stores = FakeStores()
    return TestClient(create_app(probe=FakeProbe(), stores=stores)), stores


def test_transaction_found_without_ground_truth():
    client, stores = _client()
    with client:
        body = client.get("/transactions/TX-1").json()
        missing = client.get("/transactions/TX-404")
    assert body["amount"] == 12.5 and "is_fraud_label" not in body
    assert missing.status_code == 404
    assert stores.closed  # lifespan shutdown releases the store clients


def test_customer_day_defaults_to_last_active_day():
    client, stores = _client()
    with client:
        body = client.get("/customers/C-1/transactions").json()
        client.get("/customers/C-1/transactions", params={"day": "2026-01-15", "limit": 5})
    assert body["day"] == "2026-02-01" and len(body["transactions"]) == 1
    assert stores.reader.calls == [("C-1", date(2026, 2, 1), 50), ("C-1", date(2026, 1, 15), 5)]


def test_unknown_customer_and_invalid_limit():
    client, _ = _client()
    with client:
        assert client.get("/customers/C-404").status_code == 404
        assert client.get("/customers/C-404/network").status_code == 404
        # the analyst's confirmation mark reaches the dashboard
        assert client.get("/customers/C-1/network").json()["counterparties"][0]["confirmed_fraud"]
        assert client.get("/customers/C-1/transactions?limit=0").status_code == 422
        assert client.get("/customers/C-1").json()["devices"] == ["D-1"]


def test_alert_feed_and_level_filter():
    client, stores = _client()
    with client:
        body = client.get("/fraud/alerts", params={"day": "2026-02-01"}).json()
        high = client.get("/fraud/alerts", params={"day": "2026-02-01", "level": "HIGH"}).json()
        assert client.get("/fraud/alerts", params={"level": "LOW"}).status_code == 422
    assert [e["transaction_id"] for e in body["events"]] == ["TX-1", "TX-2"]
    assert [e["risk_level"] for e in high["events"]] == ["HIGH"]
    assert ("feed", date(2026, 2, 1), 100) in stores.reader.calls


def test_customer_risk_from_redis():
    client, _ = _client()
    with client:
        assert client.get("/customers/C-1/risk").json()["level"] == "HIGH"
        assert client.get("/customers/C-404/risk").status_code == 404


def test_simulate_publishes_a_valid_event_to_the_raw_topic():
    client, stores = _client()
    body = {"customer_id": "C-1", "source_account": "A-1", "destination_account": "A-9",
            "amount": 2500, "device_id": "D-1"}  # fmt: skip
    with client:
        resp = client.post("/transactions/simulate", json=body)
        bad = client.post("/transactions/simulate", json={**body, "amount": -1})
    assert resp.status_code == 202 and bad.status_code == 422
    topic, event, key = stores.kafka.sent[0]
    assert (topic, key) == ("transactions.raw", b"C-1")
    assert event.transaction_id == resp.json()["transaction_id"].strip()
    assert event.transaction_id.startswith("SIM-") and event.is_fraud_label is None


def test_investigation_queue_detail_and_cited_explanation():
    client, _ = _client()
    with client:
        queue = client.get("/fraud/investigations").json()
        detail = client.get("/fraud/TX-1").json()
        explanation = client.get("/fraud/TX-1/explanation").json()
        assert client.get("/fraud/TX-404").status_code == 404
    assert [q["transaction_id"] for q in queue] == ["TX-1"] and "evidence" not in queue[0]
    assert detail["evidence"]["features.balance_drain_ratio"] == 0.95
    assert explanation["reasons"][0] == {
        "text": "Takeover rule fired.",
        "evidence": {"rules.takeover_pattern": True, "features.balance_drain_ratio": 0.95},
    }


def test_analyst_decision():
    client, stores = _client()
    with client:
        resp = client.patch("/fraud/TX-1", json={"status": "FALSE_POSITIVE", "note": "travel"})
        bad = client.patch("/fraud/TX-1", json={"status": "MAYBE"})
        missing = client.patch("/fraud/TX-404", json={"status": "DISMISSED"})
    assert resp.json()["status"] == "FALSE_POSITIVE" and resp.json()["analyst_note"] == "travel"
    assert (bad.status_code, missing.status_code) == (422, 404)
    assert stores.cases.decisions[0] == ("TX-1", "FALSE_POSITIVE", "travel")


def test_overview_kpis_and_analyst_precision():
    client, stores = _client()
    with client:
        body = client.get("/stats/overview").json()
        stores.prom.up = False
        degraded = client.get("/stats/overview").json()
    assert body["scored"] == 1000 and body["by_level"]["HIGH"] == 30
    assert (body["alerts"], body["avg_risk_score"], body["transactions_per_s"]) == (30.0, 12.5, 4.2)
    assert body["confirmed_precision"] == 0.75 and len(body["throughput"]) == 2
    assert degraded["metrics_available"] is False and degraded["investigations"]["OPEN"] == 5


def test_live_stream_sends_compact_scored_events_and_closes_its_consumer():
    item = _featured(1, amount=5000)
    risk = RiskAssessment(score=84, level="HIGH", components={"ml": 1.0}, assessed_at=T)
    scored = ScoredTransaction(event=item.event, features=item.features, risk=risk, scored_at=T)
    client, stores = _client()
    stores.consumer = FakeConsumer([scored.model_dump_json().encode()])
    with client, client.stream("GET", "/stream/scored?limit=1") as resp:
        lines = [line for line in resp.iter_lines() if line.startswith("data: ")]
    assert resp.headers["content-type"].startswith("text/event-stream")
    event = json.loads(lines[0][len("data: ") :])
    assert (event["transaction_id"], event["score"], event["level"]) == ("TX-1", 84, "HIGH")
    assert stores.consumer.closed


def test_store_outages_answer_503_naming_the_store():
    from cassandra.cluster import NoHostAvailable
    from neo4j.exceptions import ServiceUnavailable
    from pymongo.errors import ServerSelectionTimeoutError
    from redis.exceptions import ConnectionError as RedisConnectionError

    outages = {
        "history": (NoHostAvailable("down", {}), "/transactions/TX-1", "Cassandra"),
        "profiles": (ServerSelectionTimeoutError("down"), "/customers/C-1", "MongoDB"),
        "graph": (ServiceUnavailable("down"), "/customers/C-1/network", "Neo4j"),
        "risk": (RedisConnectionError("down"), "/customers/C-1/risk", "Redis"),
    }
    for method, (error, path, store) in outages.items():
        client, stores = _client()

        def broken(*args, error=error):
            raise error

        setattr(stores, method, broken)
        with client:
            resp = client.get(path)
        assert resp.status_code == 503, path
        assert store in resp.json()["detail"]


def test_simulate_rejects_what_the_pipeline_would_reject():
    client, stores = _client()
    body = {"customer_id": "C-1", "source_account": "A-1", "destination_account": "A-9",
            "amount": 2500}  # fmt: skip
    with client:
        same = client.post("/transactions/simulate", json={**body, "destination_account": "A-1"})
        huge = client.post("/transactions/simulate", json={**body, "amount": 10**20})
    assert (same.status_code, huge.status_code) == (422, 422)
    assert stores.kafka.sent == []


def test_live_stream_skips_an_unreadable_message():
    item = _featured(2, amount=10)
    risk = RiskAssessment(score=1, level="LOW", components={"ml": 0.0}, assessed_at=T)
    good = ScoredTransaction(event=item.event, features=item.features, risk=risk, scored_at=T)
    client, stores = _client()
    stores.consumer = FakeConsumer(
        [b'{"not": "a scored transaction"}', good.model_dump_json().encode()]
    )
    with client, client.stream("GET", "/stream/scored?limit=1") as resp:
        lines = [line for line in resp.iter_lines() if line.startswith("data: ")]
    assert json.loads(lines[0][len("data: ") :])["transaction_id"] == "TX-2"


def test_decision_feeds_the_graph():
    client, stores = _client()
    with client:
        confirmed = client.patch("/fraud/TX-1", json={"status": "CONFIRMED_FRAUD"}).json()
        client.patch("/fraud/TX-1", json={"status": "FALSE_POSITIVE"})
    assert confirmed["feedback"] == {"account": "A-9", "flagged": True, "confirmations": 1}
    assert stores.graph_feedback.calls == [("TX-1", True), ("TX-1", False)]


def test_predict_returns_the_scorer_decision_with_cited_reasons():
    client, stores = _client()
    stores.models = DETECTORS
    with client:
        resp = client.post("/models/predict", json=EXAMPLE)
        partial_graph = client.post(
            "/models/predict", json={**EXAMPLE, "graph_features": {"is_new_payee": 1}}
        )
        stores.models = {}
        no_models = client.post("/models/predict", json=EXAMPLE)
    body = resp.json()
    assert resp.status_code == 200 and body["level"] == "HIGH"  # 9,200 > 1,000: fake ML says fraud
    assert set(body["models"]) == {"ml", "anomaly", "graph"} and body["missing_signals"] == []
    assert "takeover_pattern" in body["rule_hits"]  # new device abroad drains 97 %
    assert body["reasons"] and all(r["evidence"] for r in body["reasons"])
    assert (partial_graph.status_code, no_models.status_code) == (422, 503)
