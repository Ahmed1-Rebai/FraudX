from datetime import UTC, date, datetime

from fastapi.testclient import TestClient

from fraudx.api.app import create_app

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
        return {"customer_id": "C-1", "devices": [], "counterparties": []}


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
        self.closed = False

    def history(self):
        return self.reader

    profiles = graph = risk = history

    def producer(self):
        return self.kafka

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
