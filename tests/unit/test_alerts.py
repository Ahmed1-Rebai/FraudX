from datetime import UTC, datetime

from fraudx.alerts.handler import AlertHandler
from fraudx.explain.template import TemplateExplainer
from fraudx.messaging.consumer import PermanentError

from .test_explain import CONFIG, _scored

NOW = datetime(2026, 1, 10, tzinfo=UTC)


class Msg:
    def __init__(self, model):
        self._value = model.model_dump_json().encode() if model else b"{bad"

    def value(self):
        return self._value


class FakeCursor(list):
    def sort(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self


class FakeCollection:
    def __init__(self, docs=()):
        self.docs = {d["_id"]: d for d in docs}
        self.writes = []

    def find(self, query, projection=None):
        if "_id" in query and "$in" in query["_id"]:
            return FakeCursor([self.docs[i] for i in query["_id"]["$in"] if i in self.docs])
        if "status" in query and "$in" in query["status"]:
            return FakeCursor(
                [d for d in self.docs.values() if d.get("status") in query["status"]["$in"]]
            )
        return FakeCursor(list(self.docs.values()))

    def bulk_write(self, ops, ordered):
        for op in ops:
            doc = op._doc["$setOnInsert"]
            self.docs.setdefault(doc["_id"], doc)
            self.writes.append(doc["_id"])


class FakeDb:
    def __init__(self, investigations=()):
        self.investigations = FakeCollection(investigations)
        self.customers = FakeCollection([{"_id": "C-42", "country": "TN", "devices": ["D-1"]}])


def test_new_alert_opens_an_explained_investigation_once():
    db, sent = FakeDb(), []
    handler = AlertHandler(db, TemplateExplainer(), CONFIG, webhook_url="http://hook",
                           notify=lambda url, p: sent.append(p), now=lambda: NOW)  # fmt: skip
    bad = Msg(None)
    rejects = handler([Msg(_scored(["takeover_pattern"])), bad])
    assert [m for m, _ in rejects] == [bad] and isinstance(rejects[0][1], PermanentError)
    doc = db.investigations.docs["TX-7"]
    assert (doc["status"], doc["risk"]["score"], doc["explanation"]["generator"]) == (
        "OPEN", 61, "template")  # fmt: skip
    assert doc["evidence"]["profile.home_country"] == "TN"
    assert len(doc["explanation"]["reasons"]) == len(doc["explanation"]["reason_evidence"])
    assert sent[0]["transaction_id"] == "TX-7" and sent[0]["level"] == "HIGH"
    text = sent[0]["text"]  # Slack-ready, in the spec's format
    assert text.startswith("🚨 HIGH RISK TRANSACTION")
    assert "Amount: 900.00 TND" in text and "Risk Score: 61/100" in text
    assert "- Account-takeover pattern" in text
    handler([Msg(_scored(["takeover_pattern"]))])  # redelivery
    assert db.investigations.writes == ["TX-7"] and len(sent) == 1


def test_alert_handler_cites_similar_precedent():
    past_fraud = {
        "_id": "TX-OLD",
        "status": "CONFIRMED_FRAUD",
        "evidence": {
            "features.amount_to_mean_ratio": 9.0,
            "features.balance_drain_ratio": 0.95,
            "features.is_new_device": True,
            "rules.takeover_pattern": True,
            "signals.ml": 0.9,
        },
        "risk": {"score": 75},
        "analyst_note": "Account takeover verified",
    }
    db = FakeDb(investigations=[past_fraud])
    handler = AlertHandler(db, TemplateExplainer(), CONFIG, webhook_url=None, now=lambda: NOW)
    handler([Msg(_scored(["takeover_pattern"]))])
    doc = db.investigations.docs["TX-7"]
    assert "precedents" in doc and len(doc["precedents"]) == 1
    assert doc["precedents"][0]["transaction_id"] == "TX-OLD"
    assert doc["precedents"][0]["similarity"] >= 0.85
    assert any("TX-OLD" in r for r in doc["explanation"]["reasons"])


def test_existing_investigation_keeps_the_analyst_decision():
    decided = {"_id": "TX-7", "status": "FALSE_POSITIVE"}
    db = FakeDb([decided])
    AlertHandler(db, TemplateExplainer(), CONFIG)([Msg(_scored())])
    assert db.investigations.docs["TX-7"]["status"] == "FALSE_POSITIVE"
    assert db.investigations.writes == []


def test_webhook_failure_does_not_lose_the_investigation():
    def broken(url, payload):
        raise OSError("connection refused")

    db = FakeDb()
    AlertHandler(db, TemplateExplainer(), CONFIG, webhook_url="http://hook", notify=broken)(
        [Msg(_scored())]
    )
    assert "TX-7" in db.investigations.docs
