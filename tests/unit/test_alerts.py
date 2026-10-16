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


class FakeCollection:
    def __init__(self, docs=()):
        self.docs = {d["_id"]: d for d in docs}
        self.writes = []

    def find(self, query, projection=None):
        return [self.docs[i] for i in query["_id"]["$in"] if i in self.docs]

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
    handler([Msg(_scored(["takeover_pattern"]))])  # redelivery
    assert db.investigations.writes == ["TX-7"] and len(sent) == 1


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
