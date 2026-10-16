"""Alert handler and investigations against the real MongoDB (and its validator)."""

import uuid
from datetime import UTC, datetime

import pytest

from fraudx.alerts.handler import AlertHandler
from fraudx.core.config import MongoSettings
from fraudx.explain.template import TemplateExplainer
from fraudx.storage import clients
from fraudx.storage.investigations import Investigations
from tests.unit.test_alerts import Msg
from tests.unit.test_explain import CONFIG, _scored

pytestmark = pytest.mark.integration


@pytest.fixture
def db():
    settings = MongoSettings()
    client = clients.mongo_client(settings)
    yield client[settings.db]
    client.close()


def test_alert_to_investigation_queue_and_decision(db):
    scored = _scored(["takeover_pattern"])
    tx = f"TX-it-{uuid.uuid4().hex[:8]}"
    scored = scored.model_copy(
        update={"event": scored.event.model_copy(update={"transaction_id": tx})}
    )
    try:
        AlertHandler(db, TemplateExplainer(), CONFIG, now=lambda: datetime.now(UTC))([Msg(scored)])
        store = Investigations(db)
        doc = store.get(tx)
        assert doc["status"] == "OPEN" and doc["evidence"]["rules.takeover_pattern"] is True
        assert tx in [d["transaction_id"] for d in store.queue("OPEN", 500)]
        decided = store.decide(tx, "CONFIRMED_FRAUD", "called the customer")
        assert (decided["status"], decided["analyst_note"]) == (
            "CONFIRMED_FRAUD",
            "called the customer",
        )
        assert store.decide("TX-does-not-exist", "DISMISSED", None) is None
    finally:
        db.investigations.delete_one({"_id": tx})
