from datetime import UTC, datetime

from fraudx.core.schemas import FraudDecisionEvent, InvestigationStatus
from fraudx.storage.feedback import FeedbackHandler


class FakeGraphFeedback:
    def __init__(self):
        self.recorded = []

    def record(self, transaction, confirmed):
        self.recorded.append((transaction, confirmed))
        return {
            "account": transaction["destination_account"],
            "flagged": confirmed,
            "confirmations": 1 if confirmed else 0,
        }


class FakeInvestigations:
    def __init__(self):
        self.feedbacks = {}

    def get(self, transaction_id):
        return None

    def set_feedback(self, transaction_id, feedback):
        self.feedbacks[transaction_id] = feedback
        return {"_id": transaction_id, "feedback": feedback}


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def value(self):
        return self._value

    def error(self):
        return None


def test_feedback_handler_processes_confirmed_fraud():
    gf = FakeGraphFeedback()
    inv = FakeInvestigations()
    handler = FeedbackHandler(gf, inv)

    event = FraudDecisionEvent(
        transaction_id="TX-55",
        status=InvestigationStatus.CONFIRMED_FRAUD,
        customer_id="C-55",
        source_account="A-1",
        destination_account="A-2",
        transaction_type="TRANSFER",
        decided_at=datetime.now(UTC),
    )
    msg = FakeMessage(event.model_dump_json().encode("utf-8"))
    rejects = handler([msg])

    assert rejects == []
    assert len(gf.recorded) == 1
    tx_data, confirmed = gf.recorded[0]
    assert tx_data["destination_account"] == "A-2"
    assert confirmed is True
    assert inv.feedbacks["TX-55"]["flagged"] is True


def test_feedback_handler_rejects_corrupted_message():
    gf = FakeGraphFeedback()
    handler = FeedbackHandler(gf)

    bad_msg = FakeMessage(b"not json at all")
    rejects = handler([bad_msg])
    assert len(rejects) == 1
    assert rejects[0][0] is bad_msg
