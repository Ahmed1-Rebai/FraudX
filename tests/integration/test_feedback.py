"""Analyst feedback in the real Neo4j: confirmation marks and known-fraud proximity."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from fraudx.core.config import Neo4jSettings
from fraudx.core.schemas import TransactionEvent
from fraudx.storage import clients
from fraudx.storage.feedback import GraphFeedback
from fraudx.storage.graph_features import known_fraud
from fraudx.storage.graph_writer import GraphWriter

pytestmark = pytest.mark.integration

T0 = datetime(2026, 5, 1, 9, 0, tzinfo=UTC)


@pytest.fixture
def graph():
    run = uuid.uuid4().hex[:8]
    driver = clients.neo4j_driver(Neo4jSettings())
    yield driver, run
    driver.execute_query("MATCH (n) WHERE n.id ENDS WITH $s DETACH DELETE n", s=f"-{run}")
    driver.close()


def _event(run, n, customer, source, dest, minutes):
    return TransactionEvent.model_validate({
        "transaction_id": f"TX-{run}-{n}", "event_time": T0 + timedelta(minutes=minutes),
        "customer_id": f"{customer}-{run}", "source_account": f"{source}-{run}",
        "destination_account": f"{dest}-{run}", "amount": 100, "transaction_type": "TRANSFER",
    })  # fmt: skip


def _row(event):
    return {
        "transaction_id": event.transaction_id,
        "transaction_type": event.transaction_type.value,
        "source_account": event.source_account,
        "destination_account": event.destination_account,
    }


def test_confirmation_marks_the_payee_and_scoring_sees_it(graph):
    driver, run = graph
    scam1 = _event(run, 1, "V1", "AV1", "AMULE", 0)  # victim 1 pays the mule
    scam2 = _event(run, 2, "V2", "AV2", "AMULE", 5)  # victim 2 pays the same mule
    onward = _event(run, 3, "MULE", "AMULE", "AOUT", 10)  # the mule moves the money on
    neighbour = _event(run, 4, "V3", "AV3", "AOUT", 20)  # someone pays the mule's counterparty
    GraphWriter(driver).write([scam1, scam2, onward, neighbour])
    feedback = GraphFeedback(driver)

    assert feedback.record(_row(scam1), confirmed=True) == {
        "account": f"AMULE-{run}", "flagged": True, "confirmations": 1,
    }  # fmt: skip
    next_scam = _event(run, 5, "V4", "AV4", "AMULE", 30)
    to_mule, from_mule, to_neighbour = known_fraud(driver, [next_scam, onward, neighbour])
    assert to_mule["distance_to_known_fraud"] == 0  # the payee was confirmed
    assert from_mule["payer_confirmed_fraud"] == 1  # the mule moving money on
    assert to_neighbour["distance_to_known_fraud"] == 1  # AOUT trades with the mule
    assert to_neighbour["fraud_neighbor_count"] == 1

    feedback.record(_row(scam2), confirmed=True)  # a second case confirms the same mule
    assert feedback.record(_row(scam1), confirmed=False)["flagged"] is True  # one remains
    assert feedback.record(_row(scam2), confirmed=False) == {
        "account": f"AMULE-{run}", "flagged": False, "confirmations": 0,
    }  # fmt: skip
    assert "distance_to_known_fraud" not in known_fraud(driver, [next_scam])[0]
