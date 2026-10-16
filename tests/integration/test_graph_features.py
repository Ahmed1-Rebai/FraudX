"""Point-in-time graph features against the real Neo4j."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from fraudx.core.config import Neo4jSettings
from fraudx.core.schemas import TransactionEvent
from fraudx.storage import clients
from fraudx.storage.graph_features import GRAPH_FEATURE_NAMES, graph_features
from fraudx.storage.graph_writer import GraphWriter

pytestmark = pytest.mark.integration

T0 = datetime(2026, 4, 1, 9, 0, tzinfo=UTC)


@pytest.fixture
def graph():
    run = uuid.uuid4().hex[:8]
    driver = clients.neo4j_driver(Neo4jSettings())
    yield driver, run
    driver.execute_query("MATCH (n) WHERE n.id ENDS WITH $s DETACH DELETE n", s=f"-{run}")
    driver.close()


def _event(run, n, customer, source, dest, device, minutes):
    return TransactionEvent.model_validate({
        "transaction_id": f"TX-{run}-{n}", "event_time": T0 + timedelta(minutes=minutes),
        "customer_id": f"{customer}-{run}", "source_account": f"{source}-{run}",
        "destination_account": f"{dest}-{run}", "amount": 100, "transaction_type": "TRANSFER",
        "device_id": f"{device}-{run}",
    })  # fmt: skip


def test_features_see_only_the_past_and_never_the_transaction_itself(graph):
    driver, run = graph
    history = [
        _event(run, 1, "V1", "AV1", "AMULE", "DV1", 0),  # victim 1 pays the mule
        _event(run, 2, "MULE", "AMULE", "AOUT", "DRING", 1),  # mule (owner of AMULE), ring device
        _event(run, 3, "M2", "AM2", "AOUT", "DRING", 2),  # second mule on the ring device
    ]
    scam = _event(run, 4, "V2", "AV2", "AMULE", "DV2", 10)  # new victim pays the mule
    GraphWriter(driver).write([*history, scam])  # the graph already holds the scam itself

    features = dict(zip(GRAPH_FEATURE_NAMES, graph_features(driver, [scam])[0], strict=True))
    assert features["payee_payer_count"] == 1  # victim 1 only, not V2's own edge
    assert features["is_new_payee"] == 1.0  # V2 -> AMULE first seen at this very event
    assert features["payee_owner_ring_size"] == 1  # MULE shares DRING with M2
    assert features["customer_ring_size"] == 0

    early = dict(zip(GRAPH_FEATURE_NAMES, graph_features(driver, [history[0]])[0], strict=True))
    assert early["payee_owner_ring_size"] == 0  # the ring device is used only later
