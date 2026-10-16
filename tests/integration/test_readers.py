"""Readers against the real stores: write with the writers, read it back."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from fraudx.core.config import CassandraSettings, MongoSettings, Neo4jSettings
from fraudx.core.schemas import TransactionEvent
from fraudx.storage import clients
from fraudx.storage.graph_writer import GraphWriter
from fraudx.storage.history_writer import HistoryWriter
from fraudx.storage.profile_writer import ProfileWriter
from fraudx.storage.readers import GraphReader, HistoryReader, ProfileReader

pytestmark = pytest.mark.integration

T0 = datetime(2026, 3, 1, 9, 0, tzinfo=UTC)


@pytest.fixture
def run():
    return uuid.uuid4().hex[:8]


def _event(run, n, customer, device, minutes=0):
    return TransactionEvent.model_validate({
        "transaction_id": f"TX-{run}-{n}", "event_time": T0 + timedelta(minutes=minutes),
        "customer_id": f"{customer}-{run}", "source_account": f"A{customer}-{run}",
        "destination_account": f"B-{run}", "amount": 10 * n, "transaction_type": "TRANSFER",
        "device_id": f"{device}-{run}", "ip_address": "10.1.2.3",
    })  # fmt: skip


def test_history_reader(run):
    settings = CassandraSettings()
    cluster = clients.cassandra_cluster(settings, timeout_s=10)
    session = clients.cassandra_session(cluster)
    events = [_event(run, n, "C", "D", minutes=n) for n in (1, 2, 3)]
    HistoryWriter(session, settings.keyspace).write(events)
    reader = HistoryReader(session, settings.keyspace)
    try:
        one = reader.transaction(f"TX-{run}-2")
        day = reader.customer_day(f"C-{run}", T0.date(), limit=2)
        assert one["amount"] == 20 and one["event_time"] == events[1].event_time
        assert [r["transaction_id"] for r in day] == [f"TX-{run}-3", f"TX-{run}-2"]
        assert reader.transaction(f"TX-{run}-404") is None
    finally:
        for e in events:
            session.execute(f"DELETE FROM {settings.keyspace}.transactions_by_id "  # noqa: S608
                            "WHERE transaction_id = %s", (e.transaction_id,))  # fmt: skip
        session.execute(f"DELETE FROM {settings.keyspace}.transactions_by_customer "  # noqa: S608
                        "WHERE customer_id = %s AND event_date = %s",
                        (f"C-{run}", T0.date()))  # fmt: skip
        cluster.shutdown()


def test_profile_reader(run):
    settings = MongoSettings()
    client = clients.mongo_client(settings)
    db = client[settings.db]
    try:
        ProfileWriter(db).write([_event(run, 1, "C", "D")])
        profile = ProfileReader(db).customer(f"C-{run}")
        assert profile["customer_id"] == f"C-{run}" and profile["devices"] == [f"D-{run}"]
        assert ProfileReader(db).customer(f"C-404-{run}") is None
    finally:
        for name in ("customers", "accounts", "devices"):
            db[name].delete_many({"_id": {"$regex": f"-{run}$"}})
        client.close()


def test_graph_reader_shared_device_and_flows(run):
    driver = clients.neo4j_driver(Neo4jSettings())
    try:
        GraphWriter(driver).write([
            _event(run, 1, "C", "D"), _event(run, 2, "X", "D", minutes=1),
        ])  # fmt: skip
        network = GraphReader(driver).customer_network(f"C-{run}", limit=10)
        assert network["devices"][0]["shared_with"] == [f"X-{run}"]
        flow = network["counterparties"][0]
        assert (flow["counterparty"], flow["direction"], flow["count"]) == (f"B-{run}", "out", 1)
        assert isinstance(flow["last_seen"], datetime)
        assert GraphReader(driver).customer_network(f"C-404-{run}", limit=10) is None
    finally:
        driver.execute_query("MATCH (n) WHERE n.id ENDS WITH $s DETACH DELETE n", s=f"-{run}")
        driver.close()
