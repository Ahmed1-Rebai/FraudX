"""Store writers against the real Neo4j, Cassandra and MongoDB."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from fraudx.core.config import CassandraSettings, MongoSettings, Neo4jSettings
from fraudx.core.schemas import (
    RiskAssessment,
    ScoredTransaction,
    TransactionEvent,
    TransactionFeatures,
)
from fraudx.storage import clients
from fraudx.storage.graph_writer import GraphWriter
from fraudx.storage.history_writer import HistoryWriter, bucket
from fraudx.storage.profile_writer import MAX_RECENT, ProfileWriter

pytestmark = pytest.mark.integration

T0 = datetime(2026, 2, 1, 9, 0, tzinfo=UTC)


@pytest.fixture
def run():
    return uuid.uuid4().hex[:8]


def _event(run, n, minutes=0, customer="C", source="A", dest="B", device="D", **kw):
    data = {
        "transaction_id": f"TX-{run}-{n}",
        "event_time": T0 + timedelta(minutes=minutes),
        "customer_id": f"{customer}-{run}",
        "source_account": f"{source}-{run}",
        "destination_account": f"{dest}-{run}",
        "amount": 100,
        "transaction_type": "TRANSFER",
        "device_id": f"{device}-{run}",
        "ip_address": "41.224.9.9",
        "location": {"country": "TN", "city": "Sfax"},
    }
    data.update(kw)
    return TransactionEvent.model_validate(data)


# --- Neo4j ---------------------------------------------------------------------


@pytest.fixture
def graph(run):
    driver = clients.neo4j_driver(Neo4jSettings())
    yield GraphWriter(driver), driver
    driver.execute_query("MATCH (n) WHERE n.id ENDS WITH $suffix DETACH DELETE n", suffix=f"-{run}")
    driver.close()


def test_graph_redelivery_does_not_double_count(graph, run):
    writer, driver = graph
    batch = [_event(run, 1, 0), _event(run, 2, 5, amount=50)]
    writer.write(batch)
    writer.write(batch)  # redelivered batch
    records, _, _ = driver.execute_query(
        "MATCH (:Account {id: $a})-[t:TRANSFERRED_TO]->(:Account {id: $b}) "
        "RETURN t.count AS count, t.total_amount AS total",
        a=f"A-{run}", b=f"B-{run}",
    )  # fmt: skip
    assert (records[0]["count"], records[0]["total"]) == (2, 150.0)


def test_graph_counts_distinct_transactions_with_the_same_timestamp(graph, run):
    writer, driver = graph
    first, second = _event(run, 1, 0), _event(run, 2, 0, amount=50)  # same minute
    writer.write([first])
    writer.write([first, second])  # redelivery of the first + a new same-time transfer
    writer.write([second])
    records, _, _ = driver.execute_query(
        "MATCH (:Account {id: $a})-[t:TRANSFERRED_TO]->(:Account {id: $b}) "
        "MATCH (:Customer {id: $c})-[u:USES]->(:Device {id: $d}) "
        "RETURN t.count AS count, t.total_amount AS total, u.count AS uses",
        a=f"A-{run}", b=f"B-{run}", c=f"C-{run}", d=f"D-{run}",
    )  # fmt: skip
    assert (records[0]["count"], records[0]["total"], records[0]["uses"]) == (2, 150.0, 2)


def test_graph_cash_in_flows_from_agent_and_devices_are_shared(graph, run):
    writer, driver = graph
    writer.write([
        _event(run, 1, 0, transaction_type="CASH_IN", dest="M"),
        _event(run, 2, 1, customer="X", source="AX", device="D"),
    ])  # fmt: skip
    flows, _, _ = driver.execute_query(
        "MATCH (p:Account)-[:TRANSFERRED_TO]->(q:Account {id: $a}) RETURN p.id AS payer",
        a=f"A-{run}",
    )
    assert [r["payer"] for r in flows] == [f"M-{run}"]
    users, _, _ = driver.execute_query(
        "MATCH (c:Customer)-[:USES]->(:Device {id: $d}) RETURN count(c) AS n", d=f"D-{run}"
    )
    assert users[0]["n"] == 2
    loc, _, _ = driver.execute_query(
        "MATCH (:Device {id: $d})-[:CONNECTED_FROM]->(:IP)-[:LOCATED_IN]->(l:Location) "
        "RETURN l.id AS id", d=f"D-{run}",
    )  # fmt: skip
    assert loc[0]["id"] == "TN|Sfax"


# --- Cassandra -------------------------------------------------------------------


@pytest.fixture
def history(run):
    settings = CassandraSettings()
    cluster = clients.cassandra_cluster(settings, timeout_s=10)
    session = clients.cassandra_session(cluster)
    session.set_keyspace(settings.keyspace)  # test queries only
    yield HistoryWriter(session, settings.keyspace), session
    session.execute(
        "DELETE FROM transactions_by_customer WHERE customer_id = %s AND event_date = %s",
        (f"C-{run}", T0.date()),
    )
    for n in range(1, 4):
        session.execute("DELETE FROM transactions_by_id WHERE transaction_id = %s",
                        (f"TX-{run}-{n}",))  # fmt: skip
    cluster.shutdown()


def test_history_query_tables_and_idempotent_upsert(history, run):
    writer, session = history
    events = [_event(run, n, minutes=n) for n in (1, 2, 3)]
    writer.write(events)
    writer.write(events)
    rows = list(session.execute(
        "SELECT transaction_id FROM transactions_by_customer "
        "WHERE customer_id = %s AND event_date = %s", (f"C-{run}", T0.date()),
    ))  # fmt: skip
    assert [r.transaction_id for r in rows] == [f"TX-{run}-{n}" for n in (3, 2, 1)]  # newest first
    one = session.execute("SELECT amount, risk_score FROM transactions_by_id "
                          "WHERE transaction_id = %s", (f"TX-{run}-1",)).one()  # fmt: skip
    assert str(one.amount) == "100.00"
    assert one.risk_score is None


def test_history_rewrite_keeps_columns_it_does_not_set(history, run):
    writer, session = history
    event = _event(run, 1)
    writer.write([event])
    session.execute("UPDATE transactions_by_id SET risk_score = 88 WHERE transaction_id = %s",
                    (event.transaction_id,))  # fmt: skip
    writer.write([event])  # redelivery without risk must not erase it (UNSET, not null)
    row = session.execute("SELECT risk_score FROM transactions_by_id WHERE transaction_id = %s",
                          (event.transaction_id,)).one()  # fmt: skip
    assert row.risk_score == 88


# --- MongoDB ---------------------------------------------------------------------


@pytest.fixture
def profiles(run):
    settings = MongoSettings()
    client = clients.mongo_client(settings)
    db = client[settings.db]
    yield ProfileWriter(db), db
    for name in ("customers", "accounts", "devices"):
        db[name].delete_many({"_id": {"$regex": f"-{run}$"}})
    client.close()


def test_profile_idempotent_bounded_and_order_insensitive(profiles, run):
    writer, db = profiles
    later, earlier = _event(run, 1, minutes=60), _event(run, 2, minutes=0)
    writer.write([later])
    writer.write([earlier, later])  # out of order + redelivery
    doc = db.customers.find_one({"_id": f"C-{run}"})
    assert doc["devices"] == [f"D-{run}"]
    assert doc["first_seen"] == earlier.event_time
    assert doc["last_seen"] == later.event_time
    assert doc["country"] == "TN"
    assert db.accounts.find_one({"_id": f"A-{run}"})["customer_id"] == f"C-{run}"
    assert db.devices.find_one({"_id": f"D-{run}"})["first_seen"] == earlier.event_time

    many = [_event(run, 100 + i, minutes=i, device=f"D{i}") for i in range(MAX_RECENT + 10)]
    writer.write(many)
    devices = db.customers.find_one({"_id": f"C-{run}"})["devices"]
    assert len(devices) == MAX_RECENT
    assert devices[-1] == f"D{MAX_RECENT + 9}-{run}"  # most recent kept
    db.devices.delete_many({"_id": {"$regex": f"^D\\d+-{run}$"}})


# --- scored transactions (Phase 8) -------------------------------------------------


def _scored(event, score, level, rules=()):
    features = TransactionFeatures(
        cust_txn_count_5m=1, cust_txn_count_1h=1, cust_txn_count_24h=1, cust_amount_sum_5m=1,
        cust_amount_sum_1h=1, cust_amount_sum_24h=1, cust_history_count=0, is_new_device=False,
        is_new_ip=False, is_new_country=False, hour_of_day=9,
    )  # fmt: skip
    risk = RiskAssessment(score=score, level=level, components={"ml": score / 100},
                          rule_hits=tuple(rules), assessed_at=T0)  # fmt: skip
    return ScoredTransaction(event=event, features=features, risk=risk, scored_at=T0)


def test_history_scored_rows_and_alert_feed(history, run):
    writer, session = history
    low, high = _event(run, 1, minutes=1), _event(run, 2, minutes=2)
    writer.write_scored([_scored(low, 3, "LOW"), _scored(high, 91, "HIGH", ["takeover_pattern"])])
    row = session.execute("SELECT risk_score, risk_level, rule_hits FROM transactions_by_id "
                          "WHERE transaction_id = %s", (high.transaction_id,)).one()  # fmt: skip
    assert (row.risk_score, row.risk_level, row.rule_hits) == (91, "HIGH", ["takeover_pattern"])
    feed = [
        r.transaction_id
        for b in range(8)
        for r in session.execute(
            "SELECT transaction_id FROM risk_events_by_day WHERE event_date = %s AND bucket = %s",
            (T0.date(), b),
        )
        if r.transaction_id.startswith(f"TX-{run}")
    ]
    assert feed == [high.transaction_id]  # LOW stays out of the alert feed
    session.execute("DELETE FROM risk_events_by_day WHERE event_date = %s AND bucket = %s "
                    "AND event_time = %s AND transaction_id = %s",
                    (T0.date(), bucket(high.transaction_id), high.event_time,
                     high.transaction_id))  # fmt: skip


def test_profile_risk_is_the_latest_by_event_time(profiles, run):
    writer, db = profiles
    newer, older = _event(run, 2, minutes=60), _event(run, 1, minutes=0)
    writer.write_scored([_scored(newer, 12, "MEDIUM")])
    writer.write_scored([_scored(older, 95, "HIGH")])  # late redelivery of an older event
    profile = db.customers.find_one({"_id": f"C-{run}"})["risk_profile"]
    assert (profile["score"], profile["level"], profile["transaction_id"]) == (
        12, "MEDIUM", newer.transaction_id,
    )  # fmt: skip
