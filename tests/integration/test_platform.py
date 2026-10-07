"""Checks the provisioned stack matches what the code expects.

Run with ``make up && make test-integration``. Connection settings come from
``.env`` (host ports on localhost).
"""

import os
import re
from pathlib import Path

import httpx2
import pytest
from neo4j.exceptions import AuthError
from pymongo.errors import OperationFailure
from redis.exceptions import AuthenticationError

from fraudx.core.config import (
    CassandraSettings,
    KafkaSettings,
    MongoSettings,
    Neo4jSettings,
    RedisSettings,
)
from fraudx.core.topics import ALL_TOPICS
from fraudx.storage import clients

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def mongo():
    client = clients.mongo_client(MongoSettings())
    yield client[MongoSettings().db]
    client.close()


@pytest.fixture(scope="module")
def cassandra():
    settings = CassandraSettings()
    cluster = clients.cassandra_cluster(settings, timeout_s=10)
    yield clients.cassandra_session(cluster)
    cluster.shutdown()


@pytest.fixture(scope="module")
def neo4j():
    driver = clients.neo4j_driver(Neo4jSettings())
    yield driver
    driver.close()


def test_api_ready():
    port = os.environ.get("API_PORT", "8000")
    resp = httpx2.get(f"http://localhost:{port}/health/ready", timeout=15)
    assert resp.status_code == 200, resp.text
    assert all(dep["ok"] for dep in resp.json()["dependencies"].values())


def test_kafka_topics_match_code_and_script():
    metadata = clients.kafka_admin(KafkaSettings()).list_topics(timeout=10)
    assert set(ALL_TOPICS) <= set(metadata.topics)
    script = (REPO_ROOT / "infra/kafka/create-topics.sh").read_text()
    scripted = set(re.findall(r"^create (\S+)", script, flags=re.MULTILINE))
    assert scripted == set(ALL_TOPICS)
    # Keyed pipeline topics must be co-partitioned.
    keyed = ("transactions.raw", "transactions.features", "transactions.scored")
    assert len({len(metadata.topics[t].partitions) for t in keyed}) == 1


def test_cassandra_tables_exist(cassandra):
    rows = cassandra.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'fraudx'"
    )
    assert {r.table_name for r in rows} >= {
        "transactions_by_customer",
        "transactions_by_id",
        "risk_events_by_day",
    }


def test_mongo_collections_validators_and_indexes(mongo):
    expected = {"customers", "accounts", "devices", "investigations", "model_registry"}
    infos = {c["name"]: c for c in mongo.list_collections()}
    assert expected <= set(infos)
    assert all("$jsonSchema" in infos[name]["options"]["validator"] for name in expected)
    index_keys = [list(ix["key"]) for ix in mongo.investigations.list_indexes()]
    assert ["status", "created_at"] in index_keys


def test_mongo_validator_rejects_bad_document(mongo):
    with pytest.raises(OperationFailure, match="validation"):
        mongo.customers.insert_one({"_id": "C-INVALID", "country": "Tunisia"})


def test_mongo_app_user_is_least_privilege():
    client = clients.mongo_client(MongoSettings())
    try:
        with pytest.raises(OperationFailure, match="not authorized"):
            client["not_fraudx"].probe.insert_one({"x": 1})
    finally:
        client.close()


def test_redis_requires_auth():
    settings = RedisSettings(password="wrong-password")
    with pytest.raises(AuthenticationError):
        clients.redis_client(settings).ping()
    assert clients.redis_client(RedisSettings()).ping()


def test_neo4j_constraints(neo4j):
    records, _, _ = neo4j.execute_query("SHOW CONSTRAINTS YIELD name RETURN name")
    assert {r["name"] for r in records} >= {
        "customer_id",
        "account_id",
        "device_id",
        "ip_address",
        "location_id",
    }


def test_neo4j_rejects_wrong_password():
    driver = clients.neo4j_driver(Neo4jSettings(password="wrong-password"))
    try:
        with pytest.raises(AuthError):
            driver.verify_connectivity()
    finally:
        driver.close()
