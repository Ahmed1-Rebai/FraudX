"""Client factories for every backing store.

Services build their clients here so connection options (timeouts, load
balancing, auth) are defined once. Callers own the returned objects and must
close them.
"""

from __future__ import annotations

from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from confluent_kafka.admin import AdminClient
from neo4j import Driver, GraphDatabase
from pymongo import MongoClient
from redis import Redis

from fraudx.core.config import (
    CassandraSettings,
    KafkaSettings,
    MongoSettings,
    Neo4jSettings,
    RedisSettings,
)


def kafka_admin(settings: KafkaSettings) -> AdminClient:
    return AdminClient(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": f"{settings.client_id}-admin",
        }
    )


def mongo_client(settings: MongoSettings, timeout_ms: int = 3000) -> MongoClient:
    return MongoClient(
        settings.uri,
        serverSelectionTimeoutMS=timeout_ms,
        connectTimeoutMS=timeout_ms,
        tz_aware=True,
        appname="fraudx",
    )


def redis_client(settings: RedisSettings, timeout_s: float = 3.0) -> Redis:
    return Redis(
        host=settings.host,
        port=settings.port,
        db=settings.db,
        password=settings.password.get_secret_value(),
        socket_timeout=timeout_s,
        socket_connect_timeout=timeout_s,
        decode_responses=True,
    )


def cassandra_cluster(settings: CassandraSettings, timeout_s: float = 3.0) -> Cluster:
    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=settings.local_dc)),
        request_timeout=timeout_s * 3,
    )
    return Cluster(
        contact_points=settings.hosts,
        port=settings.port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=5,
        connect_timeout=timeout_s,
    )


def cassandra_session(cluster: Cluster, settings: CassandraSettings) -> Session:
    return cluster.connect(settings.keyspace)


def neo4j_driver(settings: Neo4jSettings, timeout_s: float = 3.0) -> Driver:
    return GraphDatabase.driver(
        settings.uri,
        auth=(settings.user, settings.password.get_secret_value()),
        connection_timeout=timeout_s,
    )
