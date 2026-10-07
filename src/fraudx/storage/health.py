"""Readiness probes for every backing store.

Clients are created lazily on first check and reused afterwards, so a probe
never blocks process start-up and a store that comes up late is picked up on
the next check. The probes are synchronous driver calls executed in worker
threads with a hard timeout.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fraudx.core.config import (
    CassandraSettings,
    KafkaSettings,
    MongoSettings,
    Neo4jSettings,
    RedisSettings,
)
from fraudx.core.topics import ALL_TOPICS
from fraudx.storage import clients

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    ok: bool
    latency_ms: float
    detail: str = ""


class DependencyProbe:
    def __init__(
        self,
        kafka: KafkaSettings,
        mongo: MongoSettings,
        cassandra: CassandraSettings,
        redis: RedisSettings,
        neo4j: Neo4jSettings,
        timeout_s: float = 3.0,
    ) -> None:
        self._settings = {
            "kafka": kafka,
            "mongodb": mongo,
            "cassandra": cassandra,
            "redis": redis,
            "neo4j": neo4j,
        }
        self._timeout_s = timeout_s
        self._clients: dict[str, Any] = {}
        self._checks: dict[str, Callable[[], str]] = {
            "kafka": self._check_kafka,
            "mongodb": self._check_mongo,
            "cassandra": self._check_cassandra,
            "redis": self._check_redis,
            "neo4j": self._check_neo4j,
        }

    async def check_all(self) -> list[CheckResult]:
        return list(await asyncio.gather(*(self._run(n, f) for n, f in self._checks.items())))

    async def _run(self, name: str, check: Callable[[], str]) -> CheckResult:
        start = time.perf_counter()
        try:
            detail = await asyncio.wait_for(asyncio.to_thread(check), self._timeout_s)
            ok = True
        except TimeoutError:
            ok, detail = False, f"timed out after {self._timeout_s}s"
        except Exception as exc:
            ok, detail = False, f"{type(exc).__name__}: {exc}"
            self._drop_client(name)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        if not ok:
            log.warning("dependency check failed", extra={"dependency": name, "detail": detail})
        return CheckResult(name=name, ok=ok, latency_ms=latency_ms, detail=detail)

    def _client(self, name: str, factory: Callable[[], Any]) -> Any:
        if name not in self._clients:
            self._clients[name] = factory()
        return self._clients[name]

    def _drop_client(self, name: str) -> None:
        client = self._clients.pop(name, None)
        if client is not None:
            _close_quietly(client)

    def close(self) -> None:
        for name in list(self._clients):
            self._drop_client(name)

    # --- individual checks: return a short detail string or raise -----------

    def _check_kafka(self) -> str:
        admin = self._client("kafka", lambda: clients.kafka_admin(self._settings["kafka"]))
        metadata = admin.list_topics(timeout=self._timeout_s)
        missing = sorted(set(ALL_TOPICS) - set(metadata.topics))
        if missing:
            raise RuntimeError(f"missing topics: {', '.join(missing)}")
        return f"{len(metadata.brokers)} broker(s), {len(ALL_TOPICS)} topics present"

    def _check_mongo(self) -> str:
        settings: MongoSettings = self._settings["mongodb"]
        client = self._client(
            "mongodb", lambda: clients.mongo_client(settings, int(self._timeout_s * 1000))
        )
        client[settings.db].command("ping")
        return "ping ok"

    def _check_cassandra(self) -> str:
        settings: CassandraSettings = self._settings["cassandra"]

        def connect() -> Any:
            cluster = clients.cassandra_cluster(settings, self._timeout_s)
            try:
                return cluster, clients.cassandra_session(cluster)
            except Exception:
                cluster.shutdown()
                raise

        _, session = self._client("cassandra", connect)
        row = session.execute("SELECT release_version FROM system.local").one()
        return f"release {row.release_version}"

    def _check_redis(self) -> str:
        client = self._client(
            "redis", lambda: clients.redis_client(self._settings["redis"], self._timeout_s)
        )
        client.ping()
        return "ping ok"

    def _check_neo4j(self) -> str:
        driver = self._client(
            "neo4j", lambda: clients.neo4j_driver(self._settings["neo4j"], self._timeout_s)
        )
        driver.verify_connectivity()
        return "connectivity ok"


def _close_quietly(client: Any) -> None:
    if isinstance(client, tuple):  # cassandra: (cluster, session)
        client = client[0]
    closer = getattr(client, "shutdown", None) or getattr(client, "close", None)
    if closer is None:
        return
    try:
        closer()
    except Exception:
        log.debug("error while closing client", exc_info=True)
