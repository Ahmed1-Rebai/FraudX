"""Store readers for the API, connected on first use and shared by requests.

Connecting lazily lets the API start (and report ``/health/ready``) while a
store is still down; a failed connection is retried on the next request.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from fastapi import Request

from fraudx.core.config import CassandraSettings, MongoSettings, Neo4jSettings
from fraudx.storage import clients
from fraudx.storage.readers import GraphReader, HistoryReader, ProfileReader


class Stores:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._readers: dict[str, Any] = {}
        self._closers: list[Callable[[], None]] = []

    def history(self) -> HistoryReader:
        return self._get("history", self._connect_history)

    def profiles(self) -> ProfileReader:
        return self._get("profiles", self._connect_profiles)

    def graph(self) -> GraphReader:
        return self._get("graph", self._connect_graph)

    def close(self) -> None:
        with self._lock:
            for close in reversed(self._closers):
                close()
            self._closers.clear()
            self._readers.clear()

    def _get(self, name: str, connect: Callable[[], Any]) -> Any:
        with self._lock:
            if name not in self._readers:
                self._readers[name] = connect()
            return self._readers[name]

    def _connect_history(self) -> HistoryReader:
        settings = CassandraSettings()
        cluster = clients.cassandra_cluster(settings)
        try:
            reader = HistoryReader(clients.cassandra_session(cluster), settings.keyspace)
        except Exception:
            cluster.shutdown()
            raise
        self._closers.append(cluster.shutdown)
        return reader

    def _connect_profiles(self) -> ProfileReader:
        settings = MongoSettings()
        client = clients.mongo_client(settings)
        self._closers.append(client.close)
        return ProfileReader(client[settings.db])

    def _connect_graph(self) -> GraphReader:
        driver = clients.neo4j_driver(Neo4jSettings())
        self._closers.append(driver.close)
        return GraphReader(driver)


def get_stores(request: Request) -> Stores:
    return request.app.state.stores
