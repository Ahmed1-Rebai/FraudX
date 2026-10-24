"""Run a store writer: ``python -m fraudx.storage graph|history|profile``."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from contextlib import ExitStack
from typing import Any

from prometheus_client import start_http_server

from fraudx.core.config import (
    CassandraSettings,
    KafkaSettings,
    MongoSettings,
    Neo4jSettings,
    WorkerSettings,
    get_app_settings,
)
from fraudx.core.logging import configure_logging
from fraudx.core.topics import FRAUD_DECISIONS, TRANSACTIONS_FEATURES, TRANSACTIONS_SCORED
from fraudx.messaging.consumer import StreamWorker
from fraudx.messaging.producer import EventProducer
from fraudx.storage import clients
from fraudx.storage.feedback import FeedbackHandler, GraphFeedback
from fraudx.storage.graph_writer import GraphWriter
from fraudx.storage.history_writer import HistoryWriter
from fraudx.storage.investigations import Investigations
from fraudx.storage.profile_writer import ProfileWriter
from fraudx.storage.sink import ScoredSinkHandler, SinkHandler


def _graph(stack: ExitStack) -> GraphWriter:
    driver = clients.neo4j_driver(Neo4jSettings(), timeout_s=10)
    stack.callback(driver.close)
    return GraphWriter(driver)


def _history(stack: ExitStack) -> HistoryWriter:
    settings = CassandraSettings()
    cluster = clients.cassandra_cluster(settings, timeout_s=10)
    stack.callback(cluster.shutdown)
    return HistoryWriter(clients.cassandra_session(cluster), settings.keyspace)


def _profile(stack: ExitStack) -> ProfileWriter:
    settings = MongoSettings()
    client = clients.mongo_client(settings, timeout_ms=10_000)
    stack.callback(client.close)
    return ProfileWriter(client[settings.db])


def _feedback(stack: ExitStack) -> FeedbackHandler:
    driver = clients.neo4j_driver(Neo4jSettings(), timeout_s=10)
    stack.callback(driver.close)
    mongo_settings = MongoSettings()
    client = clients.mongo_client(mongo_settings, timeout_ms=10_000)
    stack.callback(client.close)
    return FeedbackHandler(
        GraphFeedback(driver),
        Investigations(client[mongo_settings.db]),
    )


# The graph is built from every featured transaction (the scorer reads it);
# history and profiles are written once the transaction is scored (ADR-021);
# feedback consumes analyst decisions to mark confirmed fraud accounts.
WRITERS: dict[str, tuple[Callable[[ExitStack], Any], str]] = {
    "graph": (_graph, TRANSACTIONS_FEATURES),
    "history": (_history, TRANSACTIONS_SCORED),
    "profile": (_profile, TRANSACTIONS_SCORED),
    "feedback": (_feedback, FRAUD_DECISIONS),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m fraudx.storage")
    parser.add_argument("writer", choices=sorted(WRITERS))
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args(argv)

    app = get_app_settings()
    configure_logging(app.log_level, app.log_format)
    worker_settings = WorkerSettings()
    kafka = KafkaSettings()
    start_http_server(worker_settings.metrics_port)

    name = f"{args.writer}-writer"
    with ExitStack() as stack:
        factory, topic = WRITERS[args.writer]
        writer = factory(stack)
        if args.writer == "feedback":
            handler = writer
        else:
            handler = (
                SinkHandler(writer) if topic == TRANSACTIONS_FEATURES else ScoredSinkHandler(writer)
            )

        worker = StreamWorker(
            name=name,
            topics=[topic],
            handler=None,
            batch_handler=handler,
            batch_size=args.batch_size,
            settings=kafka,
            producer=EventProducer(kafka, name),
            heartbeat_path=worker_settings.heartbeat_path,
        )
        worker.install_signal_handlers()
        worker.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
