"""Run the scorer: ``python -m fraudx.scoring``."""

from __future__ import annotations

import sys
from functools import partial

from prometheus_client import start_http_server

from fraudx.core.config import (
    KafkaSettings,
    MongoSettings,
    Neo4jSettings,
    RedisSettings,
    WorkerSettings,
    get_app_settings,
    get_risk_config_watcher,
)
from fraudx.core.logging import configure_logging
from fraudx.core.topics import TRANSACTIONS_FEATURES
from fraudx.messaging.consumer import StreamWorker
from fraudx.messaging.producer import EventProducer
from fraudx.scoring.detectors import ActiveModels, load_active, registry_ids
from fraudx.scoring.scorer import Scorer
from fraudx.storage import clients
from fraudx.storage.graph_features import graph_features, known_fraud


def main() -> int:
    app = get_app_settings()
    configure_logging(app.log_level, app.log_format)
    worker_settings = WorkerSettings()
    kafka = KafkaSettings()
    start_http_server(worker_settings.metrics_port)

    mongo_settings = MongoSettings()
    mongo = clients.mongo_client(mongo_settings, timeout_ms=10_000)
    db = mongo[mongo_settings.db]
    driver = clients.neo4j_driver(Neo4jSettings(), timeout_s=10)
    redis = clients.redis_client(RedisSettings())

    producer = EventProducer(kafka, "scorer")
    worker = StreamWorker(
        name="scorer",
        topics=[TRANSACTIONS_FEATURES],
        handler=None,
        batch_handler=Scorer(
            models=ActiveModels(partial(registry_ids, db), partial(load_active, db)),
            graph_features=partial(graph_features, driver),
            known_fraud=partial(known_fraud, driver),
            config=get_risk_config_watcher().get,
            producer=producer,
            redis=redis,
        ),
        batch_size=500,
        settings=kafka,
        producer=producer,
        heartbeat_path=worker_settings.heartbeat_path,
    )
    worker.install_signal_handlers()
    try:
        worker.run()
    finally:
        redis.close()
        driver.close()
        mongo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
