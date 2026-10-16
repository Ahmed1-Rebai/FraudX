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
    get_risk_config,
)
from fraudx.core.logging import configure_logging
from fraudx.core.topics import TRANSACTIONS_FEATURES
from fraudx.messaging.consumer import StreamWorker
from fraudx.messaging.producer import EventProducer
from fraudx.ml import registry
from fraudx.scoring.detectors import SIGNAL_MODELS, load_active
from fraudx.scoring.scorer import ActiveModels, Scorer
from fraudx.storage import clients
from fraudx.storage.graph_features import graph_features


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

    def active_ids() -> dict[str, str]:
        return {
            name: doc["_id"]
            for name in SIGNAL_MODELS.values()
            if (doc := registry.active(db, name)) is not None
        }

    producer = EventProducer(kafka, "scorer")
    worker = StreamWorker(
        name="scorer",
        topics=[TRANSACTIONS_FEATURES],
        handler=None,
        batch_handler=Scorer(
            models=ActiveModels(active_ids, partial(load_active, db)),
            graph_features=partial(graph_features, driver),
            config=get_risk_config(),
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
