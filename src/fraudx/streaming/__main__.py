"""Run the feature processor: ``python -m fraudx.streaming``."""

from __future__ import annotations

import sys

from prometheus_client import start_http_server

from fraudx.core.config import KafkaSettings, RedisSettings, WorkerSettings, get_app_settings
from fraudx.core.logging import configure_logging
from fraudx.core.topics import TRANSACTIONS_RAW
from fraudx.messaging.consumer import StreamWorker
from fraudx.messaging.producer import EventProducer
from fraudx.storage.clients import redis_client
from fraudx.streaming.features import OnlineFeatureStore
from fraudx.streaming.processor import FeatureProcessor


def main() -> int:
    app = get_app_settings()
    configure_logging(app.log_level, app.log_format)
    worker_settings = WorkerSettings()
    kafka = KafkaSettings()
    start_http_server(worker_settings.metrics_port)

    redis = redis_client(RedisSettings())
    producer = EventProducer(kafka, "feature-processor")
    worker = StreamWorker(
        name="feature-processor",
        topics=[TRANSACTIONS_RAW],
        handler=None,
        batch_handler=FeatureProcessor(OnlineFeatureStore(redis), producer),
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
