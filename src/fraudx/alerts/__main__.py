"""Run the alert handler: ``python -m fraudx.alerts``."""

from __future__ import annotations

import sys

from prometheus_client import start_http_server

from fraudx.core.config import (
    AlertSettings,
    ExplainSettings,
    KafkaSettings,
    MongoSettings,
    WorkerSettings,
    get_app_settings,
    get_risk_config_watcher,
)
from fraudx.core.logging import configure_logging
from fraudx.core.topics import FRAUD_ALERTS
from fraudx.explain import template
from fraudx.messaging.consumer import StreamWorker
from fraudx.messaging.producer import EventProducer
from fraudx.storage import clients

from .handler import AlertHandler, Explainer


def _explainer(settings: ExplainSettings) -> Explainer:
    if settings.provider == "anthropic":
        from fraudx.explain.llm import ClaudeExplainer

        return ClaudeExplainer(settings.model, settings.timeout_s)
    return template.TemplateExplainer()


def main() -> int:
    app = get_app_settings()
    configure_logging(app.log_level, app.log_format)
    worker_settings = WorkerSettings()
    kafka = KafkaSettings()
    start_http_server(worker_settings.metrics_port)

    mongo_settings = MongoSettings()
    mongo = clients.mongo_client(mongo_settings, timeout_ms=10_000)
    worker = StreamWorker(
        name="alert-handler",
        topics=[FRAUD_ALERTS],
        handler=None,
        batch_handler=AlertHandler(
            mongo[mongo_settings.db],
            _explainer(ExplainSettings()),
            get_risk_config_watcher().get,
            webhook_url=AlertSettings().webhook_url,
        ),
        batch_size=100,
        settings=kafka,
        producer=EventProducer(kafka, "alert-handler"),
        heartbeat_path=worker_settings.heartbeat_path,
    )
    worker.install_signal_handlers()
    try:
        worker.run()
    finally:
        mongo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
