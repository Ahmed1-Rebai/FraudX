"""FastAPI application factory.

Run locally with ``uv run uvicorn fraudx.api.app:app --reload``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import make_asgi_app

from fraudx import __version__
from fraudx.api import errors
from fraudx.api.metrics import MetricsMiddleware
from fraudx.api.routes import customers, fraud, health, stats, stream, transactions
from fraudx.api.stores import Stores
from fraudx.core.config import (
    CassandraSettings,
    KafkaSettings,
    MongoSettings,
    Neo4jSettings,
    RedisSettings,
    get_app_settings,
)
from fraudx.core.logging import configure_logging
from fraudx.storage.health import DependencyProbe

log = logging.getLogger(__name__)


def build_probe() -> DependencyProbe:
    return DependencyProbe(
        kafka=KafkaSettings(),
        mongo=MongoSettings(),
        cassandra=CassandraSettings(),
        redis=RedisSettings(),
        neo4j=Neo4jSettings(),
        timeout_s=get_app_settings().dependency_timeout_s,
    )


def create_app(probe: DependencyProbe | None = None, stores: Stores | None = None) -> FastAPI:
    settings = get_app_settings()
    configure_logging(settings.log_level, settings.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.probe = probe or build_probe()
        app.state.stores = stores or Stores()
        log.info("api started", extra={"version": __version__, "env": settings.env})
        try:
            yield
        finally:
            app.state.stores.close()
            app.state.probe.close()
            log.info("api stopped")

    app = FastAPI(title="FraudX API", version=__version__, lifespan=lifespan)
    app.add_middleware(MetricsMiddleware)
    errors.install(app)
    app.include_router(health.router)
    app.include_router(transactions.router)
    app.include_router(customers.router)
    app.include_router(fraud.router)
    app.include_router(stats.router)
    app.include_router(stream.router)
    app.mount("/metrics", make_asgi_app())
    return app


app = create_app()
