"""Store outages answer 503 with the store's name, not an opaque 500.

The API reads one store per endpoint; when that store is down the client
should learn which one and that retrying later is the right move.
``/health/ready`` gives the full picture.
"""

from __future__ import annotations

import logging

from cassandra import OperationTimedOut, ReadTimeout, Unavailable
from cassandra.cluster import NoHostAvailable
from confluent_kafka import KafkaException
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from neo4j.exceptions import ServiceUnavailable, SessionExpired
from pymongo.errors import ConnectionFailure
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from fraudx.messaging.producer import DeliveryError

log = logging.getLogger(__name__)

UNAVAILABLE: tuple[tuple[type[Exception], str], ...] = (
    (NoHostAvailable, "Cassandra"),
    (OperationTimedOut, "Cassandra"),
    (ReadTimeout, "Cassandra"),
    (Unavailable, "Cassandra"),
    (ConnectionFailure, "MongoDB"),  # includes server-selection timeouts
    (ServiceUnavailable, "Neo4j"),
    (SessionExpired, "Neo4j"),
    (RedisConnectionError, "Redis"),
    (RedisTimeoutError, "Redis"),
    (KafkaException, "Kafka"),
    (DeliveryError, "Kafka"),
)


def install(app: FastAPI) -> None:
    for exc_type, store in UNAVAILABLE:

        async def handler(request: Request, exc: Exception, store: str = store) -> JSONResponse:
            error = f"{type(exc).__name__}: {exc}"
            log.warning(
                "store unavailable",
                extra={"store": store, "path": request.url.path, "error": error},
            )
            return JSONResponse(
                status_code=503, content={"detail": f"{store} is unavailable; retry shortly"}
            )

        app.add_exception_handler(exc_type, handler)
