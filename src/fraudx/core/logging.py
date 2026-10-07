"""Structured logging with the standard library only.

JSON lines are the default so logs can be shipped to any aggregator without
parsing rules. Extra context is attached with ``logger.info(msg, extra={...})``.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

# Attributes every LogRecord has; anything else was passed through ``extra``.
_RESERVED = set(logging.makeLogRecord({}).__dict__) | {
    "message",
    "asctime",
    "taskName",
    "color_message",  # uvicorn's ANSI duplicate of msg
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update({k: v for k, v in record.__dict__.items() if k not in _RESERVED})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # Route uvicorn through the same handler so every line has one format.
    # uvicorn.access is left alone: request metrics cover it, and the CLI's
    # --no-access-log keeps healthcheck polling out of the logs.
    for name in ("uvicorn", "uvicorn.error"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True
    # Driver chatter is rarely useful at INFO.
    for noisy in ("cassandra", "neo4j", "pymongo", "kafka"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
