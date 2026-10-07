"""HTTP metrics exposed at ``/metrics`` for Prometheus."""

from __future__ import annotations

import time

from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

REQUESTS = Counter("fraudx_http_requests_total", "HTTP requests", ["method", "route", "status"])
LATENCY = Histogram(
    "fraudx_http_request_duration_seconds", "HTTP request latency", ["method", "route"]
)


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        # Label by route template (/customers/{id}), never the raw path, to
        # keep label cardinality bounded.
        route = request.scope.get("route")
        template = getattr(route, "path", "unmatched")
        LATENCY.labels(request.method, template).observe(time.perf_counter() - start)
        REQUESTS.labels(request.method, template, str(response.status_code)).inc()
        return response
