"""Minimal Prometheus HTTP API client for the dashboard KPIs."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any


class Prometheus:
    def __init__(self, url: str, timeout_s: float = 3.0) -> None:
        self.url, self.timeout_s = url.rstrip("/"), timeout_s

    def _get(self, path: str, **params: Any) -> list[dict[str, Any]]:
        query = urllib.parse.urlencode(params)
        with urllib.request.urlopen(f"{self.url}{path}?{query}", timeout=self.timeout_s) as resp:  # noqa: S310
            body = json.load(resp)
        if body.get("status") != "success":
            raise RuntimeError(f"prometheus: {body.get('error', 'query failed')}")
        return body["data"]["result"]

    def scalar(self, promql: str) -> float | None:
        """First value of an instant query (None when the series doesn't exist yet)."""
        result = self._get("/api/v1/query", query=promql)
        return float(result[0]["value"][1]) if result else None

    def by_label(self, promql: str, label: str) -> dict[str, float]:
        return {r["metric"].get(label, ""): float(r["value"][1])
                for r in self._get("/api/v1/query", query=promql)}  # fmt: skip

    def series_by_label(
        self, promql: str, label: str, start: float, end: float, step_s: int
    ) -> dict[str, list[tuple[float, float]]]:
        result = self._get("/api/v1/query_range", query=promql, start=start, end=end, step=step_s)
        return {
            r["metric"].get(label, ""): [(float(t), float(v)) for t, v in r["values"]]
            for r in result
        }

    def series(
        self, promql: str, start: float, end: float, step_s: int
    ) -> list[tuple[float, float]]:
        result = self._get("/api/v1/query_range", query=promql, start=start, end=end, step=step_s)
        return [(float(t), float(v)) for t, v in result[0]["values"]] if result else []
