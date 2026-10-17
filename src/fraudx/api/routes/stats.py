"""Dashboard KPIs: pipeline counters (Prometheus) and analyst outcomes (MongoDB)."""

from __future__ import annotations

import logging
import math
import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from fraudx.api.stores import Stores, get_stores

router = APIRouter(prefix="/stats", tags=["stats"])
log = logging.getLogger(__name__)

WINDOW = "24h"


class Point(BaseModel):
    t: float
    value: float


class LevelSeries(BaseModel):
    level: str
    points: list[Point]


class Stage(BaseModel):
    """One stream worker: throughput, backlog and latency."""

    worker: str
    messages_per_s: float | None = None
    lag: float | None = None
    latency_p95_s: float | None = None  # produce -> processed, p95 over 5 minutes


# Pipeline order, for display.
STAGES = (
    "feature-processor", "graph-writer", "scorer", "history-writer", "profile-writer",
    "alert-handler",
)  # fmt: skip


class Overview(BaseModel):
    window: str
    scored: float | None = None
    by_level: dict[str, float] = {}
    alerts: float | None = None
    transactions_per_s: float | None = None
    avg_risk_score: float | None = None
    throughput: list[Point] = []  # scored/s over the last 30 min, one point per 30 s
    alerts_series: list[LevelSeries] = []  # MEDIUM / HIGH per minute, last 60 min
    stages: list[Stage] = []
    investigations: dict[str, int] = {}
    confirmed_precision: float | None = None  # confirmed / (confirmed + false positive)
    thresholds: dict[str, int] = {}  # minimum score per level, from config/risk.yaml
    alert_threshold: int | None = None
    metrics_available: bool = True


def _stages(prom: Any) -> list[Stage]:
    rate = prom.by_label(
        'sum by (worker) (rate(fraudx_consumer_messages_total{outcome="ok"}[1m]))', "worker"
    )
    lag = prom.by_label("sum by (group) (fraudx_consumer_lag)", "group")
    latency = prom.by_label(
        "histogram_quantile(0.95, sum by (le, worker) "
        "(rate(fraudx_consumer_message_age_seconds_bucket[5m])))",
        "worker",
    )
    return [
        Stage(
            worker=worker,
            messages_per_s=_num(rate.get(worker)),
            lag=_num(lag.get(f"fraudx-{worker}")),
            latency_p95_s=_num(latency.get(worker)),
        )
        for worker in STAGES
    ]


def _num(value: float | None) -> float | None:
    return None if value is None or math.isnan(value) else round(value, 3)


@router.get("/overview", response_model=Overview)
def overview(stores: Annotated[Stores, Depends(get_stores)]):
    """Last 24 h of the pipeline, and the analysts' verdicts so far.

    Live traffic carries no fraud labels (ADR-019), so model accuracy is
    measured where truth exists: analysts' decisions on investigations.
    """
    config = stores.config()
    out = Overview(
        window=WINDOW,
        thresholds={"MEDIUM": config.thresholds.medium, "HIGH": config.thresholds.high},
        alert_threshold=config.alert_threshold,
    )
    prom = stores.prometheus()
    try:
        out.by_level = {
            k: round(v) for k, v in prom.by_label(
                f"sum by (level) (increase(fraudx_scored_total[{WINDOW}]))", "level").items()
        }  # fmt: skip
        out.scored = float(sum(out.by_level.values()))
        out.alerts = _num(prom.scalar(f"sum(increase(fraudx_alerts_total[{WINDOW}]))"))
        out.transactions_per_s = _num(prom.scalar("sum(rate(fraudx_scored_total[1m]))"))
        out.avg_risk_score = _num(prom.scalar(
            f"sum(increase(fraudx_risk_score_sum[{WINDOW}])) / "
            f"sum(increase(fraudx_risk_score_count[{WINDOW}]))"))  # fmt: skip
        end = time.time()
        out.throughput = [
            Point(t=t, value=round(v, 2))
            for t, v in prom.series("sum(rate(fraudx_scored_total[1m]))", end - 1800, end, 30)
        ]
        by_level = prom.series_by_label(
            'sum by (level) (increase(fraudx_scored_total{level!="LOW"}[1m]))',
            "level", end - 3600, end, 60,
        )  # fmt: skip
        out.alerts_series = [
            LevelSeries(
                level=level, points=[Point(t=t, value=round(v)) for t, v in by_level[level]]
            )
            for level in ("HIGH", "MEDIUM")
            if level in by_level
        ]
        out.stages = _stages(prom)
    except (OSError, RuntimeError, ValueError) as exc:
        log.warning("prometheus unavailable for KPIs", extra={"error": str(exc)})
        out.metrics_available = False
    counts = stores.investigations().counts()
    out.investigations = counts
    decided = counts.get("CONFIRMED_FRAUD", 0) + counts.get("FALSE_POSITIVE", 0)
    if decided:
        out.confirmed_precision = round(counts["CONFIRMED_FRAUD"] / decided, 4)
    return out
