"""Dashboard KPIs: pipeline counters (Prometheus) and analyst outcomes (MongoDB)."""

from __future__ import annotations

import logging
import math
import time
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from fraudx.api.stores import Stores, get_stores

router = APIRouter(prefix="/stats", tags=["stats"])
log = logging.getLogger(__name__)

WINDOW = "24h"


class Point(BaseModel):
    t: float
    value: float


class Overview(BaseModel):
    window: str
    scored: float | None = None
    by_level: dict[str, float] = {}
    alerts: float | None = None
    transactions_per_s: float | None = None
    avg_risk_score: float | None = None
    throughput: list[Point] = []  # scored/s over the last 30 min, one point per 30 s
    investigations: dict[str, int] = {}
    confirmed_precision: float | None = None  # confirmed / (confirmed + false positive)
    metrics_available: bool = True


def _num(value: float | None) -> float | None:
    return None if value is None or math.isnan(value) else round(value, 3)


@router.get("/overview", response_model=Overview)
def overview(stores: Annotated[Stores, Depends(get_stores)]):
    """Last 24 h of the pipeline, and the analysts' verdicts so far.

    Live traffic carries no fraud labels (ADR-019), so model accuracy is
    measured where truth exists: analysts' decisions on investigations.
    """
    out = Overview(window=WINDOW)
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
    except (OSError, RuntimeError, ValueError) as exc:
        log.warning("prometheus unavailable for KPIs", extra={"error": str(exc)})
        out.metrics_available = False
    counts = stores.investigations().counts()
    out.investigations = counts
    decided = counts.get("CONFIRMED_FRAUD", 0) + counts.get("FALSE_POSITIVE", 0)
    if decided:
        out.confirmed_precision = round(counts["CONFIRMED_FRAUD"] / decided, 4)
    return out
