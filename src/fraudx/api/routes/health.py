from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status
from pydantic import BaseModel

from fraudx import __version__
from fraudx.storage.health import DependencyProbe

router = APIRouter(tags=["health"])


class Liveness(BaseModel):
    status: str
    version: str


class DependencyStatus(BaseModel):
    ok: bool
    latency_ms: float
    detail: str


class Readiness(BaseModel):
    status: str
    dependencies: dict[str, DependencyStatus]


def get_probe(request: Request) -> DependencyProbe:
    return request.app.state.probe


@router.get("/health", response_model=Liveness)
async def liveness() -> Liveness:
    """Process is up. Does not touch any dependency (safe for restart policies)."""
    return Liveness(status="ok", version=__version__)


@router.get(
    "/health/ready",
    response_model=Readiness,
    responses={503: {"model": Readiness, "description": "A dependency is unavailable"}},
)
async def readiness(
    response: Response, probe: Annotated[DependencyProbe, Depends(get_probe)]
) -> Readiness:
    """All backing stores reachable and Kafka topics provisioned."""
    results = await probe.check_all()
    ready = all(r.ok for r in results)
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return Readiness(
        status="ready" if ready else "degraded",
        dependencies={
            r.name: DependencyStatus(ok=r.ok, latency_ms=r.latency_ms, detail=r.detail)
            for r in results
        },
    )
