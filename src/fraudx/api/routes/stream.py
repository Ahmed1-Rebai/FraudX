"""Live stream of scored transactions (Server-Sent Events from ``transactions.scored``).

Each connection reads the topic from its end with its own throw-away
consumer group (nothing is committed), so viewers never affect the pipeline.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse

from fraudx.api.stores import Stores, get_stores
from fraudx.core.schemas import ScoredTransaction

router = APIRouter(prefix="/stream", tags=["stream"])

KEEPALIVE_S = 15.0


def compact(scored: ScoredTransaction) -> dict[str, Any]:
    event, risk = scored.event, scored.risk
    return {
        "transaction_id": event.transaction_id,
        "customer_id": event.customer_id,
        "event_time": event.event_time.isoformat(),
        "amount": float(event.amount),
        "currency": event.currency,
        "transaction_type": event.transaction_type.value,
        "country": event.location.country if event.location else None,
        "score": risk.score,
        "level": risk.level.value,
        "components": risk.components,
        "rule_hits": list(risk.rule_hits),
        "scored_at": scored.scored_at.isoformat(),
    }


@router.get("/scored")
async def stream_scored(
    request: Request,
    stores: Annotated[Stores, Depends(get_stores)],
    limit: Annotated[int | None, Query(ge=1, description="stop after N events")] = None,
) -> StreamingResponse:
    """``text/event-stream``: one ``data:`` line (JSON) per scored transaction."""

    async def events() -> AsyncIterator[str]:
        consumer = stores.stream_consumer()
        sent, idle = 0, 0.0
        try:
            while not await request.is_disconnected():
                messages = await asyncio.to_thread(consumer.consume, 200, 1.0)
                batch = [m for m in messages if m.error() is None and m.value()]
                for message in batch:
                    scored = ScoredTransaction.model_validate_json(message.value())
                    yield f"data: {json.dumps(compact(scored))}\n\n"
                    sent += 1
                    if limit is not None and sent >= limit:
                        return
                idle = 0.0 if batch else idle + 1.0
                if idle >= KEEPALIVE_S:
                    idle = 0.0
                    yield ": keepalive\n\n"
        finally:
            await asyncio.to_thread(consumer.close)

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(events(), media_type="text/event-stream", headers=headers)
