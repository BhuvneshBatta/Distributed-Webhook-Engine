import json
import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, status
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis, get_stream_manager
from app.broker.stream import RedisStreamManager
from app.models import DeadLetterQueue, DeliveryAttempt, WebhookEvent
from app.schemas import DashboardStats, DeadLetterResponse, ReplayResponse

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


@router.get("/stats", response_model=DashboardStats)
async def get_stats(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    stream_manager: RedisStreamManager = Depends(get_stream_manager),
) -> DashboardStats:
    queue_depth = await stream_manager.queue_depth()
    pending_retries = await redis.zcard("webhooks:retry:zset")

    async def count_by_status(status_value: str) -> int:
        result = await db.execute(
            select(func.count()).select_from(WebhookEvent).where(
                WebhookEvent.status == status_value
            )
        )
        return result.scalar_one()

    total_queued = await count_by_status("QUEUED")
    total_delivered = await count_by_status("DELIVERED")
    total_retrying = await count_by_status("RETRYING")

    dlq_result = await db.execute(
        select(func.count()).select_from(DeadLetterQueue).where(
            DeadLetterQueue.resolved.is_(False)
        )
    )
    total_dlq = dlq_result.scalar_one()

    latency_result = await db.execute(select(func.avg(DeliveryAttempt.execution_latency_ms)))
    average_latency_ms = latency_result.scalar_one()

    return DashboardStats(
        queue_depth=queue_depth,
        pending_retries=pending_retries,
        total_queued=total_queued,
        total_delivered=total_delivered,
        total_retrying=total_retrying,
        total_dlq=total_dlq,
        average_latency_ms=(
            float(average_latency_ms) if average_latency_ms is not None else None
        ),
    )


@router.get("/dlq", response_model=list[DeadLetterResponse])
async def list_dlq(db: AsyncSession = Depends(get_db)) -> list[DeadLetterQueue]:
    result = await db.execute(
        select(DeadLetterQueue)
        .where(DeadLetterQueue.resolved.is_(False))
        .order_by(DeadLetterQueue.created_at.desc())
    )
    return list(result.scalars().all())


@router.post("/dlq/{dlq_id}/replay", response_model=ReplayResponse)
async def replay_dlq_entry(
    dlq_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    stream_manager: RedisStreamManager = Depends(get_stream_manager),
) -> ReplayResponse:
    dlq_entry = await db.get(DeadLetterQueue, dlq_id)
    if dlq_entry is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="DLQ entry not found")

    event = await db.get(WebhookEvent, dlq_entry.event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source event not found")

    event.status = "QUEUED"
    event.total_attempts = 0
    dlq_entry.resolved = True
    dlq_entry.resolved_at = datetime.now(UTC)

    await db.commit()

    await stream_manager.add_event(
        {
            "event_id": str(event.id),
            "endpoint_id": str(event.endpoint_id),
            "event_type": event.event_type,
            "payload": json.dumps(event.payload, separators=(",", ":"), sort_keys=True),
            "attempt_number": "0",
        }
    )

    return ReplayResponse(event_id=event.id, status="QUEUED")
