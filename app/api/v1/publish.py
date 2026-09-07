import json
import logging
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db, get_redis, get_stream_manager
from app.broker.stream import RedisStreamManager
from app.models import Endpoint, WebhookEvent
from app.schemas import WebhookPublishRequest, WebhookPublishResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/webhooks", tags=["webhooks"])

IDEMPOTENCY_TTL_SECONDS = 86400


@router.post(
    "/publish", response_model=WebhookPublishResponse, status_code=status.HTTP_202_ACCEPTED
)
async def publish_webhook(
    body: WebhookPublishRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
    stream_manager: RedisStreamManager = Depends(get_stream_manager),
) -> WebhookPublishResponse:
    endpoint = await db.get(Endpoint, body.endpoint_id)
    if endpoint is None or not endpoint.is_active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Endpoint not found or inactive"
        )

    # Generated up front (rather than relying on the ORM's flush-time default)
    # so the same id can be written to Redis for idempotency *before* the
    # row is committed.
    event_id = uuid.uuid4()
    event = WebhookEvent(
        id=event_id,
        endpoint_id=endpoint.id,
        event_type=body.event_type,
        payload=body.payload,
        idempotency_key=idempotency_key,
        status="QUEUED",
    )

    idempotency_redis_key = f"idempotency:{idempotency_key}"
    is_new = await redis.set(
        idempotency_redis_key, str(event_id), ex=IDEMPOTENCY_TTL_SECONDS, nx=True
    )

    if not is_new:
        cached_event_id = await redis.get(idempotency_redis_key)
        cached_event_id = (
            cached_event_id.decode() if isinstance(cached_event_id, bytes) else cached_event_id
        )
        return WebhookPublishResponse(event_id=cached_event_id, status="QUEUED")

    try:
        db.add(event)
        await db.commit()
        await db.refresh(event)

        await stream_manager.add_event(
            {
                "event_id": str(event.id),
                "endpoint_id": str(endpoint.id),
                "event_type": event.event_type,
                "payload": json.dumps(body.payload, separators=(",", ":"), sort_keys=True),
                "attempt_number": "0",
            }
        )
    except Exception:
        await redis.delete(idempotency_redis_key)
        raise

    return WebhookPublishResponse(event_id=event.id, status="QUEUED")
