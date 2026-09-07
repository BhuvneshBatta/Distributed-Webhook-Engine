import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker.rate_limiter import TokenBucketLimiter
from app.broker.stream import RedisStreamManager
from app.config import Settings
from app.models import DeadLetterQueue, DeliveryAttempt, Endpoint, WebhookEvent
from app.security import canonical_json, generate_webhook_headers

logger = logging.getLogger(__name__)

RATE_LIMIT_RETRY_DELAY_SECONDS = 1.0
ENDPOINT_CACHE_TTL_SECONDS = 30.0


@dataclass(frozen=True)
class EndpointSnapshot:
    id: uuid.UUID
    url: str
    secret_key: str
    rate_limit_per_second: int
    burst_capacity: int
    is_active: bool


class EndpointCache:
    """Short-TTL in-memory cache so every dispatch doesn't hit PostgreSQL.

    Endpoint metadata (URL, secret, rate limits) changes rarely relative to
    delivery volume, so a small TTL keeps read pressure off the database
    without risking long-lived staleness after a subscriber rotates a secret.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], ttl_seconds: float) -> None:
        self._session_factory = session_factory
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, tuple[EndpointSnapshot, float]] = {}

    async def get(self, endpoint_id: str) -> EndpointSnapshot | None:
        cached = self._cache.get(endpoint_id)
        if cached is not None and cached[1] > time.monotonic():
            return cached[0]

        async with self._session_factory() as session:
            endpoint = await session.get(Endpoint, uuid.UUID(endpoint_id))

        if endpoint is None:
            return None

        snapshot = EndpointSnapshot(
            id=endpoint.id,
            url=endpoint.url,
            secret_key=endpoint.secret_key,
            rate_limit_per_second=endpoint.rate_limit_per_second,
            burst_capacity=endpoint.burst_capacity,
            is_active=endpoint.is_active,
        )
        self._cache[endpoint_id] = (snapshot, time.monotonic() + self._ttl_seconds)
        return snapshot


def compute_backoff_delay(attempt_number: int, base_delay: float, max_delay: float) -> float:
    """Exponential backoff with full jitter (Section 5.4)."""
    calculated_ceiling = min(max_delay, base_delay * (2 ** (attempt_number - 1)))
    return random.uniform(0, calculated_ceiling)


class WorkerDispatcher:
    def __init__(
        self,
        redis: Redis,
        stream_manager: RedisStreamManager,
        rate_limiter: TokenBucketLimiter,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        self._redis = redis
        self._stream_manager = stream_manager
        self._rate_limiter = rate_limiter
        self._session_factory = session_factory
        self._settings = settings
        self._endpoint_cache = EndpointCache(session_factory, ENDPOINT_CACHE_TTL_SECONDS)
        self._http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.http_timeout_seconds),
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )
        self._running = False

    async def aclose(self) -> None:
        await self._http_client.aclose()

    def stop(self) -> None:
        self._running = False

    async def run_pool(self) -> None:
        await self._stream_manager.ensure_group()
        self._running = True
        workers = [
            asyncio.create_task(self._consumer_loop(f"worker-{i}"))
            for i in range(self._settings.worker_concurrency)
        ]
        logger.info("Started %s worker consumer tasks", len(workers))
        await asyncio.gather(*workers)

    async def _consumer_loop(self, consumer_name: str) -> None:
        while self._running:
            try:
                messages = await self._stream_manager.read_batch(
                    consumer_name=consumer_name,
                    count=1,
                    block_ms=self._settings.stream_block_ms,
                )
            except Exception:
                logger.exception("Error reading from stream as %s", consumer_name)
                await asyncio.sleep(1.0)
                continue

            for message_id, fields in messages:
                try:
                    await self._process_message(message_id, fields)
                except Exception:
                    logger.exception(
                        "Unhandled error processing message %s; leaving unacked for reclaim",
                        message_id,
                    )

    async def _process_message(self, message_id: str, fields: dict[str, str]) -> None:
        event_id = fields["event_id"]
        endpoint_id = fields["endpoint_id"]
        event_type = fields["event_type"]
        payload = json.loads(fields["payload"])
        attempt_number = int(fields.get("attempt_number", "0")) + 1

        endpoint = await self._endpoint_cache.get(endpoint_id)
        if endpoint is None or not endpoint.is_active:
            logger.warning(
                "Dropping event %s: endpoint %s not found or inactive", event_id, endpoint_id
            )
            await self._stream_manager.ack(message_id)
            return

        allowed = await self._rate_limiter.acquire(
            endpoint_id=str(endpoint.id),
            capacity=endpoint.burst_capacity,
            fill_rate=endpoint.rate_limit_per_second,
        )
        if not allowed:
            await self._schedule_retry(
                event_id=event_id,
                endpoint_id=endpoint_id,
                event_type=event_type,
                payload=payload,
                attempt_number=attempt_number - 1,
                delay_seconds=RATE_LIMIT_RETRY_DELAY_SECONDS,
            )
            await self._stream_manager.ack(message_id)
            return

        raw_body = canonical_json(payload)
        headers = generate_webhook_headers(payload, endpoint.secret_key)

        status_code: int | None = None
        response_body: str | None = None
        error_message: str | None = None
        start = time.perf_counter()
        try:
            response = await self._http_client.post(
                endpoint.url, content=raw_body.encode("utf-8"), headers=headers
            )
            status_code = response.status_code
            response_body = response.text[:4000]
        except httpx.TimeoutException as exc:
            error_message = f"Timeout: {exc}"
        except httpx.HTTPError as exc:
            error_message = f"HTTP error: {exc}"
        latency_ms = int((time.perf_counter() - start) * 1000)

        success = status_code is not None and 200 <= status_code < 300

        should_retry = False
        retry_delay_seconds = 0.0

        async with self._session_factory() as session:
            await self._record_attempt(
                session,
                event_id=event_id,
                attempt_number=attempt_number,
                status_code=status_code,
                latency_ms=latency_ms,
                request_headers=headers,
                response_body=response_body,
                error_message=error_message,
            )

            if success:
                await self._mark_delivered(session, event_id, attempt_number)
            elif attempt_number < self._settings.max_retry_attempts:
                await self._mark_retrying(session, event_id, attempt_number)
                should_retry = True
                retry_delay_seconds = compute_backoff_delay(
                    attempt_number,
                    self._settings.base_retry_delay_seconds,
                    self._settings.max_retry_delay_seconds,
                )
            else:
                final_error = error_message or f"HTTP {status_code}"
                await self._route_to_dlq(session, event_id, attempt_number, final_error)

            await session.commit()

        if should_retry:
            await self._schedule_retry(
                event_id=event_id,
                endpoint_id=endpoint_id,
                event_type=event_type,
                payload=payload,
                attempt_number=attempt_number,
                delay_seconds=retry_delay_seconds,
            )

        await self._stream_manager.ack(message_id)

    async def _record_attempt(
        self,
        session: AsyncSession,
        *,
        event_id: str,
        attempt_number: int,
        status_code: int | None,
        latency_ms: int,
        request_headers: dict[str, str],
        response_body: str | None,
        error_message: str | None,
    ) -> None:
        attempt = DeliveryAttempt(
            event_id=uuid.UUID(event_id),
            attempt_number=attempt_number,
            response_status_code=status_code,
            execution_latency_ms=latency_ms,
            request_headers=request_headers,
            response_body=response_body,
            error_message=error_message,
        )
        session.add(attempt)

    async def _mark_delivered(self, session: AsyncSession, event_id: str, attempt_number: int) -> None:
        event = await session.get(WebhookEvent, uuid.UUID(event_id))
        if event is None:
            return
        event.status = "DELIVERED"
        event.total_attempts = attempt_number

    async def _mark_retrying(self, session: AsyncSession, event_id: str, attempt_number: int) -> None:
        event = await session.get(WebhookEvent, uuid.UUID(event_id))
        if event is None:
            return
        event.status = "RETRYING"
        event.total_attempts = attempt_number

    async def _route_to_dlq(
        self, session: AsyncSession, event_id: str, attempt_number: int, final_error: str
    ) -> None:
        event = await session.get(WebhookEvent, uuid.UUID(event_id))
        if event is None:
            return
        event.status = "FAILED"
        event.total_attempts = attempt_number

        dlq_entry = DeadLetterQueue(
            event_id=event.id,
            total_attempts=attempt_number,
            final_error=final_error,
            last_attempted_at=datetime.now(UTC),
            resolved=False,
        )
        session.add(dlq_entry)
        logger.warning("Event %s routed to DLQ after %s attempts", event_id, attempt_number)

    async def _schedule_retry(
        self,
        *,
        event_id: str,
        endpoint_id: str,
        event_type: str,
        payload: dict,
        attempt_number: int,
        delay_seconds: float,
    ) -> None:
        execution_timestamp = time.time() + delay_seconds
        retry_payload = {
            "event_id": event_id,
            "endpoint_id": endpoint_id,
            "event_type": event_type,
            "payload": payload,
            "attempt_number": attempt_number,
        }
        serialized = json.dumps(retry_payload, separators=(",", ":"), sort_keys=True)
        await self._redis.zadd(self._settings.retry_zset_key, {serialized: execution_timestamp})
