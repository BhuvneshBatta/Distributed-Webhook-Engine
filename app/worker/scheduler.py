import asyncio
import json
import logging
import time

from redis.asyncio import Redis

from app.broker.stream import RedisStreamManager
from app.config import Settings

logger = logging.getLogger(__name__)


class RetryScheduler:
    """Polls the delayed-retry ZSET and re-queues due events onto the main stream.

    Retries are stored in a Redis Sorted Set keyed by their scheduled Unix
    epoch (see Section 5.4 of the spec). Polling on an interval, rather than
    sleeping per-item, keeps a single lightweight loop able to drain
    arbitrarily many pending retries without spawning a timer per event.
    """

    def __init__(
        self,
        redis: Redis,
        stream_manager: RedisStreamManager,
        retry_zset_key: str,
        poll_interval_seconds: float,
    ) -> None:
        self._redis = redis
        self._stream_manager = stream_manager
        self._retry_zset_key = retry_zset_key
        self._poll_interval_seconds = poll_interval_seconds
        self._running = False

    async def run_forever(self) -> None:
        self._running = True
        logger.info("Retry scheduler started (poll interval=%ss)", self._poll_interval_seconds)
        while self._running:
            try:
                await self._drain_ready()
            except Exception:
                logger.exception("Error while draining retry ZSET")
            await asyncio.sleep(self._poll_interval_seconds)

    def stop(self) -> None:
        self._running = False

    async def _drain_ready(self) -> None:
        now = time.time()
        ready_members = await self._redis.zrangebyscore(
            self._retry_zset_key, min=0, max=now
        )
        for raw_member in ready_members:
            member = raw_member if isinstance(raw_member, str) else raw_member.decode()

            removed = await self._redis.zrem(self._retry_zset_key, member)
            if not removed:
                # Another scheduler instance already claimed this member.
                continue

            try:
                retry_payload = json.loads(member)
            except json.JSONDecodeError:
                logger.error("Discarding malformed retry payload: %s", member)
                continue

            await self._stream_manager.add_event(
                {
                    "event_id": retry_payload["event_id"],
                    "endpoint_id": retry_payload["endpoint_id"],
                    "event_type": retry_payload["event_type"],
                    "payload": json.dumps(
                        retry_payload["payload"], separators=(",", ":"), sort_keys=True
                    ),
                    "attempt_number": str(retry_payload["attempt_number"]),
                }
            )
            logger.info(
                "Requeued event %s for attempt %s",
                retry_payload["event_id"],
                retry_payload["attempt_number"],
            )


def build_scheduler(redis: Redis, stream_manager: RedisStreamManager, settings: Settings) -> RetryScheduler:
    return RetryScheduler(
        redis=redis,
        stream_manager=stream_manager,
        retry_zset_key=settings.retry_zset_key,
        poll_interval_seconds=settings.scheduler_poll_interval_seconds,
    )
