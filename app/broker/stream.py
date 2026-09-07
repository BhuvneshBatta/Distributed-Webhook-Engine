import logging
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)

StreamMessage = tuple[str, dict[str, Any]]


class RedisStreamManager:
    """Thin wrapper around Redis Streams providing durable at-least-once queueing.

    Uses a persistent consumer group so that unacknowledged messages remain
    claimable (via XPENDING/XCLAIM) if a worker crashes mid-processing.
    """

    def __init__(self, redis: Redis, stream_name: str, group_name: str) -> None:
        self._redis = redis
        self.stream_name = stream_name
        self.group_name = group_name

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                name=self.stream_name, groupname=self.group_name, id="0", mkstream=True
            )
            logger.info(
                "Created consumer group %s on stream %s", self.group_name, self.stream_name
            )
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                return
            raise

    async def add_event(self, fields: dict[str, str]) -> str:
        message_id: bytes | str = await self._redis.xadd(self.stream_name, fields)
        return message_id if isinstance(message_id, str) else message_id.decode()

    async def read_batch(
        self, consumer_name: str, count: int, block_ms: int
    ) -> list[StreamMessage]:
        response = await self._redis.xreadgroup(
            groupname=self.group_name,
            consumername=consumer_name,
            streams={self.stream_name: ">"},
            count=count,
            block=block_ms,
        )
        if not response:
            return []

        messages: list[StreamMessage] = []
        for _stream_key, entries in response:
            for entry_id, fields in entries:
                normalized_id = entry_id if isinstance(entry_id, str) else entry_id.decode()
                normalized_fields = {
                    (k if isinstance(k, str) else k.decode()): (
                        v if isinstance(v, str) else v.decode()
                    )
                    for k, v in fields.items()
                }
                messages.append((normalized_id, normalized_fields))
        return messages

    async def ack(self, message_id: str) -> None:
        await self._redis.xack(self.stream_name, self.group_name, message_id)

    async def claim_stale(
        self, consumer_name: str, min_idle_ms: int, count: int
    ) -> list[StreamMessage]:
        """Reclaim messages left pending by a crashed worker consumer."""
        pending = await self._redis.xpending_range(
            self.stream_name, self.group_name, min="-", max="+", count=count
        )
        if not pending:
            return []

        stale_ids = [
            entry["message_id"] if isinstance(entry["message_id"], str) else entry["message_id"].decode()
            for entry in pending
            if entry["time_since_delivered"] >= min_idle_ms
        ]
        if not stale_ids:
            return []

        claimed = await self._redis.xclaim(
            self.stream_name,
            self.group_name,
            consumer_name,
            min_idle_time=min_idle_ms,
            message_ids=stale_ids,
        )
        messages: list[StreamMessage] = []
        for entry_id, fields in claimed:
            normalized_id = entry_id if isinstance(entry_id, str) else entry_id.decode()
            normalized_fields = {
                (k if isinstance(k, str) else k.decode()): (
                    v if isinstance(v, str) else v.decode()
                )
                for k, v in fields.items()
            }
            messages.append((normalized_id, normalized_fields))
        return messages

    async def queue_depth(self) -> int:
        return await self._redis.xlen(self.stream_name)

    async def pending_count(self) -> int:
        summary = await self._redis.xpending(self.stream_name, self.group_name)
        if not summary:
            return 0
        return summary.get("pending", 0)
