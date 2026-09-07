import time

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local fill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
else
    local delta = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + delta * fill_rate)
    last_updated = now
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    redis.call("EXPIRE", key, 3600)
    return 1
else
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    return 0
end
"""


class TokenBucketLimiter:
    """Distributed token-bucket rate limiter backed by an atomic Redis Lua script.

    Sharing the script across every worker process guarantees no drift: token
    accounting always happens inside a single-threaded Lua execution on the
    Redis server, so concurrent callers never race on read-modify-write.
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._script: AsyncScript = redis.register_script(TOKEN_BUCKET_LUA)

    def _bucket_key(self, endpoint_id: str) -> str:
        return f"webhooks:ratelimit:{endpoint_id}"

    async def acquire(
        self,
        endpoint_id: str,
        capacity: int,
        fill_rate: float,
        requested: int = 1,
    ) -> bool:
        result = await self._script(
            keys=[self._bucket_key(endpoint_id)],
            args=[capacity, fill_rate, time.time(), requested],
        )
        return bool(result)
