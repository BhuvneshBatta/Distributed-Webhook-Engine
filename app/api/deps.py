from fastapi import Request
from redis.asyncio import Redis

from app.broker.rate_limiter import TokenBucketLimiter
from app.broker.stream import RedisStreamManager
from app.database import get_db

__all__ = ["get_db", "get_redis", "get_stream_manager", "get_rate_limiter"]


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_stream_manager(request: Request) -> RedisStreamManager:
    return request.app.state.stream_manager


def get_rate_limiter(request: Request) -> TokenBucketLimiter:
    return request.app.state.rate_limiter
