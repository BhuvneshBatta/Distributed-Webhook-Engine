import asyncio

import pytest

pytestmark = pytest.mark.asyncio


async def test_token_bucket_allows_up_to_burst_capacity(rate_limiter):
    endpoint_id = "endpoint-burst-test"
    capacity = 5

    results = [
        await rate_limiter.acquire(endpoint_id, capacity=capacity, fill_rate=1.0)
        for _ in range(capacity)
    ]

    assert all(results)


async def test_token_bucket_rejects_once_exhausted(rate_limiter):
    endpoint_id = "endpoint-exhaust-test"
    capacity = 3

    for _ in range(capacity):
        assert await rate_limiter.acquire(endpoint_id, capacity=capacity, fill_rate=1.0)

    exhausted = await rate_limiter.acquire(endpoint_id, capacity=capacity, fill_rate=1.0)
    assert exhausted is False


async def test_token_bucket_refills_over_time(rate_limiter):
    endpoint_id = "endpoint-refill-test"
    capacity = 2
    fill_rate = 10.0  # 10 tokens/sec, so ~100ms should refill one token

    assert await rate_limiter.acquire(endpoint_id, capacity=capacity, fill_rate=fill_rate)
    assert await rate_limiter.acquire(endpoint_id, capacity=capacity, fill_rate=fill_rate)
    assert not await rate_limiter.acquire(endpoint_id, capacity=capacity, fill_rate=fill_rate)

    await asyncio.sleep(0.2)

    assert await rate_limiter.acquire(endpoint_id, capacity=capacity, fill_rate=fill_rate)


async def test_token_bucket_is_isolated_per_endpoint(rate_limiter):
    capacity = 1
    assert await rate_limiter.acquire("endpoint-a", capacity=capacity, fill_rate=1.0)
    assert await rate_limiter.acquire("endpoint-b", capacity=capacity, fill_rate=1.0)
