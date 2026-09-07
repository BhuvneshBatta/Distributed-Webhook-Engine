import os
import uuid

# Point at locally-reachable services (e.g. `docker compose up -d postgres redis`)
# rather than the in-container hostnames used by docker-compose.yml. Must be
# set before importing anything from `app.config`, since Settings are read
# once at module import time.
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.deps import get_db
from app.broker.rate_limiter import TokenBucketLimiter
from app.broker.stream import RedisStreamManager
from app.config import settings
from app.database import Base
from app.main import app
from app.models import Endpoint

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def engine():
    engine = create_async_engine(settings.database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(engine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def db_session(session_factory) -> AsyncSession:
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def redis_client():
    client = Redis.from_url(settings.redis_url, decode_responses=False)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def stream_manager(redis_client):
    manager = RedisStreamManager(redis_client, settings.stream_name, settings.consumer_group)
    await manager.ensure_group()
    return manager


@pytest_asyncio.fixture
async def rate_limiter(redis_client):
    return TokenBucketLimiter(redis_client)


@pytest_asyncio.fixture
async def test_endpoint(db_session: AsyncSession) -> Endpoint:
    endpoint = Endpoint(
        name="test-endpoint",
        url="http://localhost:9000/webhook-sink?mode=success",
        secret_key="test-secret-key",
        rate_limit_per_second=10,
        burst_capacity=20,
    )
    db_session.add(endpoint)
    await db_session.commit()
    await db_session.refresh(endpoint)
    return endpoint


@pytest_asyncio.fixture
async def api_client(redis_client, engine, session_factory):
    # Override get_db to use this test's own engine/session factory instead
    # of the app's module-level global engine, which is bound to whichever
    # event loop first touched it. Reusing that global engine across tests
    # under pytest-asyncio's per-test event loops breaks asyncpg connections
    # ("Event loop is closed") once a later test's loop tries to use them.
    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db

    app.state.redis = redis_client
    app.state.stream_manager = RedisStreamManager(
        redis_client, settings.stream_name, settings.consumer_group
    )
    await app.state.stream_manager.ensure_group()
    app.state.rate_limiter = TokenBucketLimiter(redis_client)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.pop(get_db, None)


def unique_idempotency_key() -> str:
    return f"idem-{uuid.uuid4()}"
