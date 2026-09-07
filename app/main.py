import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from redis.asyncio import Redis

from app.api.v1.dashboard import router as dashboard_router
from app.api.v1.endpoints import router as endpoints_router
from app.api.v1.publish import router as publish_router
from app.broker.rate_limiter import TokenBucketLimiter
from app.broker.stream import RedisStreamManager
from app.config import settings
from app.database import init_models

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await init_models()

    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    stream_manager = RedisStreamManager(redis, settings.stream_name, settings.consumer_group)
    await stream_manager.ensure_group()

    app.state.redis = redis
    app.state.stream_manager = stream_manager
    app.state.rate_limiter = TokenBucketLimiter(redis)

    logger.info("Ingestion API started")
    yield

    await redis.aclose()
    logger.info("Ingestion API shut down")


app = FastAPI(
    title="Distributed Webhook Delivery Engine",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(endpoints_router)
app.include_router(publish_router)
app.include_router(dashboard_router)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def dashboard_index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
