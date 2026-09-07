import asyncio
import logging
import signal

from redis.asyncio import Redis

from app.broker.rate_limiter import TokenBucketLimiter
from app.broker.stream import RedisStreamManager
from app.config import settings
from app.database import AsyncSessionLocal
from app.worker.dispatcher import WorkerDispatcher
from app.worker.scheduler import build_scheduler

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    redis = Redis.from_url(settings.redis_url, decode_responses=False)
    stream_manager = RedisStreamManager(redis, settings.stream_name, settings.consumer_group)
    rate_limiter = TokenBucketLimiter(redis)
    scheduler = build_scheduler(redis, stream_manager, settings)
    dispatcher = WorkerDispatcher(
        redis=redis,
        stream_manager=stream_manager,
        rate_limiter=rate_limiter,
        session_factory=AsyncSessionLocal,
        settings=settings,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Shutdown signal received")
        dispatcher.stop()
        scheduler.stop()
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    logger.info(
        "Starting worker daemon: concurrency=%s stream=%s group=%s",
        settings.worker_concurrency,
        settings.stream_name,
        settings.consumer_group,
    )

    scheduler_task = asyncio.create_task(scheduler.run_forever())
    dispatcher_task = asyncio.create_task(dispatcher.run_pool())

    try:
        await stop_event.wait()
    finally:
        scheduler.stop()
        dispatcher.stop()
        await dispatcher.aclose()
        await redis.aclose()
        for task in (scheduler_task, dispatcher_task):
            task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
