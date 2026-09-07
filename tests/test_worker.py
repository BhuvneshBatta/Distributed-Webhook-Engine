import uuid

import httpx
import pytest
import pytest_asyncio
import respx
from sqlalchemy import select

from app.config import settings
from app.models import DeadLetterQueue, DeliveryAttempt, WebhookEvent
from app.worker.dispatcher import WorkerDispatcher, compute_backoff_delay

MOCK_URL = "http://mock.test/webhook-sink"


@pytest_asyncio.fixture
async def dispatch_endpoint(db_session, test_endpoint):
    test_endpoint.url = MOCK_URL
    db_session.add(test_endpoint)
    await db_session.commit()
    await db_session.refresh(test_endpoint)
    return test_endpoint


@pytest_asyncio.fixture
async def dispatch_event(db_session, dispatch_endpoint) -> WebhookEvent:
    event = WebhookEvent(
        endpoint_id=dispatch_endpoint.id,
        event_type="order.created",
        payload={"order_id": "worker-test"},
        idempotency_key=f"idem-{uuid.uuid4()}",
        status="QUEUED",
    )
    db_session.add(event)
    await db_session.commit()
    await db_session.refresh(event)
    return event


@pytest_asyncio.fixture
async def dispatcher(redis_client, stream_manager, rate_limiter, session_factory):
    d = WorkerDispatcher(
        redis=redis_client,
        stream_manager=stream_manager,
        rate_limiter=rate_limiter,
        session_factory=session_factory,
        settings=settings,
    )
    yield d
    await d.aclose()


def _fields_for(event: WebhookEvent, attempt_number: int) -> dict[str, str]:
    return {
        "event_id": str(event.id),
        "endpoint_id": str(event.endpoint_id),
        "event_type": event.event_type,
        "payload": '{"order_id":"worker-test"}',
        "attempt_number": str(attempt_number),
    }


class TestBackoffJitter:
    @pytest.mark.parametrize("attempt_number", [1, 2, 3, 4, 5])
    def test_delay_within_full_jitter_bounds(self, attempt_number):
        base_delay = 2.0
        max_delay = 60.0
        ceiling = min(max_delay, base_delay * (2 ** (attempt_number - 1)))

        for _ in range(50):
            delay = compute_backoff_delay(attempt_number, base_delay, max_delay)
            assert 0 <= delay <= ceiling

    def test_delay_is_capped_at_max_delay(self):
        delay = compute_backoff_delay(10, base_delay=2.0, max_delay=60.0)
        assert delay <= 60.0


class TestDispatchOutcomes:
    pytestmark = pytest.mark.asyncio

    @respx.mock
    async def test_successful_delivery_marks_event_delivered(
        self, dispatcher, dispatch_event, db_session, redis_client
    ):
        respx.post(MOCK_URL).mock(return_value=httpx.Response(200, json={"ok": True}))

        await dispatcher._process_message("0-1", _fields_for(dispatch_event, 0))

        refreshed = await db_session.get(WebhookEvent, dispatch_event.id, populate_existing=True)
        assert refreshed.status == "DELIVERED"
        assert refreshed.total_attempts == 1

        attempts = (
            await db_session.execute(
                select(DeliveryAttempt).where(DeliveryAttempt.event_id == dispatch_event.id)
            )
        ).scalars().all()
        assert len(attempts) == 1
        assert attempts[0].response_status_code == 200

    @respx.mock
    async def test_failure_below_max_attempts_schedules_retry(
        self, dispatcher, dispatch_event, db_session, redis_client
    ):
        respx.post(MOCK_URL).mock(return_value=httpx.Response(500, text="server error"))

        await dispatcher._process_message("0-1", _fields_for(dispatch_event, 0))

        refreshed = await db_session.get(WebhookEvent, dispatch_event.id, populate_existing=True)
        assert refreshed.status == "RETRYING"
        assert refreshed.total_attempts == 1

        pending_retries = await redis_client.zcard(settings.retry_zset_key)
        assert pending_retries == 1

    @respx.mock
    async def test_exhausted_retries_routes_to_dead_letter_queue(
        self, dispatcher, dispatch_event, db_session, redis_client
    ):
        respx.post(MOCK_URL).mock(return_value=httpx.Response(500, text="server error"))

        # Attempts 1-4 (fields attempt_number 0..3) should retry; attempt 5
        # (fields attempt_number 4) exceeds MAX_RETRY_ATTEMPTS and dead-letters.
        for prior_attempts in range(settings.max_retry_attempts):
            await dispatcher._process_message("0-1", _fields_for(dispatch_event, prior_attempts))

        refreshed = await db_session.get(WebhookEvent, dispatch_event.id, populate_existing=True)
        assert refreshed.status == "FAILED"
        assert refreshed.total_attempts == settings.max_retry_attempts

        dlq_rows = (
            await db_session.execute(
                select(DeadLetterQueue).where(DeadLetterQueue.event_id == dispatch_event.id)
            )
        ).scalars().all()
        assert len(dlq_rows) == 1
        assert dlq_rows[0].total_attempts == settings.max_retry_attempts
        assert dlq_rows[0].resolved is False

        attempts = (
            await db_session.execute(
                select(DeliveryAttempt).where(DeliveryAttempt.event_id == dispatch_event.id)
            )
        ).scalars().all()
        assert len(attempts) == settings.max_retry_attempts

    @respx.mock
    async def test_timeout_is_treated_as_failure_and_retried(
        self, dispatcher, dispatch_event, db_session, redis_client
    ):
        respx.post(MOCK_URL).mock(side_effect=httpx.TimeoutException("timed out"))

        await dispatcher._process_message("0-1", _fields_for(dispatch_event, 0))

        refreshed = await db_session.get(WebhookEvent, dispatch_event.id, populate_existing=True)
        assert refreshed.status == "RETRYING"

        attempts = (
            await db_session.execute(
                select(DeliveryAttempt).where(DeliveryAttempt.event_id == dispatch_event.id)
            )
        ).scalars().all()
        assert attempts[0].error_message is not None
        assert "Timeout" in attempts[0].error_message
