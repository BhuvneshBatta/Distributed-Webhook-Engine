import pytest

from tests.conftest import unique_idempotency_key

pytestmark = pytest.mark.asyncio


async def test_create_endpoint_returns_generated_secret(api_client):
    response = await api_client.post(
        "/api/v1/endpoints",
        json={"name": "orders-service", "url": "http://localhost:9000/webhook-sink"},
    )
    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "orders-service"
    assert len(body["secret_key"]) >= 32
    assert body["rate_limit_per_second"] == 10
    assert body["burst_capacity"] == 20


async def test_publish_webhook_returns_202_and_queues_event(api_client, test_endpoint, stream_manager):
    response = await api_client.post(
        "/api/v1/webhooks/publish",
        json={
            "endpoint_id": str(test_endpoint.id),
            "event_type": "order.created",
            "payload": {"order_id": "abc123"},
        },
        headers={"Idempotency-Key": unique_idempotency_key()},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "QUEUED"
    assert body["event_id"]

    depth = await stream_manager.queue_depth()
    assert depth >= 1


async def test_publish_webhook_requires_idempotency_key_header(api_client, test_endpoint):
    response = await api_client.post(
        "/api/v1/webhooks/publish",
        json={
            "endpoint_id": str(test_endpoint.id),
            "event_type": "order.created",
            "payload": {"order_id": "abc123"},
        },
    )
    assert response.status_code == 422


async def test_publish_webhook_unknown_endpoint_returns_404(api_client):
    response = await api_client.post(
        "/api/v1/webhooks/publish",
        json={
            "endpoint_id": "00000000-0000-0000-0000-000000000000",
            "event_type": "order.created",
            "payload": {"order_id": "abc123"},
        },
        headers={"Idempotency-Key": unique_idempotency_key()},
    )
    assert response.status_code == 404


async def test_duplicate_idempotency_key_returns_same_event_id(api_client, test_endpoint):
    key = unique_idempotency_key()
    payload = {
        "endpoint_id": str(test_endpoint.id),
        "event_type": "order.created",
        "payload": {"order_id": "dup-test"},
    }

    first = await api_client.post(
        "/api/v1/webhooks/publish", json=payload, headers={"Idempotency-Key": key}
    )
    second = await api_client.post(
        "/api/v1/webhooks/publish", json=payload, headers={"Idempotency-Key": key}
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["event_id"] == second.json()["event_id"]


async def test_dashboard_stats_returns_expected_shape(api_client):
    response = await api_client.get("/api/v1/dashboard/stats")
    assert response.status_code == 200
    body = response.json()
    for key in (
        "queue_depth",
        "pending_retries",
        "total_queued",
        "total_delivered",
        "total_retrying",
        "total_dlq",
        "average_latency_ms",
    ):
        assert key in body
