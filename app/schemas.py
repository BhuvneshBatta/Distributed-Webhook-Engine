import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
class EndpointCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    url: HttpUrl
    rate_limit_per_second: int = Field(default=10, gt=0)
    burst_capacity: int = Field(default=20, gt=0)


class EndpointResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    url: str
    secret_key: str
    rate_limit_per_second: int
    burst_capacity: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Webhook publish
# ---------------------------------------------------------------------------
class WebhookPublishRequest(BaseModel):
    endpoint_id: uuid.UUID
    event_type: str = Field(..., min_length=1, max_length=100)
    payload: dict[str, Any]


class WebhookPublishResponse(BaseModel):
    event_id: uuid.UUID
    status: str


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
class DashboardStats(BaseModel):
    queue_depth: int
    pending_retries: int
    total_queued: int
    total_delivered: int
    total_retrying: int
    total_dlq: int
    average_latency_ms: float | None


class DeliveryAttemptResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    attempt_number: int
    response_status_code: int | None
    execution_latency_ms: int
    error_message: str | None
    created_at: datetime


class DeadLetterResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    total_attempts: int
    final_error: str
    last_attempted_at: datetime
    resolved: bool
    resolved_at: datetime | None
    created_at: datetime


class ReplayResponse(BaseModel):
    event_id: uuid.UUID
    status: str
