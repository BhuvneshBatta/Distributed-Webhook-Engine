# TECHNICAL SPECIFICATION & ARCHITECTURAL BLUEPRINT
## Production-Grade Distributed Webhook Delivery Engine

---

## 1. Executive Summary & Core Purpose

### 1.1 In One Sentence
A high-throughput, fault-tolerant backend service that reliably delivers asynchronous HTTP webhook notifications to third-party endpoints, guaranteeing at-least-once delivery semantics even when downstream receivers experience network partitions, latency degradation, or server outages, utilizing a durable event log, token-bucket rate limiting, exponential backoff with jitter, cryptographic HMAC signing, and dead-letter queue (DLQ) isolation.

### 1.2 Real-World Problem & Analogy
When a primary business transaction completes (e.g., payment capture, order placement, or KYC verification), the system must notify external subscriber servers via HTTP POST notifications. Direct, synchronous HTTP requests from the transaction thread introduce unacceptable operational risks:
- Downstream server outages or network drops cause unrecoverable event loss.
- High downstream latency (e.g., 5-second database locks) ties up caller worker threads, causing cascade starvation across upstream services.
- Burst traffic from promotional spikes can accidentally perform a distributed denial-of-service (DDoS) attack against third-party partner servers.

This engine isolates ingestion from delivery, persisting events into durable storage within single-digit milliseconds and offloading dispatch to an asynchronous worker fleet with automated resilience policies.

---

## 2. Technology Stack & Component Justifications

| Tier | Technology | Justification & Architectural Responsibility |
| :--- | :--- | :--- |
| **Ingestion API** | **FastAPI (Python 3.11+)** | High-performance, async-native ASGI web framework. Validates incoming schemas via Pydantic V2 and immediately enqueues messages without blocking. |
| **Message Broker** | **Redis Streams (v7+)** | Distributed append-only log with persistent consumer groups (`XREADGROUP`, `XACK`, `XPENDING`). Provides bounded memory usage, disk durability, and sub-millisecond pub/sub latencies. |
| **Relational Storage** | **PostgreSQL 15+** | ACID-compliant persistence for endpoint credentials, audit delivery attempts, dead-letter records, and historical metrics via asyncpg and SQLAlchemy 2.0. |
| **Concurrency & Workers**| **Python `asyncio`** | Non-blocking event loop enabling single-process workers to maintain thousands of concurrent outbound HTTP connections without the overhead of heavy OS threads. |
| **Outbound Transport** | **`httpx` (Async)** | Modern async HTTP/1.1 and HTTP/2 client featuring connection pooling, strict read/write timeouts, and custom header injection. |
| **Rate Limiter** | **Redis Token Bucket** | Distributed token-bucket algorithm executed atomically using Lua scripts in Redis memory, shared across all active worker instances. |
| **Security Layer** | **Python `hmac` + `hashlib`** | Cryptographic verification layer calculating `HMAC-SHA256` signatures across headers and raw payloads to safeguard against tampering and replay attacks. |
| **Orchestration** | **Docker Compose** | Single-command multi-container environment bootstrapping PostgreSQL, Redis, Ingestion API, Worker Daemon, and a Mock Endpoint. |
| **Verification & Load** | **Async Python Benchmarks** | Concurrency test runner leveraging `asyncio` and `httpx` to simulate sustained loads up to 10,000 concurrent payloads. |
| **Observability UI** | **FastAPI Static HTML + JS** | Embedded real-time operational dashboard monitoring queue depth, worker heartbeats, delivery success rates, and DLQ inspection. |

---

## 3. End-to-End System Data Flow

```
[ External System / Producer ]
               |
               | (1) POST /api/v1/webhooks/publish (JSON Payload + Idempotency-Key)
               v
+------------------------------------------------------------------------------------+
|                         INGESTION SERVICE (FastAPI)                                 |
| - Validate request schema via Pydantic                                             |
| - Verify endpoint exists & fetch metadata from PostgreSQL/Cache                    |
| - Atomic Idempotency Check: Redis SET key EX 86400 NX                              |
| - Append payload to Redis Stream `webhooks:events:stream` via XADD                 |
| - Return 202 Accepted { "event_id": "...", "status": "QUEUED" }                    |
+------------------------------------------------------------------------------------+
               |
               v
+------------------------------------------------------------------------------------+
|                      DISTRIBUTED BROKER (Redis Streams)                            |
| - Stream: `webhooks:events:stream`                                                 |
| - Consumer Group: `engine_workers_group`                                           |
| - Delayed Retry Storage: `webhooks:retry:zset` (Sorted Set keyed by Unix Epoch)    |
+------------------------------------------------------------------------------------+
               |                                            ^
               | (2) XREADGROUP (Prefetch batch)             | (7) ZADD delayed retry
               v                                            |     (Backoff + Jitter)
+-----------------------------------------------------+     |
|               WORKER DAEMON (asyncio)               |     |
| - Parse stream message & extract event metadata     |     |
| - Evaluate Token Bucket for target domain via Lua   |-----+
| - If no tokens: Sleep/Yield or reschedule           |
| - Construct payload signature (HMAC-SHA256)         |
| - Dispatch HTTP POST via httpx with 3.0s timeout    |
+-----------------------------------------------------+
        |                                       |
        | (3) Outbound HTTP POST                | (4) Write attempt logs & metrics
        v                                       v
+-----------------------+   +--------------------------------------------------------+
| RECIPIENT ENDPOINT    |   |                POSTGRESQL DATABASE                     |
| (Third-Party Server)  |   | - `endpoints`: Target URLs, secrets, rate limits       |
+-----------------------+   | - `delivery_attempts`: Latency, response status, errors|
        |                   | - `dead_letter_queue`: Permanently failed tasks        |
        | 2xx OK / Error    +--------------------------------------------------------+
        v                                       |
[ Worker evaluates response ]                   | (5) Query stats for real-time UI
- 2xx: XACK stream, write success to DB         v
- 4xx/5xx/Timeout & attempts < 5:              +-------------------------------------+
    Schedule exponential backoff retry         |        ADMIN DASHBOARD UI           |
- Max attempts reached (5):                    | (Live Queue Depth, DLQ, Latency P99)|
    Write to DLQ table, XACK original stream   +-------------------------------------+
```

---

## 4. Database Schema (PostgreSQL)

```sql
-- Extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Registered subscriber endpoints
CREATE TABLE endpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(120) NOT NULL,
    url VARCHAR(2048) NOT NULL,
    secret_key VARCHAR(128) NOT NULL,
    rate_limit_per_second INTEGER NOT NULL DEFAULT 10,
    burst_capacity INTEGER NOT NULL DEFAULT 20,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Master log of published webhook events
CREATE TABLE webhook_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    endpoint_id UUID NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    idempotency_key VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'QUEUED', -- QUEUED, DELIVERED, RETRYING, FAILED
    total_attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 3. Detailed audit trail of every single HTTP transmission
CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id UUID NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    response_status_code INTEGER NULL,
    execution_latency_ms INTEGER NOT NULL,
    request_headers JSONB NOT NULL,
    response_body TEXT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 4. Isolation repository for exhausted/permanently unrecoverable events
CREATE TABLE dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id UUID UNIQUE NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
    total_attempts INTEGER NOT NULL,
    final_error TEXT NOT NULL,
    last_attempted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indices for rapid querying and foreign key traversal
CREATE INDEX idx_events_endpoint ON webhook_events(endpoint_id);
CREATE INDEX idx_events_status ON webhook_events(status);
CREATE INDEX idx_attempts_event ON delivery_attempts(event_id);
CREATE INDEX idx_dlq_resolved ON dead_letter_queue(resolved);
```

---

## 5. Algorithmic Specifications & Implementation Logic

### 5.1 Distributed Idempotency Guard (Redis)
To prevent duplicate processing caused by upstream retries:
1. When receiving `POST /api/v1/webhooks/publish`, inspect the `Idempotency-Key` HTTP header.
2. Execute atomic command:
   ```python
   is_new = await redis.set(f"idempotency:{idempotency_key}", event_id, ex=86400, nx=True)
   ```
3. If `is_new` is `None`: Return HTTP `409 Conflict` (or return the original cached `event_id` with status `QUEUED`).
4. If `is_new` is `True`: Proceed to write to the PostgreSQL event record and Redis Stream.

### 5.2 Cryptographic HMAC-SHA256 Payload Signing
To protect downstream receivers against man-in-the-middle manipulation and spoofing:
1. Generate a current Unix timestamp integer `timestamp = int(time.time())`.
2. Construct the canonical payload signature string: `f"{timestamp}.{raw_json_string}"`.
3. Compute the digest using the recipient endpoint's unique `secret_key`:
   ```python
   signature = hmac.new(
       key=secret_key.encode("utf-8"),
       msg=canonical_payload.encode("utf-8"),
       digestmod=hashlib.sha256
   ).hexdigest()
   ```
4. Attach the following headers to the outbound request:
   - `Content-Type: application/json`
   - `X-Webhook-Timestamp: str(timestamp)`
   - `X-Webhook-Signature: f"t={timestamp},v1={signature}"`

### 5.3 Distributed Token-Bucket Rate Limiter (Redis Lua)
To enforce per-destination throughput controls without drift across distributed workers:
- **Algorithm:** Each endpoint maintains a token bucket with `burst_capacity` and fills at `rate_limit_per_second`.
- **Lua Script (`token_bucket.lua`):**
```lua
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
```
- If the script returns `0`: The worker pauses delivery for that endpoint by sleeping or re-scheduling the event with a 1-second delay.

### 5.4 Exponential Backoff with Full Jitter
To mitigate retry-storm synchronization:
1. Parameters: `base_delay = 2.0`, `max_delay = 60.0`, `max_attempts = 5`.
2. Compute delay for `attempt_number` (1 to 5):
   ```python
   calculated_ceiling = min(max_delay, base_delay * (2 ** (attempt_number - 1)))
   sleep_delay = random.uniform(0, calculated_ceiling)
   ```
3. Store scheduled execution time in Redis Sorted Set `webhooks:retry:zset`:
   ```python
   execution_timestamp = time.time() + sleep_delay
   await redis.zadd("webhooks:retry:zset", {serialized_retry_payload: execution_timestamp})
   ```
4. A lightweight async scheduler loop polls the ZSET every 500ms via `ZRANGEBYSCORE webhooks:retry:zset 0 {current_time}` and re-queues ready events back into `webhooks:events:stream`.

---

## 6. Complete Project Directory Layout

```
webhook-engine/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── app/
│   ├── __init__.py
│   ├── config.py             # Pydantic BaseSettings for DB, Redis, Worker concurrency
│   ├── database.py           # Async SQLAlchemy sessionmaker and engine setup
│   ├── models.py             # Declarative SQLAlchemy models (Endpoints, Events, Attempts, DLQ)
│   ├── schemas.py            # Pydantic request/response schemas
│   ├── security.py           # HMAC generation and header builder functions
│   ├── api/
│   │   ├── __init__.py
│   │   ├── deps.py           # Database and Redis dependency injection
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── endpoints.py  # CRUD for registering subscribers & secrets
│   │       ├── publish.py    # High-throughput ingestion endpoint
│   │       └── dashboard.py  # Aggregated metrics for monitoring UI
│   ├── broker/
│   │   ├── __init__.py
│   │   ├── stream.py         # Redis Streams manager (XADD, XREADGROUP, XACK)
│   │   └── rate_limiter.py   # Lua-backed distributed token bucket client
│   ├── worker/
│   │   ├── __init__.py
│   │   ├── dispatcher.py     # HTTP worker loop using httpx and backoff logic
│   │   └── scheduler.py      # ZSET retry reader moving ready events to stream
│   ├── static/
│   │   ├── index.html        # Clean, reactive dashboard monitoring stats
│   │   ├── style.css
│   │   └── app.js
│   └── main.py               # FastAPI entrypoint mounting routers & lifecycle hooks
├── mock_server/
│   ├── Dockerfile
│   └── mock_receiver.py      # Configurable receiver (simulates 200, 500, timeouts)
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Fixtures for test DB, Redis client, mock endpoints
│   ├── test_ingestion.py     # Tests for schema validation & idempotency locks
│   ├── test_rate_limiter.py  # Tests for token-bucket exhaustion
│   ├── test_security.py      # Tests for HMAC computation & tampering detection
│   └── test_worker.py        # Tests for retry progression, jitter, and DLQ routing
└── benchmarks/
    └── load_test.py          # Async httpx stress-test firing 10,000 requests
```

---

## 7. Concrete Step-by-Step Implementation Instructions for Claude Code / Cursor

1. **Environment & Configuration (`app/config.py`, `.env.example`, `requirements.txt`):**
   - Define settings for `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `REDIS_URL`, `WORKER_CONCURRENCY` (default 50), and `MAX_RETRY_ATTEMPTS` (5).
   - Require dependencies: `fastapi`, `uvicorn[standard]`, `asyncpg`, `sqlalchemy`, `redis`, `httpx`, `pydantic-settings`, `pytest`, `pytest-asyncio`, `respx`.

2. **Persistence Layer (`app/database.py`, `app/models.py`):**
   - Initialize SQLAlchemy asynchronous engine using `postgresql+asyncpg://`.
   - Implement complete ORM models matching the exact schema definitions in Section 4.

3. **Broker & Rate Limiter (`app/broker/`):**
   - Implement `RedisStreamManager`: auto-create consumer group `engine_workers_group` via `XGROUP CREATE ... MKSTREAM` if not present.
   - Implement `TokenBucketLimiter` executing the Lua script atomically with dynamic fallbacks.

4. **Security Helper (`app/security.py`):**
   - Expose function `generate_webhook_headers(payload: dict, secret: str) -> dict` that returns timestamp and hex signature.

5. **API Layer (`app/api/`):**
   - `POST /api/v1/endpoints`: Register subscriber URL and return auto-generated 32-byte secret key.
   - `POST /api/v1/webhooks/publish`: Check idempotency key in Redis, insert pending row in `webhook_events`, invoke `XADD` onto Redis stream, and return HTTP 202.
   - `GET /api/v1/dashboard/stats`: Return real-time figures (stream queue depth via `XLEN`, total queued, total delivered, total in DLQ, average latency).

6. **Worker Daemon & Retry Daemon (`app/worker/`):**
   - Continuous `asyncio.create_task` worker consuming batches via `XREADGROUP`.
   - Send HTTP requests with `httpx.AsyncClient(timeout=3.0)`.
   - Check status codes: if \(200 \le \text{code} < 300\), record success, commit `XACK`.
   - If transient error: increment `attempt_number`. If `< 5`, compute exponential backoff with full jitter, push payload to `webhooks:retry:zset`, and commit `XACK`. If $\ge 5$, persist into `dead_letter_queue` table.
   - Run `scheduler.py` concurrently to drain ready items from `webhooks:retry:zset` back into `webhooks:events:stream`.

7. **Mock Server & Simulation (`mock_server/mock_receiver.py`):**
   - Minimal FastAPI server exposing `POST /webhook-sink`.
   - Use URL parameters or state toggles to simulate failure modes:
     - `?mode=success`: returns HTTP 200.
     - `?mode=flaky`: returns HTTP 500 for the first 2 attempts, then HTTP 200.
     - `?mode=down`: returns HTTP 503 continuously.
     - `?mode=timeout`: sleeps for 5 seconds to test worker timeout cancellation.

8. **Testing & Benchmark Suite (`tests/`, `benchmarks/`):**
   - Unit test HMAC signature verification against fixed test vectors.
   - Integration test verifying that a failing endpoint correctly routes to the `dead_letter_queue` table on attempt 5.
   - Python load test script running 10,000 asynchronous publish requests to measure API acceptance throughput and worker delivery latency.

9. **Dashboard UI (`app/static/`):**
   - Single-page application using vanilla HTML5/Tailwind CDN and JavaScript `fetch`.
   - Auto-refreshes every 2 seconds to graph active stream backlog, active retries, completed deliveries, and DLQ rows with a manual "Replay Dead Letter" button.
