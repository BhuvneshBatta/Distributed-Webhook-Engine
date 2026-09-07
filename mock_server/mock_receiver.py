import asyncio
import logging
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Webhook Mock Receiver")

# Tracks how many times each (mode, client-supplied id) pair has been hit so
# that "flaky" mode can fail deterministically for its first N attempts before
# succeeding, exercising the caller's retry logic end-to-end.
_flaky_attempt_counts: dict[str, int] = defaultdict(int)

FLAKY_FAILURES_BEFORE_SUCCESS = 2
TIMEOUT_SLEEP_SECONDS = 5.0


@app.post("/webhook-sink")
async def webhook_sink(
    request: Request,
    mode: str = Query(default="success"),
    flaky_key: str = Query(default="default"),
) -> JSONResponse:
    body: dict[str, Any] = await request.json()
    signature = request.headers.get("X-Webhook-Signature", "")
    timestamp = request.headers.get("X-Webhook-Timestamp", "")

    logger.info("Received webhook mode=%s ts=%s sig=%s body=%s", mode, timestamp, signature, body)

    if mode == "success":
        return JSONResponse(status_code=200, content={"received": True})

    if mode == "flaky":
        _flaky_attempt_counts[flaky_key] += 1
        attempts_so_far = _flaky_attempt_counts[flaky_key]
        if attempts_so_far <= FLAKY_FAILURES_BEFORE_SUCCESS:
            return JSONResponse(
                status_code=500,
                content={"error": f"simulated transient failure ({attempts_so_far})"},
            )
        return JSONResponse(status_code=200, content={"received": True})

    if mode == "down":
        return JSONResponse(status_code=503, content={"error": "service unavailable"})

    if mode == "timeout":
        await asyncio.sleep(TIMEOUT_SLEEP_SECONDS)
        return JSONResponse(status_code=200, content={"received": True})

    return JSONResponse(status_code=400, content={"error": f"unknown mode '{mode}'"})


@app.post("/reset")
async def reset_state() -> JSONResponse:
    _flaky_attempt_counts.clear()
    return JSONResponse(status_code=200, content={"reset": True})


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
