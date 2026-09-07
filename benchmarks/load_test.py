"""Async load test for the webhook ingestion API.

Fires a configurable number of concurrent POST /api/v1/webhooks/publish
requests to measure API acceptance throughput, then polls the dashboard
stats endpoint to observe end-to-end worker delivery drain-down.

Usage:
    python benchmarks/load_test.py --total 10000 --concurrency 500
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from dataclasses import dataclass, field

import httpx


@dataclass
class RequestResult:
    success: bool
    status_code: int | None
    latency_ms: float
    error: str | None = None


@dataclass
class LoadTestReport:
    results: list[RequestResult] = field(default_factory=list)

    @property
    def successes(self) -> list[RequestResult]:
        return [r for r in self.results if r.success]

    @property
    def failures(self) -> list[RequestResult]:
        return [r for r in self.results if not r.success]

    def latency_percentile(self, pct: float) -> float:
        latencies = sorted(r.latency_ms for r in self.successes)
        if not latencies:
            return 0.0
        index = min(len(latencies) - 1, int(len(latencies) * pct / 100))
        return latencies[index]


async def ensure_endpoint(client: httpx.AsyncClient, target_url: str) -> str:
    response = await client.post(
        "/api/v1/endpoints",
        json={
            "name": f"load-test-{uuid.uuid4().hex[:8]}",
            "url": target_url,
            "rate_limit_per_second": 1000,
            "burst_capacity": 2000,
        },
    )
    response.raise_for_status()
    return response.json()["id"]


async def fire_one(
    client: httpx.AsyncClient, endpoint_id: str, semaphore: asyncio.Semaphore
) -> RequestResult:
    async with semaphore:
        start = time.perf_counter()
        try:
            response = await client.post(
                "/api/v1/webhooks/publish",
                json={
                    "endpoint_id": endpoint_id,
                    "event_type": "load_test.ping",
                    "payload": {"nonce": uuid.uuid4().hex},
                },
                headers={"Idempotency-Key": f"loadtest-{uuid.uuid4()}"},
            )
            latency_ms = (time.perf_counter() - start) * 1000
            return RequestResult(
                success=response.status_code == 202,
                status_code=response.status_code,
                latency_ms=latency_ms,
            )
        except httpx.HTTPError as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return RequestResult(success=False, status_code=None, latency_ms=latency_ms, error=str(exc))


async def run_load_test(
    base_url: str, target_url: str, total: int, concurrency: int
) -> LoadTestReport:
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0, limits=limits) as client:
        endpoint_id = await ensure_endpoint(client, target_url)
        print(f"Registered load-test endpoint {endpoint_id} -> {target_url}")

        semaphore = asyncio.Semaphore(concurrency)
        start = time.perf_counter()
        tasks = [
            asyncio.create_task(fire_one(client, endpoint_id, semaphore)) for _ in range(total)
        ]
        results = await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start

        report = LoadTestReport(results=list(results))
        print_ingestion_summary(report, elapsed, total)
        return report


def print_ingestion_summary(report: LoadTestReport, elapsed: float, total: int) -> None:
    successes = report.successes
    failures = report.failures
    throughput = total / elapsed if elapsed > 0 else 0.0

    print("\n--- Ingestion throughput ---")
    print(f"Total requests:     {total}")
    print(f"Successful (202):   {len(successes)}")
    print(f"Failed:             {len(failures)}")
    print(f"Wall time:          {elapsed:.2f}s")
    print(f"Throughput:         {throughput:.1f} req/s")
    if successes:
        latencies = [r.latency_ms for r in successes]
        print(f"Latency avg:        {statistics.mean(latencies):.1f}ms")
        print(f"Latency p50:        {report.latency_percentile(50):.1f}ms")
        print(f"Latency p95:        {report.latency_percentile(95):.1f}ms")
        print(f"Latency p99:        {report.latency_percentile(99):.1f}ms")
    if failures[:5]:
        print("Sample failures:")
        for failure in failures[:5]:
            print(f"  status={failure.status_code} error={failure.error}")


async def poll_delivery_drain(base_url: str, timeout_seconds: float, poll_interval: float = 1.0) -> None:
    print("\n--- Worker delivery drain-down ---")
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        deadline = time.perf_counter() + timeout_seconds
        while time.perf_counter() < deadline:
            response = await client.get("/api/v1/dashboard/stats")
            response.raise_for_status()
            stats = response.json()
            print(
                f"queue_depth={stats['queue_depth']:<6} "
                f"pending_retries={stats['pending_retries']:<6} "
                f"delivered={stats['total_delivered']:<6} "
                f"retrying={stats['total_retrying']:<6} "
                f"dlq={stats['total_dlq']:<6} "
                f"avg_latency_ms={stats['average_latency_ms']}"
            )
            if stats["queue_depth"] == 0 and stats["pending_retries"] == 0:
                print("Queue drained.")
                break
            await asyncio.sleep(poll_interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Webhook engine async load test")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--target-url",
        default="http://mock-receiver:9000/webhook-sink?mode=success",
        help=(
            "Mock receiver URL the generated endpoint will deliver to. This is "
            "dialed by the worker process, not this script, so it must resolve "
            "from wherever the worker runs: the docker-compose service hostname "
            "'mock-receiver' when the worker runs in Compose (the default), or "
            "'http://localhost:9000/webhook-sink?mode=success' if running the "
            "worker directly on the host."
        ),
    )
    parser.add_argument("--total", type=int, default=10_000)
    parser.add_argument("--concurrency", type=int, default=500)
    parser.add_argument(
        "--watch-drain-seconds",
        type=float,
        default=0.0,
        help="If > 0, poll dashboard stats until the queue drains or this timeout elapses",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    await run_load_test(args.base_url, args.target_url, args.total, args.concurrency)
    if args.watch_drain_seconds > 0:
        await poll_delivery_drain(args.base_url, args.watch_drain_seconds)


if __name__ == "__main__":
    asyncio.run(main())
