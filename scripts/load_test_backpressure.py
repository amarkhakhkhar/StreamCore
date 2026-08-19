#!/usr/bin/env python
"""
Load test script for backpressure detection.

Runs a deliberate traffic burst and verifies backpressure kicks in -
throughput stabilizes instead of the broker degrading or crashing.

Usage:
    python scripts/load_test_backpressure.py [--base-url URL] [--duration SECONDS] [--rate RPS]
"""

import argparse
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def run_load_test(
    base_url: str,
    duration: int = 30,
    target_rps: int = 1000,
    burst_duration: int = 5,
    burst_rps: int = 5000,
):
    """
    Run load test with sustained load followed by a burst.

    Args:
        base_url: Broker base URL (e.g., http://localhost:8000)
        duration: Total test duration in seconds
        target_rps: Normal sustained requests per second
        burst_duration: Duration of burst phase in seconds
        burst_rps: Request rate during burst phase

    Returns:
        Dict with test results
    """
    results = {
        "success": True,
        "total_requests": 0,
        "successful": 0,
        "backpressure_rejected": 0,
        "errors": 0,
        "latencies": [],
        "backpressure_activated": False,
        "throughput_stable": True,
    }

    def make_request(i: int, latency_ms: float = 0):
        """Make a single append request."""
        start = time.time()
        try:
            resp = requests.post(
                f"{base_url}/append",
                json={"data": f"test-message-{i}", "timestamp": int(start * 1000)},
                timeout=5,
            )
            elapsed = (time.time() - start) * 1000
            return resp.status_code, elapsed
        except requests.exceptions.RequestException as e:
            elapsed = (time.time() - start) * 1000
            return 0, elapsed

    print(f"Starting load test: {duration}s total, {target_rps} RPS normal, {burst_rps} RPS burst for {burst_duration}s")

    start_time = time.time()
    request_id = 0
    burst_started = False
    burst_ended = False
    last_bp_check = 0

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = []

        while time.time() - start_time < duration:
            elapsed = time.time() - start_time

            # Check if we should enter burst phase
            if not burst_started and elapsed >= 10:
                print(f"  [{elapsed:.1f}s] Starting burst phase: {burst_rps} RPS")
                burst_started = True
            elif burst_started and not burst_ended and elapsed >= 10 + burst_duration:
                print(f"  [{elapsed:.1f}s] Ending burst phase")
                burst_ended = True

            # Determine current RPS
            if burst_started and not burst_ended:
                current_rps = burst_rps
            else:
                current_rps = target_rps

            # Check backpressure status periodically
            if elapsed - last_bp_check >= 1.0:
                try:
                    bp_resp = requests.get(f"{base_url}/backpressure/stats", timeout=2)
                    if bp_resp.status_code == 200:
                        bp_data = bp_resp.json()
                        if bp_data.get("backpressure_active") and not results["backpressure_activated"]:
                            results["backpressure_activated"] = True
                            print(f"  [{elapsed:.1f}s] BACKPRESSURE ACTIVATED!")
                except Exception:
                    pass
                last_bp_check = elapsed

            # Submit batch of requests
            batch_size = current_rps // 10  # Submit in small batches
            for _ in range(batch_size):
                futures.append(executor.submit(make_request, request_id))
                request_id += 1

            # Process completed futures
            for future in as_completed(futures):
                status, latency = future.result()
                results["total_requests"] += 1
                results["latencies"].append(latency)

                if status == 200:
                    results["successful"] += 1
                elif status == 429:
                    results["backpressure_rejected"] += 1
                    if not results["backpressure_activated"]:
                        results["backpressure_activated"] = True
                else:
                    results["errors"] += 1

            futures = [f for f in futures if not f.done()]

            # Rate limiting
            time.sleep(0.1)

        # Wait for remaining
        for future in as_completed(futures):
            status, latency = future.result()
            results["total_requests"] += 1
            results["latencies"].append(latency)

            if status == 200:
                results["successful"] += 1
            elif status == 429:
                results["backpressure_rejected"] += 1
            else:
                results["errors"] += 1

    # Analyze results
    if results["latencies"]:
        results["avg_latency_ms"] = statistics.mean(results["latencies"])
        results["p50_latency_ms"] = statistics.median(results["latencies"])
        results["p99_latency_ms"] = sorted(results["latencies"])[int(len(results["latencies"]) * 0.99)]
        results["max_latency_ms"] = max(results["latencies"])

    # Check if throughput stabilized (p99 didn't explode)
    if results.get("p99_latency_ms", 0) > 5000:  # 5 second p99 is bad
        results["throughput_stable"] = False

    print(f"\nResults:")
    print(f"  Total requests: {results['total_requests']}")
    print(f"  Successful: {results['successful']}")
    print(f"  Backpressure rejected (429): {results['backpressure_rejected']}")
    print(f"  Errors: {results['errors']}")
    print(f"  Backpressure activated: {results['backpressure_activated']}")
    print(f"  Throughput stable: {results['throughput_stable']}")
    if results.get("avg_latency_ms"):
        print(f"  Avg latency: {results['avg_latency_ms']:.1f}ms")
        print(f"  P50 latency: {results['p50_latency_ms']:.1f}ms")
        print(f"  P99 latency: {results['p99_latency_ms']:.1f}ms")
        print(f"  Max latency: {results['max_latency_ms']:.1f}ms")

    # Determine overall success
    if not results["backpressure_activated"]:
        print("  FAIL: Backpressure never activated during burst")
        results["success"] = False
    if not results["throughput_stable"]:
        print("  FAIL: Throughput did not stabilize (P99 too high)")
        results["success"] = False
    if results["errors"] > results["total_requests"] * 0.1:
        print("  FAIL: Too many errors")
        results["success"] = False

    if results["success"]:
        print("\n  PASS: Backpressure working correctly")
    else:
        print("\n  FAIL: Backpressure test failed")

    return results


def main():
    parser = argparse.ArgumentParser(description="Backpressure load test")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Broker base URL")
    parser.add_argument("--duration", type=int, default=30, help="Test duration (seconds)")
    parser.add_argument("--rate", type=int, default=1000, help="Normal RPS")
    parser.add_argument("--burst-rate", type=int, default=5000, help="Burst RPS")
    parser.add_argument("--burst-duration", type=int, default=5, help="Burst duration (seconds)")
    args = parser.parse_args()

    results = run_load_test(
        base_url=args.base_url,
        duration=args.duration,
        target_rps=args.rate,
        burst_duration=args.burst_duration,
        burst_rps=args.burst_rate,
    )

    sys.exit(0 if results["success"] else 1)


if __name__ == "__main__":
    main()