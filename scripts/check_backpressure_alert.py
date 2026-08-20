#!/usr/bin/env python
"""
Backpressure alerting script for CI.

Checks if broker latency crosses threshold and triggers alert.
Used in CI pipeline to catch backpressure regressions.
"""

import sys
import time
import requests
import argparse


def check_backpressure_alert(
    base_url: str,
    latency_threshold_ms: float = 100.0,
    window_ms: int = 1000,
    trigger_windows: int = 3,
):
    """
    Check if backpressure should be triggered based on current stats.

    Returns:
        (should_alert: bool, stats: dict)
    """
    try:
        resp = requests.get(f"{base_url}/backpressure/stats", timeout=5)
        resp.raise_for_status()
        stats = resp.json()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch backpressure stats: {e}")
        return False, {}

    backpressure_active = stats.get("backpressure_active", False)
    consecutive_high = stats.get("consecutive_high_windows", 0)
    current_avg = stats.get("current_window_avg_ms", 0)
    recent_avg = stats.get("recent_avg_ms", 0)
    threshold = stats.get("latency_threshold_ms", latency_threshold_ms)

    print(f"Backpressure Stats:")
    print(f"  Active: {backpressure_active}")
    print(f"  Consecutive high windows: {consecutive_high}/{trigger_windows}")
    print(f"  Current window avg: {current_avg:.2f}ms")
    print(f"  Recent avg: {recent_avg:.2f}ms")
    print(f"  Threshold: {threshold:.2f}ms")

    # Alert conditions
    should_alert = False
    alert_reasons = []

    if backpressure_active:
        should_alert = True
        alert_reasons.append("Backpressure is ACTIVE - broker overloaded")

    if current_avg > latency_threshold_ms:
        should_alert = True
        alert_reasons.append(f"Current window avg ({current_avg:.2f}ms) exceeds threshold ({latency_threshold_ms}ms)")

    if consecutive_high >= trigger_windows:
        should_alert = True
        alert_reasons.append(f"Consecutive high windows ({consecutive_high}) >= trigger ({trigger_windows})")

    if should_alert:
        print("\n⚠️  ALERT TRIGGERED:")
        for reason in alert_reasons:
            print(f"  - {reason}")
    else:
        print("\n✅ No alert: All metrics within normal range")

    return should_alert, stats


def inject_test_latency(base_url: str, latency_ms: float, count: int = 10):
    """Inject test latency to trigger backpressure for CI verification."""
    print(f"\nInjecting {count} requests with {latency_ms}ms latency...")
    for i in range(count):
        try:
            resp = requests.post(
                f"{base_url}/backpressure/test/inject-latency",
                json={"latency_ms": latency_ms},
                timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                print(f"  [{i+1}/{count}] Injected: {data['injected_ms']}ms, BP active: {data['backpressure_active']}")
            else:
                print(f"  [{i+1}/{count}] Failed: {resp.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"  [{i+1}/{count}] Error: {e}")
        time.sleep(0.1)


def wait_for_backpressure(base_url: str, timeout: int = 30):
    """Wait for backpressure to activate."""
    print(f"\nWaiting for backpressure to activate (timeout: {timeout}s)...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{base_url}/backpressure/stats", timeout=2)
            if resp.status_code == 200:
                stats = resp.json()
                if stats.get("backpressure_active"):
                    print("✅ Backpressure activated!")
                    return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)
    print("❌ Timeout: Backpressure did not activate")
    return False


def main():
    parser = argparse.ArgumentParser(description="Backpressure alerting for CI")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Broker base URL")
    parser.add_argument("--threshold", type=float, default=100.0, help="Latency threshold (ms)")
    parser.add_argument("--inject-latency", type=float, help="Inject test latency to trigger BP")
    parser.add_argument("--wait-bp", action="store_true", help="Wait for backpressure to activate")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout for wait")
    args = parser.parse_args()

    if args.inject_latency:
        inject_test_latency(args.base_url, args.inject_latency)

    if args.wait_bp:
        wait_for_backpressure(args.base_url, args.timeout)

    should_alert, stats = check_backpressure_alert(
        base_url=args.base_url,
        latency_threshold_ms=args.threshold,
    )

    # Exit with code 1 if alert should fire (for CI failure)
    sys.exit(1 if should_alert else 0)


if __name__ == "__main__":
    main()