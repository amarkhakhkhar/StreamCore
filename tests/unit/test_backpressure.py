"""Tests for backpressure / rate limiting using monotonic stack pattern."""

import threading
import time

import pytest

from services.broker.backpressure import (
    BackpressureConfig,
    BackpressureMiddleware,
    LatencyWindow,
    MonotonicStackTracker,
    create_backpressure_middleware,
)


class TestLatencyWindow:
    """Tests for the LatencyWindow dataclass."""

    def test_window_avg_latency(self):
        """Test average latency calculation."""
        window = LatencyWindow(
            timestamp_ms=0,
            count=3,
            total_latency_ms=150.0,
            max_latency_ms=80.0
        )
        assert window.avg_latency_ms == 50.0

    def test_window_empty_avg(self):
        """Test empty window has zero avg."""
        window = LatencyWindow(
            timestamp_ms=0,
            count=0,
            total_latency_ms=0.0,
            max_latency_ms=0.0
        )
        assert window.avg_latency_ms == 0.0


class TestMonotonicStackTracker:
    """Tests for the monotonic stack-based tracker."""

    def _make_tracker(self, threshold: float = 50.0, trigger_windows: int = 3):
        config = BackpressureConfig(
            window_ms=50,  # Short window for tests
            trigger_windows=trigger_windows,
            latency_threshold_ms=threshold,
            cooldown_ms=200
        )
        return MonotonicStackTracker(config)

    def test_no_backpressure_normal_load(self):
        """Test normal load doesn't trigger backpressure."""
        tracker = self._make_tracker()

        # Record 10 windows of normal latency (all under threshold)
        for _ in range(10):
            for _ in range(20):
                tracker.record_latency(20.0)
            time.sleep(0.05)  # Wait for window rotation

        assert not tracker.is_backpressure_active()

    def test_backpressure_triggers_on_sustained_spike(self):
        """Test sustained spike triggers backpressure."""
        tracker = self._make_tracker(threshold=50.0, trigger_windows=3)

        # 3 consecutive high-latency windows
        for _ in range(5):
            for _ in range(20):
                tracker.record_latency(100.0)  # Above threshold
            time.sleep(0.06)  # Wait for window rotation

        assert tracker.is_backpressure_active()
        stats = tracker.get_stats()
        assert stats["backpressure_triggers"] >= 1

    def test_backpressure_clears_after_recovery(self):
        """Test backpressure clears when latency drops."""
        tracker = self._make_tracker(threshold=50.0, trigger_windows=3)

        # Spike
        for _ in range(5):
            for _ in range(20):
                tracker.record_latency(100.0)
            time.sleep(0.06)

        assert tracker.is_backpressure_active()

        # Recovery - low latency
        for _ in range(10):
            for _ in range(20):
                tracker.record_latency(10.0)
            time.sleep(0.06)

        assert not tracker.is_backpressure_active()

    def test_monotonic_stack_behavior(self):
        """Test the monotonic stack tracking stack depth correctly."""
        tracker = self._make_tracker()

        # Increasing latencies: stack should collapse (monotonic decrease)
        for _ in range(5):
            for _ in range(10):
                tracker.record_latency(10.0 * (_ + 1))
            time.sleep(0.06)

        # Stack should have minimal depth (only the increases remain)
        stats = tracker.get_stats()
        assert stats["stack_depth"] <= 5

    def test_consecutive_high_counter(self):
        """Test consecutive high window counter."""
        tracker = self._make_tracker(threshold=50.0, trigger_windows=3)

        # Need 3 windows to finalize 2 (current is still building)
        for _ in range(3):
            for _ in range(20):
                tracker.record_latency(80.0)
            time.sleep(0.06)

        stats = tracker.get_stats()
        assert stats["consecutive_high_windows"] >= 2
        assert not stats["backpressure_active"]

    def test_retry_after_when_active(self):
        """Test retry-after is returned when backpressure active."""
        tracker = self._make_tracker(threshold=50.0, trigger_windows=2)

        for _ in range(4):
            for _ in range(20):
                tracker.record_latency(90.0)
            time.sleep(0.06)

        assert tracker.is_backpressure_active()
        retry = tracker.get_retry_after_ms()
        assert retry is not None
        assert retry > 0

    def test_reset_clears_state(self):
        """Test reset clears all state."""
        tracker = self._make_tracker()

        for _ in range(3):
            for _ in range(20):
                tracker.record_latency(100.0)
            time.sleep(0.06)

        tracker.reset()

        stats = tracker.get_stats()
        assert stats["total_requests"] == 0
        assert stats["backpressure_active"] is False
        assert stats["stack_depth"] == 0


class TestBackpressureMiddleware:
    """Tests for the backpressure middleware wrapper."""

    def test_middleware_record_and_check(self):
        """Test middleware records latency and checks backpressure."""
        mw = create_backpressure_middleware(
            window_ms=50,
            trigger_windows=3,
            latency_threshold_ms=50.0
        )

        # Normal load
        for _ in range(10):
            mw.record_append_latency(20.0)
            time.sleep(0.05)

        assert not mw.should_throttle()

    def test_middleware_backpressure(self):
        """Test middleware triggers backpressure on sustained spike."""
        mw = create_backpressure_middleware(
            window_ms=50,
            trigger_windows=2,
            latency_threshold_ms=50.0
        )

        for _ in range(4):
            mw.record_append_latency(100.0)
            time.sleep(0.06)

        assert mw.should_throttle()

    def test_middleware_disabled(self):
        """Test disabled middleware never triggers backpressure."""
        mw = create_backpressure_middleware()
        mw.disable()

        for _ in range(10):
            mw.record_append_latency(500.0)
            time.sleep(0.05)

        assert not mw.should_throttle()

    def test_middleware_get_stats(self):
        """Test middleware stats include enabled flag."""
        mw = create_backpressure_middleware()
        stats = mw.get_stats()
        assert stats["enabled"] is True
        assert "backpressure_active" in stats


class TestBackpressureConcurrency:
    """Tests for thread safety."""

    def test_concurrent_latency_recording(self):
        """Test concurrent latency recording doesn't crash."""
        mw = create_backpressure_middleware(
            window_ms=50,
            trigger_windows=3,
            latency_threshold_ms=50.0
        )

        def worker():
            for _ in range(50):
                mw.record_append_latency(30.0)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should not crash and stats should be consistent
        stats = mw.get_stats()
        assert stats["total_requests"] == 200
