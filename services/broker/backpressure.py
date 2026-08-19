"""
Backpressure / Rate Limiter using Monotonic Stack Pattern (Daily Temperatures).

Tracks recent request latencies using a monotonic decreasing stack to detect
sustained load spikes. When latency crosses a threshold for N consecutive windows,
signals producers to slow down.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LatencyWindow:
    """A single time window with latency measurements."""
    timestamp_ms: int
    count: int
    total_latency_ms: float
    max_latency_ms: float

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.count if self.count > 0 else 0.0


@dataclass
class BackpressureConfig:
    """Configuration for backpressure detection."""
    # Window size in milliseconds
    window_ms: int = 1000  # 1 second windows

    # Number of consecutive windows above threshold to trigger backpressure
    trigger_windows: int = 3

    # Latency threshold (ms) - sustained above this triggers backpressure
    latency_threshold_ms: float = 50.0

    # Maximum requests per window before considering overload
    max_requests_per_window: int = 10000

    # Cooldown period after backpressure clears (ms)
    cooldown_ms: int = 5000


class MonotonicStackTracker:
    """
    Monotonic stack-based rate tracker (Daily Temperatures pattern).

    Maintains a decreasing stack of latency windows. When a new window's
    latency exceeds the stack top, we pop until we find a window with
    higher latency - this tells us how many consecutive windows were
    below the current spike, which indicates sustained load.
    """

    def __init__(self, config: BackpressureConfig):
        self.config = config
        self._lock = threading.RLock()

        # Current window being filled
        self._current_window: Optional[LatencyWindow] = None
        self._window_start_ms: int = 0

        # Monotonic decreasing stack of (avg_latency, window_index)
        # Used to track "span" of consecutive elevated latencies
        self._stack: list[tuple[float, int]] = []

        # All completed windows for analysis
        self._windows: deque[LatencyWindow] = deque(maxlen=60)  # Keep 60 seconds

        # Backpressure state
        self._backpressure_active: bool = False
        self._backpressure_since_ms: int = 0
        self._consecutive_high: int = 0

        # Stats
        self._total_requests: int = 0
        self._backpressure_triggers: int = 0

    def _get_current_time_ms(self) -> int:
        return int(time.time() * 1000)

    def _maybe_rotate_window(self) -> None:
        """Rotate to a new window if current one has expired."""
        now = self._get_current_time_ms()

        if self._current_window is None:
            self._window_start_ms = now
            self._current_window = LatencyWindow(
                timestamp_ms=now,
                count=0,
                total_latency_ms=0.0,
                max_latency_ms=0.0
            )
            return

        if now - self._window_start_ms >= self.config.window_ms:
            # Finalize current window
            self._windows.append(self._current_window)

            # Process with monotonic stack
            self._process_window_with_stack()

            # Start new window
            self._window_start_ms = now
            self._current_window = LatencyWindow(
                timestamp_ms=now,
                count=0,
                total_latency_ms=0.0,
                max_latency_ms=0.0
            )

    def _process_window_with_stack(self) -> None:
        """Process completed window using monotonic stack (Daily Temperatures)."""
        if self._current_window is None:
            return

        avg_latency = self._current_window.avg_latency_ms
        window_index = len(self._windows)  # Index in the windows deque

        # Monotonic decreasing stack: pop while current >= stack top
        # This finds the "span" of consecutive windows with lower latency
        span = 1
        while self._stack and self._stack[-1][0] <= avg_latency:
            span += 1
            self._stack.pop()

        self._stack.append((avg_latency, window_index))

        # Check for backpressure trigger: consecutive windows above threshold
        if avg_latency >= self.config.latency_threshold_ms:
            self._consecutive_high += 1
        else:
            self._consecutive_high = 0

        # Trigger backpressure if sustained high latency
        if (self._consecutive_high >= self.config.trigger_windows
                and not self._backpressure_active):
            self._activate_backpressure(now=self._get_current_time_ms())

        # Clear backpressure if latency drops
        if (self._backpressure_active
                and avg_latency < self.config.latency_threshold_ms * 0.5):
            self._deactivate_backpressure()

    def _activate_backpressure(self, now: int) -> None:
        """Activate backpressure mode."""
        self._backpressure_active = True
        self._backpressure_since_ms = now
        self._backpressure_triggers += 1

    def _deactivate_backpressure(self) -> None:
        """Deactivate backpressure mode."""
        self._backpressure_active = False
        self._backpressure_since_ms = 0
        self._consecutive_high = 0
        # Clear stack on cooldown
        self._stack.clear()

    def record_latency(self, latency_ms: float) -> bool:
        """
        Record a request latency and check if backpressure is active.

        Returns:
            True if backpressure is active (producer should slow down)
        """
        with self._lock:
            self._maybe_rotate_window()

            if self._current_window is None:
                return self._backpressure_active

            self._current_window.count += 1
            self._current_window.total_latency_ms += latency_ms
            if latency_ms > self._current_window.max_latency_ms:
                self._current_window.max_latency_ms = latency_ms

            self._total_requests += 1

            return self._backpressure_active

    def is_backpressure_active(self) -> bool:
        """Check if backpressure is currently active."""
        with self._lock:
            # Check cooldown
            if self._backpressure_active:
                now = self._get_current_time_ms()
                if now - self._backpressure_since_ms >= self.config.cooldown_ms:
                    self._deactivate_backpressure()
            return self._backpressure_active

    def get_retry_after_ms(self) -> Optional[int]:
        """Get suggested retry-after time in milliseconds if backpressure active."""
        with self._lock:
            if not self._backpressure_active:
                return None
            now = self._get_current_time_ms()
            remaining = self.config.cooldown_ms - (now - self._backpressure_since_ms)
            return max(100, remaining)  # At least 100ms

    def get_stats(self) -> dict:
        """Get current backpressure statistics."""
        with self._lock:
            current_avg = 0.0
            if self._current_window and self._current_window.count > 0:
                current_avg = self._current_window.avg_latency_ms

            recent_avg = 0.0
            if self._windows:
                recent_avg = sum(w.avg_latency_ms for w in self._windows) / len(self._windows)

            return {
                "backpressure_active": self._backpressure_active,
                "consecutive_high_windows": self._consecutive_high,
                "trigger_windows_required": self.config.trigger_windows,
                "latency_threshold_ms": self.config.latency_threshold_ms,
                "current_window_avg_ms": round(current_avg, 2),
                "recent_avg_ms": round(recent_avg, 2),
                "total_requests": self._total_requests,
                "backpressure_triggers": self._backpressure_triggers,
                "windows_tracked": len(self._windows),
                "stack_depth": len(self._stack),
            }

    def reset(self) -> None:
        """Reset the tracker state."""
        with self._lock:
            self._current_window = None
            self._window_start_ms = 0
            self._stack.clear()
            self._windows.clear()
            self._backpressure_active = False
            self._backpressure_since_ms = 0
            self._consecutive_high = 0
            self._total_requests = 0
            self._backpressure_triggers = 0


class BackpressureMiddleware:
    """
    Simple backpressure middleware for the broker.

    Tracks append latency and signals backpressure to producers.
    """

    def __init__(self, config: Optional[BackpressureConfig] = None):
        self.config = config or BackpressureConfig()
        self.tracker = MonotonicStackTracker(self.config)
        self._enabled = True

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    def record_append_latency(self, latency_ms: float) -> bool:
        """Record append latency. Returns True if backpressure active."""
        if not self._enabled:
            return False
        return self.tracker.record_latency(latency_ms)

    def should_throttle(self) -> bool:
        """Check if producer should be throttled."""
        if not self._enabled:
            return False
        return self.tracker.is_backpressure_active()

    def get_retry_after(self) -> Optional[int]:
        """Get retry-after hint for producers."""
        if not self._enabled:
            return None
        return self.tracker.get_retry_after_ms()

    def get_stats(self) -> dict:
        """Get backpressure statistics."""
        stats = self.tracker.get_stats()
        stats["enabled"] = self._enabled
        return stats


def create_backpressure_middleware(
    window_ms: int = 1000,
    trigger_windows: int = 3,
    latency_threshold_ms: float = 50.0,
) -> BackpressureMiddleware:
    """Factory function to create backpressure middleware with custom config."""
    config = BackpressureConfig(
        window_ms=window_ms,
        trigger_windows=trigger_windows,
        latency_threshold_ms=latency_threshold_ms,
    )
    return BackpressureMiddleware(config)