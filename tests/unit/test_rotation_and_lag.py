"""Tests for segment rotation and consumer lag detection."""

import tempfile
import time
from pathlib import Path

import pytest

from services.broker.partition_log import PartitionLog, LagDetector, ConsumerLag


class TestSegmentRotation:
    """Tests for segment rotation at size and time thresholds."""

    @pytest.fixture
    def temp_log_dir(self):
        """Create a temporary directory for log files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_rotation_at_size_threshold(self, temp_log_dir):
        """Test that segment rotates when hitting size limit."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100  # Very small for testing
        )

        assert partition.segment_count == 1
        assert partition.sealed_segment_count == 0

        # Write until we trigger rotation
        for i in range(20):
            partition.append(b"x" * 20)  # Each record ~32 bytes

        # Should have rotated at least once
        assert partition.segment_count >= 2
        assert partition.sealed_segment_count >= 1
        assert partition.rotation_count >= 1
        assert partition.last_rotation_reason == "size"

    def test_rotation_reason_tracked(self, temp_log_dir):
        """Test that rotation reason is tracked correctly."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        # Write enough to trigger rotation
        for i in range(20):
            partition.append(b"x" * 20)

        assert partition.last_rotation_reason == "size"

    def test_rotation_info(self, temp_log_dir):
        """Test rotation info endpoint."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100,
            segment_max_age_ms=5000
        )

        info = partition.get_rotation_info()

        assert "rotation_count" in info
        assert "last_rotation_reason" in info
        assert "segment_max_size" in info
        assert "segment_max_age_ms" in info
        assert "active_segment_age_ms" in info
        assert info["segment_max_size"] == 100
        assert info["segment_max_age_ms"] == 5000

    def test_rotation_at_time_threshold(self, temp_log_dir):
        """Test that segment rotates when hitting time limit."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=1024 * 1024,  # Large so size doesn't trigger
            segment_max_age_ms=100  # 100ms for testing
        )

        # Write some records
        for i in range(10):
            partition.append(f"record-{i}".encode())

        assert partition.segment_count == 1
        assert partition.sealed_segment_count == 0

        # Wait for time threshold
        time.sleep(0.15)  # 150ms

        # Write another record - should trigger time-based rotation
        partition.append(b"trigger-rotation")

        assert partition.segment_count >= 2
        assert partition.sealed_segment_count >= 1
        assert partition.last_rotation_reason == "age"

    def test_no_time_rotation_when_disabled(self, temp_log_dir):
        """Test that time-based rotation doesn't happen when disabled (0)."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=1024 * 1024,  # Large
            segment_max_age_ms=0  # Disabled
        )

        # Write records
        for i in range(10):
            partition.append(f"record-{i}".encode())

        # Wait - should NOT trigger rotation
        time.sleep(0.1)

        # Write more - still should not rotate
        for i in range(10):
            partition.append(f"more-{i}".encode())

        # No rotation should have occurred
        assert partition.segment_count == 1
        assert partition.rotation_count == 0

    def test_multiple_rotations(self, temp_log_dir):
        """Test multiple consecutive rotations."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=50  # Very small
        )

        # Write many records to trigger multiple rotations
        for i in range(100):
            partition.append(f"record-{i}".encode())

        assert partition.rotation_count >= 3
        assert partition.segment_count >= 4
        assert partition.sealed_segment_count >= 3

    def test_records_preserved_across_rotation(self, temp_log_dir):
        """Test that records are preserved and readable after rotation."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        # Write records
        expected_data = [f"record-{i}".encode() for i in range(30)]
        for data in expected_data:
            partition.append(data)

        # Read back all records
        records = list(partition.read())

        assert len(records) == len(expected_data)
        for i, record in enumerate(records):
            assert record.data == expected_data[i]

    def test_rotation_after_restart(self, temp_log_dir):
        """Test that rotation works correctly after restart."""
        # Create partition and write records
        partition1 = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        for i in range(20):
            partition1.append(f"record-{i}".encode())

        initial_rotations = partition1.rotation_count
        partition1.close()

        # Reopen
        partition2 = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        # Rotation count is reset on restart (only tracks this session)
        assert partition2.rotation_count == 0

        # But we can still trigger new rotations
        for i in range(20):
            partition2.append(f"new-record-{i}".encode())

        assert partition2.rotation_count >= 1


class TestLagDetector:
    """Tests for the consumer lag detector."""

    def test_register_consumer(self):
        """Test registering a consumer."""
        detector = LagDetector()
        consumer = detector.register_consumer("consumer-1")

        assert consumer.consumer_id == "consumer-1"
        assert consumer.committed_offset == 0

    def test_commit_offset(self):
        """Test committing consumer offset."""
        detector = LagDetector()
        detector.register_consumer("consumer-1")

        detector.commit("consumer-1", 10)

        assert detector.consumers["consumer-1"].committed_offset == 10

    def test_lag_calculation(self):
        """Test lag calculation (high_watermark - committed_offset)."""
        detector = LagDetector()
        detector.register_consumer("consumer-1")
        detector.commit("consumer-1", 50)

        lag = detector.get_lag("consumer-1", high_watermark=100)

        assert lag == 50  # 100 - 50 = 50

    def test_lag_for_unknown_consumer(self):
        """Test lag for unregistered consumer returns full lag."""
        detector = LagDetector()

        lag = detector.get_lag("unknown", high_watermark=100)

        assert lag == 100  # Unknown consumer = full lag

    def test_is_lagging(self):
        """Test lagging detection."""
        detector = LagDetector()
        detector.register_consumer("consumer-1")
        detector.commit("consumer-1", 50)

        # High watermark 100, committed 50, lag = 50
        assert detector.is_lagging("consumer-1", 100, threshold=10) is True
        assert detector.is_lagging("consumer-1", 100, threshold=100) is False

    def test_get_lagging_consumers(self):
        """Test getting list of lagging consumers."""
        detector = LagDetector()

        # Register 3 consumers
        detector.register_consumer("fast-consumer")
        detector.register_consumer("slow-consumer")
        detector.register_consumer("medium-consumer")

        # Commit at different rates
        detector.commit("fast-consumer", 90)  # Lag = 10
        detector.commit("slow-consumer", 10)  # Lag = 90
        detector.commit("medium-consumer", 50)  # Lag = 50

        lagging = detector.get_lagging_consumers(high_watermark=100, threshold=40)

        assert "slow-consumer" in lagging
        assert "medium-consumer" in lagging
        assert "fast-consumer" not in lagging

    def test_get_all_lag(self):
        """Test getting lag for all consumers."""
        detector = LagDetector()

        detector.register_consumer("consumer-a")
        detector.register_consumer("consumer-b")

        detector.commit("consumer-a", 20)
        detector.commit("consumer-b", 80)

        all_lag = detector.get_all_lag(high_watermark=100)

        assert all_lag["consumer-a"]["lag"] == 80
        assert all_lag["consumer-b"]["lag"] == 20
        assert all_lag["consumer-a"]["committed_offset"] == 20
        assert all_lag["consumer-b"]["committed_offset"] == 80


class TestPartitionLagIntegration:
    """Integration tests for partition log with lag detection."""

    @pytest.fixture
    def temp_log_dir(self):
        """Create a temporary directory for log files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_register_consumer_on_partition(self, temp_log_dir):
        """Test registering consumer on partition log."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        partition.register_consumer("consumer-1")

        assert "consumer-1" in partition._lag_detector.consumers

    def test_commit_and_check_lag(self, temp_log_dir):
        """Test committing and checking lag on partition."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        partition.register_consumer("consumer-1")

        # Write 100 records
        for i in range(100):
            partition.append(f"record-{i}".encode())

        # Consumer commits they've read up to offset 50
        partition.commit("consumer-1", 50)

        # Check lag
        lag = partition.get_consumer_lag("consumer-1")

        assert lag == 50  # 100 written - 50 committed = 50 lag

    def test_lagging_consumer_detection(self, temp_log_dir):
        """Test detecting lagging consumers."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        partition.register_consumer("fast")
        partition.register_consumer("slow")

        # Write 200 records
        for i in range(200):
            partition.append(f"record-{i}".encode())

        # Fast consumer keeps up
        partition.commit("fast", 190)

        # Slow consumer falls behind
        partition.commit("slow", 50)

        lagging = partition.get_lagging_consumers(threshold=100)

        assert "slow" in lagging
        assert "fast" not in lagging

    def test_all_consumer_lag(self, temp_log_dir):
        """Test getting all consumer lag."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        partition.register_consumer("c1")
        partition.register_consumer("c2")

        for i in range(100):
            partition.append(f"r-{i}".encode())

        partition.commit("c1", 80)
        partition.commit("c2", 20)

        all_lag = partition.get_all_consumer_lag()

        assert all_lag["c1"]["lag"] == 20
        assert all_lag["c2"]["lag"] == 80
        assert all_lag["c1"]["high_watermark"] == 100

    def test_consumer_lag_with_rotation(self, temp_log_dir):
        """Test lag detection works correctly across segment rotations."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        partition.register_consumer("test-consumer")

        # Write enough to trigger rotations
        for i in range(100):
            partition.append(f"record-{i:04d}".encode())

        # Consumer commits halfway
        partition.commit("test-consumer", 50)

        lag = partition.get_consumer_lag("test-consumer")
        assert lag == 50

        # Verify all records still readable
        records = list(partition.read(start_offset=50, max_records=50))
        assert len(records) == 50


class TestFastSlowPointer:
    """Tests proving the fast/slow pointer lag detection pattern."""

    @pytest.fixture
    def temp_log_dir(self):
        """Create a temporary directory for log files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_fast_pointer_advances_with_producer(self, temp_log_dir):
        """Test that fast pointer (high watermark) advances with writes."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        # Fast pointer starts at 0
        assert partition.high_watermark == 0

        # Each append advances the fast pointer
        for i in range(10):
            partition.append(f"record-{i}".encode())
            assert partition.high_watermark == i + 1

    def test_slow_pointer_advances_with_consumer_commit(self, temp_log_dir):
        """Test that slow pointer (committed offset) advances with commits."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)
        partition.register_consumer("consumer")

        # Slow pointer starts at 0
        assert partition._lag_detector.consumers["consumer"].committed_offset == 0

        # Commits advance the slow pointer
        partition.commit("consumer", 5)
        assert partition._lag_detector.consumers["consumer"].committed_offset == 5

        partition.commit("consumer", 10)
        assert partition._lag_detector.consumers["consumer"].committed_offset == 10

    def test_growing_gap_signals_lag(self, temp_log_dir):
        """Test that a growing gap between fast and slow pointers signals lag."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)
        partition.register_consumer("lagging-consumer")

        # Producer writes 100 records (fast pointer = 100)
        for i in range(100):
            partition.append(f"record-{i}".encode())

        # Consumer only commits 10 (slow pointer = 10)
        partition.commit("lagging-consumer", 10)

        # Gap = 90 (lagging)
        lag = partition.get_consumer_lag("lagging-consumer")
        assert lag == 90

        # Check if flagged as lagging
        lagging = partition.get_lagging_consumers(threshold=50)
        assert "lagging-consumer" in lagging

    def test_consumer_catches_up_reduces_lag(self, temp_log_dir):
        """Test that consumer catching up reduces lag."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)
        partition.register_consumer("consumer")

        # Write 100 records
        for i in range(100):
            partition.append(f"record-{i}".encode())

        # Consumer is behind
        partition.commit("consumer", 20)
        assert partition.get_consumer_lag("consumer") == 80

        # Consumer catches up
        partition.commit("consumer", 90)
        assert partition.get_consumer_lag("consumer") == 10

        # Consumer fully caught up
        partition.commit("consumer", 100)
        assert partition.get_consumer_lag("consumer") == 0

    def test_deliberately_falling_behind_scenario(self, temp_log_dir):
        """
        Simulate a consumer deliberately made to fall behind.

        This is the key test for the lag detector requirement.
        """
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=500
        )

        partition.register_consumer("fast-consumer")
        partition.register_consumer("slow-consumer")

        # Producer writes records
        for i in range(200):
            partition.append(f"record-{i:04d}".encode())

        # Fast consumer keeps up (commits immediately)
        partition.commit("fast-consumer", 195)

        # Slow consumer falls behind (only processed 20)
        partition.commit("slow-consumer", 20)

        # Get lagging consumers with threshold of 50
        lagging = partition.get_lagging_consumers(threshold=50)

        # Only slow consumer should be lagging
        assert "slow-consumer" in lagging
        assert "fast-consumer" not in lagging

        # Verify lag values
        fast_lag = partition.get_consumer_lag("fast-consumer")
        slow_lag = partition.get_consumer_lag("slow-consumer")

        assert fast_lag == 5  # 200 - 195 = 5 (not lagging)
        assert slow_lag == 180  # 200 - 20 = 180 (lagging)
