"""Tests for PartitionLog implementation."""

import os
import tempfile
from pathlib import Path

import pytest

from services.broker.partition_log import PartitionLog


class TestPartitionLog:
    """Tests for the PartitionLog class."""

    @pytest.fixture
    def temp_log_dir(self):
        """Create a temporary directory for log files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_partition_creation(self, temp_log_dir):
        """Test creating a new partition log."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        assert partition.name == "test"
        assert partition.record_count == 0
        assert partition.segment_count == 1  # Active segment
        assert partition.sealed_segment_count == 0

    def test_append_single_record(self, temp_log_dir):
        """Test appending a single record."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        offset = partition.append(b"hello world")
        assert offset == 0
        assert partition.record_count == 1

    def test_append_multiple_records(self, temp_log_dir):
        """Test appending multiple records."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        offsets = []
        for i in range(100):
            offset = partition.append(f"record-{i}".encode())
            offsets.append(offset)

        assert offsets == list(range(100))
        assert partition.record_count == 100

    def test_read_records_in_order(self, temp_log_dir):
        """Test that records are read back in exact append order."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        # Append records with specific order
        test_data = [b"first", b"second", b"third", b"fourth", b"fifth"]
        for data in test_data:
            partition.append(data)

        # Read back and verify order
        records = list(partition.read())
        assert len(records) == 5

        for i, record in enumerate(records):
            assert record.offset == i
            assert record.data == test_data[i]

    def test_read_with_start_offset(self, temp_log_dir):
        """Test reading from a specific offset."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        for i in range(20):
            partition.append(f"record-{i}".encode())

        records = list(partition.read(start_offset=10))
        assert len(records) == 10
        assert records[0].offset == 10
        assert records[9].offset == 19

    def test_read_with_max_records(self, temp_log_dir):
        """Test reading with a maximum record limit."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        for i in range(100):
            partition.append(f"record-{i}".encode())

        records = list(partition.read(max_records=10))
        assert len(records) == 10

    def test_read_at_offset(self, temp_log_dir):
        """Test reading a single record at a specific offset."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        for i in range(10):
            partition.append(f"record-{i}".encode())

        record = partition.read_at(5)
        assert record is not None
        assert record.offset == 5
        assert record.data == b"record-5"

    def test_read_at_nonexistent_offset(self, temp_log_dir):
        """Test reading at a non-existent offset."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)
        partition.append(b"only record")

        record = partition.read_at(100)
        assert record is None

    def test_segment_rotation(self, temp_log_dir):
        """Test that segments rotate when full."""
        # Use small segment size to trigger rotation
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100  # Very small for testing
        )

        # Write enough records to trigger multiple rotations
        for i in range(50):
            partition.append(b"x" * 20)  # Each record is ~32 bytes with header

        # Should have multiple segments
        assert partition.segment_count >= 2
        assert partition.sealed_segment_count >= 1

    def test_read_across_segment_boundaries(self, temp_log_dir):
        """Test that reads work correctly across segment boundaries."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        # Write records that span multiple segments
        test_records = [f"record-{i:03d}".encode() for i in range(30)]
        for data in test_records:
            partition.append(data)

        # Read all records and verify order is preserved across boundaries
        records = list(partition.read())

        assert len(records) == len(test_records)
        for i, record in enumerate(records):
            assert record.data == test_records[i], f"Mismatch at index {i}"

    def test_durability_after_close(self, temp_log_dir):
        """Test that records persist after close."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        for i in range(20):
            partition.append(f"persistent-{i}".encode())

        partition.close()

        # Reopen partition
        partition2 = PartitionLog(name="test", log_dir=temp_log_dir)

        assert partition2.record_count == 20

        records = list(partition2.read())
        for i, record in enumerate(records):
            assert record.data == f"persistent-{i}".encode()

    def test_load_existing_segments(self, temp_log_dir):
        """Test loading a partition with existing segments."""
        # Create and write records
        partition1 = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        for i in range(30):
            partition1.append(f"data-{i}".encode())

        first_segment_count = partition1.segment_count
        partition1.close()

        # Load existing partition
        partition2 = PartitionLog(name="test", log_dir=temp_log_dir)

        # After close, the active segment is sealed, so when we reload
        # a new empty active segment is created
        # Segments: original sealed segments + newly sealed active + new empty active
        assert partition2.segment_count == first_segment_count + 1
        assert partition2.record_count == 30

        records = list(partition2.read())
        assert len(records) == 30

    def test_high_watermark(self, temp_log_dir):
        """Test high watermark tracking."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        assert partition.high_watermark == 0

        partition.append(b"one")
        assert partition.high_watermark == 1

        partition.append(b"two")
        assert partition.high_watermark == 2

    def test_get_segments_info(self, temp_log_dir):
        """Test getting segment information."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=100
        )

        for i in range(20):
            partition.append(b"x" * 10)

        info = partition.get_segments_info()

        assert len(info) == partition.segment_count
        for seg_info in info:
            assert "path" in seg_info
            assert "base_offset" in seg_info
            assert "record_count" in seg_info
            assert "size" in seg_info
            assert "sealed" in seg_info

    def test_concurrent_appends(self, temp_log_dir):
        """Test thread-safe concurrent appends."""
        import threading

        partition = PartitionLog(name="test", log_dir=temp_log_dir)
        num_threads = 10
        records_per_thread = 100
        errors = []

        def append_records(thread_id):
            try:
                for i in range(records_per_thread):
                    partition.append(f"thread-{thread_id}-record-{i}".encode())
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=append_records, args=(i,))
            for i in range(num_threads)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert partition.record_count == num_threads * records_per_thread


class TestPartitionLogOrderingGuarantee:
    """Tests proving records are always read in exact write order."""

    @pytest.fixture
    def temp_log_dir(self):
        """Create a temporary directory for log files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_order_preserved_with_timestamps(self, temp_log_dir):
        """Test that read order matches append order regardless of timestamps."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        # Append with out-of-order timestamps
        timestamps = [500, 100, 900, 200, 800]
        for i, ts in enumerate(timestamps):
            partition.append(f"record-{i}".encode(), timestamp=ts)

        records = list(partition.read())

        # Records should be in append order, not timestamp order
        assert records[0].data == b"record-0"
        assert records[1].data == b"record-1"
        assert records[2].data == b"record-2"
        assert records[3].data == b"record-3"
        assert records[4].data == b"record-4"

        # But timestamps should be preserved as written
        assert records[0].timestamp == 500
        assert records[1].timestamp == 100

    def test_order_preserved_across_restarts(self, temp_log_dir):
        """Test that order is preserved across partition restarts."""
        # Write records
        partition1 = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=80
        )

        expected_order = [f"record-{i:03d}".encode() for i in range(50)]
        for data in expected_order:
            partition1.append(data)

        partition1.close()

        # Reopen and verify
        partition2 = PartitionLog(name="test", log_dir=temp_log_dir)
        records = list(partition2.read())

        actual_order = [r.data for r in records]
        assert actual_order == expected_order

    def test_order_preserved_across_multiple_segments(self, temp_log_dir):
        """
        Critical test: Prove records are read in exact write order
        even when they span multiple segment boundaries.
        """
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=64  # Force small segments
        )

        # Write enough records to create multiple segments
        num_records = 200
        expected_data = [f"data-{i:04d}".encode() for i in range(num_records)]

        for data in expected_data:
            partition.append(data)

        # Verify multiple segments were created
        assert partition.segment_count >= 3, "Test requires multiple segments"

        # Read all records
        records = list(partition.read())

        # Verify exact order
        assert len(records) == num_records
        for i, record in enumerate(records):
            assert record.offset == i, f"Offset mismatch at index {i}"
            assert record.data == expected_data[i], f"Data mismatch at index {i}"
