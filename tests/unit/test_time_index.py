"""Tests for TimeIndex and seek_by_timestamp functionality."""

import tempfile
from pathlib import Path

import pytest

from services.broker.partition_log import PartitionLog
from services.broker.time_index import TimeIndex, create_time_index, rebuild_time_index


class TestTimeIndex:
    """Unit tests for the TimeIndex class."""

    @pytest.fixture
    def temp_log_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_time_index_create_and_append(self, temp_log_dir):
        """Test creating a time index and appending entries."""
        segment_path = temp_log_dir / "0.log"
        segment_path.touch()

        time_index = create_time_index(segment_path, base_offset=0)

        # Append entries in timestamp order
        timestamps = [100, 200, 300, 400, 500]
        for i, ts in enumerate(timestamps):
            time_index.append(ts, i)

        assert time_index.entry_count == 5
        assert time_index.min_timestamp == 100
        assert time_index.max_timestamp == 500

    def test_time_index_find_first_ge_exact(self, temp_log_dir):
        """Test finding exact timestamp match."""
        segment_path = temp_log_dir / "0.log"
        segment_path.touch()

        time_index = create_time_index(segment_path, base_offset=0)

        timestamps = [100, 200, 300, 400, 500]
        for i, ts in enumerate(timestamps):
            time_index.append(ts, i)

        # Exact match at 300 -> relative offset 2
        result = time_index.find_first_ge(300)
        assert result == 2

    def test_time_index_find_first_ge_between(self, temp_log_dir):
        """Test finding first timestamp >= target when target is between entries."""
        segment_path = temp_log_dir / "0.log"
        segment_path.touch()

        time_index = create_time_index(segment_path, base_offset=0)

        timestamps = [100, 200, 300, 400, 500]
        for i, ts in enumerate(timestamps):
            time_index.append(ts, i)

        # Target 250 -> should return 300 (offset 2)
        result = time_index.find_first_ge(250)
        assert result == 2

    def test_time_index_find_first_ge_before_all(self, temp_log_dir):
        """Test when target is before all timestamps."""
        segment_path = temp_log_dir / "0.log"
        segment_path.touch()

        time_index = create_time_index(segment_path, base_offset=0)

        timestamps = [100, 200, 300, 400, 500]
        for i, ts in enumerate(timestamps):
            time_index.append(ts, i)

        # Target 50 -> should return first entry (offset 0)
        result = time_index.find_first_ge(50)
        assert result == 0

    def test_time_index_find_first_ge_after_all(self, temp_log_dir):
        """Test when target is after all timestamps."""
        segment_path = temp_log_dir / "0.log"
        segment_path.touch()

        time_index = create_time_index(segment_path, base_offset=0)

        timestamps = [100, 200, 300, 400, 500]
        for i, ts in enumerate(timestamps):
            time_index.append(ts, i)

        # Target 600 -> should return None
        result = time_index.find_first_ge(600)
        assert result is None

    def test_time_index_persistence(self, temp_log_dir):
        """Test that time index persists to disk and reloads."""
        segment_path = temp_log_dir / "0.log"
        segment_path.touch()

        # Create and populate
        time_index = create_time_index(segment_path, base_offset=0)
        for i, ts in enumerate([100, 200, 300]):
            time_index.append(ts, i)

        # Create fresh instance - should load from disk
        time_index2 = TimeIndex(path=segment_path.with_suffix(".timeindex"), base_offset=0)
        assert time_index2._loaded is True
        assert time_index2.entry_count == 3
        assert time_index2.find_first_ge(200) == 1


class TestSeekByTimestamp:
    """Integration tests for seek_by_timestamp on PartitionLog."""

    @pytest.fixture
    def temp_log_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_seek_empty_partition(self, temp_log_dir):
        """Seeking in empty partition returns None."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)
        result = partition.seek_by_timestamp(123456)
        assert result is None

    def test_seek_single_segment(self, temp_log_dir):
        """Test seeking within a single segment."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        base_time = 1000000
        for i in range(100):
            partition.append(f"rec-{i}".encode(), timestamp=base_time + i * 100)

        # Seek to timestamp of record 50 (base_time + 5000)
        target = base_time + 5000
        offset = partition.seek_by_timestamp(target)
        assert offset == 50

        # Verify by reading at that offset
        record = partition.read_at(offset)
        assert record is not None
        assert record.timestamp == target
        assert record.data == b"rec-50"

    def test_seek_across_segments(self, temp_log_dir):
        """Test seeking across multiple segments."""
        partition = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=512  # Force multiple segments
        )

        base_time = 1000000
        num_records = 500
        for i in range(num_records):
            partition.append(f"rec-{i:04d}".encode(), timestamp=base_time + i * 100)

        assert partition.segment_count >= 2, "Test requires multiple segments"

        # Seek to a record in the second segment (offset 250 -> timestamp base+25000)
        target = base_time + 25000
        offset = partition.seek_by_timestamp(target)
        assert offset == 250

        record = partition.read_at(offset)
        assert record is not None
        assert record.timestamp == target

    def test_seek_returns_first_match(self, temp_log_dir):
        """Test that seek returns the FIRST record >= target."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        base_time = 1000000
        # Records at timestamps: 100, 200, 300, 400, 500
        for i in range(5):
            partition.append(f"rec-{i}".encode(), timestamp=base_time + (i + 1) * 100)

        # Seek to 250 -> should return record at 300 (offset 2)
        target = base_time + 250
        offset = partition.seek_by_timestamp(target)
        assert offset == 2

        record = partition.read_at(offset)
        assert record.timestamp == base_time + 300

    def test_seek_before_all(self, temp_log_dir):
        """Test seeking before all timestamps returns offset 0."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        base_time = 1000000
        for i in range(10):
            partition.append(f"rec-{i}".encode(), timestamp=base_time + i * 100)

        # Seek to timestamp before all records
        target = base_time - 5000
        offset = partition.seek_by_timestamp(target)
        assert offset == 0

    def test_seek_after_all(self, temp_log_dir):
        """Test seeking after all timestamps returns None."""
        partition = PartitionLog(name="test", log_dir=temp_log_dir)

        base_time = 1000000
        for i in range(10):
            partition.append(f"rec-{i}".encode(), timestamp=base_time + i * 100)

        # Seek to timestamp after all records
        target = base_time + 999999
        offset = partition.seek_by_timestamp(target)
        assert offset is None

    def test_seek_after_rotation_and_reload(self, temp_log_dir):
        """Test seeking works after segments are sealed and reloaded."""
        partition1 = PartitionLog(
            name="test",
            log_dir=temp_log_dir,
            segment_max_size=512
        )

        base_time = 1000000
        num_records = 300
        for i in range(num_records):
            partition1.append(f"rec-{i:04d}".encode(), timestamp=base_time + i * 100)

        partition1.close()

        # Reload
        partition2 = PartitionLog(name="test", log_dir=temp_log_dir)

        # Seek should still work after reload (time index rebuilt from disk)
        target = base_time + 15000  # offset 150
        offset = partition2.seek_by_timestamp(target)
        assert offset == 150

        record = partition2.read_at(offset)
        assert record is not None
        assert record.timestamp == target


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
