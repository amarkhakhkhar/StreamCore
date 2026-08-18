"""Tests for Segment implementation."""

import os
import struct
import tempfile
from pathlib import Path

import pytest

from services.broker.segment import (
    Record,
    Segment,
    SegmentNode,
    MAGIC_BYTES,
    RECORD_HEADER_SIZE,
    RECORD_HEADER_FORMAT,
)


class TestRecord:
    """Tests for the Record class."""

    def test_record_creation(self):
        """Test basic record creation."""
        record = Record(offset=0, timestamp=1000, data=b"hello")
        assert record.offset == 0
        assert record.timestamp == 1000
        assert record.data == b"hello"

    def test_record_serialization(self):
        """Test record to_bytes and from_bytes roundtrip."""
        original = Record(offset=42, timestamp=1234567890, data=b"test data")
        serialized = original.to_bytes()
        recovered = Record.from_bytes(serialized, offset=42)

        assert recovered.offset == 42
        assert recovered.timestamp == 1234567890
        assert recovered.data == b"test data"

    def test_record_serialization_empty_data(self):
        """Test record with empty data."""
        original = Record(offset=0, timestamp=0, data=b"")
        serialized = original.to_bytes()
        recovered = Record.from_bytes(serialized)

        assert recovered.data == b""

    def test_record_serialization_binary_data(self):
        """Test record with binary data."""
        binary_data = bytes(range(256))
        original = Record(offset=100, timestamp=999, data=binary_data)
        serialized = original.to_bytes()
        recovered = Record.from_bytes(serialized, offset=100)

        assert recovered.data == binary_data


class TestSegment:
    """Tests for the Segment class."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for segment files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_segment_creation(self, temp_dir):
        """Test creating a new segment."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0)

        assert segment.path.exists()
        assert segment.base_offset == 0
        assert not segment.is_sealed
        assert segment.record_count == 0
        assert segment.size == 4  # Magic bytes only

    def test_segment_append_single_record(self, temp_dir):
        """Test appending a single record."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0)

        record = Record(offset=0, timestamp=1000, data=b"test")
        offset = segment.append(record)

        assert offset == 0
        assert segment.record_count == 1
        assert segment.size == 4 + RECORD_HEADER_SIZE + 4  # magic + header + data

    def test_segment_append_multiple_records(self, temp_dir):
        """Test appending multiple records."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0)

        for i in range(10):
            record = Record(offset=i, timestamp=1000 + i, data=f"record-{i}".encode())
            segment.append(record)

        assert segment.record_count == 10

    def test_segment_read_records(self, temp_dir):
        """Test reading records back."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=100)  # Non-zero base offset

        # Append records
        test_data = [b"first", b"second", b"third"]
        for i, data in enumerate(test_data):
            record = Record(offset=100 + i, timestamp=1000 * i, data=data)
            segment.append(record)

        # Read back
        records = list(segment.read())
        assert len(records) == 3

        assert records[0].offset == 100
        assert records[0].data == b"first"

        assert records[1].offset == 101
        assert records[1].data == b"second"

        assert records[2].offset == 102
        assert records[2].data == b"third"

    def test_segment_read_with_start_offset(self, temp_dir):
        """Test reading from a specific relative offset."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0)

        for i in range(10):
            segment.append(Record(offset=i, timestamp=i, data=f"record-{i}".encode()))

        # Read from offset 5
        records = list(segment.read(start_offset=5))
        assert len(records) == 5
        assert records[0].offset == 5
        assert records[4].offset == 9

    def test_segment_read_with_max_records(self, temp_dir):
        """Test reading with max records limit."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0)

        for i in range(100):
            segment.append(Record(offset=i, timestamp=i, data=b"x"))

        records = list(segment.read(max_records=10))
        assert len(records) == 10

    def test_segment_seal(self, temp_dir):
        """Test sealing a segment."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0)

        segment.append(Record(offset=0, timestamp=0, data=b"test"))
        assert not segment.is_sealed

        segment.seal()
        assert segment.is_sealed

        # Cannot append to sealed segment
        with pytest.raises(RuntimeError, match="Cannot append to sealed segment"):
            segment.append(Record(offset=1, timestamp=1, data=b"fail"))

    def test_segment_load_existing(self, temp_dir):
        """Test loading an existing segment from disk."""
        segment_path = temp_dir / "0.log"

        # Create and write to segment
        original = Segment(path=segment_path, base_offset=0)
        for i in range(5):
            original.append(Record(offset=i, timestamp=i, data=f"data-{i}".encode()))
        original.seal()

        # Load segment in a new instance
        loaded = Segment(path=segment_path, base_offset=0)
        assert loaded.record_count == 5
        assert loaded.is_sealed

        records = list(loaded.read())
        assert len(records) == 5

    def test_segment_is_full(self, temp_dir):
        """Test segment full detection."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0, max_size=100)

        # Write until full
        while not segment.is_full:
            segment.append(Record(offset=segment.record_count, timestamp=0, data=b"x" * 10))

        assert segment.is_full

    def test_segment_read_at(self, temp_dir):
        """Test reading a single record at a specific relative offset."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=100)

        for i in range(5):
            segment.append(Record(offset=100 + i, timestamp=i * 100, data=f"r{i}".encode()))

        record = segment.read_at(2)
        assert record is not None
        assert record.offset == 102
        assert record.data == b"r2"

    def test_segment_read_at_nonexistent(self, temp_dir):
        """Test reading at a non-existent offset."""
        segment_path = temp_dir / "0.log"
        segment = Segment(path=segment_path, base_offset=0)
        segment.append(Record(offset=0, timestamp=0, data=b"only"))

        record = segment.read_at(100)
        assert record is None


class TestSegmentNode:
    """Tests for the SegmentNode linked list node."""

    def test_segment_node_creation(self):
        """Test creating a segment node."""
        segment_path = Path("/tmp/test.log")
        segment = Segment.__new__(Segment)  # Create without init
        segment.path = segment_path

        node = SegmentNode(segment=segment)
        assert node.segment is segment
        assert node.prev is None
        assert node.next is None
        assert not node.is_sentinel

    def test_sentinel_node(self):
        """Test that nodes with no segment are sentinels."""
        node = SegmentNode(segment=None)
        assert node.is_sentinel

    def test_linked_list_operations(self):
        """Test linking nodes together."""
        head = SegmentNode(segment=None)  # Sentinel
        middle = SegmentNode(segment=object())
        tail = SegmentNode(segment=None)  # Sentinel

        head.next = middle
        middle.prev = head
        middle.next = tail
        tail.prev = middle

        assert head.next == middle
        assert middle.prev == head
        assert middle.next == tail
        assert tail.prev == middle
