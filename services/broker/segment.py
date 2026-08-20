"""
Segment management for the partition log.

Each segment is an immutable file containing records in append order.
Segments are linked together to form the complete partition log.
"""

from __future__ import annotations

import os
import struct
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional


# Record format: [4 bytes length][8 bytes timestamp][data]
RECORD_HEADER_FORMAT = "!IQ"  # 4-byte unsigned int + 8-byte unsigned long
_RECORD_HEADER = struct.Struct(RECORD_HEADER_FORMAT)
RECORD_HEADER_SIZE = _RECORD_HEADER.size  # 12 bytes
MAGIC_BYTES = b"SCSG"  # StreamCore Segment magic number
SEGMENT_HEADER_SIZE = 4  # magic bytes only
FOOTER_MAGIC = b"SCEF"  # StreamCore Segment Footer magic
SEGMENT_FOOTER_SIZE = 4 + 8 + 8  # magic + record count + checksum


@dataclass
class Record:
    """A single record in the partition log."""

    offset: int
    timestamp: int  # Unix timestamp in milliseconds
    data: bytes

    def to_bytes(self) -> bytes:
        """Serialize record to bytes."""
        return _RECORD_HEADER.pack(len(self.data), self.timestamp) + self.data

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> Record:
        """Deserialize record from bytes."""
        length, timestamp = _RECORD_HEADER.unpack_from(data)
        record_data = data[RECORD_HEADER_SIZE:RECORD_HEADER_SIZE + length]
        return cls(offset=offset, timestamp=timestamp, data=record_data)


@dataclass
class Segment:
    """
    An immutable segment file containing records.

    Segments are sealed once they reach max_size and become read-only.
    New records go to the active segment only.
    """

    path: Path
    base_offset: int  # The offset of the first record in this segment
    max_size: int = 1024 * 1024 * 1024  # 1GB default

    # Linked list pointers (segment file paths)
    prev_segment: Optional[Path] = None
    next_segment: Optional[Path] = None

    _record_count: int = field(default=0, repr=False)
    _sealed: bool = field(default=False, repr=False)
    _data_size: int = field(default=0, repr=False)  # Size of header + records only
    _cached_records: Optional[tuple[Record, ...]] = field(default=None, repr=False)
    _cache_lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self) -> None:
        """Initialize segment from existing file or create new one."""
        if self.path.exists():
            self._load_existing()
        else:
            self._create_new()

    def _create_new(self) -> None:
        """Create a new segment file with header."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "wb") as f:
            f.write(MAGIC_BYTES)
        self._data_size = SEGMENT_HEADER_SIZE
        self._sealed = False
        self._record_count = 0

    def _load_existing(self) -> None:
        """Load metadata from existing segment file."""
        file_size = self.path.stat().st_size

        with open(self.path, "rb") as f:
            magic = f.read(4)
            if magic != MAGIC_BYTES:
                raise ValueError(f"Invalid segment file: {self.path}")

            # Check for footer (sealed segment)
            if file_size >= SEGMENT_FOOTER_SIZE:
                f.seek(-SEGMENT_FOOTER_SIZE, os.SEEK_END)
                footer_magic = f.read(4)
                if footer_magic == FOOTER_MAGIC:
                    self._sealed = True
                    # Read record count from footer
                    record_count_bytes = f.read(8)
                    self._record_count = struct.unpack("!Q", record_count_bytes)[0]
                    self._data_size = file_size - SEGMENT_FOOTER_SIZE
                else:
                    self._sealed = False
                    self._data_size = file_size
                    self._record_count = self._count_records(f)
            else:
                self._sealed = False
                self._data_size = file_size
                self._record_count = self._count_records(f)

    def _count_records(self, f) -> int:
        """Count records by scanning the file."""
        f.seek(SEGMENT_HEADER_SIZE)
        count = 0
        while f.tell() < self._data_size:
            header = f.read(RECORD_HEADER_SIZE)
            if len(header) < RECORD_HEADER_SIZE:
                break
            length, _ = _RECORD_HEADER.unpack(header)
            f.seek(length, os.SEEK_CUR)
            count += 1
        return count

    @property
    def is_sealed(self) -> bool:
        """Return True if this segment is sealed (read-only)."""
        return self._sealed

    @property
    def record_count(self) -> int:
        """Return the number of records in this segment."""
        return self._record_count

    @property
    def size(self) -> int:
        """Return the current data size (header + records, no footer)."""
        return self._data_size

    @property
    def is_full(self) -> bool:
        """Return True if segment has reached max size."""
        return self._data_size >= self.max_size

    def append(self, record: Record) -> int:
        """
        Append a record to this segment.

        Returns the offset of the appended record.
        Raises RuntimeError if segment is sealed.
        """
        if self._sealed:
            raise RuntimeError(f"Cannot append to sealed segment: {self.path}")

        record_bytes = record.to_bytes()

        with open(self.path, "ab") as f:
            f.write(record_bytes)

        self._record_count += 1
        self._data_size += len(record_bytes)

        return record.offset

    def seal(self) -> None:
        """Seal this segment, making it immutable."""
        if self._sealed:
            return

        # Write footer: magic + record count + checksum placeholder
        with open(self.path, "ab") as f:
            f.write(FOOTER_MAGIC)
            f.write(struct.pack("!Q", self._record_count))
            f.write(struct.pack("!Q", 0))  # checksum = 0 for now

        self._sealed = True

    def _read_from_disk(self) -> tuple[Record, ...]:
        """Load all records from disk once for an immutable segment."""
        records: list[Record] = []
        with open(self.path, "rb") as f:
            f.seek(SEGMENT_HEADER_SIZE)
            relative_offset = 0
            data_size = self._data_size
            unpack = _RECORD_HEADER.unpack

            while f.tell() < data_size:
                header = f.read(RECORD_HEADER_SIZE)
                if len(header) < RECORD_HEADER_SIZE:
                    break

                length, timestamp = unpack(header)
                data = f.read(length)
                if len(data) < length:
                    break

                records.append(Record(
                    offset=self.base_offset + relative_offset,
                    timestamp=timestamp,
                    data=data,
                ))
                relative_offset += 1

        return tuple(records)

    def cache_records(self, records: tuple[Record, ...]) -> None:
        """Install an immutable in-memory record snapshot for this sealed segment."""
        if not self._sealed:
            return
        with self._cache_lock:
            self._cached_records = records

    def clear_cached_records(self) -> None:
        """Release the in-memory record snapshot used by the segment cache."""
        with self._cache_lock:
            self._cached_records = None

    def _get_cached_records(self) -> Optional[tuple[Record, ...]]:
        with self._cache_lock:
            return self._cached_records

    def read(self, start_offset: int = 0, max_records: int = -1) -> Iterator[Record]:
        """Read records in append order, using the in-memory snapshot when cached."""
        cached = self._get_cached_records() if self._sealed else None
        if cached is not None:
            end = len(cached)
            if max_records > 0:
                end = min(end, start_offset + max_records)
            yield from cached[start_offset:end]
            return

        with open(self.path, "rb") as f:
            f.seek(SEGMENT_HEADER_SIZE)
            relative_offset = 0
            records_yielded = 0
            data_size = self._data_size
            unpack = _RECORD_HEADER.unpack

            while f.tell() < data_size:
                if max_records > 0 and records_yielded >= max_records:
                    break

                header = f.read(RECORD_HEADER_SIZE)
                if len(header) < RECORD_HEADER_SIZE:
                    break

                length, timestamp = unpack(header)
                data = f.read(length)
                if len(data) < length:
                    break

                if relative_offset >= start_offset:
                    yield Record(
                        offset=self.base_offset + relative_offset,
                        timestamp=timestamp,
                        data=data,
                    )
                    records_yielded += 1

                relative_offset += 1

    def read_at(self, relative_offset: int) -> Optional[Record]:
        """Read a single record at the given relative offset."""
        for record in self.read(start_offset=relative_offset, max_records=1):
            return record
        return None


@dataclass
class SegmentNode:
    """
    A node in the doubly-linked list of segments.

    Uses dummy head/tail sentinels for clean segment management.
    """

    segment: Optional[Segment] = None  # None for sentinel nodes
    prev: Optional[SegmentNode] = None
    next: Optional[SegmentNode] = None

    @property
    def is_sentinel(self) -> bool:
        """Return True if this is a sentinel node (no actual segment)."""
        return self.segment is None
