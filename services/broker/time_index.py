"""
Time-based index for partition segments.

Each segment has a corresponding time index file (.timeindex) that maps
timestamps to record offsets, enabling O(log n) seeks by timestamp.

Format:
- 4-byte magic: 'STMI' (StreamCore Time Index)
- 4-byte entry count (uint32)
- 4-byte flags (uint32): bit 0 = is_complete
- 8-byte record_count (uint64): total records in segment this index covers
- N entries of [8-byte timestamp][8-byte relative_offset] (16 bytes each)
- Sorted by timestamp ascending

Optimizations:
- mmap for zero-copy reads
- Sparse index option (one entry per ~4KB of data)
- Batch seek for multi-timestamp queries
"""

from __future__ import annotations

import mmap
import os
import struct
import bisect
from dataclasses import dataclass, field
from typing import List, Optional


# Time index format
MAGIC_BYTES = b"STMI"  # StreamCore Time Index
INDEX_ENTRY_FORMAT = "!QQ"  # 8-byte timestamp + 8-byte relative offset
_INDEX_ENTRY = struct.Struct(INDEX_ENTRY_FORMAT)
INDEX_ENTRY_SIZE = _INDEX_ENTRY.size  # 16 bytes
HEADER_SIZE = 20  # 4 magic + 4 entry_count + 4 flags + 8 record_count

# Sparse index: one entry per ~4KB of segment data
DEFAULT_SPARSE_BYTES = 4096

# Flags
FLAG_COMPLETE = 1 << 0  # Index fully built (segment sealed)


@dataclass
class TimeIndexEntry:
    """A single entry in the time index: timestamp -> relative offset."""
    timestamp: int
    relative_offset: int

    def to_bytes(self) -> bytes:
        return _INDEX_ENTRY.pack(self.timestamp, self.relative_offset)

    @classmethod
    def from_bytes(cls, data: bytes, offset: int = 0) -> TimeIndexEntry:
        timestamp, relative_offset = _INDEX_ENTRY.unpack_from(data, offset)
        return cls(timestamp=timestamp, relative_offset=relative_offset)


@dataclass
class TimeIndex:
    """
    Time-based index for a segment.

    Stores (timestamp, relative_offset) pairs sorted by timestamp.
    Enables binary search for "first record with timestamp >= X".

    Supports both dense (every record) and sparse (periodic) indexing.
    """
    path: Path
    base_offset: int  # Base offset of the segment this index belongs to
    sparse_bytes: int = DEFAULT_SPARSE_BYTES  # 0 = dense (every record)

    _entries: List[TimeIndexEntry] = field(default_factory=list, repr=False)
    _loaded: bool = field(default=False, repr=False)
    _is_complete: bool = field(default=False, repr=False)
    _record_count: int = field(default=0, repr=False)

    def __post_init__(self) -> None:
        if self.path.exists():
            self._load_existing()

    def _load_existing(self) -> None:
        """Load time index from disk."""
        file_size = self.path.stat().st_size
        if file_size < HEADER_SIZE:
            return  # Empty or corrupt

        with open(self.path, "rb") as f:
            header = f.read(HEADER_SIZE)
            magic = header[0:4]
            if magic != MAGIC_BYTES:
                return  # Not a valid time index file

            entry_count, flags, record_count = struct.unpack("!IIQ", header[4:20])
            self._is_complete = bool(flags & FLAG_COMPLETE)
            self._record_count = record_count

            if entry_count <= 0:
                self._entries = []
                self._loaded = True
                return

            # Load entries into memory (small - 16 bytes each, typically < 100KB)
            entries_data = f.read(entry_count * INDEX_ENTRY_SIZE)
            self._entries = [
                TimeIndexEntry.from_bytes(entries_data, i * INDEX_ENTRY_SIZE)
                for i in range(entry_count)
            ]
            self._loaded = True

    def _create_new(self) -> None:
        """Create a new empty time index file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "wb") as f:
            f.write(MAGIC_BYTES)
            f.write(struct.pack("!I", 0))  # entry count = 0
            f.write(struct.pack("!I", 0))  # flags = 0
            f.write(struct.pack("!Q", 0))  # record_count = 0

    def append(self, timestamp: int, relative_offset: int) -> None:
        """
        Append a new entry to the time index (must be called in timestamp order).

        For sparse indexing, the caller decides which entries to append
        (based on byte positions); this method just stores them.
        """
        entry = TimeIndexEntry(timestamp=timestamp, relative_offset=relative_offset)
        self._entries.append(entry)

        # Append to file: update count then write entry at end
        with open(self.path, "r+b") as f:
            # Update entry count at byte offset 4
            f.seek(4, os.SEEK_SET)
            f.write(struct.pack("!I", len(self._entries)))
            # Seek to end and append entry
            f.seek(0, os.SEEK_END)
            f.write(entry.to_bytes())

    def _mark_complete(self, record_count: int) -> None:
        """Mark the index as complete and record total record count."""
        self._is_complete = True
        self._record_count = record_count

        with open(self.path, "r+b") as f:
            # Update flags at byte offset 8
            f.seek(8, os.SEEK_SET)
            f.write(struct.pack("!I", FLAG_COMPLETE))
            # Update record_count at byte offset 12
            f.seek(12, os.SEEK_SET)
            f.write(struct.pack("!Q", record_count))

    def find_first_ge(self, target_timestamp: int) -> Optional[int]:
        """
        Find the relative offset of the first record with timestamp >= target_timestamp.

        Returns None if no such record exists in this segment.
        Uses binary search: O(log n)
        """
        if not self._entries:
            return None

        # Binary search for first entry with timestamp >= target
        idx = bisect.bisect_left(self._entries, target_timestamp, key=lambda e: e.timestamp)
        if idx < len(self._entries):
            return self._entries[idx].relative_offset
        return None

    def find_first_ge_mmap(self, target_timestamp: int) -> Optional[int]:
        """
        Find the relative offset using a fresh mmap for zero-copy binary search.

        Opens its own mmap (closed before returning), avoiding long-held file
        locks while still avoiding per-read syscalls during the search.
        Falls back to in-memory search if the file cannot be mapped.
        """
        if not self._loaded or self.path.stat().st_size < HEADER_SIZE:
            return self.find_first_ge(target_timestamp)

        try:
            fd = os.open(self.path, os.O_RDONLY)
            mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
        except OSError:
            return self.find_first_ge(target_timestamp)

        try:
            entry_count = struct.unpack("!I", mm[4:8])[0]
            if entry_count == 0:
                return None

            lo, hi = 0, entry_count - 1
            result = None

            while lo <= hi:
                mid = (lo + hi) // 2
                offset = HEADER_SIZE + mid * INDEX_ENTRY_SIZE
                ts, rel_off = _INDEX_ENTRY.unpack_from(mm, offset)

                if ts >= target_timestamp:
                    result = rel_off
                    hi = mid - 1
                else:
                    lo = mid + 1

            return result
        finally:
            mm.close()
            os.close(fd)

    @property
    def entry_count(self) -> int:
        return len(self._entries)

    @property
    def is_complete(self) -> bool:
        return self._is_complete

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def min_timestamp(self) -> Optional[int]:
        return self._entries[0].timestamp if self._entries else None

    @property
    def max_timestamp(self) -> Optional[int]:
        return self._entries[-1].timestamp if self._entries else None


def create_time_index(segment_path: Path, base_offset: int, sparse_bytes: int = 0) -> TimeIndex:
    """Factory to create time index for a segment."""
    index_path = segment_path.with_suffix(".timeindex")
    time_index = TimeIndex(path=index_path, base_offset=base_offset, sparse_bytes=sparse_bytes)
    time_index._create_new()
    return time_index


def rebuild_time_index(segment_path: Path, base_offset: int, sparse_bytes: int = DEFAULT_SPARSE_BYTES) -> TimeIndex:
    """
    Rebuild time index from segment records.
    Used when segment is sealed or on startup.

    Supports sparse indexing: one entry per sparse_bytes of segment data.
    Marks index as complete when done.
    """
    index_path = segment_path.with_suffix(".timeindex")

    # Remove old index if exists
    if index_path.exists():
        index_path.unlink()

    time_index = TimeIndex(path=index_path, base_offset=base_offset, sparse_bytes=sparse_bytes)
    time_index._create_new()

    # Read segment records and build index
    from .segment import Segment, RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, _RECORD_HEADER

    segment = Segment(path=segment_path, base_offset=base_offset, max_size=1024*1024*1024)

    with open(segment_path, "rb") as f:
        f.seek(SEGMENT_HEADER_SIZE)
        relative_offset = 0
        bytes_since_index = 0
        data_size = segment._data_size

        while f.tell() < data_size:
            header = f.read(RECORD_HEADER_SIZE)
            if len(header) < RECORD_HEADER_SIZE:
                break
            length, timestamp = _RECORD_HEADER.unpack(header)
            f.seek(length, os.SEEK_CUR)

            # Sparse index: only add entry if enough bytes have passed
            if sparse_bytes == 0 or bytes_since_index >= sparse_bytes:
                time_index.append(timestamp, relative_offset)
                bytes_since_index = 0
            bytes_since_index += length + RECORD_HEADER_SIZE
            relative_offset += 1

        total_records = relative_offset

    # Mark complete with total record count
    time_index._mark_complete(total_records)

    return time_index


# ==================== Batch Seek Support ====================

def seek_batch_mmap(time_index: TimeIndex, timestamps: List[int]) -> List[Optional[int]]:
    """
    Perform multiple seeks on the same index.

    Sorts queries internally for single-pass efficiency and uses a fresh mmap
    for zero-copy binary searches (mmap is opened/closed per call).
    Returns results in original order.
    """
    if not timestamps:
        return []

    if not time_index._loaded or time_index.path.stat().st_size < HEADER_SIZE:
        return [None] * len(timestamps)

    # Pair each timestamp with its original index
    indexed = [(ts, i) for i, ts in enumerate(timestamps)]
    indexed.sort(key=lambda x: x[0])

    results = [None] * len(timestamps)

    try:
        fd = os.open(time_index.path, os.O_RDONLY)
        mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
    except OSError:
        # Fallback to in-memory binary search
        for target_ts, orig_idx in indexed:
            idx = bisect.bisect_left(time_index._entries, target_ts, key=lambda e: e.timestamp)
            if idx < len(time_index._entries):
                results[orig_idx] = time_index._entries[idx].relative_offset
        return results

    try:
        entry_count = struct.unpack("!I", mm[4:8])[0]
        if entry_count == 0:
            return results

        for target_ts, orig_idx in indexed:
            lo, hi = 0, entry_count - 1
            result = None
            while lo <= hi:
                mid = (lo + hi) // 2
                offset = HEADER_SIZE + mid * INDEX_ENTRY_SIZE
                ts, rel_off = _INDEX_ENTRY.unpack_from(mm, offset)
                if ts >= target_ts:
                    result = rel_off
                    hi = mid - 1
                else:
                    lo = mid + 1
            results[orig_idx] = result
    finally:
        mm.close()
        os.close(fd)

    return results
