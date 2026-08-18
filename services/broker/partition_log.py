"""
Partition Log - The core primitive of StreamCore.

An append-only log structured as a doubly linked list of segments:
- Dummy head sentinel for clean prepend operations
- Zero or more sealed (immutable) segments
- Exactly one active segment for writes
- Dummy tail sentinel for clean append operations

This is exactly how Kafka partitions are structured.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional
import os

from .segment import Record, Segment, SegmentNode


@dataclass
class PartitionLog:
    """
    An append-only partition log backed by segmented files.

    Structure (doubly linked list):
        HEAD <-> [sealed segments...] <-> [active segment] <-> TAIL

    - HEAD and TAIL are dummy sentinels (no actual segment data)
    - Sealed segments are immutable and can only be read
    - Active segment accepts writes until full, then gets sealed
    """

    name: str
    log_dir: Path
    segment_max_size: int = 1024 * 1024 * 1024  # 1GB

    # Linked list: head sentinel <-> segments <-> tail sentinel
    _head: SegmentNode = field(default=None, repr=False)
    _tail: SegmentNode = field(default=None, repr=False)
    _active: Optional[SegmentNode] = field(default=None, repr=False)
    _next_offset: int = field(default=0, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self) -> None:
        """Initialize the partition log with sentinel nodes."""
        self.log_dir = Path(self.log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Create dummy head and tail sentinels
        self._head = SegmentNode(segment=None)
        self._tail = SegmentNode(segment=None)
        self._head.next = self._tail
        self._tail.prev = self._head

        # Load existing segments or create first active segment
        self._load_segments()

    def _load_segments(self) -> None:
        """Load existing segments from disk and rebuild the linked list."""
        # Get all segment files and sort by base offset (numeric, not lexicographic)
        segment_files = list(self.log_dir.glob("*.log"))
        segment_files.sort(key=lambda p: int(p.stem))

        if not segment_files:
            # No existing segments, create first active segment
            self._create_active_segment(base_offset=0)
            return

        # Load each segment and link them in order
        prev_node = self._head

        for seg_path in segment_files:
            # Extract base offset from filename: {base_offset}.log
            base_offset = int(seg_path.stem)

            segment = Segment(
                path=seg_path,
                base_offset=base_offset,
                max_size=self.segment_max_size
            )

            node = SegmentNode(segment=segment)
            node.prev = prev_node
            prev_node.next = node

            # Update next_offset based on this segment
            self._next_offset = base_offset + segment.record_count

            # If segment is not sealed, it becomes active
            if not segment.is_sealed:
                self._active = node

            prev_node = node

        # Link last node to tail
        prev_node.next = self._tail
        self._tail.prev = prev_node

        # If no active segment found (all were sealed), create one
        if self._active is None:
            self._create_active_segment(base_offset=self._next_offset)

    def _create_active_segment(self, base_offset: int) -> SegmentNode:
        """Create a new active segment and add it to the linked list."""
        segment_path = self.log_dir / f"{base_offset}.log"

        segment = Segment(
            path=segment_path,
            base_offset=base_offset,
            max_size=self.segment_max_size
        )

        node = SegmentNode(segment=segment)

        # Insert before tail sentinel
        with self._lock:
            prev_node = self._tail.prev
            node.prev = prev_node
            node.next = self._tail
            prev_node.next = node
            self._tail.prev = node

            # Update prev_segment link
            if prev_node.segment:
                prev_node.segment.next_segment = segment_path
                segment.prev_segment = prev_node.segment.path

            self._active = node

        return node

    def _rotate_segment(self) -> None:
        """Seal current active segment and create a new one."""
        with self._lock:
            if self._active and self._active.segment:
                self._active.segment.seal()

            self._create_active_segment(base_offset=self._next_offset)

    def append(self, data: bytes, timestamp: Optional[int] = None) -> int:
        """
        Append a record to the partition log.

        Args:
            data: The record data (bytes)
            timestamp: Optional timestamp (defaults to current time in ms)

        Returns:
            The offset of the appended record
        """
        if timestamp is None:
            timestamp = int(time.time() * 1000)

        with self._lock:
            # Check if we need to rotate to a new segment
            if self._active and self._active.segment and self._active.segment.is_full:
                self._rotate_segment()

            record = Record(
                offset=self._next_offset,
                timestamp=timestamp,
                data=data
            )

            self._active.segment.append(record)
            self._next_offset += 1

            return record.offset

    def read(self, start_offset: int = 0, max_records: int = -1) -> Iterator[Record]:
        """
        Read records from the partition log in exact write order.

        Args:
            start_offset: The offset to start reading from
            max_records: Maximum number of records to read (-1 for all)

        Yields:
            Record objects in exact append order
        """
        with self._lock:
            # Find the segment containing start_offset
            current = self._head.next
            records_yielded = 0

            while current != self._tail:
                if current.segment is None:
                    current = current.next
                    continue

                seg = current.segment

                # Check if this segment could contain our start offset
                if seg.base_offset + seg.record_count <= start_offset:
                    current = current.next
                    continue

                # Calculate relative offset within this segment
                relative_start = max(0, start_offset - seg.base_offset)

                # Read from this segment
                for record in seg.read(start_offset=relative_start):
                    if max_records > 0 and records_yielded >= max_records:
                        return
                    yield record
                    records_yielded += 1

                current = current.next

    def read_at(self, offset: int) -> Optional[Record]:
        """Read a single record at the given offset."""
        for record in self.read(start_offset=offset, max_records=1):
            return record
        return None

    @property
    def segment_count(self) -> int:
        """Return the total number of segments (including active)."""
        count = 0
        current = self._head.next
        while current != self._tail:
            if current.segment:
                count += 1
            current = current.next
        return count

    @property
    def sealed_segment_count(self) -> int:
        """Return the number of sealed segments."""
        count = 0
        current = self._head.next
        while current != self._tail:
            if current.segment and current.segment.is_sealed:
                count += 1
            current = current.next
        return count

    @property
    def record_count(self) -> int:
        """Return the total number of records across all segments."""
        return self._next_offset

    @property
    def high_watermark(self) -> int:
        """Return the offset of the last committed record + 1."""
        return self._next_offset

    def close(self) -> None:
        """Close the partition log and seal the active segment."""
        with self._lock:
            if self._active and self._active.segment and not self._active.segment.is_sealed:
                self._active.segment.seal()

    def get_segments_info(self) -> List[dict]:
        """Get information about all segments for debugging/monitoring."""
        segments_info = []
        current = self._head.next

        while current != self._tail:
            if current.segment:
                seg = current.segment
                segments_info.append({
                    "path": str(seg.path),
                    "base_offset": seg.base_offset,
                    "record_count": seg.record_count,
                    "size": seg.size,
                    "sealed": seg.is_sealed
                })
            current = current.next

        return segments_info
