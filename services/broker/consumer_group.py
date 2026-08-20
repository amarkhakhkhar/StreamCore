"""
Consumer Group - Multiple independent consumers sharing a partition.

Each consumer group tracks its own offset into the same partition.
Multiple groups can read the same partition at different speeds.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ConsumerGroupMember:
    """A single consumer within a consumer group."""
    member_id: str
    group_id: str
    assigned_partitions: List[str] = field(default_factory=list)
    last_heartbeat_ms: int = 0
    active: bool = True

    def heartbeat(self) -> None:
        self.last_heartbeat_ms = int(time.time() * 1000)


@dataclass
class ConsumerGroupState:
    """
    Tracks the state of a consumer group.

    Each group has its own offset per partition.
    Multiple groups can read the same partition independently.
    """
    group_id: str
    # partition_name -> committed_offset
    partition_offsets: Dict[str, int] = field(default_factory=dict)
    members: Dict[str, ConsumerGroupMember] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def get_offset(self, partition: str) -> int:
        """Get the current committed offset for a partition."""
        with self._lock:
            return self.partition_offsets.get(partition, 0)

    def commit_offset(self, partition: str, offset: int) -> None:
        """Commit offset for a partition (only moves forward)."""
        with self._lock:
            current = self.partition_offsets.get(partition, 0)
            if offset > current:
                self.partition_offsets[partition] = offset

    def add_member(self, member: ConsumerGroupMember) -> None:
        """Add a member to the group."""
        with self._lock:
            self.members[member.member_id] = member

    def remove_member(self, member_id: str) -> None:
        """Remove a member from the group."""
        with self._lock:
            self.members.pop(member_id, None)

    def get_active_members(self) -> List[ConsumerGroupMember]:
        """Get all active members."""
        with self._lock:
            return [m for m in self.members.values() if m.active]


class CircularQueueBuffer:
    """
    Circular queue backed in-memory buffer for a partition.

    Provides fast O(1) reads/writes without disk I/O for recent data.
    Uses a fixed-size array with head/tail pointers.
    """

    def __init__(self, capacity: int = 10000):
        self.capacity = capacity
        self._buffer: List[Optional[bytes]] = [None] * capacity
        self._head = 0  # Next write position
        self._tail = 0  # Next read position (oldest element)
        self._size = 0
        self._lock = threading.RLock()
        self._next_offset = 0  # Monotonically increasing global offset

    def append(self, data: bytes) -> int:
        """
        Append data to the circular buffer.

        Returns the global offset assigned to this record.
        """
        with self._lock:
            # If buffer is full, evict oldest element
            if self._size == self.capacity:
                self._tail = (self._tail + 1) % self.capacity

            # Assign monotonically increasing offset
            offset = self._next_offset
            self._next_offset += 1

            # Write to buffer at head position
            self._buffer[self._head] = data
            self._head = (self._head + 1) % self.capacity

            if self._size < self.capacity:
                self._size += 1

            return offset

    def read_at(self, offset: int) -> Optional[bytes]:
        """Read a record at a specific global offset."""
        with self._lock:
            if self._size == 0:
                return None

            # Oldest offset in buffer
            oldest_offset = self._next_offset - self._size
            newest_offset = self._next_offset - 1

            if offset < oldest_offset or offset > newest_offset:
                return None

            # Calculate index in buffer
            # The element at oldest_offset is at _tail position
            index = (self._tail + (offset - oldest_offset)) % self.capacity
            return self._buffer[index]

    def read_range(self, start_offset: int, max_records: int) -> List[tuple[int, bytes]]:
        """Read a range of records starting from start_offset."""
        with self._lock:
            if self._size == 0:
                return []

            oldest_offset = self._next_offset - self._size
            newest_offset = self._next_offset - 1

            if start_offset > newest_offset:
                return []

            actual_start = max(start_offset, oldest_offset)
            records = []

            for i in range(min(max_records, newest_offset - actual_start + 1)):
                offset = actual_start + i
                index = (self._tail + (offset - oldest_offset)) % self.capacity
                data = self._buffer[index]
                if data is not None:
                    records.append((offset, data))

            return records

    def get_latest_offset(self) -> int:
        """Get the latest (highest) offset in the buffer."""
        with self._lock:
            if self._size == 0:
                return self._next_offset - 1
            return self._next_offset - 1

    def get_oldest_offset(self) -> int:
        """Get the oldest offset in the buffer."""
        with self._lock:
            if self._size == 0:
                return self._next_offset
            return self._next_offset - self._size

    def is_empty(self) -> bool:
        return self._size == 0


class ConsumerGroupManager:
    """
    Manages multiple consumer groups for a partition.

    Each group maintains independent offsets. Groups don't interfere with each other.
    """

    def __init__(self):
        self._groups: Dict[str, ConsumerGroupState] = {}
        self._lock = threading.RLock()
        # Partition -> circular buffer
        self._partition_buffers: Dict[str, CircularQueueBuffer] = {}
        self._buffer_lock = threading.RLock()

    def create_group(self, group_id: str) -> ConsumerGroupState:
        """Create a new consumer group."""
        with self._lock:
            if group_id not in self._groups:
                self._groups[group_id] = ConsumerGroupState(group_id=group_id)
            return self._groups[group_id]

    def get_group(self, group_id: str) -> Optional[ConsumerGroupState]:
        """Get an existing consumer group."""
        with self._lock:
            return self._groups.get(group_id)

    def delete_group(self, group_id: str) -> bool:
        """Delete a consumer group."""
        with self._lock:
            if group_id in self._groups:
                del self._groups[group_id]
                return True
            return False

    def list_groups(self) -> List[str]:
        """List all consumer group IDs."""
        with self._lock:
            return list(self._groups.keys())

    def register_member(self, group_id: str, member_id: str, partitions: List[str]) -> ConsumerGroupMember:
        """Register a consumer member to a group."""
        group = self.create_group(group_id)
        member = ConsumerGroupMember(
            member_id=member_id,
            group_id=group_id,
            assigned_partitions=partitions
        )
        group.add_member(member)
        return member

    def heartbeat(self, group_id: str, member_id: str) -> bool:
        """Member sends heartbeat."""
        with self._lock:
            group = self._groups.get(group_id)
            if group and member_id in group.members:
                group.members[member_id].heartbeat()
                return True
            return False

    def get_partition_buffer(self, partition: str, capacity: int = 10000) -> CircularQueueBuffer:
        """Get or create circular buffer for a partition."""
        with self._buffer_lock:
            if partition not in self._partition_buffers:
                self._partition_buffers[partition] = CircularQueueBuffer(capacity=capacity)
            return self._partition_buffers[partition]

    def append_to_partition(self, partition: str, data: bytes) -> int:
        """Append data to a partition's circular buffer."""
        buffer = self.get_partition_buffer(partition)
        return buffer.append(data)

    def read_from_partition(
        self,
        partition: str,
        group_id: str,
        max_records: int = 100
    ) -> List[tuple[int, bytes]]:
        """
        Read records from a partition for a specific consumer group.

        Uses the group's committed offset as the starting point.
        """
        group = self.get_group(group_id)
        if not group:
            return []

        buffer = self.get_partition_buffer(partition)
        current_offset = group.get_offset(partition)

        records = buffer.read_range(current_offset, max_records)

        # Update group offset if we read anything
        if records:
            last_offset = records[-1][0]
            group.commit_offset(partition, last_offset + 1)

        return records

    def get_group_lag(self, group_id: str, partition: str, high_watermark: int) -> int:
        """Get lag for a consumer group on a partition."""
        group = self.get_group(group_id)
        if not group:
            return high_watermark

        committed = group.get_offset(partition)
        return max(0, high_watermark - committed)

    def get_all_group_lags(self, partition: str, high_watermark: int) -> Dict[str, int]:
        """Get lag for all groups on a partition."""
        with self._lock:
            result = {}
            for group_id, group in self._groups.items():
                result[group_id] = self.get_group_lag(group_id, partition, high_watermark)
            return result


def create_consumer_group_manager() -> ConsumerGroupManager:
    """Factory function to create a consumer group manager."""
    return ConsumerGroupManager()