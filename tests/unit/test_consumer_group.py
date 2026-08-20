"""Unit tests for consumer group mechanics (Day 6)."""

import threading
import time
import pytest

from services.broker.consumer_group import (
    ConsumerGroupMember,
    ConsumerGroupState,
    ConsumerGroupManager,
    CircularQueueBuffer,
    create_consumer_group_manager,
)


class TestCircularQueueBuffer:
    """Test circular queue buffer with offset mapping."""

    def test_append_returns_sequential_offsets(self):
        """Append should return 0, 1, 2, ..."""
        buffer = CircularQueueBuffer(capacity=100)
        offsets = [buffer.append(f"msg-{i}".encode()) for i in range(5)]
        assert offsets == [0, 1, 2, 3, 4]

    def test_read_at_returns_correct_data(self):
        """read_at should return data written at that offset."""
        buffer = CircularQueueBuffer(capacity=100)
        for i in range(5):
            buffer.append(f"msg-{i}".encode())
        assert buffer.read_at(0) == b"msg-0"
        assert buffer.read_at(3) == b"msg-3"

    def test_read_range_returns_sequential_records(self):
        """read_range should return records from start_offset."""
        buffer = CircularQueueBuffer(capacity=100)
        for i in range(10):
            buffer.append(f"msg-{i}".encode())
        records = buffer.read_range(2, max_records=3)
        assert [r[0] for r in records] == [2, 3, 4]
        assert [r[1] for r in records] == [b"msg-2", b"msg-3", b"msg-4"]

    def test_read_range_respects_max_records(self):
        """read_range should not exceed max_records."""
        buffer = CircularQueueBuffer(capacity=100)
        for i in range(10):
            buffer.append(f"msg-{i}".encode())
        records = buffer.read_range(0, max_records=2)
        assert len(records) == 2

    def test_read_range_starts_at_offset(self):
        """read_range should start at the given offset."""
        buffer = CircularQueueBuffer(capacity=100)
        for i in range(10):
            buffer.append(f"msg-{i}".encode())
        records = buffer.read_range(5, max_records=3)
        assert [r[0] for r in records] == [5, 6, 7]

    def test_read_past_newest_returns_empty(self):
        """read_range past newest_offset should return empty."""
        buffer = CircularQueueBuffer(capacity=100)
        buffer.append(b"msg-0")
        records = buffer.read_range(5, max_records=3)
        assert records == []

    def test_get_latest_offset(self):
        """get_latest_offset should return highest offset."""
        buffer = CircularQueueBuffer(capacity=100)
        for i in range(5):
            buffer.append(f"msg-{i}".encode())
        assert buffer.get_latest_offset() == 4

    def test_get_oldest_offset(self):
        """get_oldest_offset should return lowest offset."""
        buffer = CircularQueueBuffer(capacity=100)
        for i in range(5):
            buffer.append(f"msg-{i}".encode())
        assert buffer.get_oldest_offset() == 0

    def test_empty_buffer_reads_empty(self):
        """Empty buffer should return empty reads."""
        buffer = CircularQueueBuffer(capacity=100)
        assert buffer.read_at(0) is None
        assert buffer.read_range(0, 10) == []
        assert buffer.get_latest_offset() == -1

    def test_circular_wrap_around(self):
        """When buffer fills, old records wrap and get overwritten."""
        buffer = CircularQueueBuffer(capacity=3)
        # Fill buffer
        for i in range(3):
            buffer.append(f"msg-{i}".encode())
        # Add one more — should overwrite oldest
        buffer.append(b"msg-3")
        # Now buffer holds [msg-1, msg-2, msg-3], offset_base=1
        assert buffer.read_at(1) == b"msg-1"
        assert buffer.read_at(3) == b"msg-3"
        assert buffer.get_oldest_offset() == 1
        assert buffer.get_latest_offset() == 3

    def test_thread_safety(self):
        """Concurrent appends should not corrupt buffer."""
        buffer = CircularQueueBuffer(capacity=10000)
        errors = []

        def producer(start: int):
            try:
                for i in range(start, start + 1000):
                    buffer.append(f"msg-{i}".encode())
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=producer, args=(i * 1000,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert buffer.get_latest_offset() == 4999
        assert buffer.get_oldest_offset() == 0


class TestConsumerGroupState:
    """Test consumer group state tracking."""

    def test_get_offset_default_zero(self):
        """Uncommitted offset should default to 0."""
        group = ConsumerGroupState(group_id="test")
        assert group.get_offset("orders") == 0

    def test_commit_offset_only_moves_forward(self):
        """commit_offset should not move backward."""
        group = ConsumerGroupState(group_id="test")
        group.commit_offset("orders", 10)
        assert group.get_offset("orders") == 10
        group.commit_offset("orders", 5)  # Should not move back
        assert group.get_offset("orders") == 10

    def test_add_member(self):
        """add_member should register a member."""
        group = ConsumerGroupState(group_id="test")
        member = ConsumerGroupMember(member_id="m1", group_id="test")
        group.add_member(member)
        assert "m1" in group.members

    def test_remove_member(self):
        """remove_member should unregister a member."""
        group = ConsumerGroupState(group_id="test")
        member = ConsumerGroupMember(member_id="m1", group_id="test")
        group.add_member(member)
        group.remove_member("m1")
        assert "m1" not in group.members

    def test_get_active_members(self):
        """get_active_members should return only active members."""
        group = ConsumerGroupState(group_id="test")
        m1 = ConsumerGroupMember(member_id="m1", group_id="test", active=True)
        m2 = ConsumerGroupMember(member_id="m2", group_id="test", active=False)
        group.add_member(m1)
        group.add_member(m2)
        active = group.get_active_members()
        assert len(active) == 1
        assert active[0].member_id == "m1"


class TestConsumerGroupManager:
    """Test consumer group manager with multiple groups."""

    def test_create_group(self):
        """create_group should create a new group."""
        manager = create_consumer_group_manager()
        group = manager.create_group("group-a")
        assert group.group_id == "group-a"

    def test_get_group_existing(self):
        """get_group should return existing group."""
        manager = create_consumer_group_manager()
        manager.create_group("group-a")
        assert manager.get_group("group-a") is not None

    def test_get_group_missing(self):
        """get_group should return None for missing group."""
        manager = create_consumer_group_manager()
        assert manager.get_group("missing") is None

    def test_delete_group(self):
        """delete_group should remove a group."""
        manager = create_consumer_group_manager()
        manager.create_group("group-a")
        assert manager.delete_group("group-a") is True
        assert manager.get_group("group-a") is None

    def test_list_groups(self):
        """list_groups should return all group IDs."""
        manager = create_consumer_group_manager()
        manager.create_group("group-a")
        manager.create_group("group-b")
        groups = manager.list_groups()
        assert "group-a" in groups
        assert "group-b" in groups

    def test_register_member(self):
        """register_member should add member to group."""
        manager = create_consumer_group_manager()
        member = manager.register_member("group-a", "worker-1", ["orders"])
        assert member.member_id == "worker-1"
        assert member.assigned_partitions == ["orders"]
        group = manager.get_group("group-a")
        assert "worker-1" in group.members

    def test_append_to_partition(self):
        """append_to_partition should add data to buffer."""
        manager = create_consumer_group_manager()
        offset = manager.append_to_partition("orders", b"msg-1")
        assert offset == 0

    def test_read_from_partition_advances_offset(self):
        """Reading should advance the group's offset."""
        manager = create_consumer_group_manager()
        manager.create_group("group-a")
        for i in range(5):
            manager.append_to_partition("orders", f"msg-{i}".encode())

        records = manager.read_from_partition("orders", "group-a", max_records=2)
        assert len(records) == 2
        # Offset should now be at 2
        group = manager.get_group("group-a")
        assert group.get_offset("orders") == 2

    def test_two_groups_independent_offsets(self):
        """Two groups reading same partition should be independent."""
        manager = create_consumer_group_manager()
        manager.create_group("fast")
        manager.create_group("slow")

        # Produce 10 messages
        for i in range(10):
            manager.append_to_partition("orders", f"msg-{i}".encode())

        # Fast group reads 10
        fast_records = manager.read_from_partition("orders", "fast", max_records=10)
        assert len(fast_records) == 10

        # Slow group reads 3
        slow_records = manager.read_from_partition("orders", "slow", max_records=3)
        assert len(slow_records) == 3

        # Verify independent offsets
        fast_group = manager.get_group("fast")
        slow_group = manager.get_group("slow")
        assert fast_group.get_offset("orders") == 10
        assert slow_group.get_offset("orders") == 3

    def test_group_lag_tracking(self):
        """get_group_lag should calculate correct lag."""
        manager = create_consumer_group_manager()
        manager.create_group("group-a")
        for i in range(20):
            manager.append_to_partition("orders", f"msg-{i}".encode())

        # Read first 5
        manager.read_from_partition("orders", "group-a", max_records=5)

        # High watermark = 20, committed = 5, lag = 15
        lag = manager.get_group_lag("group-a", "orders", high_watermark=20)
        assert lag == 15

    def test_all_group_lags(self):
        """get_all_group_lags should return lags for all groups."""
        manager = create_consumer_group_manager()
        manager.create_group("group-a")
        manager.create_group("group-b")
        for i in range(10):
            manager.append_to_partition("orders", f"msg-{i}".encode())

        manager.read_from_partition("orders", "group-a", max_records=10)
        # group-b hasn't read anything

        lags = manager.get_all_group_lags("orders", high_watermark=10)
        assert lags["group-a"] == 0
        assert lags["group-b"] == 10

    def test_heartbeat_updates_timestamp(self):
        """heartbeat should update member's last_heartbeat_ms."""
        manager = create_consumer_group_manager()
        manager.register_member("group-a", "worker-1", ["orders"])
        member = manager.get_group("group-a").members["worker-1"]
        initial = member.last_heartbeat_ms
        time.sleep(0.01)
        manager.heartbeat("group-a", "worker-1")
        assert member.last_heartbeat_ms > initial
