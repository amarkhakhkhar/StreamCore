"""Tests for LRU Cache implementation."""

import threading
import time
from services.broker.lru_cache import LRUCache, SegmentCache, CacheStats


class TestLRUCache:
    """Tests for the generic LRUCache class."""

    def test_basic_put_get(self):
        """Test basic put and get operations."""
        cache = LRUCache[str, int](capacity=3)

        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)

        assert cache.get("a") == 1
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_get_nonexistent_returns_none(self):
        """Test that getting non-existent key returns None."""
        cache = LRUCache[str, int](capacity=3)
        assert cache.get("nonexistent") is None

    def test_eviction_on_capacity_exceeded(self):
        """Test that LRU item is evicted when capacity exceeded."""
        cache = LRUCache[str, int](capacity=2)

        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)  # Should evict "a"

        assert cache.get("a") is None  # Evicted
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_lru_order_on_access(self):
        """Test that accessed items move to front (most recently used)."""
        cache = LRUCache[str, int](capacity=3)

        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)

        # Access "a" - should move to front
        cache.get("a")

        # Add "d" - should evict "b" (least recently used)
        cache.put("d", 4)

        assert cache.get("a") == 1  # Still there (was accessed)
        assert cache.get("b") is None  # Evicted
        assert cache.get("c") == 3
        assert cache.get("d") == 4

    def test_update_existing_key(self):
        """Test that updating existing key moves it to front."""
        cache = LRUCache[str, int](capacity=2)

        cache.put("a", 1)
        cache.put("b", 2)

        # Update "a"
        cache.put("a", 10)

        # Add "c" - should evict "b" (least recently used)
        cache.put("c", 3)

        assert cache.get("a") == 10
        assert cache.get("b") is None  # Evicted
        assert cache.get("c") == 3

    def test_delete(self):
        """Test deleting a key from cache."""
        cache = LRUCache[str, int](capacity=3)

        cache.put("a", 1)
        cache.put("b", 2)

        assert cache.delete("a") is True
        assert cache.get("a") is None
        assert cache.get("b") == 2
        assert cache.size == 1

        # Delete non-existent
        assert cache.delete("nonexistent") is False

    def test_clear(self):
        """Test clearing the cache."""
        cache = LRUCache[str, int](capacity=3)

        cache.put("a", 1)
        cache.put("b", 2)

        cache.clear()

        assert cache.size == 0
        assert cache.get("a") is None
        assert cache.get("b") is None

    def test_contains(self):
        """Test __contains__ operator."""
        cache = LRUCache[str, int](capacity=3)

        cache.put("a", 1)

        assert "a" in cache
        assert "b" not in cache

    def test_len(self):
        """Test __len__ operator."""
        cache = LRUCache[str, int](capacity=3)

        assert len(cache) == 0
        cache.put("a", 1)
        assert len(cache) == 1
        cache.put("b", 2)
        assert len(cache) == 2

    def test_keys_order(self):
        """Test keys() returns keys in MRU to LRU order."""
        cache = LRUCache[str, int](capacity=3)

        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)

        # Access "a" - moves to front
        cache.get("a")

        keys = cache.keys()
        assert keys[0] == "a"  # Most recently used
        assert keys[1] == "c"
        assert keys[2] == "b"  # Least recently used

    def test_capacity_zero_raises(self):
        """Test that capacity must be positive."""
        try:
            LRUCache[str, int](capacity=0)
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_thread_safety(self):
        """Test thread-safe operations under concurrent access."""
        cache = LRUCache[int, int](capacity=100)
        num_threads = 5
        ops_per_thread = 200
        errors = []

        def worker(thread_id):
            try:
                for i in range(ops_per_thread):
                    key = thread_id * ops_per_thread + i
                    cache.put(key, key * 2)
                    val = cache.get(key)
                    if val != key * 2:
                        errors.append(f"Mismatch: {key} -> {val}")
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Thread safety errors: {errors}"
        assert cache.size <= 100


class TestCacheStats:
    """Tests for CacheStats tracking."""

    def test_hit_miss_tracking(self):
        """Test hit/miss recording."""
        stats = CacheStats()

        stats.record_hit()
        stats.record_hit()
        stats.record_miss()

        assert stats.hits == 2
        assert stats.misses == 1
        assert stats.total_requests == 3
        assert abs(stats.hit_rate - 2/3) < 0.001
        assert abs(stats.miss_rate - 1/3) < 0.001

    def test_eviction_tracking(self):
        """Test eviction recording."""
        stats = CacheStats()

        stats.record_eviction()
        stats.record_eviction()

        assert stats.evictions == 2

    def test_snapshot(self):
        """Test stats snapshot."""
        stats = CacheStats()
        stats.record_hit()
        stats.record_miss()

        snapshot = stats.snapshot()
        assert snapshot["hits"] == 1
        assert snapshot["misses"] == 1
        assert snapshot["total_requests"] == 2

    def test_reset(self):
        """Test resetting stats."""
        stats = CacheStats()
        stats.record_hit()
        stats.record_miss()
        stats.record_eviction()

        stats.reset()

        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.evictions == 0

    def test_thread_safety(self):
        """Test thread-safe stats recording."""
        stats = CacheStats()
        num_threads = 5
        ops_per_thread = 200

        def worker():
            for _ in range(ops_per_thread):
                stats.record_hit()
                stats.record_miss()

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert stats.hits == num_threads * ops_per_thread
        assert stats.misses == num_threads * ops_per_thread


class TestSegmentCache:
    """Tests for SegmentCache wrapper."""

    def test_segment_cache_basic(self):
        """Test basic segment cache operations."""
        cache = SegmentCache(capacity=3)

        # Create mock segment objects
        class MockSegment:
            def __init__(self, base_offset):
                self.base_offset = base_offset

        seg1 = MockSegment(0)
        seg2 = MockSegment(100)
        seg3 = MockSegment(200)

        cache.put_segment(0, seg1)
        cache.put_segment(100, seg2)
        cache.put_segment(200, seg3)

        assert cache.get_segment(0) is seg1
        assert cache.get_segment(100) is seg2
        assert cache.get_segment(200) is seg3

    def test_segment_cache_hit_rate(self):
        """Test hit rate tracking."""
        cache = SegmentCache(capacity=3)

        class MockSegment:
            def __init__(self, base_offset):
                self.base_offset = base_offset

        seg = MockSegment(0)
        cache.put_segment(0, seg)

        # First access - miss (not in cache yet)
        # Actually, put then get = hit
        cache.get_segment(0)
        assert cache.hit_rate > 0

        # Non-existent - miss
        cache.get_segment(999)
        assert cache.hit_rate < 1.0

    def test_segment_cache_eviction(self):
        """Test eviction tracking."""
        cache = SegmentCache(capacity=2)

        class MockSegment:
            def __init__(self, base_offset):
                self.base_offset = base_offset

        cache.put_segment(0, MockSegment(0))
        cache.put_segment(100, MockSegment(100))
        cache.put_segment(200, MockSegment(200))  # Should evict

        stats = cache.get_stats_snapshot()
        assert stats["evictions"] >= 1

    def test_segment_cache_invalidate(self):
        """Test invalidating a specific segment."""
        cache = SegmentCache(capacity=3)

        class MockSegment:
            def __init__(self, base_offset):
                self.base_offset = base_offset

        seg = MockSegment(0)
        cache.put_segment(0, seg)
        assert cache.get_segment(0) is seg

        cache.invalidate(0)
        assert cache.get_segment(0) is None

    def test_segment_cache_clear(self):
        """Test clearing segment cache."""
        cache = SegmentCache(capacity=3)

        class MockSegment:
            def __init__(self, base_offset):
                self.base_offset = base_offset

        cache.put_segment(0, MockSegment(0))
        cache.put_segment(100, MockSegment(100))

        cache.clear()

        assert cache.size == 0
        stats = cache.get_stats_snapshot()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert cache.get_segment(0) is None


class TestLRUCacheCorrectness:
    """Tests proving LRU cache correctness guarantees."""

    def test_no_stale_reads_after_eviction(self):
        """
        CRITICAL TEST: Prove that evicted items cannot be read (no stale reads).

        This is the cache-consistency guarantee that must never be broken.
        """
        cache = LRUCache[int, str](capacity=2)

        cache.put(1, "value-1")
        cache.put(2, "value-2")
        cache.put(3, "value-3")  # Evicts 1

        # Key 1 should be completely gone - no stale read possible
        assert cache.get(1) is None
        assert 1 not in cache

        # Keys 2 and 3 should still be accessible
        assert cache.get(2) == "value-2"
        assert cache.get(3) == "value-3"

    def test_access_order_maintained(self):
        """
        Prove that access order is strictly maintained.

        LRU = least recently used = the one at the tail (before dummy tail).
        """
        cache = LRUCache[int, str](capacity=3)

        cache.put(1, "a")
        cache.put(2, "b")
        cache.put(3, "c")

        # Access 1 -> order: 1 (MRU), 3, 2 (LRU)
        cache.get(1)

        # Add 4 -> should evict 2 (LRU)
        cache.put(4, "d")

        assert cache.keys() == [4, 1, 3]
        assert cache.get(2) is None  # Evicted

        # Access 3 -> order: 3 (MRU), 4, 1 (LRU)
        cache.get(3)
        assert cache.keys() == [3, 4, 1]

        # Add 5 -> should evict 1 (LRU)
        cache.put(5, "e")

        assert cache.keys() == [5, 3, 4]
        assert cache.get(1) is None  # Evicted

    def test_update_does_not_break_lru_order(self):
        """Test that updating a key preserves LRU semantics."""
        cache = LRUCache[int, str](capacity=2)

        cache.put(1, "a")
        cache.put(2, "b")

        # Update 1 - should move to MRU
        cache.put(1, "a-updated")

        # Add 3 -> should evict 2 (LRU)
        cache.put(3, "c")

        assert cache.get(1) == "a-updated"
        assert cache.get(2) is None
        assert cache.get(3) == "c"

    def test_concurrent_access_maintains_correctness(self):
        """
        Prove that concurrent access doesn't break LRU invariants.

        No data corruption, no duplicate keys, size never exceeds capacity.
        """
        cache = LRUCache[int, int](capacity=50)
        num_threads = 5
        ops_per_thread = 100
        errors = []

        def worker(tid):
            try:
                for i in range(ops_per_thread):
                    key = tid * ops_per_thread + i
                    cache.put(key, key * 10)

                    # Randomly read some keys
                    if i % 7 == 0:
                        read_key = tid * ops_per_thread + (i % 10)
                        cache.get(read_key)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Concurrent errors: {errors}"
        assert cache.size <= 50, f"Size {cache.size} exceeds capacity 50"

        # Verify all stored values are correct
        for key in cache.keys():
            val = cache.get(key)
            assert val == key * 10, f"Corrupted value for key {key}: {val}"


class TestSegmentCacheIntegration:
    """Integration tests for SegmentCache with PartitionLog."""

    def test_cache_stats_exposed(self):
        """Test that cache statistics are properly exposed."""
        cache = SegmentCache(capacity=5)

        class MockSegment:
            def __init__(self, base_offset):
                self.base_offset = base_offset

        # Put some segments
        for i in range(3):
            cache.put_segment(i * 100, MockSegment(i * 100))

        stats = cache.get_stats_snapshot()

        assert stats["size"] == 3
        assert stats["capacity"] == 5
        assert "hits" in stats
        assert "misses" in stats
        assert "evictions" in stats
        assert "hit_rate" in stats
        assert "miss_rate" in stats

    def test_cache_hit_rate_calculation(self):
        """Test hit rate calculation accuracy."""
        cache = SegmentCache(capacity=5)

        class MockSegment:
            def __init__(self, base_offset):
                self.base_offset = base_offset

        cache.put_segment(0, MockSegment(0))
        cache.put_segment(100, MockSegment(100))

        # 2 hits
        cache.get_segment(0)
        cache.get_segment(100)

        # 3 misses
        cache.get_segment(200)
        cache.get_segment(300)
        cache.get_segment(400)

        stats = cache.get_stats_snapshot()
        assert stats["hits"] == 2
        assert stats["misses"] == 3
        assert abs(stats["hit_rate"] - 0.4) < 0.001  # 2/5 = 0.4
        assert abs(stats["miss_rate"] - 0.6) < 0.001  # 3/5 = 0.6