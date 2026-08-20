"""
LRU Page Cache for segment reads.

Classic HashMap + doubly linked list implementation for O(1) get/put operations.
Used to cache recently-read segments in memory to avoid disk reads.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Optional, Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


@dataclass
class Node(Generic[K, V]):
    """A node in the doubly linked list."""
    key: K
    value: V
    prev: Optional[Node[K, V]] = None
    next: Optional[Node[K, V]] = None


class LRUCache(Generic[K, V]):
    """
    LRU Cache implemented with HashMap + Doubly Linked List.

    Operations:
    - get(key): O(1) - returns value and moves to front (most recently used)
    - put(key, value): O(1) - adds/updates, evicts LRU if at capacity
    - evict_lru(): O(1) - removes and returns the least recently used item

    Thread-safe with RLock.
    """

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self._capacity = capacity
        self._map: dict[K, Node[K, V]] = {}
        self._lock = threading.RLock()

        # Dummy head and tail sentinels
        self._head = Node(key=None, value=None)  # type: ignore
        self._tail = Node(key=None, value=None)  # type: ignore
        self._head.next = self._tail
        self._tail.prev = self._head

    def _remove(self, node: Node[K, V]) -> None:
        """Remove a node from the linked list."""
        prev_node = node.prev
        next_node = node.next
        prev_node.next = next_node
        next_node.prev = prev_node

    def _add_to_front(self, node: Node[K, V]) -> None:
        """Add a node right after head (most recently used position)."""
        node.next = self._head.next
        node.prev = self._head
        self._head.next.prev = node
        self._head.next = node

    def _move_to_front(self, node: Node[K, V]) -> None:
        """Move an existing node to the front (most recently used)."""
        self._remove(node)
        self._add_to_front(node)

    def _evict_lru(self) -> tuple[K, V] | None:
        """Remove and return the least recently used item (before tail)."""
        if self._tail.prev == self._head:
            return None
        lru = self._tail.prev
        self._remove(lru)
        del self._map[lru.key]
        return (lru.key, lru.value)

    def get(self, key: K) -> V | None:
        """
        Get value by key, marking it as recently used.
        Returns None if key not found.
        """
        with self._lock:
            node = self._map.get(key)
            if node is None:
                return None
            self._move_to_front(node)
            return node.value

    def put(self, key: K, value: V) -> tuple[K, V] | None:
        """Put a value and return the evicted item, if any."""
        with self._lock:
            node = self._map.get(key)
            if node is not None:
                node.value = value
                self._move_to_front(node)
                return None

            evicted = self._evict_lru() if len(self._map) >= self._capacity else None
            new_node = Node(key=key, value=value)
            self._map[key] = new_node
            self._add_to_front(new_node)
            return evicted

    def pop(self, key: K) -> tuple[K, V] | None:
        """Remove and return an item without affecting cache statistics."""
        with self._lock:
            node = self._map.get(key)
            if node is None:
                return None
            self._remove(node)
            del self._map[key]
            return key, node.value

    def delete(self, key: K) -> bool:
        """Delete a key from the cache. Returns True if key was found."""
        with self._lock:
            node = self._map.get(key)
            if node is None:
                return False
            self._remove(node)
            del self._map[key]
            return True

    def clear(self) -> list[V]:
        """Clear all entries and return their values."""
        with self._lock:
            values = [node.value for node in self._map.values()]
            self._map.clear()
            self._head.next = self._tail
            self._tail.prev = self._head
            return values

    @property
    def size(self) -> int:
        """Current number of entries in cache."""
        with self._lock:
            return len(self._map)

    @property
    def capacity(self) -> int:
        """Maximum capacity of the cache."""
        return self._capacity

    @property
    def hit_rate(self) -> float:
        """Calculate hit rate (requires external tracking)."""
        # This is tracked externally via the SegmentCache
        return getattr(self, '_hit_rate', 0.0)

    def __contains__(self, key: K) -> bool:
        with self._lock:
            return key in self._map

    def __len__(self) -> int:
        return self.size

    def keys(self) -> list[K]:
        """Return keys in order from most to least recently used."""
        with self._lock:
            keys = []
            current = self._head.next
            while current != self._tail:
                keys.append(current.key)
                current = current.next
            return keys


@dataclass
class CacheStats:
    """Statistics for cache performance tracking."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_hit(self) -> None:
        with self._lock:
            self.hits += 1

    def record_miss(self) -> None:
        with self._lock:
            self.misses += 1

    def record_eviction(self) -> None:
        with self._lock:
            self.evictions += 1

    @property
    def total_requests(self) -> int:
        with self._lock:
            return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        with self._lock:
            total = self.hits + self.misses
            return self.hits / total if total > 0 else 0.0

    @property
    def miss_rate(self) -> float:
        with self._lock:
            total = self.hits + self.misses
            return self.misses / total if total > 0 else 0.0

    def snapshot(self) -> dict:
        """Return a consistent snapshot without recursive lock acquisition."""
        with self._lock:
            hits = self.hits
            misses = self.misses
            total = hits + misses
            return {
                "hits": hits,
                "misses": misses,
                "evictions": self.evictions,
                "total_requests": total,
                "hit_rate": hits / total if total else 0.0,
                "miss_rate": misses / total if total else 0.0,
            }

    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self.hits = 0
            self.misses = 0
            self.evictions = 0


class SegmentCache:
    """
    LRU cache specifically for Segment objects.

    Wraps LRUCache with Segment-specific logic and statistics tracking.
    Cache key: segment base_offset (int)
    Cache value: Segment object
    """

    def __init__(self, capacity: int = 10, max_cacheable_bytes: int = 8 * 1024 * 1024):
        if max_cacheable_bytes <= 0:
            raise ValueError("max_cacheable_bytes must be positive")
        self._cache = LRUCache[int, Any](capacity)
        self._stats = CacheStats()
        self._max_cacheable_bytes = max_cacheable_bytes

    def get_segment(self, base_offset: int) -> Any | None:
        """Get segment from cache if present."""
        segment = self._cache.get(base_offset)
        if segment is not None:
            self._stats.record_hit()
            return segment
        self._stats.record_miss()
        return None

    def put_segment(self, base_offset: int, segment: Any) -> None:
        """Put segment in cache, evicting LRU if needed."""
        evicted = self._cache.put(base_offset, segment)
        if evicted is not None:
            self._stats.record_eviction()
            _, evicted_segment = evicted
            clear_cache = getattr(evicted_segment, "clear_cached_records", None)
            if clear_cache is not None:
                clear_cache()

    def invalidate(self, base_offset: int) -> bool:
        """Invalidate a specific segment and release its cached payload."""
        removed = self._cache.pop(base_offset)
        if removed is None:
            return False
        _, segment = removed
        clear_cache = getattr(segment, "clear_cached_records", None)
        if clear_cache is not None:
            clear_cache()
        return True

    def clear(self) -> None:
        """Clear the entire cache and release cached segment payloads."""
        values = self._cache.clear()
        for segment in values:
            clear_cache = getattr(segment, "clear_cached_records", None)
            if clear_cache is not None:
                clear_cache()
        self._stats.reset()

    @property
    def stats(self) -> CacheStats:
        return self._stats

    @property
    def size(self) -> int:
        return self._cache.size

    @property
    def capacity(self) -> int:
        return self._cache.capacity

    @property
    def hit_rate(self) -> float:
        return self._stats.hit_rate

    @property
    def max_cacheable_bytes(self) -> int:
        return self._max_cacheable_bytes

    def get_stats_snapshot(self) -> dict:
        """Get cache statistics snapshot."""
        return {
            "size": self.size,
            "capacity": self.capacity,
            **self._stats.snapshot()
        }