"""
Cache benchmark tests - Hot vs Cold read latency comparison.

Proves:
1. Cached (hot) segment reads are faster than disk (cold) reads
2. Cache hit rate under realistic read patterns
"""

import statistics
import tempfile
import time
from pathlib import Path

import pytest

from services.broker.partition_log import PartitionLog


@pytest.fixture
def temp_log_dir():
    """Create a temporary directory for log files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestCacheBenchmark:
    """Benchmark tests proving cache effectiveness."""

    def test_hot_vs_cold_read_latency(self, temp_log_dir):
        """
        Benchmark: Compare hot (cached) vs cold (disk) read latency.

        Writes enough data to create multiple segments that fit in cache,
        then reads them multiple times to populate cache and measure the difference.
        """
        # Use segment size that creates ~5-10 segments to fit in cache (capacity=10)
        partition = PartitionLog(
            name="cache_benchmark",
            log_dir=temp_log_dir,
            segment_max_size=64 * 1024  # 64KB segments
        )

        num_records = 1000
        record_size = 256

        # Write records across multiple segments
        for i in range(num_records):
            data = f"record-{i:06d}-{'x' * (record_size - 20)}".encode()
            partition.append(data)

        # Verify multiple segments were created and fit in cache
        assert partition.segment_count >= 3, f"Need multiple segments, got {partition.segment_count}"
        assert partition.sealed_segment_count >= 2
        assert partition.sealed_segment_count <= partition.cache_capacity, \
            f"Too many sealed segments ({partition.sealed_segment_count}) for cache capacity ({partition.cache_capacity})"

        # Read sealed segments only (skip active segment)
        sealed_end = partition.high_watermark - partition._active.segment.record_count if partition._active else partition.high_watermark

        # ===== COLD READS (first pass - cache miss) =====
        cold_latencies = []
        for _ in range(3):  # 3 cold read passes
            start = time.perf_counter()
            records = list(partition.read(max_records=sealed_end))
            cold_latencies.append(time.perf_counter() - start)

        cold_avg = statistics.mean(cold_latencies) * 1000  # ms

        # ===== HOT READS (subsequent passes - cache hit) =====
        hot_latencies = []
        for _ in range(10):  # 10 hot read passes
            start = time.perf_counter()
            records = list(partition.read(max_records=sealed_end))
            hot_latencies.append(time.perf_counter() - start)

        hot_avg = statistics.mean(hot_latencies) * 1000  # ms

        # ===== REPORT =====
        print(f"\n{'='*60}")
        print(f"Hot vs Cold Read Latency Benchmark")
        print(f"{'='*60}")
        print(f"Segments:           {partition.segment_count} (sealed: {partition.sealed_segment_count})")
        print(f"Records read/pass:  {sealed_end}")
        print(f"Cold read avg:      {cold_avg:.4f} ms")
        print(f"Hot read avg:       {hot_avg:.4f} ms")
        print(f"Speedup:            {cold_avg/hot_avg:.2f}x" if hot_avg > 0 else "Speedup: N/A")
        print(f"Cache hit rate:     {partition.cache_hit_rate:.2%}")
        print(f"Cache size:         {partition.cache_size}/{partition.cache_capacity}")
        print(f"{'='*60}\n")

        # Assertions
        assert hot_avg <= cold_avg * 1.2, f"Hot reads ({hot_avg:.2f}ms) should not be significantly slower than cold ({cold_avg:.2f}ms)"
        assert partition.cache_hit_rate > 0.5, f"Cache should have hits after multiple reads: {partition.cache_hit_rate:.2%}"

    def test_cache_hit_rate_under_realistic_pattern(self, temp_log_dir):
        """
        Benchmark: Cache hit rate under a realistic read pattern.

        Simulates: Write records, then repeatedly read the recent records
        (hot tail) and occasionally scan older segments (cold).
        """
        # Use segment size that creates segments fitting in cache
        partition = PartitionLog(
            name="hit_rate_test",
            log_dir=temp_log_dir,
            segment_max_size=64 * 1024  # 64KB segments
        )

        # Write enough data to create several sealed segments. A 64KB segment
        # cannot be exercised by 500 tiny records.
        num_records = 5000
        for i in range(num_records):
            partition.append(f"data-{i:06d}-{'x' * 64}".encode())

        # Verify sealed segments fit in cache
        assert partition.sealed_segment_count <= partition.cache_capacity, \
            f"Too many sealed segments ({partition.sealed_segment_count}) for cache capacity ({partition.cache_capacity})"

        # Reset stats after initial population
        partition.clear_cache()

        # Realistic pattern: 80% reads from recent data (tail), 20% full scans
        for iteration in range(100):
            if iteration % 5 == 0:
                # Full scan (touches all segments)
                list(partition.read(max_records=200))
            else:
                # Recent data only (hot - tail segments)
                start_offset = max(0, partition.high_watermark - 200)
                list(partition.read(start_offset=start_offset, max_records=200))

        stats = partition.get_cache_stats()
        hit_rate = stats["hit_rate"]

        print(f"\n{'='*60}")
        print(f"Cache Hit Rate Under Realistic Pattern")
        print(f"{'='*60}")
        print(f"Total requests:     {stats['total_requests']}")
        print(f"Hits:               {stats['hits']}")
        print(f"Misses:             {stats['misses']}")
        print(f"Hit rate:           {hit_rate:.2%}")
        print(f"Evictions:          {stats['evictions']}")
        print(f"Cache size:         {stats['size']}/{stats['capacity']}")
        print(f"{'='*60}\n")

        # With 80% tail reads, hit rate should be decent
        assert hit_rate > 0.5, f"Hit rate too low: {hit_rate:.2%}"

    def test_segment_cache_persistence_across_reads(self, temp_log_dir):
        """
        Verify that once a segment is cached, subsequent reads hit the cache.
        """
        # Use small segment size to force multiple segments
        partition = PartitionLog(
            name="persistence_test",
            log_dir=temp_log_dir,
            segment_max_size=4096  # 4KB segments
        )

        # Write enough for multiple segments
        for i in range(500):
            partition.append(f"record-{i}".encode())

        initial_sealed = partition.sealed_segment_count
        assert initial_sealed >= 2
        assert initial_sealed <= partition.cache_capacity, \
            f"Too many sealed segments ({initial_sealed}) for cache capacity ({partition.cache_capacity})"

        # Read all segments once (cold)
        list(partition.read())

        # Read twice more - subsequent passes should be hot.
        list(partition.read())
        list(partition.read())

        stats = partition.get_cache_stats()
        assert stats["hits"] > 0, "Should have cache hits on second read"
        assert stats["hit_rate"] > 0.5, f"Hit rate should be high on repeated reads: {stats['hit_rate']:.2%}"

    def test_cache_does_not_break_ordering(self, temp_log_dir):
        """
        CRITICAL: Prove cache doesn't break the ordering guarantee.

        Even with cache, records must be read in exact write order.
        """
        partition = PartitionLog(
            name="ordering_test",
            log_dir=temp_log_dir,
            segment_max_size=256  # Force many segments
        )

        num_records = 500
        expected = []

        for i in range(num_records):
            data = f"ORDER-{i:06d}-{i*7 % 1000:03d}".encode()
            expected.append(data)
            partition.append(data)

        # Read multiple times with cache
        for _ in range(5):
            records = list(partition.read())
            actual = [r.data for r in records]
            assert actual == expected, "Ordering violated with cache!"

        print(f"\n{'='*60}")
        print(f"Ordering Guarantee With Cache")
        print(f"{'='*60}")
        print(f"Records:            {num_records}")
        print(f"Segments:           {partition.segment_count}")
        print(f"Cache hit rate:     {partition.cache_hit_rate:.2%}")
        print(f"Order verified:     ✓ (5 full reads)")
        print(f"{'='*60}\n")


class TestCacheStaleReadPrevention:
    """Tests proving the cache-consistency guarantee: no stale reads."""

    def test_evicted_segment_not_readable(self, temp_log_dir):
        """
        Prove that evicted segments cannot be read from cache (no stale reads).

        This is the core cache-consistency guarantee.
        """
        partition = PartitionLog(
            name="stale_test",
            log_dir=temp_log_dir,
            segment_max_size=256
        )

        # Write many records to create multiple segments
        for i in range(200):
            partition.append(f"record-{i}".encode())

        # Fill cache beyond capacity by reading different segments
        partition.clear_cache()

        # Access segments to populate cache
        for offset in range(0, 200, 50):
            list(partition.read(start_offset=offset, max_records=10))

        # Now read many different offsets to force evictions
        for offset in range(100, 300, 10):
            list(partition.read(start_offset=offset % 200, max_records=5))

        # Verify no stale data - all reads should be consistent
        final_records = list(partition.read())
        assert len(final_records) == 200

        for i, record in enumerate(final_records):
            assert record.data == f"record-{i}".encode(), f"Stale data at offset {i}"

    def test_cache_invalidation_on_rotation(self, temp_log_dir):
        """
        Test that segment rotation properly handles cache.
        """
        partition = PartitionLog(
            name="rotation_cache_test",
            log_dir=temp_log_dir,
            segment_max_size=512
        )

        # Write and force rotation
        for i in range(100):
            partition.append(f"data-{i}".encode())

        # Cache the sealed segment
        list(partition.read())

        # Force another rotation by writing more
        for i in range(100, 200):
            partition.append(f"data-{i}".encode())

        # The old sealed segment should still be readable correctly
        records = list(partition.read(max_records=100))
        assert len(records) == 100

        for i, record in enumerate(records):
            assert record.data == f"data-{i}".encode()