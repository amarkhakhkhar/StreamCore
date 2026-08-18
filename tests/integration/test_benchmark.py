"""
Benchmark tests for StreamCore partition log.

Proves:
1. Sustained write throughput
2. Records are always read back in exact write order, even across segment boundaries
"""

import statistics
import tempfile
import time
from pathlib import Path

import pytest

from services.broker.partition_log import PartitionLog


class TestPartitionLogBenchmark:
    """Benchmark tests proving throughput and ordering guarantees."""

    @pytest.fixture
    def temp_log_dir(self):
        """Create a temporary directory for log files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_append_throughput_sustained(self, temp_log_dir):
        """
        Benchmark append throughput at sustained write load.

        Measures records/second for continuous writes.
        """
        partition = PartitionLog(
            name="benchmark",
            log_dir=temp_log_dir,
            segment_max_size=10 * 1024 * 1024  # 10MB segments
        )

        # Warm-up
        for _ in range(1000):
            partition.append(b"x" * 100)

        # Benchmark
        num_records = 50000
        record_size = 100  # bytes
        latencies = []

        start_time = time.perf_counter()

        for i in range(num_records):
            record_start = time.perf_counter()
            partition.append(b"x" * record_size)
            latencies.append(time.perf_counter() - record_start)

        total_time = time.perf_counter() - start_time

        throughput = num_records / total_time
        throughput_mb = (num_records * record_size) / (1024 * 1024) / total_time

        # Latency statistics
        p50 = statistics.median(latencies) * 1000  # ms
        p99 = statistics.quantiles(latencies, n=100)[98] * 1000  # ms

        print(f"\n{'='*50}")
        print(f"Append Throughput Benchmark Results")
        print(f"{'='*50}")
        print(f"Records written:    {num_records:,}")
        print(f"Record size:        {record_size} bytes")
        print(f"Total time:         {total_time:.3f} seconds")
        print(f"Throughput:         {throughput:,.0f} records/sec")
        print(f"Throughput:         {throughput_mb:.2f} MB/sec")
        print(f"Latency p50:        {p50:.4f} ms")
        print(f"Latency p99:        {p99:.4f} ms")
        print(f"Segments created:   {partition.segment_count}")
        print(f"{'='*50}\n")

        # Assertions - minimum acceptable performance
        assert throughput > 10000, f"Throughput too low: {throughput} records/sec"
        assert p99 < 10, f"P99 latency too high: {p99} ms"

    def test_append_throughput_varying_sizes(self, temp_log_dir):
        """Benchmark throughput with varying record sizes."""
        partition = PartitionLog(name="benchmark", log_dir=temp_log_dir)

        sizes = [64, 256, 1024, 4096, 16384]  # 64B to 16KB
        results = {}

        print(f"\n{'='*60}")
        print(f"Throughput by Record Size")
        print(f"{'='*60}")

        for size in sizes:
            num_records = 10000

            start = time.perf_counter()
            for _ in range(num_records):
                partition.append(b"x" * size)
            elapsed = time.perf_counter() - start

            throughput = num_records / elapsed
            throughput_mb = (num_records * size) / (1024 * 1024) / elapsed
            results[size] = throughput

            print(f"Size {size:>6} bytes: {throughput:>8,.0f} rec/sec | {throughput_mb:>6.2f} MB/sec")

        print(f"{'='*60}\n")

        # Larger records should still maintain reasonable throughput
        assert results[16384] > 1000

    def test_read_ordering_guarantee_across_segments(self, temp_log_dir):
        """
        CRITICAL TEST: Prove records are always read in exact write order,
        even across segment boundaries.

        This is the fundamental guarantee of an append-only log.
        """
        # Use small segments to force many boundary crossings
        partition = PartitionLog(
            name="ordering_test",
            log_dir=temp_log_dir,
            segment_max_size=1024  # Very small segments
        )

        num_records = 1000
        record_data = []

        # Write records with unique identifiers
        print(f"\n{'='*60}")
        print(f"Ordering Guarantee Test")
        print(f"{'='*60}")
        print(f"Writing {num_records} records across multiple segments...")

        for i in range(num_records):
            # Use a unique, ordered identifier
            data = f"RECORD-{i:06d}-CHECKSUM-{hash(i) % 10000:04d}".encode()
            record_data.append(data)
            offset = partition.append(data)
            assert offset == i, f"Offset mismatch: expected {i}, got {offset}"

        print(f"Segments created: {partition.segment_count}")
        print(f"Sealed segments:  {partition.sealed_segment_count}")

        # Read all records back
        print(f"Reading back all records...")
        records = list(partition.read())

        print(f"Records read:     {len(records)}")

        # Verify exact ordering
        ordering_errors = []
        for i, record in enumerate(records):
            expected_data = record_data[i]

            if record.offset != i:
                ordering_errors.append(f"Offset mismatch at {i}: expected {i}, got {record.offset}")

            if record.data != expected_data:
                ordering_errors.append(f"Data mismatch at offset {i}")

        if ordering_errors:
            print(f"\nORDERING VIOLATIONS DETECTED:")
            for err in ordering_errors[:10]:
                print(f"  - {err}")
            if len(ordering_errors) > 10:
                print(f"  ... and {len(ordering_errors) - 10} more")

        print(f"{'='*60}\n")

        # Critical assertion - NO ordering violations allowed
        assert len(ordering_errors) == 0, f"Found {len(ordering_errors)} ordering violations!"
        assert len(records) == num_records

    def test_read_throughput(self, temp_log_dir):
        """Benchmark read throughput."""
        partition = PartitionLog(name="benchmark", log_dir=temp_log_dir)

        # Write test data
        num_records = 50000
        record_size = 100

        print(f"\n{'='*50}")
        print(f"Write phase: {num_records:,} records...")
        for _ in range(num_records):
            partition.append(b"x" * record_size)
        print(f"Segments: {partition.segment_count}")

        # Benchmark sequential read
        print(f"Read phase: sequential scan...")
        start = time.perf_counter()
        records = list(partition.read())
        read_time = time.perf_counter() - start

        read_throughput = len(records) / read_time
        read_mb = (len(records) * record_size) / (1024 * 1024) / read_time

        print(f"\nRead Throughput Benchmark Results")
        print(f"{'='*50}")
        print(f"Records read:       {len(records):,}")
        print(f"Read time:          {read_time:.3f} seconds")
        print(f"Read throughput:    {read_throughput:,.0f} records/sec")
        print(f"Read throughput:    {read_mb:.2f} MB/sec")
        print(f"{'='*50}\n")

        assert len(records) == num_records
        assert read_throughput > 50000  # Should read at least 50k rec/sec

    def test_durability_after_restart_simulation(self, temp_log_dir):
        """
        Test that records survive a simulated restart.

        Writes records, 'restarts' (closes and reopens), then verifies ordering.
        """
        partition1 = PartitionLog(
            name="durability_test",
            log_dir=temp_log_dir,
            segment_max_size=2048
        )

        num_records = 500
        original_order = []

        # Write records
        for i in range(num_records):
            data = f"durable-record-{i:04d}".encode()
            original_order.append(data)
            partition1.append(data)

        segment_count_before = partition1.segment_count
        partition1.close()

        # Simulate restart - reopen partition
        partition2 = PartitionLog(name="durability_test", log_dir=temp_log_dir)

        print(f"\n{'='*50}")
        print(f"Durability Test After Simulated Restart")
        print(f"{'='*50}")
        print(f"Segments before:    {segment_count_before}")
        print(f"Segments after:     {partition2.segment_count}")
        print(f"Records before:     {num_records}")
        print(f"Records after:      {partition2.record_count}")

        # Read all records
        records = list(partition2.read())

        # Verify count
        assert len(records) == num_records, f"Record count mismatch: {len(records)} vs {num_records}"

        # Verify exact order
        for i, record in enumerate(records):
            assert record.data == original_order[i], f"Order or data mismatch at index {i}"

        print(f"Ordering:           VERIFIED (all {num_records} records in exact write order)")
        print(f"{'='*50}\n")
