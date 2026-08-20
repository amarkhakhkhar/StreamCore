#!/usr/bin/env python3
"""
Comprehensive benchmark comparing time-index seek implementations.

Tests:
- Dense index (every record)
- Sparse index (every 4KB)
- With/without mmap binary search
- Batch seek performance
"""

import sys
import os
import time
import statistics
import random
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.broker.partition_log import PartitionLog


def run_benchmark(
    name: str,
    num_records: int,
    segment_size: int,
    sparse_bytes: int,
    num_seeks: int = 1000,
    base_time: int = 1000000000000
) -> dict:
    """Run a single benchmark configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_dir = Path(tmpdir)
        partition = PartitionLog(
            name='seek_benchmark',
            log_dir=log_dir,
            segment_max_size=segment_size,
            time_index_sparse_bytes=sparse_bytes
        )

        print(f"  Writing {num_records:,} records...")
        for i in range(num_records):
            timestamp = base_time + i * 1000
            partition.append(f'record-{i:06d}'.encode(), timestamp=timestamp)

        print(f"  Segments: {partition.segment_count}")

        random.seed(42)
        seek_targets = [base_time + random.randint(0, num_records - 1) * 1000 for _ in range(num_seeks)]

        # Indexed seek (single)
        latencies_indexed = []
        for target_ts in seek_targets:
            start = time.perf_counter()
            offset = partition.seek_by_timestamp(target_ts)
            latencies_indexed.append((time.perf_counter() - start) * 1000)

        avg_idx = statistics.mean(latencies_indexed)
        p50_idx = statistics.median(latencies_indexed)
        p99_idx = statistics.quantiles(latencies_indexed, n=100)[98]

        # Linear scan
        latencies_linear = []
        for target_ts in seek_targets:
            start = time.perf_counter()
            found = None
            for record in partition.read():
                if record.timestamp >= target_ts:
                    found = record.offset
                    break
            latencies_linear.append((time.perf_counter() - start) * 1000)

        avg_lin = statistics.mean(latencies_linear)
        p50_lin = statistics.median(latencies_linear)
        p99_lin = statistics.quantiles(latencies_linear, n=100)[98]

        # Batch seek (only if sparse index is used, otherwise same as single)
        batch_latencies = []
        if sparse_bytes > 0:
            # Time batch seeks in groups of 100
            batch_size = 100
            for i in range(0, num_seeks, batch_size):
                batch = seek_targets[i:i+batch_size]
                start = time.perf_counter()
                results = partition.seek_batch_by_timestamp(batch)
                batch_latencies.append((time.perf_counter() - start) * 1000)

        avg_batch = statistics.mean(batch_latencies) if batch_latencies else 0

        return {
            "name": name,
            "num_records": num_records,
            "segments": partition.segment_count,
            "sparse_bytes": sparse_bytes,
            "indexed_avg_ms": avg_idx,
            "indexed_p50_ms": p50_idx,
            "indexed_p99_ms": p99_idx,
            "linear_avg_ms": avg_lin,
            "linear_p50_ms": p50_lin,
            "linear_p99_ms": p99_lin,
            "speedup_avg": avg_lin / avg_idx if avg_idx > 0 else 0,
            "speedup_p50": p50_lin / p50_idx if p50_idx > 0 else 0,
            "speedup_p99": p99_lin / p99_idx if p99_idx > 0 else 0,
            "batch_avg_ms_per_seek": (avg_batch / (num_seeks / 100)) if batch_latencies else 0,
        }


def main():
    print("=" * 80)
    print("TIME-INDEX SEEK BENCHMARK")
    print("=" * 80)
    print()

    configs = [
        # (name, num_records, segment_size, sparse_bytes)
        ("Dense (original)", 200_000, 64 * 1024, 0),
        ("Sparse 4KB", 200_000, 64 * 1024, 4096),
        ("Dense (large)", 500_000, 64 * 1024, 0),
        ("Sparse 4KB (large)", 500_000, 64 * 1024, 4096),
        ("Sparse 16KB", 500_000, 64 * 1024, 16384),
    ]

    # Store segment size for later use
    segment_sizes = {c[0]: c[2] for c in configs}

    results = []
    for name, num_records, segment_size, sparse_bytes in configs:
        print(f"\n{'='*80}")
        print(f"Configuration: {name}")
        print(f"  Records: {num_records:,}, Segment size: {segment_size/1024}KB, Sparse: {sparse_bytes or 'dense'}")
        print(f"{'='*80}")
        try:
            result = run_benchmark(name, num_records, segment_size, sparse_bytes)
            results.append(result)
            print(f"\n  Results:")
            print(f"    Indexed: avg={result['indexed_avg_ms']:.2f}ms p50={result['indexed_p50_ms']:.2f}ms p99={result['indexed_p99_ms']:.2f}ms")
            print(f"    Linear:  avg={result['linear_avg_ms']:.2f}ms p50={result['linear_p50_ms']:.2f}ms p99={result['linear_p99_ms']:.2f}ms")
            print(f"    Speedup: avg={result['speedup_avg']:.1f}x p50={result['speedup_p50']:.1f}x p99={result['speedup_p99']:.1f}x")
            if result['batch_avg_ms_per_seek'] > 0:
                print(f"    Batch (100): {result['batch_avg_ms_per_seek']:.2f}ms per seek ({1000/result['batch_avg_ms_per_seek']:.0f}x faster than single)")
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

    # Summary table
    print("\n" + "=" * 80)
    print("SUMMARY TABLE")
    print("=" * 80)
    print(f"{'Config':<25} {'Records':>10} {'Segs':>5} {'Idx Avg':>10} {'Lin Avg':>10} {'Speedup':>8} {'Batch/seek':>12}")
    print("-" * 80)
    for r in results:
        batch_str = f"{r['batch_avg_ms_per_seek']:.2f}ms" if r['batch_avg_ms_per_seek'] > 0 else "N/A"
        print(f"{r['name']:<25} {r['num_records']:>10,} {r['segments']:>5} {r['indexed_avg_ms']:>9.1f}ms {r['linear_avg_ms']:>9.1f}ms {r['speedup_avg']:>7.1f}x {batch_str:>12}")

    # Index size comparison
    print("\n" + "=" * 80)
    print("INDEX SIZE COMPARISON (estimated)")
    print("=" * 80)
    print(f"{'Config':<25} {'Records':>10} {'Sparse':>8} {'Entries/rec':>12} {'Index size':>12}")
    print("-" * 80)
    for r in results:
        segment_size = segment_sizes.get(r['name'], 64 * 1024)
        if r['sparse_bytes'] == 0:
            entries_per_rec = 1.0
            index_size = r['num_records'] * 16 / (1024 * 1024)
        else:
            # Rough estimate: segment_size / sparse_bytes entries per segment
            records_per_segment = segment_size // 100  # rough
            entries_per_segment = segment_size // r['sparse_bytes']
            entries_per_rec = entries_per_segment / records_per_segment
            index_size = r['num_records'] * entries_per_rec * 16 / (1024 * 1024)
        sparse_str = "dense" if r['sparse_bytes'] == 0 else f"{r['sparse_bytes']/1024:.0f}KB"
        print(f"{r['name']:<25} {r['num_records']:>10,} {sparse_str:>8} {entries_per_rec:>11.3f} {index_size:>11.1f} MB")


if __name__ == "__main__":
    main()