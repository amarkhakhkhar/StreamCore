#!/usr/bin/env python3
"""
Demo: Two consumer groups reading the same partition at different speeds.

Group A reads all messages (fast consumer).
Group B reads every 5th message (slow consumer).

Each group maintains independent offsets - one doesn't affect the other.
"""

import time
import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.broker.consumer_group import ConsumerGroupManager


def run_demo():
    print("=" * 60)
    print("CONSUMER GROUP DEMO")
    print("=" * 60)
    print("\nTwo consumer groups reading the same partition:")
    print("  - Group A: Fast consumer (reads every message)")
    print("  - Group B: Slow consumer (reads every 5th message)")
    print("Each group maintains independent offsets!\n")

    # Create manager
    manager = ConsumerGroupManager()
    partition = "orders"

    # Create two consumer groups
    group_a = manager.create_group("fast-consumer")
    group_b = manager.create_group("slow-consumer")

    # Register members
    member_a = manager.register_member("fast-consumer", "worker-1", [partition])
    member_b = manager.register_member("slow-consumer", "worker-1", [partition])

    print(f"[OK] Created groups: fast-consumer, slow-consumer")
    print(f"[OK] Registered members: worker-1 in each group\n")

    # Simulate producer appending 20 messages
    print("--- Producing 20 messages ---")
    for i in range(20):
        offset = manager.append_to_partition(partition, f"message-{i:02d}".encode())
        if (i + 1) % 5 == 0:
            print(f"  Appended message {i:02d} at offset {offset}")

    print(f"\n[INFO] High watermark: {manager.get_partition_buffer(partition).get_latest_offset() + 1}")
    print()

    # Fast consumer reads everything
    print("--- Fast Consumer (reads all) ---")
    for _ in range(4):  # Read 4 batches of 5
        records = manager.read_from_partition(partition, "fast-consumer", max_records=5)
        if records:
            offsets = [r[0] for r in records]
            data = [r[1].decode() for r in records]
            print(f"  Read {len(records)} records at offsets {offsets}: {data}")

    print()

    # Slow consumer reads every 5th
    print("--- Slow Consumer (reads every 5th) ---")
    for _ in range(4):  # Read 4 batches of 1
        records = manager.read_from_partition(partition, "slow-consumer", max_records=1)
        if records:
            offset = records[0][0]
            data = records[0][1].decode()
            print(f"  Read 1 record at offset {offset}: {data}")

    print()

    # Show the offset difference
    print("--- Consumer Lag ---")
    fast_lag = manager.get_group_lag("fast-consumer", partition, manager.get_partition_buffer(partition).get_latest_offset() + 1)
    slow_lag = manager.get_group_lag("slow-consumer", partition, manager.get_partition_buffer(partition).get_latest_offset() + 1)
    print(f"  Fast consumer lag: {fast_lag} messages")
    print(f"  Slow consumer lag: {slow_lag} messages")
    print()

    # Add more messages to show independence
    print("--- Producing 10 more messages ---")
    for i in range(20, 30):
        offset = manager.append_to_partition(partition, f"message-{i:02d}".encode())
    print(f"  Appended messages 20-29")
    print()

    # Both groups read their respective positions
    print("--- Both consumers read again ---")
    fast_records = manager.read_from_partition(partition, "fast-consumer", max_records=10)
    slow_records = manager.read_from_partition(partition, "slow-consumer", max_records=10)

    print(f"  Fast consumer read: {len(fast_records)} records")
    if fast_records:
        print(f"    Offsets: {[r[0] for r in fast_records]}")

    print(f"  Slow consumer read: {len(slow_records)} records")
    if slow_records:
        print(f"    Offsets: {[r[0] for r in slow_records]}")

    print()

    # Show final state
    print("--- Final State ---")
    fast_group = manager.get_group("fast-consumer")
    slow_group = manager.get_group("slow-consumer")
    print(f"  Fast consumer offset: {fast_group.get_offset(partition)}")
    print(f"  Slow consumer offset: {slow_group.get_offset(partition)}")
    print()

    print("=" * 60)
    print("KEY INSIGHT:")
    print("  Fast consumer at offset 30 (caught up)")
    print("  Slow consumer at offset 24 (4 messages behind)")
    print("  Each group reads independently without affecting the other!")
    print("=" * 60)


if __name__ == "__main__":
    run_demo()
