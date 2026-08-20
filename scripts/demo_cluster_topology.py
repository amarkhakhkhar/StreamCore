#!/usr/bin/env python
"""
Demo script for Day 7: Cluster Topology & Partition Reassignment.

Demonstrates:
- Tree-based cluster topology (Topic → Partition → Replica Brokers)
- BFS/DFS partition leader discovery
- Partition reassignment (leadership election)
- Broker failure handling with automatic leader reassignment
- Cluster health monitoring
"""

import time
from services.broker.cluster_topology import create_cluster_topology, BrokerState


def print_header(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def print_section(title: str):
    print(f"\n--- {title} ---")


def demo_basic_topology():
    """Demo basic cluster topology creation."""
    print_header("DEMO 1: Basic Cluster Topology")

    topology = create_cluster_topology()

    # Add brokers
    print_section("Adding 3 brokers to cluster")
    topology.add_broker("broker-1", "10.0.0.1", 8000)
    topology.add_broker("broker-2", "10.0.0.2", 8000)
    topology.add_broker("broker-3", "10.0.0.3", 8000)

    print(f"Brokers: {list(topology._brokers.keys())}")

    # Create topic with 4 partitions, replication factor 2
    print_section("Creating topic 'orders' with 4 partitions, RF=2")
    topic = topology.create_topic("orders", partition_count=4, replication_factor=2)

    print(f"Topic: {topic.topic_name}")
    print(f"Partitions: {topic.partition_count}")
    print(f"Replication Factor: {topic.replication_factor}")

    print("\nPartition Assignments:")
    for part in topic.children:
        replicas_str = ", ".join(part.replicas)
        print(f"  Partition {part.partition_id}: leader={part.leader_broker_id}, replicas=[{replicas_str}]")


def demo_bfs_dfs_discovery():
    """Demo BFS and DFS leader discovery."""
    print_header("DEMO 2: BFS vs DFS Leader Discovery")

    topology = create_cluster_topology()
    topology.add_broker("broker-1", "10.0.0.1", 8000)
    topology.add_broker("broker-2", "10.0.0.2", 8000)
    topology.add_broker("broker-3", "10.0.0.3", 8000)
    topology.create_topic("orders", partition_count=3, replication_factor=2)

    print_section("Finding partition leaders via BFS")
    for i in range(3):
        leader = topology.find_partition_leader_bfs("orders", str(i))
        print(f"  Partition {i}: leader = {leader}")

    print_section("Finding partition leaders via DFS")
    for i in range(3):
        leader = topology.find_partition_leader_dfs("orders", str(i))
        print(f"  Partition {i}: leader = {leader}")

    print_section("Verifying BFS == DFS")
    for i in range(3):
        bfs = topology.find_partition_leader_bfs("orders", str(i))
        dfs = topology.find_partition_leader_dfs("orders", str(i))
        assert bfs == dfs, f"Mismatch on partition {i}: BFS={bfs}, DFS={dfs}"
    print("  [OK] BFS and DFS return identical results")


def demo_partition_reassignment():
    """Demo partition reassignment (leadership election)."""
    print_header("DEMO 3: Partition Reassignment (Leadership Election)")

    topology = create_cluster_topology()
    topology.add_broker("broker-1", "10.0.0.1", 8000)
    topology.add_broker("broker-2", "10.0.0.2", 8000)
    topology.add_broker("broker-3", "10.0.0.3", 8000)
    topology.create_topic("orders", partition_count=2, replication_factor=3)

    print_section("Initial state")
    for i in range(2):
        leader = topology.find_partition_leader_bfs("orders", str(i))
        topic = topology._topics["orders"]
        part = topic.children[i]
        print(f"  Partition {i}: leader={leader}, replicas={part.replicas}")

    # Manual reassignment
    print_section("Reassigning partition 0 leader from broker-1 to broker-3")
    success = topology.reassign_partition("orders", "0", "broker-3")
    print(f"  Reassignment successful: {success}")

    leader = topology.find_partition_leader_bfs("orders", "0")
    part = topology._topics["orders"].children[0]
    print(f"  New leader: {leader}")
    print(f"  Replicas: {part.replicas}")

    # Reassign with new replica set
    print_section("Reassigning partition 1 with new replica set [broker-2, broker-3]")
    success = topology.reassign_partition("orders", "1", "broker-2", ["broker-2", "broker-3"])
    print(f"  Reassignment successful: {success}")

    leader = topology.find_partition_leader_bfs("orders", "1")
    part = topology._topics["orders"].children[1]
    print(f"  New leader: {leader}")
    print(f"  New replicas: {part.replicas}")


def demo_broker_failure_handling():
    """Demo automatic leader reassignment on broker failure."""
    print_header("DEMO 4: Broker Failure & Automatic Leader Reassignment")

    topology = create_cluster_topology()
    topology.add_broker("broker-1", "10.0.0.1", 8000)
    topology.add_broker("broker-2", "10.0.0.2", 8000)
    topology.add_broker("broker-3", "10.0.0.3", 8000)
    topology.create_topic("orders", partition_count=2, replication_factor=3)

    print_section("Initial state (all brokers healthy)")
    health = topology.get_cluster_health()
    print(f"  Cluster healthy: {health['healthy']}")
    print(f"  Active brokers: {health['active_brokers']}/{health['total_brokers']}")
    print(f"  Under-replicated partitions: {health['under_replicated_partitions']}")

    for i in range(2):
        leader = topology.find_partition_leader_bfs("orders", str(i))
        print(f"  Partition {i} leader: {leader}")

    # Simulate broker-1 (leader of partition 0) going offline
    print_section("Simulating broker-1 failure (was leader of partition 0)")
    topology.remove_broker("broker-1")

    health = topology.get_cluster_health()
    print(f"  Cluster healthy: {health['healthy']}")
    print(f"  Active brokers: {health['active_brokers']}/{health['total_brokers']}")
    print(f"  Under-replicated partitions: {health['under_replicated_partitions']}")

    for i in range(2):
        leader = topology.find_partition_leader_bfs("orders", str(i))
        print(f"  Partition {i} leader: {leader}")

    print_section("Simulating broker-2 failure (now leader of partition 0)")
    topology.remove_broker("broker-2")

    health = topology.get_cluster_health()
    print(f"  Cluster healthy: {health['healthy']}")
    print(f"  Active brokers: {health['active_brokers']}/{health['total_brokers']}")
    print(f"  Under-replicated partitions: {health['under_replicated_partitions']}")

    for i in range(2):
        leader = topology.find_partition_leader_bfs("orders", str(i))
        print(f"  Partition {i} leader: {leader}")

    # Verify partition 0 is now on broker-3
    assert topology.find_partition_leader_bfs("orders", "0") == "broker-3"
    print("  [OK] Leadership successfully reassigned through broker failures")


def demo_topology_export():
    """Demo exporting topology via BFS and DFS."""
    print_header("DEMO 5: Topology Export (BFS vs DFS)")

    topology = create_cluster_topology()
    topology.add_broker("broker-1", "10.0.0.1", 8000)
    topology.add_broker("broker-2", "10.0.0.2", 8000)
    topology.create_topic("orders", partition_count=2, replication_factor=2)
    topology.create_topic("payments", partition_count=1, replication_factor=2)

    print_section("Topology via BFS")
    bfs_result = topology.get_topology_bfs()
    for topic in bfs_result:
        print(f"  Topic: {topic['topic']} (partitions={topic['partition_count']}, RF={topic['replication_factor']})")
        for part in topic['partitions']:
            print(f"    Partition {part['partition_id']}: leader={part['leader']}, replicas={part['replicas']}")

    print_section("Topology via DFS")
    dfs_result = topology.get_topology_dfs()
    for topic in dfs_result:
        print(f"  Topic: {topic['topic']} (partitions={topic['partition_count']}, RF={topic['replication_factor']})")
        for part in topic['partitions']:
            print(f"    Partition {part['partition_id']}: leader={part['leader']}, replicas={part['replicas']}")


def demo_cluster_health():
    """Demo cluster health monitoring."""
    print_header("DEMO 6: Cluster Health Monitoring")

    topology = create_cluster_topology()

    print_section("Empty cluster")
    health = topology.get_cluster_health()
    print(f"  {health}")

    print_section("Adding 3 brokers, creating topic with RF=2")
    topology.add_broker("broker-1", "10.0.0.1", 8000)
    topology.add_broker("broker-2", "10.0.0.2", 8000)
    topology.add_broker("broker-3", "10.0.0.3", 8000)
    topology.create_topic("orders", partition_count=3, replication_factor=2)

    health = topology.get_cluster_health()
    print(f"  {health}")

    print_section("Simulating broker failure")
    topology.remove_broker("broker-3")

    health = topology.get_cluster_health()
    print(f"  {health}")
    print(f"  Under-replicated: {health['under_replicated_partitions']} partitions")


def demo_tree_structure():
    """Demo the tree structure relationships."""
    print_header("DEMO 7: Tree Structure Verification")

    topology = create_cluster_topology()
    topology.add_broker("broker-1", "10.0.0.1", 8000)
    topology.add_broker("broker-2", "10.0.0.2", 8000)
    topology.create_topic("orders", partition_count=2, replication_factor=2)

    topic = topology._topics["orders"]

    print_section("Parent-Child Relationships (Top-Down)")
    print(f"Topic 'orders' has {len(topic.children)} partition children")

    for part in topic.children:
        assert part.parent == topic, "Partition parent should be topic"
        print(f"  Partition {part.partition_id} -> parent = Topic({part.parent.topic_name})")

        print(f"    Partition {part.partition_id} has {len(part.children)} broker children:")
        for broker in part.children:
            print(f"      -> Broker {broker.broker_id}")

    print_section("Broker Partition Tracking (Bottom-Up)")
    for broker_id, broker in topology._brokers.items():
        print(f"  {broker_id}: assigned partitions = {broker.assigned_partitions}")

    print_section("Tree Traversal Paths")
    print("  Topic(orders)")
    for part in topic.children:
        print(f"    +-- Partition({part.partition_id}) [leader={part.leader_broker_id}]")
        for broker in part.children:
            marker = "[LEADER]" if broker.broker_id == part.leader_broker_id else "[replica]"
            print(f"        {marker} Broker({broker.broker_id})")

    print("  [OK] All tree relationships verified")


def main():
    print("\n" + "="*60)
    print("  StreamCore Day 7: Cluster Topology & Partition Reassignment")
    print("  Tree-based Topic -> Partition -> Replica Broker Model")
    print("  BFS/DFS Discovery | Leadership Election | Health Monitoring")
    print("="*60)

    demo_basic_topology()
    demo_bfs_dfs_discovery()
    demo_partition_reassignment()
    demo_broker_failure_handling()
    demo_topology_export()
    demo_cluster_health()
    demo_tree_structure()

    print_header("ALL DEMOS COMPLETED SUCCESSFULLY")
    print("""
Key Takeaways:
  * Tree structure enables O(log n) leader discovery via BFS/DFS
  * Round-robin partition assignment distributes load evenly
  * Automatic leader reassignment on broker failure (RF >= 2 required)
  * Cluster health API detects under-replication in real-time
  * Thread-safe with RLock for concurrent operations

Next: Kubernetes headless Service for automatic broker discovery
    """)


if __name__ == "__main__":
    main()