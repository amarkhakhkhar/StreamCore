#!/usr/bin/env python
"""
Demo: Kubernetes headless Service for automatic broker discovery.

Shows how StreamCore uses the K8s DNS-based headless service to discover
brokers without manual configuration. Each pod gets a stable DNS name:

    streamcore-broker-0.streamcore-broker-headless.streamcore.svc.cluster.local
    streamcore-broker-1.streamcore-broker-headless.streamcore.svc.cluster.local
    streamcore-broker-2.streamcore-broker-headless.streamcore.svc.cluster.local

The headless Service resolves to the individual pod IPs, allowing clients
to enumerate all brokers and build the cluster topology automatically.
"""

import socket
import subprocess
from services.broker.cluster_topology import create_cluster_topology


def resolve_dns(name: str) -> list:
    """Resolve a DNS name to IP addresses (simulated for demo)."""
    # In a real K8s cluster, this uses the headless service DNS
    # For the demo, we simulate the resolution behavior
    try:
        result = socket.getaddrinfo(name, 8000)
        return list(set(item[4][0] for item in result))
    except (socket.gaierror, OSError):
        return []


def demo_k8s_headless_discovery():
    """Demo how brokers are discovered via K8s headless service."""
    print("=" * 70)
    print("  DEMO: Kubernetes Headless Service - Broker Discovery")
    print("=" * 70)

    print("\n--- Headless Service Configuration ---")
    print("  apiVersion: v1")
    print("  kind: Service")
    print("  metadata:")
    print("    name: streamcore-broker-headless")
    print("    namespace: streamcore")
    print("  spec:")
    print("    clusterIP: None   # Headless - returns pod IPs, not a VIP")
    print("    selector:")
    print("      app: streamcore-broker")
    print("    ports:")
    print("      - name: http")
    print("        port: 8000")
    print("      - name: gossip")
    print("        port: 8001")

    print("\n--- Pod DNS Names (stable, from StatefulSet) ---")
    statefulset_name = "streamcore-broker"
    headless_service = "streamcore-broker-headless"
    namespace = "streamcore"
    replicas = 3

    dns_names = []
    for i in range(replicas):
        pod_name = f"{statefulset_name}-{i}"
        fqdn = f"{pod_name}.{headless_service}.{namespace}.svc.cluster.local"
        dns_names.append(fqdn)
        print(f"  Pod {i}: {fqdn}")

    print("\n--- DNS Resolution (Headless Service returns pod IPs) ---")
    # Simulate DNS resolution
    simulated_ips = ["10.244.1.10", "10.244.2.15", "10.244.3.20"]
    for i, fqdn in enumerate(dns_names):
        ip = simulated_ips[i]
        print(f"  {fqdn}")
        print(f"    -> {ip}:8000 (HTTP API)")
        print(f"    -> {ip}:8001 (Gossip/Health)")

    print("\n--- Building Cluster Topology from Discovery ---")
    topology = create_cluster_topology()

    for i, fqdn in enumerate(dns_names):
        broker_id = f"broker-{i}"
        ip = simulated_ips[i]
        topology.add_broker(broker_id, ip, 8000)
        print(f"  Added {broker_id} at {ip}:8000")

    print(f"\n  Total brokers discovered: {len(topology._brokers)}")

    print("\n--- Topic Creation with Automatic Distribution ---")
    # Create a topic - partitions auto-distributed across discovered brokers
    topic = topology.create_topic("orders", partition_count=6, replication_factor=2)

    print(f"  Created topic 'orders' with {topic.partition_count} partitions, RF={topic.replication_factor}")
    print(f"\n  Partition Assignments (round-robin across discovered brokers):")
    for part in topic.children:
        replicas_str = ", ".join(part.replicas)
        print(f"    Partition {part.partition_id}: leader={part.leader_broker_id}, replicas=[{replicas_str}]")

    print("\n--- Client Connecting to a Specific Broker ---")
    # A producer/consumer just needs ONE broker to connect to
    # It then discovers the full topology from that broker
    print("  Client connects to: streamcore-broker-0.streamcore-broker-headless.streamcore.svc.cluster.local:8000")
    print("  Broker responds with full topology (all 3 brokers, all partitions)")
    print("  Client can now route to the correct leader for any partition")

    print("\n--- Failure Detection via Health Checks ---")
    print("  Each broker runs /health liveness + readiness probes")
    print("  Headless service removes failed pods from DNS automatically")
    print("  Remaining brokers detect failure via gossip and reassign leaders")

    print("\n--- Key Benefits ---")
    print("  * No manual broker IP configuration")
    print("  * Stable DNS names per pod (StatefulSet ordinal index)")
    print("  * Automatic service discovery via K8s DNS")
    print("  * Leader reassignment on pod failure")
    print("  * Horizontal scaling: just increase StatefulSet replicas")

    print("\n" + "=" * 70)
    print("  [OK] Kubernetes headless service enables zero-config discovery")
    print("=" * 70)


if __name__ == "__main__":
    demo_k8s_headless_discovery()
