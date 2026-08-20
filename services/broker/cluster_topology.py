"""
Cluster Topology - Tree-based partition/replica/broker assignment.

Models topic → partition → replica broker as a tree.
Implements BFS/DFS-based cluster topology discovery so any broker
can find where a given partition's leader currently lives.
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set


class BrokerState(Enum):
    """State of a broker in the cluster."""
    ACTIVE = "active"
    UNDER_REPLICATION = "under_replication"
    OFFLINE = "offline"


@dataclass(eq=False)
class BrokerNode:
    """A broker node in the cluster topology tree."""
    broker_id: str
    host: str
    port: int = 8000
    state: BrokerState = BrokerState.ACTIVE
    assigned_partitions: List[str] = field(default_factory=list)
    # parent and children are managed by the tree
    parent: Optional['PartitionNode'] = None
    children: List['PartitionNode'] = field(default_factory=list)

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    def __repr__(self) -> str:
        return f"BrokerNode({self.broker_id}, {self.state.value})"

    def __hash__(self) -> int:
        return hash(self.broker_id)


@dataclass(eq=False)
class PartitionNode:
    """A partition node in the cluster topology tree."""
    partition_id: str
    topic: str
    leader_broker_id: Optional[str] = None
    replicas: List[str] = field(default_factory=list)
    # parent and children are managed by the tree
    parent: Optional['TopicNode'] = None
    children: List['BrokerNode'] = field(default_factory=list)

    def __repr__(self) -> str:
        return f"PartitionNode({self.topic}/{self.partition_id}, leader={self.leader_broker_id})"

    def __hash__(self) -> int:
        return hash((self.topic, self.partition_id))


@dataclass(eq=False)
class TopicNode:
    """A topic node in the cluster topology tree (root)."""
    topic_name: str
    partition_count: int = 0
    replication_factor: int = 1
    # children are managed by the tree
    children: List['PartitionNode'] = field(default_factory=list)

    def __repr__(self) -> str:
        return f"TopicNode({self.topic_name}, partitions={self.partition_count})"

    def __hash__(self) -> int:
        return hash(self.topic_name)


class ClusterTopology:
    """
    Tree-based cluster topology.

    Structure: Topic → Partition → Replica Brokers

    Any broker can query this tree to find where a partition's leader lives.
    Supports BFS and DFS traversal for discovery.
    """

    def __init__(self):
        self._topics: Dict[str, TopicNode] = {}
        self._brokers: Dict[str, BrokerNode] = {}
        self._partition_leaders: Dict[str, str] = {}  # partition_key -> broker_id
        self._lock = threading.RLock()

    def add_broker(self, broker_id: str, host: str, port: int = 8000) -> BrokerNode:
        """Add a broker to the cluster."""
        with self._lock:
            if broker_id not in self._brokers:
                self._brokers[broker_id] = BrokerNode(
                    broker_id=broker_id,
                    host=host,
                    port=port
                )
            return self._brokers[broker_id]

    def remove_broker(self, broker_id: str) -> bool:
        """Remove a broker from the cluster."""
        with self._lock:
            if broker_id in self._brokers:
                broker = self._brokers[broker_id]
                broker.state = BrokerState.OFFLINE
                # Remove from all partitions' replica lists
                for partition_key, leader_id in list(self._partition_leaders.items()):
                    if leader_id == broker_id:
                        # Reassign leader to another replica
                        topic, part_id = partition_key.split('/')
                        topic_node = self._topics.get(topic)
                        if topic_node:
                            for part in topic_node.children:
                                if part.partition_id == part_id:
                                    # Find next available ACTIVE replica
                                    for replica_id in part.replicas:
                                        if (replica_id != broker_id and
                                            replica_id in self._brokers and
                                            self._brokers[replica_id].state == BrokerState.ACTIVE):
                                            part.leader_broker_id = replica_id
                                            self._partition_leaders[partition_key] = replica_id
                                            break
                                    break
                return True
            return False

    def create_topic(
        self,
        topic_name: str,
        partition_count: int,
        replication_factor: int = 1,
        broker_ids: Optional[List[str]] = None
    ) -> TopicNode:
        """
        Create a topic with partition assignments.

        Assigns partitions across available brokers in round-robin fashion.
        """
        with self._lock:
            if topic_name in self._topics:
                return self._topics[topic_name]

            topic_node = TopicNode(
                topic_name=topic_name,
                partition_count=partition_count,
                replication_factor=replication_factor
            )

            available_brokers = broker_ids or list(self._brokers.keys())

            for i in range(partition_count):
                partition_id = str(i)
                partition_key = f"{topic_name}/{partition_id}"

                # Round-robin assignment
                replicas = []
                for r in range(replication_factor):
                    broker_idx = (i + r) % len(available_brokers)
                    broker_id = available_brokers[broker_idx]
                    replicas.append(broker_id)

                    # Add broker to tree
                    broker_node = self._brokers.get(broker_id)
                    if broker_node:
                        broker_node.assigned_partitions.append(partition_key)
                        if broker_node not in topic_node.children[-1:].__class__():
                            # Will be added via partition
                            pass

                # First replica is leader
                leader_id = replicas[0] if replicas else None

                partition_node = PartitionNode(
                    partition_id=partition_id,
                    topic=topic_name,
                    leader_broker_id=leader_id,
                    replicas=replicas,
                    parent=topic_node
                )

                # Link children
                partition_node.children = [self._brokers[r] for r in replicas if r in self._brokers]
                for child in partition_node.children:
                    child.parent = partition_node

                topic_node.children.append(partition_node)
                self._partition_leaders[partition_key] = leader_id

            self._topics[topic_name] = topic_node
            return topic_node

    def reassign_partition(
        self,
        topic: str,
        partition_id: str,
        new_leader: str,
        new_replicas: Optional[List[str]] = None
    ) -> bool:
        """
        Reassign a partition to a new leader.

        This simulates partition reassignment for leadership election.
        """
        with self._lock:
            partition_key = f"{topic}/{partition_id}"

            topic_node = self._topics.get(topic)
            if not topic_node:
                return False

            partition_node = None
            for part in topic_node.children:
                if part.partition_id == partition_id:
                    partition_node = part
                    break

            if not partition_node:
                return False

            if new_leader not in self._brokers:
                return False

            # Update leader
            old_leader = partition_node.leader_broker_id
            partition_node.leader_broker_id = new_leader
            self._partition_leaders[partition_key] = new_leader

            # Update replicas if provided
            if new_replicas is not None:
                # Update broker assignments
                if old_leader and old_leader in self._brokers:
                    old_broker = self._brokers[old_leader]
                    if partition_key in old_broker.assigned_partitions:
                        old_broker.assigned_partitions.remove(partition_key)

                partition_node.replicas = new_replicas
                partition_node.children = [self._brokers[r] for r in new_replicas if r in self._brokers]

                for child in partition_node.children:
                    child.parent = partition_node
                    if partition_key not in child.assigned_partitions:
                        child.assigned_partitions.append(partition_key)

            return True

    def find_partition_leader_bfs(self, topic: str, partition_id: str) -> Optional[str]:
        """
        Find partition leader using BFS traversal.

        Traverses: Topic → Partitions → Replicas (BFS)
        """
        topic_node = self._topics.get(topic)
        if not topic_node:
            return None

        queue = deque([topic_node])
        visited = set()

        while queue:
            node = queue.popleft()

            if isinstance(node, PartitionNode):
                if node.partition_id == partition_id:
                    return node.leader_broker_id

            if node in visited:
                continue
            visited.add(node)

            # Add children to queue
            if hasattr(node, 'children'):
                for child in node.children:
                    if child not in visited:
                        queue.append(child)

        return None

    def find_partition_leader_dfs(self, topic: str, partition_id: str) -> Optional[str]:
        """
        Find partition leader using DFS traversal (iterative with stack).

        Traverses: Topic → Partitions → Replicas (DFS)
        """
        topic_node = self._topics.get(topic)
        if not topic_node:
            return None

        stack = [topic_node]
        visited = set()

        while stack:
            node = stack.pop()

            if isinstance(node, PartitionNode):
                if node.partition_id == partition_id:
                    return node.leader_broker_id

            if node in visited:
                continue
            visited.add(node)

            # Add children to stack (reversed for consistent DFS order)
            if hasattr(node, 'children'):
                for child in reversed(node.children):
                    if child not in visited:
                        stack.append(child)

        return None

    def get_topology_bfs(self) -> List[Dict]:
        """
        Get full cluster topology via BFS.

        Returns list of topics with their partitions and replica assignments.
        """
        result = []
        visited = set()

        for topic_node in self._topics.values():
            if topic_node in visited:
                continue

            topic_info = {
                "topic": topic_node.topic_name,
                "partition_count": topic_node.partition_count,
                "replication_factor": topic_node.replication_factor,
                "partitions": []
            }

            queue = deque([topic_node])
            while queue:
                node = queue.popleft()

                if node in visited:
                    continue
                visited.add(node)

                if isinstance(node, PartitionNode):
                    partition_info = {
                        "partition_id": node.partition_id,
                        "leader": node.leader_broker_id,
                        "replicas": node.replicas
                    }
                    topic_info["partitions"].append(partition_info)

                if hasattr(node, 'children'):
                    for child in node.children:
                        if child not in visited:
                            queue.append(child)

            result.append(topic_info)

        return result

    def get_topology_dfs(self) -> List[Dict]:
        """
        Get full cluster topology via DFS.

        Returns list of topics with their partitions and replica assignments.
        """
        result = []
        visited = set()

        for topic_node in self._topics.values():
            if topic_node in visited:
                continue

            topic_info = {
                "topic": topic_node.topic_name,
                "partition_count": topic_node.partition_count,
                "replication_factor": topic_node.replication_factor,
                "partitions": []
            }

            stack = [topic_node]
            while stack:
                node = stack.pop()

                if node in visited:
                    continue
                visited.add(node)

                if isinstance(node, PartitionNode):
                    partition_info = {
                        "partition_id": node.partition_id,
                        "leader": node.leader_broker_id,
                        "replicas": node.replicas
                    }
                    topic_info["partitions"].append(partition_info)

                if hasattr(node, 'children'):
                    for child in reversed(node.children):
                        if child not in visited:
                            stack.append(child)

            result.append(topic_info)

        return result

    def get_broker_partitions(self, broker_id: str) -> List[str]:
        """Get all partitions assigned to a broker."""
        broker = self._brokers.get(broker_id)
        if not broker:
            return []
        return list(broker.assigned_partitions)

    def get_cluster_health(self) -> Dict:
        """Get cluster health status."""
        active_brokers = sum(1 for b in self._brokers.values() if b.state == BrokerState.ACTIVE)
        total_brokers = len(self._brokers)
        total_partitions = sum(t.partition_count for t in self._topics.values())
        under_replicated = 0

        for topic_node in self._topics.values():
            for part in topic_node.children:
                active_replicas = sum(1 for r in part.replicas if r in self._brokers and self._brokers[r].state == BrokerState.ACTIVE)
                if active_replicas < len(part.replicas):
                    under_replicated += 1

        return {
            "total_brokers": total_brokers,
            "active_brokers": active_brokers,
            "total_topics": len(self._topics),
            "total_partitions": total_partitions,
            "under_replicated_partitions": under_replicated,
            "healthy": under_replicated == 0 and active_brokers == total_brokers
        }


def create_cluster_topology() -> ClusterTopology:
    """Factory function to create a cluster topology."""
    return ClusterTopology()
