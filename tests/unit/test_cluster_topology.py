"""Unit tests for cluster topology (Day 7)."""

import pytest

from services.broker.cluster_topology import (
    ClusterTopology,
    BrokerNode,
    PartitionNode,
    TopicNode,
    BrokerState,
    create_cluster_topology,
)


class TestClusterTopologyBasics:
    """Test basic cluster topology operations."""

    def test_add_broker(self):
        """Add a broker to the cluster."""
        topology = create_cluster_topology()
        broker = topology.add_broker("broker-1", "localhost", 8001)
        assert broker.broker_id == "broker-1"
        assert broker.host == "localhost"
        assert broker.port == 8001
        assert broker.state == BrokerState.ACTIVE

    def test_add_multiple_brokers(self):
        """Add multiple brokers."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.add_broker("broker-3", "localhost", 8003)
        assert len(topology._brokers) == 3

    def test_remove_broker(self):
        """Remove a broker marks it offline."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        assert topology.remove_broker("broker-1") is True
        assert topology._brokers["broker-1"].state == BrokerState.OFFLINE
        assert topology.remove_broker("nonexistent") is False


class TestTopicCreation:
    """Test topic creation and partition assignment."""

    def test_create_topic_single_partition(self):
        """Create topic with single partition."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)

        topic = topology.create_topic("orders", partition_count=1, replication_factor=1)

        assert topic.topic_name == "orders"
        assert topic.partition_count == 1
        assert len(topic.children) == 1
        assert topic.children[0].partition_id == "0"
        assert topic.children[0].leader_broker_id == "broker-1"
        assert topic.children[0].replicas == ["broker-1"]

    def test_create_topic_multiple_partitions(self):
        """Create topic with multiple partitions distributed across brokers."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)

        topic = topology.create_topic("orders", partition_count=4, replication_factor=2)

        assert topic.partition_count == 4
        assert len(topic.children) == 4

        # Check round-robin assignment
        leaders = [p.leader_broker_id for p in topic.children]
        assert leaders == ["broker-1", "broker-2", "broker-1", "broker-2"]

        # Check replicas
        for part in topic.children:
            assert len(part.replicas) == 2
            assert part.leader_broker_id == part.replicas[0]

    def test_create_topic_replication_factor(self):
        """Create topic with replication factor > 1."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.add_broker("broker-3", "localhost", 8003)

        topic = topology.create_topic("orders", partition_count=2, replication_factor=3)

        assert topic.replication_factor == 3
        for part in topic.children:
            assert len(part.replicas) == 3
            assert part.leader_broker_id == part.replicas[0]

    def test_create_duplicate_topic_returns_existing(self):
        """Creating duplicate topic returns existing."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.create_topic("orders", 2, 1)
        topic2 = topology.create_topic("orders", 4, 2)
        assert topic2.partition_count == 2  # Original value


class TestPartitionReassignment:
    """Test partition reassignment (leadership election)."""

    def test_reassign_partition_leader(self):
        """Reassign partition to new leader."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.add_broker("broker-3", "localhost", 8003)

        topology.create_topic("orders", partition_count=1, replication_factor=2)

        # Initially leader is broker-1
        assert topology.find_partition_leader_bfs("orders", "0") == "broker-1"

        # Reassign to broker-2
        success = topology.reassign_partition("orders", "0", "broker-2")
        assert success is True
        assert topology.find_partition_leader_bfs("orders", "0") == "broker-2"

    def test_reassign_partition_with_new_replicas(self):
        """Reassign partition with new replica set."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.add_broker("broker-3", "localhost", 8003)

        topology.create_topic("orders", partition_count=1, replication_factor=2)

        # Reassign with new replicas
        success = topology.reassign_partition("orders", "0", "broker-3", ["broker-3", "broker-1"])
        assert success is True

        leader = topology.find_partition_leader_bfs("orders", "0")
        assert leader == "broker-3"

        topic = topology._topics["orders"]
        partition = topic.children[0]
        assert partition.replicas == ["broker-3", "broker-1"]

    def test_reassign_nonexistent_partition_fails(self):
        """Reassigning nonexistent partition fails."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        success = topology.reassign_partition("nonexistent", "0", "broker-1")
        assert success is False

    def test_reassign_to_nonexistent_broker_fails(self):
        """Reassigning to nonexistent broker fails."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.create_topic("orders", 1, 1)
        success = topology.reassign_partition("orders", "0", "nonexistent")
        assert success is False


class TestBFSTraversal:
    """Test BFS-based topology discovery."""

    def test_find_partition_leader_bfs(self):
        """Find partition leader via BFS."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=2)

        leader = topology.find_partition_leader_bfs("orders", "0")
        assert leader == "broker-1"

        leader = topology.find_partition_leader_bfs("orders", "1")
        assert leader == "broker-2"

    def test_find_partition_leader_bfs_nonexistent(self):
        """BFS returns None for nonexistent topic/partition."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)

        assert topology.find_partition_leader_bfs("nonexistent", "0") is None
        assert topology.find_partition_leader_bfs("orders", "999") is None


class TestDFSTraversal:
    """Test DFS-based topology discovery."""

    def test_find_partition_leader_dfs(self):
        """Find partition leader via DFS."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=2)

        leader = topology.find_partition_leader_dfs("orders", "0")
        assert leader == "broker-1"

        leader = topology.find_partition_leader_dfs("orders", "1")
        assert leader == "broker-2"

    def test_find_partition_leader_dfs_nonexistent(self):
        """DFS returns None for nonexistent topic/partition."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)

        assert topology.find_partition_leader_dfs("nonexistent", "0") is None
        assert topology.find_partition_leader_dfs("orders", "999") is None


class TestBFSDFSComparison:
    """Test BFS and DFS give same results."""

    def test_bfs_dfs_same_results(self):
        """BFS and DFS should find the same leader."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.add_broker("broker-3", "localhost", 8003)
        topology.create_topic("orders", partition_count=3, replication_factor=2)

        for i in range(3):
            bfs_leader = topology.find_partition_leader_bfs("orders", str(i))
            dfs_leader = topology.find_partition_leader_dfs("orders", str(i))
            assert bfs_leader == dfs_leader


class TestTopologyExport:
    """Test topology export via BFS/DFS."""

    def test_get_topology_bfs(self):
        """Export topology via BFS."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=2)

        result = topology.get_topology_bfs()

        assert len(result) == 1
        assert result[0]["topic"] == "orders"
        assert len(result[0]["partitions"]) == 2

    def test_get_topology_dfs(self):
        """Export topology via DFS."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=2)

        result = topology.get_topology_dfs()

        assert len(result) == 1
        assert result[0]["topic"] == "orders"
        assert len(result[0]["partitions"]) == 2

    def test_get_topology_multiple_topics(self):
        """Export multiple topics."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=1)
        topology.create_topic("payments", partition_count=1, replication_factor=2)

        result = topology.get_topology_bfs()
        assert len(result) == 2

        topics = {t["topic"] for t in result}
        assert topics == {"orders", "payments"}


class TestBrokerPartitions:
    """Test broker partition tracking."""

    def test_get_broker_partitions(self):
        """Get partitions assigned to a broker."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=3, replication_factor=2)

        # broker-1 should have partitions 0, 2 (round-robin)
        partitions = topology.get_broker_partitions("broker-1")
        assert "orders/0" in partitions
        assert "orders/2" in partitions

        # broker-2 should have partition 1
        partitions = topology.get_broker_partitions("broker-2")
        assert "orders/1" in partitions

    def test_get_broker_partitions_nonexistent(self):
        """Get partitions for nonexistent broker returns empty."""
        topology = create_cluster_topology()
        assert topology.get_broker_partitions("nonexistent") == []


class TestClusterHealth:
    """Test cluster health reporting."""

    def test_get_cluster_health_all_healthy(self):
        """Cluster health when all brokers active."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=2)

        health = topology.get_cluster_health()

        assert health["total_brokers"] == 2
        assert health["active_brokers"] == 2
        assert health["total_topics"] == 1
        assert health["total_partitions"] == 2
        assert health["under_replicated_partitions"] == 0
        assert health["healthy"] is True

    def test_get_cluster_health_under_replicated(self):
        """Cluster health when broker offline."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=2)

        topology.remove_broker("broker-2")

        health = topology.get_cluster_health()

        assert health["total_brokers"] == 2
        assert health["active_brokers"] == 1
        assert health["under_replicated_partitions"] > 0
        assert health["healthy"] is False

    def test_get_cluster_health_no_brokers(self):
        """Cluster health with no brokers."""
        topology = create_cluster_topology()
        health = topology.get_cluster_health()
        assert health["total_brokers"] == 0
        assert health["active_brokers"] == 0
        assert health["healthy"] is True  # vacuously healthy


class TestBrokerOfflineReassignment:
    """Test leader reassignment when broker goes offline."""

    def test_remove_broker_reassigns_leader(self):
        """Removing leader broker should reassign leader."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.add_broker("broker-3", "localhost", 8003)
        topology.create_topic("orders", partition_count=1, replication_factor=3)

        # Initially leader is broker-1
        assert topology.find_partition_leader_bfs("orders", "0") == "broker-1"

        # Remove broker-1
        topology.remove_broker("broker-1")

        # Leader should now be broker-2 (next replica)
        new_leader = topology.find_partition_leader_bfs("orders", "0")
        assert new_leader == "broker-2"


class TestTreeStructure:
    """Test the tree structure relationships."""

    def test_topic_parent_of_partitions(self):
        """Topic is parent of partition nodes."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.create_topic("orders", partition_count=2, replication_factor=1)

        topic = topology._topics["orders"]
        for part in topic.children:
            assert part.parent == topic

    def test_partition_parent_of_brokers(self):
        """Partition is parent of broker nodes."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=1, replication_factor=2)

        topic = topology._topics["orders"]
        partition = topic.children[0]
        for broker in partition.children:
            assert broker.parent == partition

    def test_broker_assigned_partitions_tracked(self):
        """Broker tracks its assigned partitions."""
        topology = create_cluster_topology()
        topology.add_broker("broker-1", "localhost", 8001)
        topology.add_broker("broker-2", "localhost", 8002)
        topology.create_topic("orders", partition_count=2, replication_factor=2)

        broker1 = topology._brokers["broker-1"]
        broker2 = topology._brokers["broker-2"]

        assert "orders/0" in broker1.assigned_partitions
        assert "orders/2" not in broker1.assigned_partitions  # doesn't exist
        assert "orders/1" in broker2.assigned_partitions


class TestConcurrency:
    """Test thread safety."""

    def test_concurrent_topic_creation(self):
        """Multiple threads creating topics."""
        import threading

        topology = create_cluster_topology()
        for i in range(5):
            topology.add_broker(f"broker-{i}", "localhost", 8000 + i)

        errors = []

        def create_topic(topic_name):
            try:
                topology.create_topic(topic_name, partition_count=2, replication_factor=2)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=create_topic, args=(f"topic-{i}",)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(topology._topics) == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])