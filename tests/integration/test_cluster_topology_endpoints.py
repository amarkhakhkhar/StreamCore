"""
Integration tests for cluster topology HTTP endpoints (Day 7).
"""

import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import services.broker.server as server
from services.broker.server import app
from services.broker.cluster_topology import create_cluster_topology


client = TestClient(app)


def setup_module():
    """Initialize cluster topology for testing."""
    topology = create_cluster_topology()
    topology.add_broker("broker-0", "localhost", 8000)
    topology.add_broker("broker-1", "localhost", 8001)
    topology.add_broker("broker-2", "localhost", 8002)
    server._cluster_topology = topology


def teardown_module():
    """Clean up after tests."""
    server._cluster_topology = None


def test_create_topic_endpoint():
    """Create topic via HTTP endpoint."""
    response = client.post("/topics", json={
        "topic_name": "test-orders",
        "partition_count": 3,
        "replication_factor": 2
    })
    assert response.status_code == 200
    data = response.json()
    assert data["topic"] == "test-orders"
    assert data["partition_count"] == 3
    assert data["replication_factor"] == 2
    assert len(data["partitions"]) == 3


def test_get_topology_bfs_endpoint():
    """Get full topology via BFS traversal."""
    # Create a topic first
    client.post("/topics", json={
        "topic_name": "bfs-topic",
        "partition_count": 2,
        "replication_factor": 1
    })

    response = client.get("/topics?traversal=bfs")
    assert response.status_code == 200
    data = response.json()
    assert "topics" in data
    assert "brokers" in data
    assert len(data["topics"]) >= 1


def test_get_topology_dfs_endpoint():
    """Get full topology via DFS traversal."""
    client.post("/topics", json={
        "topic_name": "dfs-topic",
        "partition_count": 2,
        "replication_factor": 1
    })

    response = client.get("/topics?traversal=dfs")
    assert response.status_code == 200
    data = response.json()
    assert "topics" in data
    assert len(data["topics"]) >= 1


def test_find_partition_leader_bfs_endpoint():
    """Find partition leader via BFS."""
    client.post("/topics", json={
        "topic_name": "leader-topic",
        "partition_count": 2,
        "replication_factor": 2
    })

    response = client.get("/topology/leader/leader-topic/0?traversal=bfs")
    assert response.status_code == 200
    data = response.json()
    assert data["topic"] == "leader-topic"
    assert data["partition_id"] == "0"
    assert data["leader_broker_id"] is not None
    assert data["traversal"] == "bfs"


def test_find_partition_leader_dfs_endpoint():
    """Find partition leader via DFS."""
    client.post("/topics", json={
        "topic_name": "leader-dfs-topic",
        "partition_count": 2,
        "replication_factor": 2
    })

    response = client.get("/topology/leader/leader-dfs-topic/0?traversal=dfs")
    assert response.status_code == 200
    data = response.json()
    assert data["traversal"] == "dfs"
    assert data["leader_broker_id"] is not None


def test_reassign_partition_endpoint():
    """Reassign partition via HTTP endpoint."""
    client.post("/topics", json={
        "topic_name": "reassign-topic",
        "partition_count": 1,
        "replication_factor": 2
    })

    # Get initial leader
    initial = client.get("/topology/leader/reassign-topic/0")
    initial_leader = initial.json()["leader_broker_id"]

    # Get broker list
    topology = client.get("/topics")
    brokers = topology.json()["brokers"]
    new_leader = next(b["broker_id"] for b in brokers if b["broker_id"] != initial_leader)

    # Reassign
    response = client.post("/topology/reassign", json={
        "topic": "reassign-topic",
        "partition_id": "0",
        "new_leader": new_leader
    })
    assert response.status_code == 200
    data = response.json()
    assert data["leader_broker_id"] == new_leader


def test_reassign_partition_invalid_broker():
    """Reassign to nonexistent broker should fail."""
    client.post("/topics", json={
        "topic_name": "invalid-topic",
        "partition_count": 1,
        "replication_factor": 1
    })

    response = client.post("/topology/reassign", json={
        "topic": "invalid-topic",
        "partition_id": "0",
        "new_leader": "nonexistent-broker"
    })
    assert response.status_code == 400


def test_cluster_health_endpoint():
    """Get cluster health."""
    client.post("/topics", json={
        "topic_name": "health-topic",
        "partition_count": 2,
        "replication_factor": 1
    })

    response = client.get("/cluster/health")
    assert response.status_code == 200
    data = response.json()
    assert "total_brokers" in data
    assert "active_brokers" in data
    assert "healthy" in data
    assert data["total_brokers"] >= 1


def test_broker_registered_on_startup():
    """Verify broker is registered in topology on startup."""
    response = client.get("/topics")
    assert response.status_code == 200
    data = response.json()
    assert len(data["brokers"]) >= 3
    broker = data["brokers"][0]
    assert "broker_id" in broker
    assert "host" in broker
    assert "port" in broker
    assert "state" in broker


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
