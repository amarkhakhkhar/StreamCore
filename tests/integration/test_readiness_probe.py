"""
Integration test: K8s readiness probe detects replica lag.

Simulates the scenario where a broker's replicas have fallen behind,
then verifies the /health/ready endpoint reports not-ready.
"""

import os
import sys
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import services.broker.server as server
from services.broker.server import app


@pytest.fixture(autouse=True)
def setup_server_env(monkeypatch):
    """Set environment variables for test to use temp directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("STREAMCORE_LOG_DIR", tmpdir)
        monkeypatch.setenv("STREAMCORE_SEGMENT_SIZE", "1048576")
        monkeypatch.setenv("STREAMCORE_SEGMENT_MAX_AGE_MS", "0")
        monkeypatch.setenv("STREAMCORE_BP_WINDOW_MS", "1000")
        monkeypatch.setenv("STREAMCORE_BP_TRIGGER_WINDOWS", "3")
        monkeypatch.setenv("STREAMCORE_BP_LATENCY_THRESHOLD_MS", "50.0")
        monkeypatch.setenv("STREAMCORE_BROKER_ID", "broker-0")
        monkeypatch.setenv("STREAMCORE_BROKER_HOST", "localhost")
        monkeypatch.setenv("STREAMCORE_PORT", "8000")
        yield


class TestReadinessProbe:
    """Tests proving the readiness probe catches replica lag."""

    @pytest.fixture
    def client(self):
        """Create a test client with fresh state."""
        with TestClient(app) as client:
            yield client

    def test_readiness_initially_ready(self, client):
        """A freshly started broker with no consumers should be ready."""
        response = client.get("/health/ready")
        assert response.status_code == 200

        data = response.json()
        assert data["ready"] is True
        assert data["reason"] == "all replicas in sync"
        assert data["max_replica_lag"] == 0

    def test_readiness_ready_with_synced_consumer(self, client):
        """When a consumer commits and stays caught up, broker is ready."""
        # Register a consumer
        client.post("/consumer/register", json={"consumer_id": "group-A"})

        # Append records
        for i in range(100):
            client.post("/append", json={"data": f"record-{i}"})

        # Consumer commits everything
        client.post("/consumer/commit", json={"consumer_id": "group-A", "offset": 100})

        # Should be ready
        response = client.get("/health/ready")
        assert response.status_code == 200
        assert response.json()["ready"] is True

    def test_readiness_not_ready_with_lagging_consumer(self, client):
        """
        CRITICAL TEST: Force a consumer to fall behind, then verify
        the readiness probe marks the broker NOT ready.
        """
        # Register consumer group
        client.post("/consumer/register", json={"consumer_id": "lagging-group"})

        # Write 15,000 records (well beyond default threshold of 10,000)
        write_count = 15000
        for i in range(write_count):
            client.post("/append", json={"data": f"bulk-{i}"})

        # Consumer commits NOTHING (stays at offset 0)
        # Lag = 15,000 records

        # Readiness probe should detect this lag
        response = client.get("/health/ready")
        assert response.status_code == 200

        data = response.json()
        assert data["ready"] is False, "Broker should be NOT ready when consumer lags 15k records (threshold=10k)"
        assert data["max_replica_lag"] >= write_count, f"Expected lag >= {write_count}, got {data['max_replica_lag']}"
        assert "lagging-group" in data["consumer_groups_lagging"], "Lagging group not detected"

    def test_readiness_threshold_custom(self, client):
        """Test that custom threshold is respected."""
        # Register consumer
        client.post("/consumer/register", json={"consumer_id": "threshold-group"})

        # Write 500 records
        for i in range(500):
            client.post("/append", json={"data": f"rec-{i}"})

        # With threshold=1000, 500 lag is acceptable
        response = client.get("/health/ready?replica_lag_threshold=1000")
        assert response.status_code == 200
        assert response.json()["ready"] is True

        # With threshold=100, 500 lag is too much
        response = client.get("/health/ready?replica_lag_threshold=100")
        assert response.status_code == 200
        assert response.json()["ready"] is False

    def test_readiness_recovers_after_commit(self, client):
        """
        Test that a lagging broker becomes ready again once the
        consumer catches up and commits.
        """
        # Register consumer
        client.post("/consumer/register", json={"consumer_id": "recovering-group"})

        # Write 15000 records (exceeds default 10000 threshold)
        for i in range(15000):
            client.post("/append", json={"data": f"rec-{i}"})

        # Don't commit — should be not-ready
        response = client.get("/health/ready")
        assert response.json()["ready"] is False

        # Now consumer catches up and commits
        client.post("/consumer/commit", json={"consumer_id": "recovering-group", "offset": 15000})

        # Should now be ready
        response = client.get("/health/ready")
        assert response.status_code == 200
        assert response.json()["ready"] is True, "Broker should recover once lag is cleared"

    def test_readiness_multiple_consumers(self, client):
        """Test that readiness considers multiple consumer groups."""
        # Register two consumers
        client.post("/consumer/register", json={"consumer_id": "group-1"})
        client.post("/consumer/register", json={"consumer_id": "group-2"})

        # Write 15000 records (exceeds default 10000 threshold)
        for i in range(15000):
            client.post("/append", json={"data": f"rec-{i}"})

        # Group-1 commits, group-2 stays behind
        client.post("/consumer/commit", json={"consumer_id": "group-1", "offset": 15000})

        # Should be NOT ready because group-2 is lagging
        response = client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["ready"] is False
        assert "group-2" in data["consumer_groups_lagging"]
        assert "group-1" not in data["consumer_groups_lagging"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
