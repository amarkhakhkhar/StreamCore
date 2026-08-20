"""
StreamCore Broker - Main entry point.

A lightweight broker service exposing the partition log via HTTP API.
"""

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .partition_log import PartitionLog
from .routing import RoutingEngine, create_routing_engine
from .backpressure import BackpressureMiddleware, create_backpressure_middleware
from .consumer_group import ConsumerGroupManager, create_consumer_group_manager
from .cluster_topology import ClusterTopology, create_cluster_topology


# Global partition log instance
_log: Optional[PartitionLog] = None

# Global backpressure middleware
_backpressure: Optional[BackpressureMiddleware] = None

# Global consumer group manager
_consumer_group_manager: Optional[ConsumerGroupManager] = None

# Global cluster topology
_cluster_topology: Optional[ClusterTopology] = None


def get_log() -> PartitionLog:
    """Get the global partition log instance."""
    if _log is None:
        raise RuntimeError("Partition log not initialized")
    return _log


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage the partition log lifecycle."""
    global _log

    log_dir = Path(os.getenv("STREAMCORE_LOG_DIR", "/data/logs"))
    segment_size = int(os.getenv("STREAMCORE_SEGMENT_SIZE", 1024 * 1024))  # 1MB default
    segment_max_age_ms = int(os.getenv("STREAMCORE_SEGMENT_MAX_AGE_MS", "0"))  # 0 = disabled

    _log = PartitionLog(
        name="default",
        log_dir=log_dir,
        segment_max_size=segment_size,
        segment_max_age_ms=segment_max_age_ms
    )

    global _backpressure
    _backpressure = create_backpressure_middleware(
        window_ms=int(os.getenv("STREAMCORE_BP_WINDOW_MS", "1000")),
        trigger_windows=int(os.getenv("STREAMCORE_BP_TRIGGER_WINDOWS", "3")),
        latency_threshold_ms=float(os.getenv("STREAMCORE_BP_LATENCY_THRESHOLD_MS", "50.0")),
    )

    global _consumer_group_manager
    _consumer_group_manager = create_consumer_group_manager()

    global _cluster_topology
    _cluster_topology = create_cluster_topology()
    # Register this broker in the cluster
    broker_id = os.getenv("STREAMCORE_BROKER_ID", "broker-0")
    broker_host = os.getenv("STREAMCORE_BROKER_HOST", "localhost")
    broker_port = int(os.getenv("STREAMCORE_PORT", "8000"))
    _cluster_topology.add_broker(broker_id, broker_host, broker_port)

    yield

    _log.close()
    _log = None
    _backpressure = None
    _consumer_group_manager = None
    _cluster_topology = None


app = FastAPI(
    title="StreamCore Broker",
    description="A Kafka-like partition log broker",
    version="0.1.0",
    lifespan=lifespan
)


class AppendRequest(BaseModel):
    """Request to append a record to the log."""
    data: str
    timestamp: Optional[int] = None


class AppendResponse(BaseModel):
    """Response after appending a record."""
    offset: int


class RecordResponse(BaseModel):
    """A single record from the log."""
    offset: int
    timestamp: int
    data: str


class SegmentInfo(BaseModel):
    """Information about a segment."""
    path: str
    base_offset: int
    record_count: int
    size: int
    sealed: bool


class RotationInfo(BaseModel):
    """Information about segment rotation."""
    rotation_count: int
    last_rotation_reason: str
    segment_max_size: int
    segment_max_age_ms: int
    active_segment_age_ms: int


class LogInfoResponse(BaseModel):
    """Information about the partition log."""
    name: str
    record_count: int
    high_watermark: int
    segment_count: int
    sealed_segment_count: int
    segments: list[SegmentInfo]
    rotation: RotationInfo


class ConsumerLagResponse(BaseModel):
    """Consumer lag information."""
    consumer_id: str
    committed_offset: int
    high_watermark: int
    lag: int
    last_heartbeat_ms: int


class AllLagResponse(BaseModel):
    """Lag for all consumers."""
    consumers: dict


class CommitRequest(BaseModel):
    """Request to commit consumer offset."""
    consumer_id: str
    offset: int


class RegisterConsumerRequest(BaseModel):
    """Request to register a consumer."""
    consumer_id: str


class LaggingConsumersResponse(BaseModel):
    """List of lagging consumers."""
    threshold: int
    lagging_consumers: list[str]


class CacheStatsResponse(BaseModel):
    """Cache statistics response."""
    size: int
    capacity: int
    hits: int
    misses: int
    evictions: int
    total_requests: int
    hit_rate: float
    miss_rate: float


# ==================== Routing Models ====================

class RouteRequest(BaseModel):
    """Request to route a message."""
    topic: str
    data: str
    headers: dict[str, str] = {}


class RouteResponse(BaseModel):
    """Response with routing decision."""
    topic: str
    target: str
    matched_rule: Optional[str] = None


class RoutingRuleRequest(BaseModel):
    """Request to add a routing rule."""
    name: str
    expression: str
    target: str


class RoutingRuleResponse(BaseModel):
    """Routing rule information."""
    name: str
    expression: str
    target: str


class RoutingRulesResponse(BaseModel):
    """All routing rules."""
    rules: list[RoutingRuleResponse]
    default_target: str


# Global routing engine
_routing_engine: Optional[RoutingEngine] = None


def get_routing_engine() -> RoutingEngine:
    """Get the global routing engine instance."""
    global _routing_engine
    if _routing_engine is None:
        _routing_engine = create_routing_engine()
    return _routing_engine


# ==================== Log Operations ====================

@app.post("/append", response_model=AppendResponse)
async def append_record(request: AppendRequest):
    """Append a record to the partition log."""
    log = get_log()

    # Check backpressure before processing
    if _backpressure and _backpressure.should_throttle():
        retry_after = _backpressure.get_retry_after()
        raise HTTPException(
            status_code=429,
            detail="Backpressure active - broker overloaded, slow down",
            headers={"Retry-After": str(retry_after or 1000)}
        )

    start_time = time.time()
    offset = log.append(request.data.encode(), request.timestamp)
    latency_ms = (time.time() - start_time) * 1000

    # Record latency for backpressure tracking
    if _backpressure:
        _backpressure.record_append_latency(latency_ms)

    # Also append to consumer group manager's circular buffer
    if _consumer_group_manager:
        _consumer_group_manager.append_to_partition("default", request.data.encode())

    return AppendResponse(offset=offset)


@app.get("/read/{offset}", response_model=RecordResponse)
async def read_record(offset: int):
    """Read a single record at the given offset."""
    log = get_log()
    record = log.read_at(offset)

    if record is None:
        raise HTTPException(status_code=404, detail=f"Record not found at offset {offset}")

    return RecordResponse(
        offset=record.offset,
        timestamp=record.timestamp,
        data=record.data.decode()
    )


@app.get("/read", response_model=list[RecordResponse])
async def read_records(start_offset: int = 0, max_records: int = 100):
    """Read records from the partition log."""
    log = get_log()
    records = list(log.read(start_offset=start_offset, max_records=max_records))

    return [
        RecordResponse(
            offset=r.offset,
            timestamp=r.timestamp,
            data=r.data.decode()
        )
        for r in records
    ]


@app.get("/info", response_model=LogInfoResponse)
async def get_info():
    """Get partition log information."""
    log = get_log()
    rotation_info = log.get_rotation_info()

    return LogInfoResponse(
        name=log.name,
        record_count=log.record_count,
        high_watermark=log.high_watermark,
        segment_count=log.segment_count,
        sealed_segment_count=log.sealed_segment_count,
        segments=[
            SegmentInfo(**s) for s in log.get_segments_info()
        ],
        rotation=RotationInfo(**rotation_info)
    )


# ==================== Consumer Lag Operations ====================

@app.post("/consumer/register")
async def register_consumer(request: RegisterConsumerRequest):
    """Register a consumer for lag tracking."""
    log = get_log()
    log.register_consumer(request.consumer_id)
    return {"status": "registered", "consumer_id": request.consumer_id}


@app.post("/consumer/commit")
async def commit_offset(request: CommitRequest):
    """Commit consumer offset (consumer confirms it processed up to this offset)."""
    log = get_log()
    log.commit(request.consumer_id, request.offset)
    return {
        "status": "committed",
        "consumer_id": request.consumer_id,
        "committed_offset": request.offset
    }


@app.get("/consumer/{consumer_id}/lag", response_model=ConsumerLagResponse)
async def get_consumer_lag(consumer_id: str):
    """Get lag for a specific consumer."""
    log = get_log()
    all_lag = log.get_all_consumer_lag()

    if consumer_id not in all_lag:
        raise HTTPException(status_code=404, detail=f"Consumer '{consumer_id}' not registered")

    lag_info = all_lag[consumer_id]
    return ConsumerLagResponse(
        consumer_id=consumer_id,
        committed_offset=lag_info["committed_offset"],
        high_watermark=lag_info["high_watermark"],
        lag=lag_info["lag"],
        last_heartbeat_ms=lag_info["last_heartbeat_ms"]
    )


@app.get("/consumer/lag", response_model=AllLagResponse)
async def get_all_lag():
    """Get lag for all tracked consumers."""
    log = get_log()
    return AllLagResponse(consumers=log.get_all_consumer_lag())


@app.get("/consumer/lagging", response_model=LaggingConsumersResponse)
async def get_lagging_consumers(threshold: int = 100):
    """Get list of consumers lagging beyond threshold."""
    log = get_log()
    lagging = log.get_lagging_consumers(threshold)
    return LaggingConsumersResponse(
        threshold=threshold,
        lagging_consumers=lagging
    )


# ==================== Cache Operations ====================

@app.get("/cache/stats", response_model=CacheStatsResponse)
async def get_cache_stats():
    """Get LRU segment cache statistics."""
    log = get_log()
    stats = log.get_cache_stats()
    return CacheStatsResponse(**stats)


@app.post("/cache/clear")
async def clear_cache():
    """Clear the LRU segment cache."""
    log = get_log()
    log.clear_cache()
    return {"status": "cleared"}


# ==================== Routing Operations ====================

@app.post("/route", response_model=RouteResponse)
async def route_message(request: RouteRequest):
    """Route a message to a topic/partition based on routing rules."""
    engine = get_routing_engine()

    message = {
        "topic": request.topic,
        "data": request.data,
        "headers": request.headers
    }

    target = engine.route(message)

    # Find which rule matched (for debugging)
    matched_rule = None
    for rule in engine.rules:
        if rule.matches(message):
            matched_rule = rule.name
            break

    return RouteResponse(
        topic=request.topic,
        target=target,
        matched_rule=matched_rule
    )


@app.post("/route/batch", response_model=list[RouteResponse])
async def route_batch(requests: list[RouteRequest]):
    """Route a batch of messages."""
    engine = get_routing_engine()

    messages = [
        {"topic": r.topic, "data": r.data, "headers": r.headers}
        for r in requests
    ]

    results = engine.route_batch(messages)

    return [
        RouteResponse(
            topic=r["topic"],
            target=target,
            matched_rule=next((rule.name for rule in engine.rules if rule.matches(r)), None)
        )
        for r, target in results
    ]


@app.get("/routing/rules", response_model=RoutingRulesResponse)
async def get_routing_rules():
    """Get all routing rules."""
    engine = get_routing_engine()
    return RoutingRulesResponse(
        rules=[
            RoutingRuleResponse(name=rule.name, expression=rule.expression, target=rule.target)
            for rule in engine.rules
        ],
        default_target=engine.default_target
    )


@app.post("/routing/rules", response_model=RoutingRuleResponse)
async def add_routing_rule(request: RoutingRuleRequest):
    """Add a new routing rule."""
    engine = get_routing_engine()
    engine.add_rule(request.name, request.expression, request.target)
    return RoutingRuleResponse(name=request.name, expression=request.expression, target=request.target)


@app.delete("/routing/rules/{name}")
async def delete_routing_rule(name: str):
    """Delete a routing rule by name."""
    engine = get_routing_engine()
    engine.rules = [r for r in engine.rules if r.name != name]
    return {"status": "deleted", "name": name}


# ==================== Backpressure Operations ====================

@app.get("/backpressure/stats")
async def get_backpressure_stats():
    """Get backpressure tracking statistics."""
    if _backpressure is None:
        raise HTTPException(status_code=503, detail="Backpressure not initialized")
    return _backpressure.get_stats()


@app.post("/backpressure/test/inject-latency")
async def inject_test_latency(latency_ms: float):
    """
    Test endpoint to inject artificial latency for backpressure testing.

    NOTE: This is a test-only endpoint for CI/load-test verification.
    """
    if _backpressure is None:
        raise HTTPException(status_code=503, detail="Backpressure not initialized")

    # Record artificially high latency to simulate load
    _backpressure.record_append_latency(latency_ms)

    return {
        "injected_ms": latency_ms,
        "backpressure_active": _backpressure.should_throttle(),
        "stats": _backpressure.get_stats()
    }


# ==================== Consumer Group Models ====================

class ConsumerGroupCreateRequest(BaseModel):
    """Request to create a consumer group."""
    group_id: str


class ConsumerGroupMemberRequest(BaseModel):
    """Request to register a member to a consumer group."""
    member_id: str
    partitions: list[str] = []


class ConsumerGroupReadRequest(BaseModel):
    """Request to read records for a consumer group."""
    partition: str
    max_records: int = 100


class ConsumerGroupResponse(BaseModel):
    """Response with consumer group information."""
    group_id: str
    members: list[str]
    offsets: dict[str, int]


class ConsumerGroupListResponse(BaseModel):
    """List of all consumer groups."""
    groups: list[str]


class ConsumerGroupReadResponse(BaseModel):
    """Records read for a consumer group."""
    records: list[RecordResponse]
    next_offset: int


class ConsumerGroupLagResponse(BaseModel):
    """Lag information for a consumer group."""
    partition: str
    high_watermark: int
    group_offsets: dict[str, int]


# ==================== Consumer Group Operations ====================

def get_consumer_group_manager() -> ConsumerGroupManager:
    """Get the global consumer group manager instance."""
    if _consumer_group_manager is None:
        raise RuntimeError("Consumer group manager not initialized")
    return _consumer_group_manager


@app.post("/consumer-groups", response_model=ConsumerGroupResponse)
async def create_consumer_group(request: ConsumerGroupCreateRequest):
    """Create a new consumer group."""
    manager = get_consumer_group_manager()
    group = manager.create_group(request.group_id)
    return ConsumerGroupResponse(
        group_id=group.group_id,
        members=list(group.members.keys()),
        offsets=group.partition_offsets
    )


@app.get("/consumer-groups", response_model=ConsumerGroupListResponse)
async def list_consumer_groups():
    """List all consumer groups."""
    manager = get_consumer_group_manager()
    return ConsumerGroupListResponse(groups=manager.list_groups())


@app.get("/consumer-groups/{group_id}", response_model=ConsumerGroupResponse)
async def get_consumer_group(group_id: str):
    """Get a consumer group's information."""
    manager = get_consumer_group_manager()
    group = manager.get_group(group_id)
    if not group:
        raise HTTPException(status_code=404, detail=f"Consumer group '{group_id}' not found")
    return ConsumerGroupResponse(
        group_id=group.group_id,
        members=list(group.members.keys()),
        offsets=group.partition_offsets
    )


@app.post("/consumer-groups/{group_id}/members", response_model=ConsumerGroupMemberRequest)
async def register_member(group_id: str, request: ConsumerGroupMemberRequest):
    """Register a member to a consumer group."""
    manager = get_consumer_group_manager()
    member = manager.register_member(group_id, request.member_id, request.partitions)
    return ConsumerGroupMemberRequest(
        member_id=member.member_id,
        assigned_partitions=member.assigned_partitions
    )


@app.post("/consumer-groups/{group_id}/heartbeat")
async def heartbeat_member(group_id: str, member_id: str):
    """Send heartbeat for a member."""
    manager = get_consumer_group_manager()
    success = manager.heartbeat(group_id, member_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Member '{member_id}' not found in group '{group_id}'")
    return {"status": "heartbeat_ok", "group_id": group_id, "member_id": member_id}


@app.post("/consumer-groups/{group_id}/read", response_model=ConsumerGroupReadResponse)
async def read_for_consumer_group(group_id: str, request: ConsumerGroupReadRequest):
    """Read records from a partition for a consumer group."""
    manager = get_consumer_group_manager()
    records = manager.read_from_partition(request.partition, group_id, request.max_records)

    return ConsumerGroupReadResponse(
        records=[
            RecordResponse(offset=offset, timestamp=0, data=data.decode())
            for offset, data in records
        ],
        next_offset=records[-1][0] + 1 if records else 0
    )


@app.post("/consumer-groups/{group_id}/commit")
async def commit_consumer_group_offset(group_id: str, partition: str, offset: int):
    """Commit offset for a consumer group."""
    manager = get_consumer_group_manager()
    group = manager.get_group(group_id)
    if not group:
        raise HTTPException(status_code=404, detail=f"Consumer group '{group_id}' not found")
    group.commit_offset(partition, offset)
    return {"status": "committed", "group_id": group_id, "partition": partition, "offset": offset}


@app.get("/consumer-groups/{group_id}/lag", response_model=ConsumerGroupLagResponse)
async def get_consumer_group_lag(group_id: str, partition: str):
    """Get lag for a consumer group on a partition."""
    manager = get_consumer_group_manager()
    log = get_log()
    group = manager.get_group(group_id)
    if not group:
        raise HTTPException(status_code=404, detail=f"Consumer group '{group_id}' not found")
    return ConsumerGroupLagResponse(
        partition=partition,
        high_watermark=log.high_watermark,
        group_offsets={group_id: group.get_offset(partition)}
    )


@app.get("/consumer-groups/lag", response_model=ConsumerGroupLagResponse)
async def get_all_consumer_group_lags(partition: str):
    """Get lag for all consumer groups on a partition."""
    manager = get_consumer_group_manager()
    log = get_log()
    lags = manager.get_all_group_lags(partition, log.high_watermark)
    return ConsumerGroupLagResponse(
        partition=partition,
        high_watermark=log.high_watermark,
        group_offsets=lags
    )


# ==================== Cluster Topology Models ====================

class BrokerInfo(BaseModel):
    """Broker information."""
    broker_id: str
    host: str
    port: int
    state: str
    assigned_partitions: list[str]


class PartitionInfo(BaseModel):
    """Partition information."""
    partition_id: str
    topic: str
    leader: Optional[str]
    replicas: list[str]


class TopicInfo(BaseModel):
    """Topic information."""
    topic: str
    partition_count: int
    replication_factor: int
    partitions: list[PartitionInfo]


class ClusterTopologyResponse(BaseModel):
    """Cluster topology response."""
    topics: list[TopicInfo]
    brokers: list[BrokerInfo]


class PartitionLeaderResponse(BaseModel):
    """Partition leader response."""
    topic: str
    partition_id: str
    leader_broker_id: Optional[str]
    traversal: str


class PartitionReassignRequest(BaseModel):
    """Partition reassignment request."""
    topic: str
    partition_id: str
    new_leader: str
    new_replicas: Optional[list[str]] = None


class CreateTopicRequest(BaseModel):
    """Create topic request."""
    topic_name: str
    partition_count: int
    replication_factor: int = 1


# ==================== Cluster Topology Operations ====================

def get_cluster_topology() -> ClusterTopology:
    """Get the global cluster topology instance."""
    if _cluster_topology is None:
        raise RuntimeError("Cluster topology not initialized")
    return _cluster_topology


@app.post("/topics", response_model=TopicInfo)
async def create_topic(request: CreateTopicRequest):
    """Create a new topic with partition assignments."""
    topology = get_cluster_topology()
    topic_node = topology.create_topic(
        request.topic_name,
        request.partition_count,
        request.replication_factor
    )
    return TopicInfo(
        topic=topic_node.topic_name,
        partition_count=topic_node.partition_count,
        replication_factor=topic_node.replication_factor,
        partitions=[
            PartitionInfo(
                partition_id=p.partition_id,
                topic=p.topic,
                leader=p.leader_broker_id,
                replicas=p.replicas
            )
            for p in topic_node.children
        ]
    )


@app.get("/topics", response_model=ClusterTopologyResponse)
async def get_topology(traversal: str = "bfs"):
    """Get full cluster topology (BFS or DFS)."""
    topology = get_cluster_topology()

    if traversal.lower() == "dfs":
        topology_data = topology.get_topology_dfs()
    else:
        topology_data = topology.get_topology_bfs()

    brokers = [
        BrokerInfo(
            broker_id=b.broker_id,
            host=b.host,
            port=b.port,
            state=b.state.value,
            assigned_partitions=b.assigned_partitions
        )
        for b in topology._brokers.values()
    ]

    topics = [
        TopicInfo(
            topic=t["topic"],
            partition_count=t["partition_count"],
            replication_factor=t["replication_factor"],
            partitions=[
                PartitionInfo(
                    partition_id=p["partition_id"],
                    topic=t["topic"],
                    leader=p["leader"],
                    replicas=p["replicas"]
                )
                for p in t["partitions"]
            ]
        )
        for t in topology_data
    ]

    return ClusterTopologyResponse(topics=topics, brokers=brokers)


@app.get("/topology/leader/{topic}/{partition_id}", response_model=PartitionLeaderResponse)
async def get_partition_leader(topic: str, partition_id: str, traversal: str = "bfs"):
    """Find partition leader using BFS or DFS traversal."""
    topology = get_cluster_topology()

    if traversal.lower() == "dfs":
        leader = topology.find_partition_leader_dfs(topic, partition_id)
    else:
        leader = topology.find_partition_leader_bfs(topic, partition_id)

    return PartitionLeaderResponse(
        topic=topic,
        partition_id=partition_id,
        leader_broker_id=leader,
        traversal=traversal.lower()
    )


@app.post("/topology/reassign", response_model=PartitionLeaderResponse)
async def reassign_partition(request: PartitionReassignRequest):
    """Reassign a partition to a new leader (simulates leadership election)."""
    topology = get_cluster_topology()

    success = topology.reassign_partition(
        request.topic,
        request.partition_id,
        request.new_leader,
        request.new_replicas
    )

    if not success:
        raise HTTPException(status_code=400, detail="Reassignment failed")

    leader = topology.find_partition_leader_bfs(request.topic, request.partition_id)

    return PartitionLeaderResponse(
        topic=request.topic,
        partition_id=request.partition_id,
        leader_broker_id=leader,
        traversal="bfs"
    )


@app.get("/cluster/health")
async def get_cluster_health():
    """Get cluster health status."""
    topology = get_cluster_topology()
    return topology.get_cluster_health()


# ==================== Health ====================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


class ReadinessResponse(BaseModel):
    """Readiness probe response."""
    ready: bool
    reason: str
    replica_lag_threshold: int
    max_replica_lag: int
    consumer_groups_lagging: List[str]


@app.get("/health/ready", response_model=ReadinessResponse)
async def readiness_check(
    replica_lag_threshold: int = 10000,
):
    """
    Kubernetes readiness probe.

    Returns ready=False if any replica has fallen behind beyond threshold.
    This prevents K8s from routing client traffic to a broker that cannot
    serve consistent reads.
    """
    log = get_log()

    # Check consumer group lags — a broker serving stale data is not "ready"
    all_lag = log.get_all_consumer_lag()
    lagging_groups = []
    max_lag = 0

    for group_id, info in all_lag.items():
        lag = info["lag"]
        max_lag = max(max_lag, lag)
        if lag > replica_lag_threshold:
            lagging_groups.append(group_id)

    ready = len(lagging_groups) == 0

    reason = "all replicas in sync" if ready else f"{len(lagging_groups)} groups lagging beyond threshold"

    return ReadinessResponse(
        ready=ready,
        reason=reason,
        replica_lag_threshold=replica_lag_threshold,
        max_replica_lag=max_lag,
        consumer_groups_lagging=lagging_groups
    )


def run():
    """Run the broker server."""
    import uvicorn
    uvicorn.run(
        "services.broker.server:app",
        host="0.0.0.0",
        port=int(os.getenv("STREAMCORE_PORT", "8000")),
        reload=False
    )


if __name__ == "__main__":
    run()
