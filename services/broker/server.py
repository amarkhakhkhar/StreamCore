"""
StreamCore Broker - Main entry point.

A lightweight broker service exposing the partition log via HTTP API.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .partition_log import PartitionLog


# Global partition log instance
_log: Optional[PartitionLog] = None


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

    yield

    _log.close()
    _log = None


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


# ==================== Log Operations ====================

@app.post("/append", response_model=AppendResponse)
async def append_record(request: AppendRequest):
    """Append a record to the partition log."""
    log = get_log()
    offset = log.append(request.data.encode(), request.timestamp)
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


# ==================== Health ====================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


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
