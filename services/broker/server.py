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
    segment_size = int(os.getenv("STREAMCORE_SEGMENT_SIZE", 1024 * 1024))  # 1MB default for testing

    _log = PartitionLog(
        name="default",
        log_dir=log_dir,
        segment_max_size=segment_size
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


class LogInfoResponse(BaseModel):
    """Information about the partition log."""
    name: str
    record_count: int
    high_watermark: int
    segment_count: int
    sealed_segment_count: int
    segments: list[SegmentInfo]


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

    return LogInfoResponse(
        name=log.name,
        record_count=log.record_count,
        high_watermark=log.high_watermark,
        segment_count=log.segment_count,
        sealed_segment_count=log.sealed_segment_count,
        segments=[
            SegmentInfo(**s) for s in log.get_segments_info()
        ]
    )


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
