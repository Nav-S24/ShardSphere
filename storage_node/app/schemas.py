from pydantic import BaseModel, Field
from typing import Optional, List


class NodeHealthResponse(BaseModel):
    status: str = Field(..., description="Node operational status, e.g. healthy, degraded")
    node_id: str
    node_name: str
    uptime_seconds: float
    timestamp: float
    version: str = "1.0.0"


class NodeInfoResponse(BaseModel):
    node_id: str
    node_name: str
    host: str
    port: int
    advertised_url: str
    storage_dir: str
    storage_limit_bytes: int
    version: str = "1.0.0"


class StorageStatsResponse(BaseModel):
    node_id: str
    total_chunks: int
    total_bytes_used: int
    storage_limit_bytes: int
    free_bytes: int
    disk_total_bytes: int
    disk_free_bytes: int
    disk_usage_percent: float


class ChunkWriteResponse(BaseModel):
    chunk_id: str
    size_bytes: int
    checksum_sha256: str
    node_id: str
    is_new: bool
    status: str = "stored"


class ChunkVerifyResponse(BaseModel):
    chunk_id: str
    node_id: str
    status: str  # "valid", "corrupted", "missing"
    stored_checksum: Optional[str] = None
    actual_checksum: Optional[str] = None
    size_bytes: Optional[int] = None
    verified_at: float


class ChunkListResponse(BaseModel):
    node_id: str
    total_chunks: int
    chunks: List[str]
