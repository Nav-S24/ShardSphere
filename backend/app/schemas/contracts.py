from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


# ------------------------------------------------------------------------------
# Leader Election Schemas
# ------------------------------------------------------------------------------
class LeaderStatusResponse(BaseModel):
    is_leader: bool
    current_leader_id: Optional[str]
    fencing_token: int
    lease_expires_at: Optional[float]
    time_remaining_seconds: float
    coordinator_instance_id: str


# ------------------------------------------------------------------------------
# Storage Node Schemas
# ------------------------------------------------------------------------------
class NodeStateSummary(BaseModel):
    node_id: str
    node_name: str
    advertised_url: str
    health_state: str  # HEALTHY, SUSPECT, UNAVAILABLE, RECOVERING, DEGRADED
    consecutive_failures: int
    last_heartbeat_at: Optional[float]
    last_failure_at: Optional[float]
    total_chunks: int
    total_bytes_used: int
    storage_limit_bytes: int
    disk_usage_percent: float


class ClusterOverviewResponse(BaseModel):
    cluster_name: str
    coordinator_id: str
    is_leader: bool
    fencing_token: int
    total_nodes: int
    healthy_nodes: int
    suspect_nodes: int
    unavailable_nodes: int
    total_files: int
    total_chunks: int
    total_bytes_stored: int
    configured_replication_factor: int
    active_recovery_jobs: int
    nodes: List[NodeStateSummary]


# ------------------------------------------------------------------------------
# File & Chunk Metadata Schemas
# ------------------------------------------------------------------------------
class ReplicaDetail(BaseModel):
    replica_id: str
    node_id: str
    node_url: str
    status: str
    generation: int
    last_verified_at: Optional[float]


class ChunkDetail(BaseModel):
    chunk_id: str
    sequence_index: int
    size_bytes: int
    checksum_sha256: str
    generation: int
    replicas: List[ReplicaDetail]
    healthy_replicas_count: int


class FileDetailResponse(BaseModel):
    file_id: str
    filename: str
    content_type: str
    size_bytes: int
    checksum_sha256: str
    chunk_count: int
    chunk_size_bytes: int
    replication_factor: int
    metadata_version: int
    status: str
    created_at: float
    updated_at: float
    chunks: List[ChunkDetail]


class FileListItem(BaseModel):
    file_id: str
    filename: str
    size_bytes: int
    checksum_sha256: str
    chunk_count: int
    replication_factor: int
    metadata_version: int
    status: str
    created_at: float


class FileListResponse(BaseModel):
    total_files: int
    page: int
    page_size: int
    files: List[FileListItem]


# ------------------------------------------------------------------------------
# Recovery Job Schemas
# ------------------------------------------------------------------------------
class RecoveryJobResponse(BaseModel):
    job_id: str
    chunk_id: str
    file_id: str
    source_node_id: str
    target_node_id: str
    generation: int
    status: str
    attempts: int
    error_message: Optional[str]
    created_at: float
    completed_at: Optional[float]


# ------------------------------------------------------------------------------
# Cluster Event Schemas
# ------------------------------------------------------------------------------
class ClusterEventResponse(BaseModel):
    event_id: str
    event_type: str
    severity: str
    node_id: Optional[str]
    file_id: Optional[str]
    message: str
    timestamp: float


# ------------------------------------------------------------------------------
# Integrity Verification Schemas
# ------------------------------------------------------------------------------
class IntegrityVerificationRequest(BaseModel):
    file_id: Optional[str] = None
    chunk_id: Optional[str] = None


class IntegrityVerificationResult(BaseModel):
    checked_files: int
    checked_chunks: int
    valid_replicas: int
    corrupted_replicas: int
    missing_replicas: int
    details: List[Dict[str, Any]]
