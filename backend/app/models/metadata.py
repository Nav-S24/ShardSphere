import time
from typing import List, Optional
from sqlalchemy import (
    Column,
    String,
    Integer,
    BigInteger,
    Float,
    Text,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class FileMetadata(Base):
    """
    Authoritative record for an uploaded file in ShardSphere.
    Guarantees version tracking and commit state lifecycle.
    """
    __tablename__ = "files"

    file_id = Column(String(64), primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    content_type = Column(String(128), default="application/octet-stream")
    size_bytes = Column(BigInteger, nullable=False)
    checksum_sha256 = Column(String(64), nullable=False, index=True)
    chunk_count = Column(Integer, nullable=False)
    chunk_size_bytes = Column(Integer, nullable=False)
    replication_factor = Column(Integer, nullable=False, default=3)
    metadata_version = Column(Integer, nullable=False, default=1)
    # Lifecycle: pending -> committed -> degraded -> deleting -> deleted
    status = Column(String(32), nullable=False, default="pending", index=True)
    created_at = Column(Float, default=lambda: time.time(), nullable=False)
    updated_at = Column(Float, default=lambda: time.time(), onupdate=lambda: time.time(), nullable=False)

    chunks = relationship("ChunkMetadata", back_populates="file", cascade="all, delete-orphan", order_by="ChunkMetadata.sequence_index")


class ChunkMetadata(Base):
    """
    Represents an individual 1 MiB chunk belonging to a file.
    Tracks exact chunk sequence index, size, checksum, and generation for stale prevention.
    """
    __tablename__ = "chunks"

    chunk_id = Column(String(64), primary_key=True, index=True)
    file_id = Column(String(64), ForeignKey("files.file_id", ondelete="CASCADE"), nullable=False, index=True)
    sequence_index = Column(Integer, nullable=False)
    size_bytes = Column(Integer, nullable=False)
    checksum_sha256 = Column(String(64), nullable=False)
    generation = Column(Integer, nullable=False, default=1)
    created_at = Column(Float, default=lambda: time.time(), nullable=False)

    file = relationship("FileMetadata", back_populates="chunks")
    replicas = relationship("ReplicaMetadata", back_populates="chunk", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("file_id", "sequence_index", name="uq_file_chunk_sequence"),
        Index("ix_chunks_file_seq", "file_id", "sequence_index"),
    )


class ReplicaMetadata(Base):
    """
    Physical placement of a chunk on a specific storage node.
    """
    __tablename__ = "replicas"

    replica_id = Column(String(64), primary_key=True)
    chunk_id = Column(String(64), ForeignKey("chunks.chunk_id", ondelete="CASCADE"), nullable=False, index=True)
    node_id = Column(String(64), nullable=False, index=True)
    node_url = Column(String(255), nullable=False)
    # State: active, corrupted, stale, unverified, deleted
    status = Column(String(32), nullable=False, default="active", index=True)
    generation = Column(Integer, nullable=False, default=1)
    last_verified_at = Column(Float, nullable=True)
    created_at = Column(Float, default=lambda: time.time(), nullable=False)

    chunk = relationship("ChunkMetadata", back_populates="replicas")

    __table_args__ = (
        UniqueConstraint("chunk_id", "node_id", name="uq_chunk_node_replica"),
    )


class StorageNodeRecord(Base):
    """
    Cluster node registry updated by the failure detector and node discovery.
    """
    __tablename__ = "storage_nodes"

    node_id = Column(String(64), primary_key=True)
    node_name = Column(String(128), nullable=False)
    advertised_url = Column(String(255), nullable=False)
    # Health states: HEALTHY, SUSPECT, UNAVAILABLE, RECOVERING, DEGRADED
    health_state = Column(String(32), nullable=False, default="HEALTHY", index=True)
    consecutive_failures = Column(Integer, default=0, nullable=False)
    last_heartbeat_at = Column(Float, nullable=True)
    last_failure_at = Column(Float, nullable=True)
    total_chunks = Column(Integer, default=0, nullable=False)
    total_bytes_used = Column(BigInteger, default=0, nullable=False)
    storage_limit_bytes = Column(BigInteger, default=10737418240, nullable=False)
    updated_at = Column(Float, default=lambda: time.time(), onupdate=lambda: time.time(), nullable=False)


class LeaderLease(Base):
    """
    PostgreSQL-backed lease table for leader election.
    Uses monotonically increasing fencing tokens to prevent split-brain / stale writes.
    """
    __tablename__ = "leader_leases"

    lease_id = Column(String(64), primary_key=True, default="coordinator_primary_lease")
    holder_id = Column(String(64), nullable=False)
    fencing_token = Column(BigInteger, nullable=False, default=1)
    acquired_at = Column(Float, nullable=False)
    expires_at = Column(Float, nullable=False, index=True)
    last_renewed_at = Column(Float, nullable=False)


class RecoveryJob(Base):
    """
    Records an automatic re-replication task when a node fails or replica is lost.
    """
    __tablename__ = "recovery_jobs"

    job_id = Column(String(64), primary_key=True)
    chunk_id = Column(String(64), nullable=False, index=True)
    file_id = Column(String(64), nullable=False, index=True)
    source_node_id = Column(String(64), nullable=False)
    target_node_id = Column(String(64), nullable=False)
    generation = Column(Integer, default=1, nullable=False)
    # Status: pending, in_progress, completed, failed
    status = Column(String(32), default="pending", nullable=False, index=True)
    attempts = Column(Integer, default=0, nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(Float, default=lambda: time.time(), nullable=False)
    completed_at = Column(Float, nullable=True)


class ClusterEvent(Base):
    """
    Cluster audit log for topology shifts, leader elections, and recovery operations.
    """
    __tablename__ = "cluster_events"

    event_id = Column(String(64), primary_key=True)
    event_type = Column(String(64), nullable=False, index=True)
    severity = Column(String(16), nullable=False, default="INFO") # INFO, WARNING, ERROR, CRITICAL
    node_id = Column(String(64), nullable=True)
    file_id = Column(String(64), nullable=True)
    message = Column(Text, nullable=False)
    timestamp = Column(Float, default=lambda: time.time(), nullable=False, index=True)
