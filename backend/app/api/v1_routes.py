import time
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, Response, status
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.metadata.database import get_db
from app.models.metadata import (
    FileMetadata,
    ChunkMetadata,
    ReplicaMetadata,
    StorageNodeRecord,
    RecoveryJob,
    ClusterEvent,
)
from app.schemas.contracts import (
    LeaderStatusResponse,
    ClusterOverviewResponse,
    FileListResponse,
    FileListItem,
    FileDetailResponse,
    ChunkDetail,
    ReplicaDetail,
    RecoveryJobResponse,
    ClusterEventResponse,
    IntegrityVerificationRequest,
    IntegrityVerificationResult,
    NodeStateSummary,
)
from app.coordinator.service import coordinator_service
from app.election.lease import leader_manager
from app.failure_detection.detector import detector
from app.storage.distributor import distributor

router = APIRouter(prefix="/api/v1")


@router.get("/health", tags=["System"])
async def coordinator_health():
    """Returns basic coordinator health status."""
    return {"status": "healthy", "service": "coordinator", "timestamp": time.time()}


@router.get("/ready", tags=["System"])
async def coordinator_ready(db: AsyncSession = Depends(get_db)):
    """Verifies database connectivity and readiness."""
    try:
        await db.execute(select(1))
        return {"status": "ready", "database": "connected", "timestamp": time.time()}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database not ready: {str(e)}")


@router.get("/leader", response_model=LeaderStatusResponse, tags=["Coordination"])
async def get_leader_status():
    """Exposes current leader election lease holder and fencing token."""
    status_info = await leader_manager.get_status()
    return LeaderStatusResponse(**status_info)


@router.get("/cluster", response_model=ClusterOverviewResponse, tags=["Cluster"])
async def get_cluster_overview(db: AsyncSession = Depends(get_db)):
    """Returns cluster nodes, file counts, storage metrics, and leader status."""
    overview = await coordinator_service.get_cluster_overview(db)
    return ClusterOverviewResponse(**overview)


@router.get("/nodes", response_model=List[NodeStateSummary], tags=["Cluster"])
async def list_storage_nodes(db: AsyncSession = Depends(get_db)):
    """Lists all registered storage nodes and their real-time health metrics."""
    result = await db.execute(select(StorageNodeRecord))
    nodes = result.scalars().all()
    summaries = []
    for n in nodes:
        used = n.total_bytes_used
        limit = n.storage_limit_bytes or 1
        pct = round((used / limit) * 100.0, 2)
        summaries.append(
            NodeStateSummary(
                node_id=n.node_id,
                node_name=n.node_name,
                advertised_url=n.advertised_url,
                health_state=n.health_state,
                consecutive_failures=n.consecutive_failures,
                last_heartbeat_at=n.last_heartbeat_at,
                last_failure_at=n.last_failure_at,
                total_chunks=n.total_chunks,
                total_bytes_used=n.total_bytes_used,
                storage_limit_bytes=n.storage_limit_bytes,
                disk_usage_percent=pct,
            )
        )
    return summaries


@router.post("/files", status_code=status.HTTP_201_CREATED, tags=["Files"])
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Accepts file upload, splits into chunks, computes SHA-256 checksums,
    distributes 3x replicas across distinct storage nodes, and transactionally commits metadata.
    """
    try:
        content = await file.read()
        file_rec = await coordinator_service.upload_file(
            session=db,
            filename=file.filename or "unnamed_file",
            content_type=file.content_type or "application/octet-stream",
            data=content,
        )
        return {
            "file_id": file_rec.file_id,
            "filename": file_rec.filename,
            "size_bytes": file_rec.size_bytes,
            "checksum_sha256": file_rec.checksum_sha256,
            "chunk_count": file_rec.chunk_count,
            "replication_factor": file_rec.replication_factor,
            "status": file_rec.status,
            "metadata_version": file_rec.metadata_version,
            "created_at": file_rec.created_at,
        }
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except RuntimeError as re:
        raise HTTPException(status_code=500, detail=str(re))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload error: {str(e)}")


@router.get("/files", response_model=FileListResponse, tags=["Files"])
async def list_files(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Lists committed files with pagination."""
    offset = (page - 1) * page_size
    query = (
        select(FileMetadata)
        .where(FileMetadata.status.in_(["committed", "degraded"]))
        .order_by(desc(FileMetadata.created_at))
        .offset(offset)
        .limit(page_size)
    )
    result = await db.execute(query)
    files = result.scalars().all()

    from sqlalchemy import func
    count_res = await db.execute(
        select(func.count(FileMetadata.file_id)).where(FileMetadata.status.in_(["committed", "degraded"]))
    )
    total = count_res.scalar() or 0

    return FileListResponse(
        total_files=total,
        page=page,
        page_size=page_size,
        files=[
            FileListItem(
                file_id=f.file_id,
                filename=f.filename,
                size_bytes=f.size_bytes,
                checksum_sha256=f.checksum_sha256,
                chunk_count=f.chunk_count,
                replication_factor=f.replication_factor,
                metadata_version=f.metadata_version,
                status=f.status,
                created_at=f.created_at,
            )
            for f in files
        ],
    )


@router.get("/files/{file_id}", response_model=FileDetailResponse, tags=["Files"])
async def get_file_detail(file_id: str, db: AsyncSession = Depends(get_db)):
    """Returns detailed chunk layout, replica status, and node assignments for a file."""
    result = await db.execute(
        select(FileMetadata)
        .where(FileMetadata.file_id == file_id)
        .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
    )
    file_record = result.scalars().first()
    if not file_record:
        raise HTTPException(status_code=404, detail="File not found")

    chunk_details = []
    for c in sorted(file_record.chunks, key=lambda x: x.sequence_index):
        replicas = [
            ReplicaDetail(
                replica_id=r.replica_id,
                node_id=r.node_id,
                node_url=r.node_url,
                status=r.status,
                generation=r.generation,
                last_verified_at=r.last_verified_at,
            )
            for r in c.replicas
        ]
        healthy_count = sum(1 for r in replicas if r.status == "active")
        chunk_details.append(
            ChunkDetail(
                chunk_id=c.chunk_id,
                sequence_index=c.sequence_index,
                size_bytes=c.size_bytes,
                checksum_sha256=c.checksum_sha256,
                generation=c.generation,
                replicas=replicas,
                healthy_replicas_count=healthy_count,
            )
        )

    return FileDetailResponse(
        file_id=file_record.file_id,
        filename=file_record.filename,
        content_type=file_record.content_type,
        size_bytes=file_record.size_bytes,
        checksum_sha256=file_record.checksum_sha256,
        chunk_count=file_record.chunk_count,
        chunk_size_bytes=file_record.chunk_size_bytes,
        replication_factor=file_record.replication_factor,
        metadata_version=file_record.metadata_version,
        status=file_record.status,
        created_at=file_record.created_at,
        updated_at=file_record.updated_at,
        chunks=chunk_details,
    )


@router.get("/files/{file_id}/download", tags=["Files"])
async def download_file(file_id: str, db: AsyncSession = Depends(get_db)):
    """Reconstructs file byte-stream from chunks and returns full validated file."""
    try:
        filename, content_type, data = await coordinator_service.reconstruct_file(db, file_id)
        return Response(
            content=data,
            media_type=content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(data)),
            },
        )
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except RuntimeError as re:
        raise HTTPException(status_code=500, detail=str(re))


@router.delete("/files/{file_id}", tags=["Files"])
async def delete_file(file_id: str, db: AsyncSession = Depends(get_db)):
    """Deletes all chunks physically from nodes and marks metadata deleted."""
    success = await coordinator_service.delete_file(db, file_id)
    if not success:
        raise HTTPException(status_code=404, detail="File not found")
    return {"file_id": file_id, "deleted": True}


@router.get("/recovery/jobs", response_model=List[RecoveryJobResponse], tags=["Self-Healing"])
async def list_recovery_jobs(db: AsyncSession = Depends(get_db)):
    """Lists self-healing re-replication jobs."""
    result = await db.execute(select(RecoveryJob).order_by(desc(RecoveryJob.created_at)).limit(50))
    jobs = result.scalars().all()
    return [
        RecoveryJobResponse(
            job_id=j.job_id,
            chunk_id=j.chunk_id,
            file_id=j.file_id,
            source_node_id=j.source_node_id,
            target_node_id=j.target_node_id,
            generation=j.generation,
            status=j.status,
            attempts=j.attempts,
            error_message=j.error_message,
            created_at=j.created_at,
            completed_at=j.completed_at,
        )
        for j in jobs
    ]


@router.get("/events", response_model=List[ClusterEventResponse], tags=["Events"])
async def list_cluster_events(
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Returns cluster event history (failures, recoveries, uploads, elections)."""
    result = await db.execute(select(ClusterEvent).order_by(desc(ClusterEvent.timestamp)).limit(limit))
    events = result.scalars().all()
    return [
        ClusterEventResponse(
            event_id=e.event_id,
            event_type=e.event_type,
            severity=e.severity,
            node_id=e.node_id,
            file_id=e.file_id,
            message=e.message,
            timestamp=e.timestamp,
        )
        for e in events
    ]


@router.post("/integrity/verify", response_model=IntegrityVerificationResult, tags=["Integrity"])
async def trigger_integrity_verification(
    request: IntegrityVerificationRequest,
    db: AsyncSession = Depends(get_db),
):
    """Triggers on-demand cryptographic verification of stored chunk replicas."""
    query = select(FileMetadata).where(FileMetadata.status == "committed")
    if request.file_id:
        query = query.where(FileMetadata.file_id == request.file_id)

    query = query.options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
    files = (await db.execute(query)).scalars().all()

    checked_files = len(files)
    checked_chunks = 0
    valid_reps = 0
    corrupt_reps = 0
    missing_reps = 0
    details = []

    for f in files:
        for c in f.chunks:
            if request.chunk_id and c.chunk_id != request.chunk_id:
                continue
            checked_chunks += 1
            for r in c.replicas:
                # Query node for verification
                import httpx
                async with httpx.AsyncClient(timeout=5.0) as client:
                    try:
                        v_resp = await client.get(f"{r.node_url.rstrip('/')}/chunks/{c.chunk_id}/verify")
                        if v_resp.status_code == 200:
                            v_data = v_resp.json()
                            status_val = v_data.get("status")
                            if status_val == "valid":
                                valid_reps += 1
                            elif status_val == "corrupted":
                                corrupt_reps += 1
                                r.status = "corrupted"
                            else:
                                missing_reps += 1
                                r.status = "missing"
                            details.append({"chunk_id": c.chunk_id, "node_id": r.node_id, "status": status_val})
                        else:
                            missing_reps += 1
                            r.status = "missing"
                    except Exception as e:
                        missing_reps += 1

    await db.commit()

    return IntegrityVerificationResult(
        checked_files=checked_files,
        checked_chunks=checked_chunks,
        valid_replicas=valid_reps,
        corrupted_replicas=corrupt_reps,
        missing_replicas=missing_reps,
        details=details,
    )


@router.post("/self-healing/reconcile", tags=["Self-Healing"])
async def trigger_reconciliation():
    """Triggers an immediate scan and recovery pass across all under-replicated chunks."""
    from app.replication.reconciler import reconciler
    recovered = await reconciler.scan_and_reconcile()
    return {"status": "completed", "recovered_chunks": recovered, "timestamp": time.time()}


@router.post("/nodes/{node_id}/toggle-state", tags=["Fault Injection"])
async def toggle_node_state(node_id: str, db: AsyncSession = Depends(get_db)):
    """Toggles node health state between HEALTHY and UNAVAILABLE to simulate failure or network partitions."""
    node = await db.get(StorageNodeRecord, node_id)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found")

    new_state = "UNAVAILABLE" if node.health_state in ("HEALTHY", "RECOVERING") else "HEALTHY"
    old_state = node.health_state
    node.health_state = new_state
    if new_state == "UNAVAILABLE":
        node.consecutive_failures = 5
        node.last_failure_at = time.time()
    else:
        node.consecutive_failures = 0

    db.add(
        ClusterEvent(
            event_id=f"evt_{int(time.time() * 1000)}_{node_id}_sim",
            event_type="NODE_HEALTH_CHANGED",
            severity="WARNING" if new_state == "UNAVAILABLE" else "INFO",
            node_id=node_id,
            message=f"Operator manually toggled node '{node_id}' from {old_state} to {new_state}.",
            timestamp=time.time(),
        )
    )
    await db.commit()
    return {"node_id": node_id, "previous_state": old_state, "new_state": new_state}


@router.post("/nodes/{node_id}/reconcile", tags=["Self-Healing"])
async def trigger_node_reconciliation(node_id: str):
    """Triggers returning-node reconciliation on an individual node."""
    from app.replication.reconciler import reconciler
    summary = await reconciler.reconcile_returning_node(node_id)
    return summary


@router.post("/leader/step-down", tags=["Coordination"])
async def leader_step_down(db: AsyncSession = Depends(get_db)):
    """Simulates leader step down / lease expiration for election testing."""
    from app.models.metadata import LeaderLease
    lease = (await db.execute(select(LeaderLease).where(LeaderLease.lease_id == "coordinator_primary_lease"))).scalars().first()
    if lease:
        lease.expires_at = time.time() - 1.0 # Force expired
        leader_manager.is_leader = False
        db.add(
            ClusterEvent(
                event_id=f"evt_{int(time.time() * 1000)}_stepdown",
                event_type="LEADER_ELECTED",
                severity="WARNING",
                message=f"Leader '{lease.holder_id}' voluntarily stepped down (lease expired).",
                timestamp=time.time(),
            )
        )
        await db.commit()
        return {"status": "stepped_down", "previous_holder": lease.holder_id, "fencing_token": lease.fencing_token}
    return {"status": "no_lease_found"}
