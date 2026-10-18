import asyncio
import time
import uuid
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import select, func, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.metadata.database import AsyncSessionLocal
from app.models.metadata import (
    FileMetadata,
    ChunkMetadata,
    ReplicaMetadata,
    StorageNodeRecord,
    RecoveryJob,
    ClusterEvent,
)
from app.storage.distributor import distributor
from app.integrity.checksum import compute_sha256, verify_checksum
from app.election.lease import leader_manager


class CoordinatorService:
    """
    Central distributed coordinator implementing Phase 1 file operations,
    placement coordination, consistency enforcement, and integrity verification.
    """

    async def upload_file(
        self,
        session: AsyncSession,
        filename: str,
        content_type: str,
        data: bytes,
    ) -> FileMetadata:
        """
        Full file upload pipeline with chunking, placement, parallel writes,
        checksum verification, and transactional commit.
        """
        # 1. Authorize: Coordinator must hold valid leader lease
        if not leader_manager.is_leader:
            # Attempt to renew or acquire
            is_lead, token, holder = await leader_manager.try_acquire_or_renew()
            if not is_lead:
                raise RuntimeError(f"Cannot accept write: instance is not cluster leader (Current leader: {holder})")

        # 2. File metadata preparation
        file_id = f"file_{uuid.uuid4().hex}"
        total_size = len(data)
        file_sha256 = compute_sha256(data)
        now = time.time()

        if total_size > settings.MAX_FILE_SIZE_BYTES:
            raise ValueError(f"File size exceeds maximum allowed ({settings.MAX_FILE_SIZE_BYTES} bytes)")

        # 3. Split into 1 MiB chunks
        chunk_tuples = distributor.split_into_chunks(data, settings.DEFAULT_CHUNK_SIZE_BYTES)
        chunk_count = len(chunk_tuples)

        file_record = FileMetadata(
            file_id=file_id,
            filename=filename,
            content_type=content_type or "application/octet-stream",
            size_bytes=total_size,
            checksum_sha256=file_sha256,
            chunk_count=chunk_count,
            chunk_size_bytes=settings.DEFAULT_CHUNK_SIZE_BYTES,
            replication_factor=settings.REPLICATION_FACTOR,
            metadata_version=1,
            status="pending",
            created_at=now,
            updated_at=now,
        )
        session.add(file_record)
        await session.flush()

        # 4. For each chunk: allocate placement, write to distinct nodes
        written_chunks: List[Tuple[ChunkMetadata, List[ReplicaMetadata]]] = []

        try:
            for seq_idx, chunk_bytes, chunk_sha in chunk_tuples:
                chunk_id = f"chk_{uuid.uuid4().hex[:16]}"
                target_nodes = await distributor.select_placement_nodes(
                    session=session,
                    sequence_index=seq_idx,
                    replication_factor=settings.REPLICATION_FACTOR,
                )

                chunk_record = ChunkMetadata(
                    chunk_id=chunk_id,
                    file_id=file_id,
                    sequence_index=seq_idx,
                    size_bytes=len(chunk_bytes),
                    checksum_sha256=chunk_sha,
                    generation=1,
                    created_at=now,
                )
                session.add(chunk_record)

                # Parallel writes to selected nodes
                write_tasks = [
                    distributor.write_chunk_replica(
                        node_url=node.advertised_url,
                        chunk_id=chunk_id,
                        chunk_data=chunk_bytes,
                        expected_checksum=chunk_sha,
                    )
                    for node in target_nodes
                ]
                write_results = await asyncio.gather(*write_tasks, return_exceptions=True)

                # Verify durability condition: all required replicas successfully acknowledged
                successful_nodes: List[StorageNodeRecord] = []
                for node, res in zip(target_nodes, write_results):
                    if res is True:
                        successful_nodes.append(node)

                if len(successful_nodes) < settings.REPLICATION_FACTOR:
                    raise RuntimeError(
                        f"Durability requirement not met for chunk {seq_idx}: "
                        f"needed {settings.REPLICATION_FACTOR} replicas, only {len(successful_nodes)} acknowledged."
                    )

                # Persist replica placements
                replicas: List[ReplicaMetadata] = []
                for node in successful_nodes:
                    replica = ReplicaMetadata(
                        replica_id=f"rep_{uuid.uuid4().hex[:12]}",
                        chunk_id=chunk_id,
                        node_id=node.node_id,
                        node_url=node.advertised_url,
                        status="active",
                        generation=1,
                        last_verified_at=now,
                        created_at=now,
                    )
                    session.add(replica)
                    replicas.append(replica)

                written_chunks.append((chunk_record, replicas))

            # 5. Commit protocol: verify fencing token before publishing as 'committed'
            await leader_manager.validate_active_lease(session, leader_manager.fencing_token)

            file_record.status = "committed"
            file_record.metadata_version += 1
            file_record.updated_at = time.time()

            session.add(
                ClusterEvent(
                    event_id=f"evt_{int(now * 1000)}_upload_{file_id[:8]}",
                    event_type="FILE_COMMITTED",
                    severity="INFO",
                    file_id=file_id,
                    message=f"File '{filename}' ({total_size} bytes, {chunk_count} chunks, {settings.REPLICATION_FACTOR}x replication) successfully committed with Fencing Token {leader_manager.fencing_token}.",
                    timestamp=time.time(),
                )
            )

            await session.commit()
            return file_record

        except Exception as e:
            await session.rollback()
            # Clean up any physically written chunks
            for chk, reps in written_chunks:
                cleanup_tasks = [
                    distributor.delete_chunk_replica(r.node_url, chk.chunk_id)
                    for r in reps
                ]
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            raise RuntimeError(f"Upload failed and rolled back: {str(e)}")

    async def reconstruct_file(self, session: AsyncSession, file_id: str) -> Tuple[str, str, bytes]:
        """
        Retrieves committed file chunks in sequence, verifies SHA-256 per chunk,
        falls back to healthy replicas if any is corrupted, and validates full file SHA-256.
        Returns (filename, content_type, file_bytes).
        """
        result = await session.execute(
            select(FileMetadata)
            .where(FileMetadata.file_id == file_id)
            .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
        )
        file_record = result.scalars().first()
        if not file_record:
            raise ValueError(f"File '{file_id}' not found")

        if file_record.status not in ("committed", "degraded"):
            raise ValueError(f"File '{file_id}' is in status '{file_record.status}' and cannot be downloaded")

        assembled_bytes = bytearray()

        for chunk in sorted(file_record.chunks, key=lambda c: c.sequence_index):
            chunk_data: Optional[bytes] = None

            # Try active replicas
            for replica in chunk.replicas:
                if replica.status == "active":
                    data = await distributor.fetch_chunk_replica(
                        node_url=replica.node_url,
                        chunk_id=chunk.chunk_id,
                        expected_checksum=chunk.checksum_sha256,
                    )
                    if data is not None:
                        chunk_data = data
                        break

            if chunk_data is None:
                raise RuntimeError(
                    f"Recoverability Error: Chunk index {chunk.sequence_index} ({chunk.chunk_id}) "
                    f"could not be retrieved from any replica!"
                )

            assembled_bytes.extend(chunk_data)

        # Verify full file checksum
        full_data = bytes(assembled_bytes)
        calculated_sha = compute_sha256(full_data)
        if not verify_checksum(calculated_sha, file_record.checksum_sha256):
            raise RuntimeError("Corrupted download: final file checksum did not match authoritative metadata!")

        return file_record.filename, file_record.content_type, full_data

    async def delete_file(self, session: AsyncSession, file_id: str) -> bool:
        """
        Coordinated safe file deletion:
        1. Verify active leader lease and fencing token
        2. Set status to 'deleting'
        3. Delete chunks from all replica nodes
        4. Remove metadata from database
        """
        await leader_manager.validate_active_lease(session, leader_manager.fencing_token)

        result = await session.execute(
            select(FileMetadata)
            .where(FileMetadata.file_id == file_id)
            .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
        )
        file_record = result.scalars().first()
        if not file_record:
            return False

        file_record.status = "deleting"
        file_record.updated_at = time.time()
        await session.flush()

        # Delete physical chunks
        delete_tasks = []
        for chunk in file_record.chunks:
            for replica in chunk.replicas:
                delete_tasks.append(distributor.delete_chunk_replica(replica.node_url, chunk.chunk_id))

        if delete_tasks:
            await asyncio.gather(*delete_tasks, return_exceptions=True)

        await session.delete(file_record)
        session.add(
            ClusterEvent(
                event_id=f"evt_{int(time.time() * 1000)}_del_{file_id[:8]}",
                event_type="FILE_DELETED",
                severity="INFO",
                file_id=file_id,
                message=f"File '{file_record.filename}' ({file_id}) deleted and storage reclaimed.",
                timestamp=time.time(),
            )
        )
        await session.commit()
        return True

    async def get_cluster_overview(self, session: AsyncSession) -> Dict[str, Any]:
        """Gathers real-time cluster metrics from PostgreSQL and node states."""
        leader_status = await leader_manager.get_status()

        nodes_res = await session.execute(select(StorageNodeRecord))
        nodes = nodes_res.scalars().all()

        total_nodes = len(nodes)
        healthy_nodes = sum(1 for n in nodes if n.health_state == "HEALTHY")
        suspect_nodes = sum(1 for n in nodes if n.health_state == "SUSPECT")
        unavailable_nodes = sum(1 for n in nodes if n.health_state == "UNAVAILABLE")

        files_res = await session.execute(
            select(
                func.count(FileMetadata.file_id),
                func.coalesce(func.sum(FileMetadata.size_bytes), 0)
            ).where(FileMetadata.status.in_(["committed", "degraded"]))
        )
        files_count, total_bytes = files_res.first() or (0, 0)

        chunks_count_res = await session.execute(select(func.count(ChunkMetadata.chunk_id)))
        total_chunks = chunks_count_res.scalar() or 0

        recovery_jobs_res = await session.execute(
            select(func.count(RecoveryJob.job_id)).where(RecoveryJob.status.in_(["pending", "in_progress"]))
        )
        active_jobs = recovery_jobs_res.scalar() or 0

        node_summaries = []
        for n in nodes:
            used = n.total_bytes_used
            limit = n.storage_limit_bytes or 1
            pct = round((used / limit) * 100.0, 2)
            node_summaries.append({
                "node_id": n.node_id,
                "node_name": n.node_name,
                "advertised_url": n.advertised_url,
                "health_state": n.health_state,
                "consecutive_failures": n.consecutive_failures,
                "last_heartbeat_at": n.last_heartbeat_at,
                "last_failure_at": n.last_failure_at,
                "total_chunks": n.total_chunks,
                "total_bytes_used": n.total_bytes_used,
                "storage_limit_bytes": n.storage_limit_bytes,
                "disk_usage_percent": pct,
            })

        return {
            "cluster_name": "ShardSphere-Cluster-Alpha",
            "coordinator_id": settings.COORDINATOR_ID,
            "is_leader": leader_status["is_leader"],
            "fencing_token": leader_status["fencing_token"],
            "total_nodes": total_nodes,
            "healthy_nodes": healthy_nodes,
            "suspect_nodes": suspect_nodes,
            "unavailable_nodes": unavailable_nodes,
            "total_files": files_count,
            "total_chunks": total_chunks,
            "total_bytes_stored": total_bytes,
            "configured_replication_factor": settings.REPLICATION_FACTOR,
            "active_recovery_jobs": active_jobs,
            "nodes": node_summaries,
        }


coordinator_service = CoordinatorService()
