import asyncio
import time
import uuid
from typing import Optional, List
from sqlalchemy import select, and_, func
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
from app.integrity.checksum import verify_checksum


class ReplicationReconciler:
    """
    Self-healing engine: discovers under-replicated chunks,
    schedules recovery jobs, copies from surviving healthy replicas,
    and updates metadata transactionally.
    """

    def __init__(self):
        self._running = False
        self._reconcile_task: Optional[asyncio.Task] = None
        self._active_jobs_count = 0

    async def scan_and_reconcile(self) -> int:
        """
        Scans all committed files to find chunks with replica count < REPLICATION_FACTOR.
        Schedules and executes recovery jobs.
        """
        recovered_count = 0
        now = time.time()

        async with AsyncSessionLocal() as session:
            # 1. Fetch all healthy nodes
            nodes_res = await session.execute(
                select(StorageNodeRecord).where(StorageNodeRecord.health_state == "HEALTHY")
            )
            healthy_nodes = {n.node_id: n for n in nodes_res.scalars().all()}

            if not healthy_nodes:
                return 0

            # 2. Fetch committed files and their chunks with replicas
            files_res = await session.execute(
                select(FileMetadata)
                .where(FileMetadata.status == "committed")
                .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
            )
            files = files_res.scalars().all()

            for file_record in files:
                for chunk in file_record.chunks:
                    # Filter for replicas on healthy nodes with active status
                    healthy_replicas = [
                        r for r in chunk.replicas
                        if r.status == "active" and r.node_id in healthy_nodes
                    ]

                    current_replica_count = len(healthy_replicas)
                    desired = file_record.replication_factor

                    if current_replica_count < desired and current_replica_count > 0:
                        # Chunk is under-replicated but surviving!
                        source_replica = healthy_replicas[0]
                        existing_node_ids = {r.node_id for r in chunk.replicas if r.status == "active"}

                        # Find eligible target node not currently having this chunk
                        eligible_targets = [
                            n for n in healthy_nodes.values()
                            if n.node_id not in existing_node_ids
                        ]

                        if not eligible_targets:
                            # No eligible healthy nodes to replicate to right now
                            continue

                        target_node = eligible_targets[0]
                        job_id = f"job_{uuid.uuid4().hex[:12]}"

                        # Check if duplicate job already pending/running
                        existing_job = await session.execute(
                            select(RecoveryJob).where(
                                and_(
                                    RecoveryJob.chunk_id == chunk.chunk_id,
                                    RecoveryJob.status.in_(["pending", "in_progress"]),
                                )
                            )
                        )
                        if existing_job.scalars().first():
                            continue

                        # Create and record recovery job
                        recovery_job = RecoveryJob(
                            job_id=job_id,
                            chunk_id=chunk.chunk_id,
                            file_id=file_record.file_id,
                            source_node_id=source_replica.node_id,
                            target_node_id=target_node.node_id,
                            generation=chunk.generation,
                            status="in_progress",
                            attempts=1,
                            created_at=now,
                        )
                        session.add(recovery_job)
                        await session.commit()

                        # Perform physical copy
                        success = await self._execute_recovery_job(
                            session=session,
                            job=recovery_job,
                            chunk=chunk,
                            source_node=healthy_nodes[source_replica.node_id],
                            target_node=target_node,
                        )

                        if success:
                            recovered_count += 1

        return recovered_count

    async def _execute_recovery_job(
        self,
        session: AsyncSession,
        job: RecoveryJob,
        chunk: ChunkMetadata,
        source_node: StorageNodeRecord,
        target_node: StorageNodeRecord,
    ) -> bool:
        """Transfers chunk from source node to target node with end-to-end verification."""
        now = time.time()
        try:
            # 1. Fetch chunk bytes from source
            chunk_data = await distributor.fetch_chunk_replica(
                node_url=source_node.advertised_url,
                chunk_id=chunk.chunk_id,
                expected_checksum=chunk.checksum_sha256,
            )

            if not chunk_data:
                job.status = "failed"
                job.error_message = f"Failed to fetch valid chunk from source node {source_node.node_id}"
                job.completed_at = time.time()
                await session.commit()
                return False

            # 2. Write to target destination node
            write_success = await distributor.write_chunk_replica(
                node_url=target_node.advertised_url,
                chunk_id=chunk.chunk_id,
                chunk_data=chunk_data,
                expected_checksum=chunk.checksum_sha256,
            )

            if not write_success:
                job.status = "failed"
                job.error_message = f"Failed to write chunk replica to target node {target_node.node_id}"
                job.completed_at = time.time()
                await session.commit()
                return False

            # 3. Add new replica metadata record
            new_replica = ReplicaMetadata(
                replica_id=f"rep_{uuid.uuid4().hex[:12]}",
                chunk_id=chunk.chunk_id,
                node_id=target_node.node_id,
                node_url=target_node.advertised_url,
                status="active",
                generation=chunk.generation,
                last_verified_at=now,
                created_at=now,
            )
            session.add(new_replica)

            # 4. Mark job completed
            job.status = "completed"
            job.completed_at = time.time()

            # 5. Log cluster event
            session.add(
                ClusterEvent(
                    event_id=f"evt_{int(now * 1000)}_rec_{job.job_id}",
                    event_type="REPLICATION_RESTORED",
                    severity="INFO",
                    node_id=target_node.node_id,
                    file_id=job.file_id,
                    message=f"Self-healing successfully re-replicated chunk {chunk.chunk_id} from {source_node.node_id} to {target_node.node_id}.",
                    timestamp=now,
                )
            )

            await session.commit()
            return True

        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = time.time()
            await session.commit()
            return False

    async def reconcile_returning_node(self, node_id: str) -> dict:
        """
        Reconciles physical chunk storage on a returning/recovering node with the database.
        - Detects stale/orphan chunks and purges them safely.
        - Verifies valid chunks and activates their replicas in metadata.
        - Flags missing or corrupted chunks and triggers re-replication if needed.
        """
        now = time.time()
        summary = {
            "node_id": node_id,
            "verified_active": 0,
            "purged_stale": 0,
            "corrupted": 0,
            "missing_updated": 0,
        }

        async with AsyncSessionLocal() as session:
            # 1. Fetch node record
            node = await session.get(StorageNodeRecord, node_id)
            if not node:
                return summary

            # 2. Query node for its stored chunks
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                try:
                    resp = await client.get(f"{node.advertised_url.rstrip('/')}/chunks")
                    if resp.status_code != 200:
                        return summary
                    on_disk_chunks = set(resp.json().get("chunks", []))
                except Exception as e:
                    print(f"[Reconciler] Could not list chunks from returning node {node_id}: {e}")
                    return summary

                # 3. For every chunk on disk, verify against metadata
                for chunk_id in list(on_disk_chunks):
                    chunk_rec = await session.get(ChunkMetadata, chunk_id)
                    if not chunk_rec:
                        # Stale chunk! Delete it from physical disk
                        await distributor.delete_chunk_replica(node.advertised_url, chunk_id)
                        summary["purged_stale"] += 1
                        continue

                    # Check associated file status
                    file_rec = await session.get(FileMetadata, chunk_rec.file_id)
                    if not file_rec or file_rec.status not in ("committed", "degraded"):
                        await distributor.delete_chunk_replica(node.advertised_url, chunk_id)
                        summary["purged_stale"] += 1
                        continue

                    # Verify chunk on disk
                    try:
                        v_resp = await client.get(f"{node.advertised_url.rstrip('/')}/chunks/{chunk_id}/verify")
                        if v_resp.status_code == 200 and v_resp.json().get("status") in ("valid", "existing_valid"):
                            # Chunk is valid! Ensure replica is marked active
                            rep_res = await session.execute(
                                select(ReplicaMetadata).where(
                                    and_(
                                        ReplicaMetadata.chunk_id == chunk_id,
                                        ReplicaMetadata.node_id == node_id,
                                    )
                                )
                            )
                            rep = rep_res.scalars().first()
                            if rep:
                                rep.status = "active"
                                rep.last_verified_at = now
                            else:
                                # Add missing replica record
                                new_rep = ReplicaMetadata(
                                    replica_id=f"rep_{uuid.uuid4().hex[:12]}",
                                    chunk_id=chunk_id,
                                    node_id=node_id,
                                    node_url=node.advertised_url,
                                    status="active",
                                    generation=chunk_rec.generation,
                                    last_verified_at=now,
                                    created_at=now,
                                )
                                session.add(new_rep)
                            summary["verified_active"] += 1
                        else:
                            summary["corrupted"] += 1
                    except Exception:
                        summary["corrupted"] += 1

                # 4. Check for replicas the database expected to be on this node
                exp_replicas = (
                    await session.execute(
                        select(ReplicaMetadata).where(ReplicaMetadata.node_id == node_id)
                    )
                ).scalars().all()

                for rep in exp_replicas:
                    if rep.chunk_id not in on_disk_chunks:
                        rep.status = "missing"
                        summary["missing_updated"] += 1

                # 5. Log reconciliation event
                session.add(
                    ClusterEvent(
                        event_id=f"evt_{int(now * 1000)}_recon_{node_id}",
                        event_type="NODE_RECONCILED",
                        severity="INFO",
                        node_id=node_id,
                        message=f"Returning node '{node_id}' reconciled: {summary['verified_active']} active chunks verified, {summary['purged_stale']} stale chunks purged, {summary['missing_updated']} missing chunks marked.",
                        timestamp=now,
                    )
                )

                await session.commit()

        # Trigger scan to heal any remaining gaps
        await self.scan_and_reconcile()
        return summary

    async def start_reconciliation_loop(self) -> None:
        self._running = True
        while self._running:
            try:
                await self.scan_and_reconcile()
            except Exception as e:
                print(f"[ReplicationReconciler] Error during loop: {e}")
            await asyncio.sleep(5.0)

    def stop_reconciliation_loop(self) -> None:
        self._running = False
        if self._reconcile_task and not self._reconcile_task.done():
            self._reconcile_task.cancel()


reconciler = ReplicationReconciler()
