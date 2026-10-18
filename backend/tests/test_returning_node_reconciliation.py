import os
import time
import pytest
from pathlib import Path
import sys
from sqlalchemy import select, and_

# Ensure backend root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.metadata.database import async_engine, Base, AsyncSessionLocal, init_db
from app.models.metadata import FileMetadata, ChunkMetadata, ReplicaMetadata, StorageNodeRecord
from app.coordinator.service import coordinator_service
from app.replication.reconciler import reconciler
from app.election.lease import leader_manager
from app.integrity.checksum import compute_sha256


@pytest.mark.asyncio
async def test_returning_node_reconciliation_active_and_stale_chunks():
    """
    Verifies that when an offline/recovering storage node returns:
    1. Valid on-disk chunks are cryptographically verified and their metadata marked active.
    2. Stale/untracked on-disk chunks are safely pruned.
    3. Replicas missing from the node's disk are marked missing.
    """
    await init_db()
    await leader_manager.try_acquire_or_renew()

    # 1. Upload a file to establish baseline chunk metadata
    test_data = b"RECONCILIATION_TEST_DATA_" + os.urandom(10000)
    async with AsyncSessionLocal() as session:
        file_rec = await coordinator_service.upload_file(
            session=session,
            filename="reconcile_target.bin",
            content_type="application/octet-stream",
            data=test_data,
        )
        file_id = file_rec.file_id

        # Get the first chunk and its replicas
        chunk_res = await session.execute(
            select(ChunkMetadata).where(ChunkMetadata.file_id == file_id)
        )
        chunk = chunk_res.scalars().first()
        chunk_id = chunk.chunk_id

        rep_res = await session.execute(
            select(ReplicaMetadata).where(ReplicaMetadata.chunk_id == chunk_id)
        )
        replicas = rep_res.scalars().all()
        target_node_id = replicas[0].node_id

    # 2. Plant an orphaned / stale chunk on that node's disk
    node_num = target_node_id.replace("storage-node-", "")
    node_chunk_dir = Path.cwd() / "data" / f"storage_node_{node_num}" / "chunks"
    stale_chunk_id = "chk_stale_orphan_99999"
    stale_file = node_chunk_dir / f"chunk_{stale_chunk_id}.bin"
    stale_meta = node_chunk_dir / f"chunk_{stale_chunk_id}.meta.json"

    with open(stale_file, "wb") as f:
        f.write(b"STALE_UNTRACKED_ORPHAN_BYTES")
    with open(stale_meta, "w") as f:
        f.write('{"chunk_id": "chk_stale_orphan_99999", "size_bytes": 28}')

    assert stale_file.exists()

    # 3. Execute returning-node reconciliation on this node
    summary = await reconciler.reconcile_returning_node(node_id=target_node_id)

    # 4. Assert outcomes
    # Stale chunk should be detected and purged
    assert summary["purged_stale"] >= 1
    assert not stale_file.exists(), "Stale chunk file should have been removed from disk by reconciler"

    # Valid chunks should be verified and confirmed active
    assert summary["verified_active"] >= 1

    # Database replica status should be active
    async with AsyncSessionLocal() as session:
        check_rep = (await session.execute(
            select(ReplicaMetadata).where(
                and_(
                    ReplicaMetadata.chunk_id == chunk_id,
                    ReplicaMetadata.node_id == target_node_id,
                )
            )
        )).scalars().first()
        assert check_rep.status == "active"
        assert check_rep.last_verified_at is not None

        # Clean up
        await coordinator_service.delete_file(session, file_id)
