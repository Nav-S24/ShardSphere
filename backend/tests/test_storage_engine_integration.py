import os
import hashlib
import time
import pytest
from pathlib import Path
import sys
from sqlalchemy import select
from sqlalchemy.orm import selectinload

# Ensure backend is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.metadata.database import async_engine, AsyncSessionLocal, init_db
from app.models.metadata import FileMetadata, ChunkMetadata, ReplicaMetadata, StorageNodeRecord
from app.coordinator.service import coordinator_service
from app.election.lease import leader_manager
from app.integrity.checksum import compute_sha256


@pytest.mark.asyncio
async def test_end_to_end_storage_engine_lifecycle():
    """
    Integration test:
    1. Multi-chunk upload (2.2 MiB) across 5 live storage nodes.
    2. Verifies 3x replication on distinct nodes and transactional metadata commit.
    3. Exact byte-for-byte reconstructed download with SHA-256 validation.
    4. Safe deletion of physical chunks and metadata.
    """
    await init_db()
    # Acquire leader lease for test coordinator
    await leader_manager.try_acquire_or_renew()

    # Create 2.2 MiB of test binary data (2 full 1 MiB chunks + 1 remaining ~209 KB chunk)
    one_mib = 1048576
    total_bytes = int(2.2 * one_mib)
    test_binary_data = os.urandom(total_bytes)
    expected_full_sha256 = hashlib.sha256(test_binary_data).hexdigest()
    test_filename = "integration_test_payload.bin"

    async with AsyncSessionLocal() as session:
        # 1. Perform full multi-chunk upload
        file_rec = await coordinator_service.upload_file(
            session=session,
            filename=test_filename,
            content_type="application/octet-stream",
            data=test_binary_data,
        )

        assert file_rec is not None
        file_id = file_rec.file_id
        assert file_rec.status == "committed"
        assert file_rec.metadata_version == 2 # incremented from 1 to 2 upon commit
        assert file_rec.chunk_count == 3
        assert file_rec.size_bytes == total_bytes
        assert file_rec.checksum_sha256 == expected_full_sha256

        # 2. Inspect persisted database records
        db_res = await session.execute(
            select(FileMetadata)
            .where(FileMetadata.file_id == file_id)
            .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
        )
        persisted_file = db_res.scalars().first()
        assert persisted_file is not None
        assert len(persisted_file.chunks) == 3

        # Verify each chunk has 3 distinct node replicas
        for chunk in persisted_file.chunks:
            assert len(chunk.replicas) == 3
            replica_nodes = [r.node_id for r in chunk.replicas]
            assert len(set(replica_nodes)) == 3, f"Expected 3 distinct nodes, got {replica_nodes}"
            for r in chunk.replicas:
                assert r.status == "active"
                assert r.generation == 1

        # 3. Perform file reconstruction and download
        dl_filename, dl_content_type, dl_data = await coordinator_service.reconstruct_file(session, file_id)
        assert dl_filename == test_filename
        assert len(dl_data) == total_bytes
        assert dl_data == test_binary_data
        assert hashlib.sha256(dl_data).hexdigest() == expected_full_sha256

        # 4. Perform safe file deletion
        deleted = await coordinator_service.delete_file(session, file_id)
        assert deleted is True

        # Verify file is deleted from database
        check_res = await session.execute(select(FileMetadata).where(FileMetadata.file_id == file_id))
        assert check_res.scalars().first() is None


@pytest.mark.asyncio
async def test_replica_corruption_failover():
    """
    Integration test:
    Purposely corrupts one physical replica on disk, and verifies that file
    reconstruction transparently fails over to a healthy surviving replica.
    """
    await init_db()
    await leader_manager.try_acquire_or_renew()

    test_content = b"CRITICAL_DATA_FAILOVER_TEST_" + os.urandom(50000)
    expected_sha = compute_sha256(test_content)

    async with AsyncSessionLocal() as session:
        # Upload
        file_rec = await coordinator_service.upload_file(
            session=session,
            filename="failover_test.dat",
            content_type="application/octet-stream",
            data=test_content,
        )
        file_id = file_rec.file_id

        # Retrieve chunk and replicas
        db_res = await session.execute(
            select(FileMetadata)
            .where(FileMetadata.file_id == file_id)
            .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
        )
        file_obj = db_res.scalars().first()
        first_chunk = file_obj.chunks[0]
        first_replica = first_chunk.replicas[0]

        # Corrupt the physical chunk file on the first replica's node disk
        node_num = first_replica.node_id.replace("storage-node-", "")
        chunk_file_path = Path.cwd() / "data" / f"storage_node_{node_num}" / "chunks" / f"chunk_{first_chunk.chunk_id}.bin"

        if chunk_file_path.exists():
            # Overwrite with garbage bytes to simulate bit rot
            with open(chunk_file_path, "wb") as f:
                f.write(b"CORRUPTED_GARBAGE_BYTES_OVERWRITTEN")

        # Now reconstruct file: should fail over to replica 2 or 3 and still succeed!
        _, _, reconstructed_bytes = await coordinator_service.reconstruct_file(session, file_id)
        assert reconstructed_bytes == test_content
        assert compute_sha256(reconstructed_bytes) == expected_sha

        # Clean up
        await coordinator_service.delete_file(session, file_id)
