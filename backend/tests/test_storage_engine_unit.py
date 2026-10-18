import os
import hashlib
import pytest
from pathlib import Path
import sys

# Ensure backend is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.storage.distributor import StorageDistributor
from app.integrity.checksum import compute_sha256, verify_checksum
from app.models.metadata import StorageNodeRecord


def test_1mib_chunk_splitting_and_checksums():
    """Verifies exact 1 MiB (1,048,576 bytes) boundary chunking and SHA-256 hashing."""
    dist = StorageDistributor()
    one_mib = 1048576
    # 2.5 MiB payload: 2 full 1 MiB chunks + 1 half MiB chunk
    total_size = int(2.5 * one_mib)
    test_data = os.urandom(total_size)
    file_sha256 = hashlib.sha256(test_data).hexdigest()

    chunks = dist.split_into_chunks(test_data, chunk_size=one_mib)

    assert len(chunks) == 3, f"Expected 3 chunks for 2.5 MiB, got {len(chunks)}"

    # Chunk 0
    seq0, data0, sha0 = chunks[0]
    assert seq0 == 0
    assert len(data0) == one_mib
    assert sha0 == hashlib.sha256(data0).hexdigest()

    # Chunk 1
    seq1, data1, sha1 = chunks[1]
    assert seq1 == 1
    assert len(data1) == one_mib
    assert sha1 == hashlib.sha256(data1).hexdigest()

    # Chunk 2 (remaining 0.5 MiB = 524,288 bytes)
    seq2, data2, sha2 = chunks[2]
    assert seq2 == 2
    assert len(data2) == 524288
    assert sha2 == hashlib.sha256(data2).hexdigest()

    # Byte-for-byte reassembly verification
    reconstructed = b"".join(c[1] for c in chunks)
    assert reconstructed == test_data
    assert hashlib.sha256(reconstructed).hexdigest() == file_sha256


def test_empty_file_handling():
    """Verifies that an empty file produces exactly 1 zero-length chunk with valid empty SHA-256."""
    dist = StorageDistributor()
    empty_bytes = b""
    expected_empty_sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    chunks = dist.split_into_chunks(empty_bytes, chunk_size=1048576)
    assert len(chunks) == 1
    assert chunks[0][0] == 0
    assert chunks[0][1] == b""
    assert chunks[0][2] == expected_empty_sha


def test_arbitrary_binary_file_integrity():
    """Verifies arbitrary binary data (including null bytes 0x00, 0xFF, random noise)."""
    dist = StorageDistributor()
    # Construct binary payload containing all possible byte values 0..255 repeated
    binary_payload = bytes(list(range(256)) * 400) # ~102.4 KB
    chunks = dist.split_into_chunks(binary_payload, chunk_size=32768)

    reconstructed = bytearray()
    for seq, chunk_bytes, chunk_sha in chunks:
        assert verify_checksum(compute_sha256(chunk_bytes), chunk_sha)
        reconstructed.extend(chunk_bytes)

    assert bytes(reconstructed) == binary_payload


@pytest.mark.asyncio
async def test_placement_strategy_distinctness_and_distribution():
    """Verifies that chunk placement selects 3 distinct nodes and balances round-robin."""
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from app.models.metadata import Base, StorageNodeRecord

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    dist = StorageDistributor()

    async with session_maker() as session:
        # Register 5 test storage nodes
        for i in range(1, 6):
            node = StorageNodeRecord(
                node_id=f"storage-node-{i}",
                node_name=f"Storage Node {i}",
                advertised_url=f"http://127.0.0.1:800{i}",
                health_state="HEALTHY",
                consecutive_failures=0,
                total_chunks=0,
                total_bytes_used=0,
                storage_limit_bytes=10737418240,
                updated_at=1000.0,
            )
            session.add(node)
        await session.commit()

        # Place chunk 0 with replication factor 3
        nodes_chunk0 = await dist.select_placement_nodes(session, sequence_index=0, replication_factor=3)
        assert len(nodes_chunk0) == 3
        node_ids_0 = [n.node_id for n in nodes_chunk0]
        # Assert strictly distinct nodes (no duplicates!)
        assert len(set(node_ids_0)) == 3
        assert node_ids_0 == ["storage-node-1", "storage-node-2", "storage-node-3"]

        # Place chunk 1 with replication factor 3 (rotated)
        nodes_chunk1 = await dist.select_placement_nodes(session, sequence_index=1, replication_factor=3)
        assert len(nodes_chunk1) == 3
        node_ids_1 = [n.node_id for n in nodes_chunk1]
        assert len(set(node_ids_1)) == 3
        assert node_ids_1 == ["storage-node-2", "storage-node-3", "storage-node-4"]

        # Place chunk 2
        nodes_chunk2 = await dist.select_placement_nodes(session, sequence_index=2, replication_factor=3)
        node_ids_2 = [n.node_id for n in nodes_chunk2]
        assert len(set(node_ids_2)) == 3
        assert node_ids_2 == ["storage-node-3", "storage-node-4", "storage-node-5"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_placement_fails_when_insufficient_healthy_nodes():
    """Verifies that placement refuses if healthy nodes < replication factor."""
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from app.models.metadata import Base, StorageNodeRecord

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    dist = StorageDistributor()

    async with session_maker() as session:
        # Only 2 healthy nodes registered
        session.add(StorageNodeRecord(node_id="node-1", node_name="N1", advertised_url="http://node1", health_state="HEALTHY"))
        session.add(StorageNodeRecord(node_id="node-2", node_name="N2", advertised_url="http://node2", health_state="HEALTHY"))
        # 1 node is UNAVAILABLE
        session.add(StorageNodeRecord(node_id="node-3", node_name="N3", advertised_url="http://node3", health_state="UNAVAILABLE"))
        await session.commit()

        # Asking for replication factor 3 should fail
        with pytest.raises(RuntimeError) as exc_info:
            await dist.select_placement_nodes(session, sequence_index=0, replication_factor=3)
        assert "Insufficient healthy storage nodes" in str(exc_info.value)

    await engine.dispose()
