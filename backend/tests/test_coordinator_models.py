import pytest
import time
from pathlib import Path
import sys
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select

# Add backend to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.models.metadata import (
    Base,
    FileMetadata,
    ChunkMetadata,
    ReplicaMetadata,
    StorageNodeRecord,
    LeaderLease,
)


@pytest.mark.asyncio
async def test_database_schema_creation_and_file_model():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_maker = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        now = time.time()
        file_rec = FileMetadata(
            file_id="file-test-1",
            filename="document.pdf",
            size_bytes=2097152,
            checksum_sha256="abcd1234ef5678",
            chunk_count=2,
            chunk_size_bytes=1048576,
            replication_factor=3,
            metadata_version=1,
            status="committed",
            created_at=now,
            updated_at=now,
        )
        session.add(file_rec)

        chunk1 = ChunkMetadata(
            chunk_id="chunk-1",
            file_id="file-test-1",
            sequence_index=0,
            size_bytes=1048576,
            checksum_sha256="chunk_sha_0",
            generation=1,
            created_at=now,
        )
        chunk2 = ChunkMetadata(
            chunk_id="chunk-2",
            file_id="file-test-1",
            sequence_index=1,
            size_bytes=1048576,
            checksum_sha256="chunk_sha_1",
            generation=1,
            created_at=now,
        )
        session.add_all([chunk1, chunk2])

        # Add 3 replicas for chunk 1
        for i in range(1, 4):
            rep = ReplicaMetadata(
                replica_id=f"rep-1-{i}",
                chunk_id="chunk-1",
                node_id=f"storage-node-{i}",
                node_url=f"http://storage-node-{i}:800{i}",
                status="active",
                generation=1,
                last_verified_at=now,
                created_at=now,
            )
            session.add(rep)

        await session.commit()

        # Query back
        res = await session.execute(select(FileMetadata).where(FileMetadata.file_id == "file-test-1"))
        fetched = res.scalars().first()
        assert fetched is not None
        assert fetched.filename == "document.pdf"
        assert fetched.chunk_count == 2
        assert fetched.replication_factor == 3
        assert fetched.status == "committed"

    await engine.dispose()
