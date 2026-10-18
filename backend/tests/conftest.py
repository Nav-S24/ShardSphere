import os
import pytest
import pytest_asyncio
from pathlib import Path
import sys

# Ensure backend root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.metadata.database import async_engine, Base, AsyncSessionLocal
from app.models.metadata import LeaderLease, StorageNodeRecord
from app.election.lease import leader_manager
from sqlalchemy import delete


@pytest_asyncio.fixture(autouse=True)
async def setup_test_db():
    """Initializes tables and ensures leader lease and nodes are available for test."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Reset any stale leases from previous tests so current coordinator instance is leader
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LeaderLease))
            # Ensure 5 storage nodes are registered
            for i in range(1, 6):
                node_id = f"storage-node-{i}"
                url = f"http://127.0.0.1:800{i}"
                existing = await session.get(StorageNodeRecord, node_id)
                if not existing:
                    session.add(
                        StorageNodeRecord(
                            node_id=node_id,
                            node_name=f"Storage Node {i}",
                            advertised_url=url,
                            health_state="HEALTHY",
                            consecutive_failures=0,
                            total_chunks=0,
                            total_bytes_used=0,
                            storage_limit_bytes=10737418240,
                            updated_at=1000.0,
                        )
                    )

    # Acquire lease for current test coordinator
    await leader_manager.try_acquire_or_renew()
    yield
