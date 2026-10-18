import pytest
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.metadata.database import async_engine, Base
from app.election.lease import LeaderElectionManager


@pytest.mark.asyncio
async def test_leader_lease_acquisition_and_fencing_token():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from app.metadata.database import AsyncSessionLocal
    from app.models.metadata import LeaderLease
    from sqlalchemy import delete

    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LeaderLease))

    coord1 = LeaderElectionManager(coordinator_id="coord-alpha")
    coord2 = LeaderElectionManager(coordinator_id="coord-beta")

    # 1. First coordinator acquires lease
    is_lead, token, holder = await coord1.try_acquire_or_renew()
    assert is_lead is True
    assert token == 1
    assert holder == "coord-alpha"

    # 2. Second coordinator attempts while lease is valid -> should NOT acquire
    is_lead2, token2, holder2 = await coord2.try_acquire_or_renew()
    assert is_lead2 is False
    assert holder2 == "coord-alpha"
    assert token2 == 1

    # 3. Renew from coord1
    is_lead_renew, token_renew, holder_renew = await coord1.try_acquire_or_renew()
    assert is_lead_renew is True
    assert holder_renew == "coord-alpha"
