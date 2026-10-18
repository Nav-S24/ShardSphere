import asyncio
import time
import pytest
from pathlib import Path
import sys

# Ensure backend root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.metadata.database import async_engine, Base, AsyncSessionLocal
from app.models.metadata import LeaderLease, FileMetadata
from app.election.lease import LeaderElectionManager
from app.coordinator.service import coordinator_service
from sqlalchemy import select, delete


@pytest.mark.asyncio
async def test_two_coordinator_instances_lease_acquisition_and_fencing():
    """
    Verifies that when two coordinator instances compete:
    1. The first acquires the lease and fencing token 1.
    2. The second is rejected while the lease is valid.
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LeaderLease))

    coord1 = LeaderElectionManager(coordinator_id="coord-primary", lease_duration=2.0)
    coord2 = LeaderElectionManager(coordinator_id="coord-standby", lease_duration=2.0)

    # 1. Primary acquires
    is_lead1, token1, holder1 = await coord1.try_acquire_or_renew()
    assert is_lead1 is True
    assert token1 == 1
    assert holder1 == "coord-primary"

    # 2. Standby attempts while lease valid
    is_lead2, token2, holder2 = await coord2.try_acquire_or_renew()
    assert is_lead2 is False
    assert token2 == 1
    assert holder2 == "coord-primary"

    # 3. Primary validates lease
    async with AsyncSessionLocal() as session:
        assert await coord1.validate_active_lease(session, expected_token=1) is True
        # Standby cannot validate
        with pytest.raises(RuntimeError) as exc_info:
            await coord2.validate_active_lease(session, expected_token=1)
        assert "does not hold the lease" in str(exc_info.value)


@pytest.mark.asyncio
async def test_lease_expiry_takeover_and_token_increment():
    """
    Verifies that when the primary's lease expires:
    1. Standby coordinator detects expiration.
    2. Standby acquires leadership with monotonically incremented fencing token (1 -> 2).
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LeaderLease))

    short_lease_sec = 0.5
    coord1 = LeaderElectionManager(coordinator_id="coord-primary", lease_duration=short_lease_sec)
    coord2 = LeaderElectionManager(coordinator_id="coord-standby", lease_duration=short_lease_sec)

    # Primary acquires lease
    await coord1.try_acquire_or_renew()
    assert coord1.fencing_token == 1

    # Primary stops renewing, wait for lease to expire
    await asyncio.sleep(short_lease_sec + 0.1)

    # Standby attempts acquisition after expiration
    is_lead2, token2, holder2 = await coord2.try_acquire_or_renew()
    assert is_lead2 is True
    assert token2 == 2 # Monotonically incremented fencing token!
    assert holder2 == "coord-standby"

    # Verify database state
    async with AsyncSessionLocal() as session:
        res = await session.execute(select(LeaderLease).where(LeaderLease.lease_id == "coordinator_primary_lease"))
        active_lease = res.scalars().first()
        assert active_lease.holder_id == "coord-standby"
        assert active_lease.fencing_token == 2


@pytest.mark.asyncio
async def test_stale_leader_split_brain_write_rejection():
    """
    Split-Brain Prevention Test:
    A stale coordinator (coord1) whose lease expired attempts to commit a write with its old token (1).
    The write is strictly rejected because coord2 now holds the lease with fencing token 2.
    """
    async with AsyncSessionLocal() as session:
        async with session.begin():
            await session.execute(delete(LeaderLease))

    short_lease_sec = 0.5
    coord1 = LeaderElectionManager(coordinator_id="coord-primary", lease_duration=short_lease_sec)
    coord2 = LeaderElectionManager(coordinator_id="coord-standby", lease_duration=short_lease_sec)

    # 1. Primary acquires token 1
    await coord1.try_acquire_or_renew()
    assert coord1.fencing_token == 1

    # 2. Lease expires, standby takes over with token 2
    await asyncio.sleep(short_lease_sec + 0.1)
    await coord2.try_acquire_or_renew()
    assert coord2.fencing_token == 2

    # 3. Stale primary attempts to validate its old token (1) in the database
    async with AsyncSessionLocal() as session:
        with pytest.raises(RuntimeError) as exc_info:
            await coord1.validate_active_lease(session, expected_token=1)
        err = str(exc_info.value)
        assert "Fencing check failed" in err
        assert "Current holder is 'coord-standby' with token 2" in err

    # 4. Primary tries to renew and discovers it is no longer leader
    renew_lead, renew_token, current_holder = await coord1.try_acquire_or_renew()
    assert renew_lead is False
    assert renew_token == 2
    assert current_holder == "coord-standby"
    assert coord1.is_leader is False
