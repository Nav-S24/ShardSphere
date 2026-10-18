import asyncio
import time
from typing import Optional, Tuple
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.metadata.database import AsyncSessionLocal
from app.models.metadata import LeaderLease, ClusterEvent


class LeaderElectionManager:
    """
    Manages database-backed leader lease with monotonically increasing fencing tokens.
    Prevents split-brain and stale metadata writes across coordinator instances.
    """

    def __init__(self, coordinator_id: Optional[str] = None, lease_duration: Optional[float] = None):
        self.coordinator_id = coordinator_id or settings.COORDINATOR_ID
        self.lease_duration = lease_duration or settings.LEASE_DURATION_SECONDS
        self.lease_id = "coordinator_primary_lease"
        self.is_leader = False
        self.fencing_token = 0
        self.lease_expires_at = 0.0
        self._renewal_task: Optional[asyncio.Task] = None
        self._running = False

    async def validate_active_lease(self, session: AsyncSession, expected_token: Optional[int] = None) -> bool:
        """
        Validates that this coordinator holds the active, unexpired lease with the expected fencing token.
        Throws RuntimeError if lease is expired, held by someone else, or token is stale.
        """
        token = expected_token if expected_token is not None else self.fencing_token
        now = time.time()

        result = await session.execute(
            select(LeaderLease).where(LeaderLease.lease_id == self.lease_id)
        )
        lease = result.scalars().first()

        if lease is None:
            self.is_leader = False
            raise RuntimeError("Fencing check failed: No active leader lease found in database.")

        if lease.holder_id != self.coordinator_id:
            self.is_leader = False
            raise RuntimeError(
                f"Fencing check failed: Coordinator '{self.coordinator_id}' does not hold the lease. "
                f"Current holder is '{lease.holder_id}' with token {lease.fencing_token}."
            )

        if now >= lease.expires_at:
            self.is_leader = False
            raise RuntimeError(
                f"Fencing check failed: Leader lease held by '{self.coordinator_id}' has expired "
                f"(expired at {lease.expires_at}, now is {now})."
            )

        if lease.fencing_token != token:
            self.is_leader = False
            raise RuntimeError(
                f"Fencing check failed: Fencing token mismatch for '{self.coordinator_id}'. "
                f"Expected token {token}, but active lease has token {lease.fencing_token}."
            )

        return True

    async def try_acquire_or_renew(self) -> Tuple[bool, int, Optional[str]]:
        """
        Attempts to acquire leadership lease or renew if currently held.
        Returns (is_leader, fencing_token, current_holder_id).
        """
        now = time.time()
        new_expires_at = now + self.lease_duration

        async with AsyncSessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(LeaderLease).where(LeaderLease.lease_id == self.lease_id).with_for_update()
                )
                lease = result.scalars().first()

                if lease is None:
                    # Initial lease creation
                    self.fencing_token = 1
                    self.is_leader = True
                    self.lease_expires_at = new_expires_at
                    new_lease = LeaderLease(
                        lease_id=self.lease_id,
                        holder_id=self.coordinator_id,
                        fencing_token=self.fencing_token,
                        acquired_at=now,
                        expires_at=new_expires_at,
                        last_renewed_at=now,
                    )
                    session.add(new_lease)
                    session.add(
                        ClusterEvent(
                            event_id=f"evt_{int(now * 1000)}_lead",
                            event_type="LEADER_ELECTED",
                            severity="INFO",
                            message=f"Coordinator '{self.coordinator_id}' initialized and acquired leadership lease (Fencing Token: {self.fencing_token}).",
                            timestamp=now,
                        )
                    )
                    return True, self.fencing_token, self.coordinator_id

                # Lease exists
                if lease.holder_id == self.coordinator_id:
                    # Current holder renewal
                    lease.expires_at = new_expires_at
                    lease.last_renewed_at = now
                    self.is_leader = True
                    self.fencing_token = lease.fencing_token
                    self.lease_expires_at = new_expires_at
                    return True, self.fencing_token, self.coordinator_id

                # Another holder currently has the lease
                if now < lease.expires_at:
                    # Lease is valid and held by someone else
                    self.is_leader = False
                    self.fencing_token = lease.fencing_token
                    self.lease_expires_at = lease.expires_at
                    return False, self.fencing_token, lease.holder_id

                # Former holder lease expired! We take over with incremented fencing token
                new_token = lease.fencing_token + 1
                previous_holder = lease.holder_id
                lease.holder_id = self.coordinator_id
                lease.fencing_token = new_token
                lease.acquired_at = now
                lease.expires_at = new_expires_at
                lease.last_renewed_at = now

                self.is_leader = True
                self.fencing_token = new_token
                self.lease_expires_at = new_expires_at

                session.add(
                    ClusterEvent(
                        event_id=f"evt_{int(now * 1000)}_lead_takeover",
                        event_type="LEADER_ELECTED",
                        severity="WARNING",
                        message=f"Lease for '{previous_holder}' expired. Coordinator '{self.coordinator_id}' elected as new leader (Fencing Token: {new_token}).",
                        timestamp=now,
                    )
                )

                return True, new_token, self.coordinator_id

    async def get_status(self) -> dict:
        now = time.time()
        remaining = max(0.0, self.lease_expires_at - now) if self.is_leader else 0.0

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(LeaderLease).where(LeaderLease.lease_id == self.lease_id)
            )
            lease = result.scalars().first()
            current_leader = lease.holder_id if lease else None
            token = lease.fencing_token if lease else 0
            expires_at = lease.expires_at if lease else None
            time_remaining = max(0.0, (expires_at or 0.0) - now)

        return {
            "is_leader": (current_leader == self.coordinator_id and now < (expires_at or 0)),
            "current_leader_id": current_leader,
            "fencing_token": token,
            "lease_expires_at": expires_at,
            "time_remaining_seconds": round(time_remaining, 2),
            "coordinator_instance_id": self.coordinator_id,
        }

    async def start_lease_loop(self) -> None:
        """Starts periodic lease renewal background task."""
        self._running = True
        while self._running:
            try:
                await self.try_acquire_or_renew()
            except Exception as e:
                # Log error and mark non-leader to avoid split-brain
                self.is_leader = False
                print(f"[LeaderElection] Lease renewal failed: {e}")
            await asyncio.sleep(settings.LEASE_RENEW_INTERVAL_SECONDS)

    def stop_lease_loop(self) -> None:
        self._running = False
        if self._renewal_task and not self._renewal_task.done():
            self._renewal_task.cancel()


leader_manager = LeaderElectionManager()
