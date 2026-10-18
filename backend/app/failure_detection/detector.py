import asyncio
import time
from typing import Dict, List, Optional
import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.metadata.database import AsyncSessionLocal
from app.models.metadata import StorageNodeRecord, ClusterEvent


class NodeHealthDetector:
    """
    Heartbeat and failure detector for cluster storage nodes.
    Maintains state transitions: HEALTHY -> SUSPECT -> UNAVAILABLE -> RECOVERING.
    """

    def __init__(self):
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._http_client: Optional[httpx.AsyncClient] = None

    async def init_node_registry(self) -> None:
        """Populates database with configured storage nodes if not already registered."""
        configured_nodes = settings.get_parsed_nodes()
        now = time.time()

        async with AsyncSessionLocal() as session:
            async with session.begin():
                for node_id, url in configured_nodes:
                    result = await session.execute(
                        select(StorageNodeRecord).where(StorageNodeRecord.node_id == node_id)
                    )
                    existing = result.scalars().first()
                    if not existing:
                        node_record = StorageNodeRecord(
                            node_id=node_id,
                            node_name=f"Storage Node {node_id.replace('storage-node-', '')}",
                            advertised_url=url,
                            health_state="HEALTHY",
                            consecutive_failures=0,
                            last_heartbeat_at=now,
                            last_failure_at=None,
                            total_chunks=0,
                            total_bytes_used=0,
                            storage_limit_bytes=10737418240,
                            updated_at=now,
                        )
                        session.add(node_record)

    async def check_node(self, node: StorageNodeRecord, client: httpx.AsyncClient) -> None:
        """Performs a real HTTP health and stats query against an individual storage node."""
        node_id = node.node_id
        url = node.advertised_url.rstrip("/")
        now = time.time()

        try:
            health_resp = await client.get(f"{url}/health", timeout=settings.HEARTBEAT_TIMEOUT_SECONDS)
            if health_resp.status_code == 200:
                stats_resp = await client.get(f"{url}/storage/stats", timeout=settings.HEARTBEAT_TIMEOUT_SECONDS)
                stats_data = stats_resp.json() if stats_resp.status_code == 200 else {}

                old_state = node.health_state
                node.consecutive_failures = 0
                node.last_heartbeat_at = now
                node.total_chunks = stats_data.get("total_chunks", node.total_chunks)
                node.total_bytes_used = stats_data.get("total_bytes_used", node.total_bytes_used)
                node.storage_limit_bytes = stats_data.get("storage_limit_bytes", node.storage_limit_bytes)
                node.updated_at = now

                if old_state in ("UNAVAILABLE", "SUSPECT"):
                    node.health_state = "RECOVERING" if old_state == "UNAVAILABLE" else "HEALTHY"
                    # Transition event
                    async with AsyncSessionLocal() as session:
                        async with session.begin():
                            session.add(
                                ClusterEvent(
                                    event_id=f"evt_{int(now * 1000)}_{node_id}_recover",
                                    event_type="NODE_HEALTH_CHANGED",
                                    severity="INFO",
                                    node_id=node_id,
                                    message=f"Node '{node_id}' responded to health check. State transitioned from {old_state} to {node.health_state}.",
                                    timestamp=now,
                                )
                            )
                    # Trigger asynchronous returning-node reconciliation if recovering from UNAVAILABLE
                    if old_state == "UNAVAILABLE":
                        try:
                            from app.replication.reconciler import reconciler
                            asyncio.create_task(reconciler.reconcile_returning_node(node_id))
                        except Exception as rec_err:
                            print(f"[Detector] Failed to dispatch reconciliation for {node_id}: {rec_err}")
                elif old_state == "RECOVERING":
                    node.health_state = "HEALTHY"

                return

        except Exception as e:
            # Heartbeat check failed
            pass

        # Handle failure
        node.consecutive_failures += 1
        node.last_failure_at = now
        old_state = node.health_state

        if node.consecutive_failures >= settings.MISSED_HEARTBEATS_SUSPECT:
            time_since_heartbeat = (now - node.last_heartbeat_at) if node.last_heartbeat_at else 9999
            if time_since_heartbeat >= settings.FAILURE_THRESHOLD_SECONDS:
                node.health_state = "UNAVAILABLE"
            else:
                node.health_state = "SUSPECT"
        else:
            node.health_state = "HEALTHY"

        if old_state != node.health_state:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    session.add(
                        ClusterEvent(
                            event_id=f"evt_{int(now * 1000)}_{node_id}_fail",
                            event_type="NODE_HEALTH_CHANGED",
                            severity="WARNING" if node.health_state == "SUSPECT" else "ERROR",
                            node_id=node_id,
                            message=f"Node '{node_id}' missed {node.consecutive_failures} consecutive heartbeats. State changed from {old_state} to {node.health_state}.",
                            timestamp=now,
                        )
                    )

    async def run_check_cycle(self) -> None:
        """Executes a single check cycle over all registered nodes."""
        if not self._http_client:
            self._http_client = httpx.AsyncClient(timeout=settings.HEARTBEAT_TIMEOUT_SECONDS)

        async with AsyncSessionLocal() as session:
            result = await session.execute(select(StorageNodeRecord))
            nodes = result.scalars().all()

            tasks = [self.check_node(node, self._http_client) for node in nodes]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                await session.commit()

    async def start_monitor_loop(self) -> None:
        """Starts periodic failure detection loop."""
        self._running = True
        self._http_client = httpx.AsyncClient(timeout=settings.HEARTBEAT_TIMEOUT_SECONDS)
        await self.init_node_registry()

        while self._running:
            try:
                await self.run_check_cycle()
            except Exception as e:
                print(f"[FailureDetector] Error during check cycle: {e}")
            await asyncio.sleep(settings.HEARTBEAT_INTERVAL_SECONDS)

    async def stop_monitor_loop(self) -> None:
        self._running = False
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
        if self._http_client:
            await self._http_client.aclose()


detector = NodeHealthDetector()
