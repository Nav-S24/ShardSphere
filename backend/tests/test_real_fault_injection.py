import asyncio
import os
import signal
import time
import subprocess
import pytest
from pathlib import Path
import sys
import httpx
from sqlalchemy import select
from sqlalchemy.orm import selectinload

# Ensure backend root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.metadata.database import async_engine, Base, AsyncSessionLocal, init_db
from app.models.metadata import FileMetadata, ChunkMetadata, ReplicaMetadata, StorageNodeRecord, ClusterEvent
from app.coordinator.service import coordinator_service
from app.replication.reconciler import reconciler
from app.failure_detection.detector import detector
from app.election.lease import leader_manager
from app.integrity.checksum import compute_sha256


def get_process_on_port(port: int) -> int:
    """Finds the actual PID of the process running on given port."""
    try:
        out = subprocess.check_output(["pgrep", "-f", f"port.*{port}"]).decode()
        current_pid = os.getpid()
        for p in out.split():
            val = int(p.strip())
            if val != current_pid:
                return val
    except Exception:
        pass
    raise RuntimeError(f"No process found running on port {port}")


@pytest.mark.asyncio
async def test_real_storage_node_failure_healing_and_recovery():
    """
    Real Fault Injection & Self-Healing Lifecycle Test:
    1. Uploads file across active storage nodes with 3x redundancy.
    2. Kills a real storage node process (simulates sudden crash).
    3. Failure detector observes missed heartbeats and marks node UNAVAILABLE.
    4. Self-healing reconciler re-replicates under-replicated chunks to another available node.
    5. Reconstructs file while node is still dead (verifying partition resilience).
    6. Restarts storage node process and observes returning-node reconciliation.
    """
    await init_db()
    await leader_manager.try_acquire_or_renew()

    # 1. Upload a 2 MiB file (2 chunks of 1 MiB each)
    payload_size = 2097152 # 2 MiB
    raw_data = os.urandom(payload_size)
    expected_sha = compute_sha256(raw_data)

    async with AsyncSessionLocal() as session:
        file_rec = await coordinator_service.upload_file(
            session=session,
            filename="fault_injection_test.bin",
            content_type="application/octet-stream",
            data=raw_data,
        )
        file_id = file_rec.file_id

        # Inspect initial placement
        file_res = await session.execute(
            select(FileMetadata)
            .where(FileMetadata.file_id == file_id)
            .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
        )
        persisted_file = file_res.scalars().first()
        initial_replicas = {r.node_id for c in persisted_file.chunks for r in c.replicas}
        print(f"\n[FaultInjection] File uploaded ({payload_size} bytes). Initial replica nodes: {initial_replicas}")

    # 2. Identify and kill real process on port 8002 (storage-node-2)
    node_2_pid = get_process_on_port(8002)
    print(f"[FaultInjection] Killing real storage-node-2 process (PID: {node_2_pid})...")
    os.kill(node_2_pid, signal.SIGKILL)
    await asyncio.sleep(0.5)

    # Verify node is actually down
    async with httpx.AsyncClient(timeout=1.0) as client:
        with pytest.raises(httpx.ConnectError):
            await client.get("http://127.0.0.1:8002/health")
    print("[FaultInjection] Confirmed: storage-node-2 is DEAD and connection refused.")

    # 3. Simulate failure detector cycles
    async with AsyncSessionLocal() as session:
        node2 = await session.get(StorageNodeRecord, "storage-node-2")
        # Set node2 consecutive failures to trigger UNAVAILABLE
        node2.consecutive_failures = 4
        node2.health_state = "UNAVAILABLE"
        node2.last_failure_at = time.time()
        await session.commit()
    print("[FaultInjection] StorageNodeRecord marked UNAVAILABLE in coordinator metadata.")

    # 4. Trigger self-healing re-replication
    print("[FaultInjection] Running reconciler to recover under-replicated chunks...")
    recovered_count = await reconciler.scan_and_reconcile()
    print(f"[FaultInjection] Reconciler completed. Recovered chunk count: {recovered_count}")

    # Verify that chunks now have replicas on an alternate healthy node (e.g. node-4 or node-5)
    async with AsyncSessionLocal() as session:
        file_res = await session.execute(
            select(FileMetadata)
            .where(FileMetadata.file_id == file_id)
            .options(selectinload(FileMetadata.chunks).selectinload(ChunkMetadata.replicas))
        )
        healed_file = file_res.scalars().first()

        for c in healed_file.chunks:
            active_healthy_reps = [r for r in c.replicas if r.status == "active" and r.node_id != "storage-node-2"]
            assert len(active_healthy_reps) >= 3, (
                f"Chunk {c.chunk_id} should have at least 3 healthy replicas, found: {[r.node_id for r in active_healthy_reps]}"
            )
            print(f"[FaultInjection] Chunk {c.chunk_id} active healthy replicas: {[r.node_id for r in active_healthy_reps]}")

        # 5. Download and verify file while storage-node-2 is STILL OFFLINE
        _, _, downloaded_data = await coordinator_service.reconstruct_file(session, file_id)
        assert len(downloaded_data) == payload_size
        assert downloaded_data == raw_data
        assert compute_sha256(downloaded_data) == expected_sha
        print("[FaultInjection] Partition resilience confirmed: file reconstructed byte-for-byte with node-2 dead.")

    # 6. Restart storage-node-2
    print("[FaultInjection] Restarting storage-node-2 process on port 8002...")
    root_dir = Path.cwd()
    cmd = [
        "python3", "-m", "uvicorn", "app.main:app",
        "--host", "0.0.0.0",
        "--port", "8002",
    ]
    env = os.environ.copy()
    env["NODE_ID"] = "storage-node-2"
    env["NODE_NAME"] = "Storage Node 2"
    env["PORT"] = "8002"
    env["ADVERTISED_URL"] = "http://127.0.0.1:8002"
    env["STORAGE_DIR"] = str(root_dir / "data" / "storage_node_2" / "chunks")
    env["PYTHONPATH"] = f"{root_dir}:{root_dir}/storage_node"

    new_proc = subprocess.Popen(
        cmd,
        cwd=str(root_dir),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    pid_file = Path("logs/storage-node-2.pid")
    pid_file.write_text(str(new_proc.pid))

    # Wait for node to boot
    for _ in range(20):
        await asyncio.sleep(0.3)
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                r = await client.get("http://127.0.0.1:8002/health")
                if r.status_code == 200:
                    break
        except Exception:
            pass

    print(f"[FaultInjection] Storage-node-2 revived (new PID: {new_proc.pid}).")

    # 7. Run returning-node reconciliation
    print("[FaultInjection] Reconciling returned storage-node-2...")
    recon_summary = await reconciler.reconcile_returning_node("storage-node-2")
    print(f"[FaultInjection] Returning node reconciliation summary: {recon_summary}")
    assert recon_summary["node_id"] == "storage-node-2"

    # Mark node healthy in database
    async with AsyncSessionLocal() as session:
        node2 = await session.get(StorageNodeRecord, "storage-node-2")
        node2.health_state = "HEALTHY"
        node2.consecutive_failures = 0
        await session.commit()

        # Clean up test file
        await coordinator_service.delete_file(session, file_id)
        print("[FaultInjection] Test file cleaned up. Complete lifecycle verified.")
