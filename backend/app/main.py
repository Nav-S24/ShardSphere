import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.metadata.database import init_db
from app.api.v1_routes import router as v1_router
from app.election.lease import leader_manager
from app.failure_detection.detector import detector
from app.replication.reconciler import reconciler


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[{settings.COORDINATOR_ID}] Starting ShardSphere Coordinator on port {settings.PORT}...")
    # 1. Initialize persistent metadata database schema
    await init_db()

    # 2. Acquire initial leader lease
    await leader_manager.try_acquire_or_renew()

    # 3. Spawn background loops
    lease_task = asyncio.create_task(leader_manager.start_lease_loop())
    monitor_task = asyncio.create_task(detector.start_monitor_loop())
    reconcile_task = asyncio.create_task(reconciler.start_reconciliation_loop())

    print(f"[{settings.COORDINATOR_ID}] Coordinator initialized successfully. Leader status: {leader_manager.is_leader}")

    yield

    # Clean shutdown
    print(f"[{settings.COORDINATOR_ID}] Initiating graceful shutdown...")
    leader_manager.stop_lease_loop()
    await detector.stop_monitor_loop()
    reconciler.stop_reconciliation_loop()

    lease_task.cancel()
    monitor_task.cancel()
    reconcile_task.cancel()
    print(f"[{settings.COORDINATOR_ID}] All coordinator background tasks stopped.")


app = FastAPI(
    title="ShardSphere Distributed Storage Coordinator",
    description="Metadata coordination, leader election, failure detection, and self-healing engine.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API v1 router
app.include_router(v1_router)


@app.get("/health", tags=["Health"])
async def root_health():
    """Root health check for load balancer and Docker healthcheck."""
    return {
        "status": "healthy",
        "coordinator_id": settings.COORDINATOR_ID,
        "is_leader": leader_manager.is_leader,
        "timestamp": time.time(),
    }


@app.get("/ready", tags=["Health"])
async def root_ready():
    """Readiness endpoint."""
    return {"status": "ready"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host=settings.HOST, port=settings.PORT, reload=False)
