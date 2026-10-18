from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings, get_resolved_storage_path
from app.api.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure storage paths exist on startup
    storage_path = get_resolved_storage_path()
    (storage_path / ".tmp").mkdir(parents=True, exist_ok=True)
    print(f"[{settings.NODE_ID}] Starting storage node on port {settings.PORT} (Storage: {storage_path})")
    yield
    print(f"[{settings.NODE_ID}] Shutting down storage node cleanly.")


app = FastAPI(
    title=f"ShardSphere Storage Node API - {settings.NODE_ID}",
    description="Independent physical chunk storage service for ShardSphere distributed filesystem.",
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

app.include_router(router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host=settings.HOST, port=settings.PORT, reload=False)
