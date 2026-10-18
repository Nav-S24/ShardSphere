import os
from typing import Dict, List, Tuple
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class CoordinatorSettings(BaseSettings):
    COORDINATOR_ID: str = Field(default_factory=lambda: os.getenv("COORDINATOR_ID", "coordinator-primary-1"))
    HOST: str = Field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    PORT: int = Field(default_factory=lambda: int(os.getenv("PORT", "8000")))

    # Database URLs (supports postgresql+asyncpg or sqlite+aiosqlite fallback)
    DATABASE_URL: str = Field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL",
            "sqlite+aiosqlite:///./shardsphere_metadata.db"
        )
    )
    DATABASE_URL_SYNC: str = Field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL_SYNC",
            "sqlite:///./shardsphere_metadata.db"
        )
    )

    # Distributed Storage Settings
    REPLICATION_FACTOR: int = Field(default_factory=lambda: int(os.getenv("REPLICATION_FACTOR", "3")))
    DEFAULT_CHUNK_SIZE_BYTES: int = Field(default_factory=lambda: int(os.getenv("DEFAULT_CHUNK_SIZE_BYTES", "1048576"))) # 1 MiB
    MAX_FILE_SIZE_BYTES: int = Field(default_factory=lambda: int(os.getenv("MAX_FILE_SIZE_BYTES", str(5 * 1024 * 1024 * 1024))))

    # Heartbeats & Failure Detector
    HEARTBEAT_INTERVAL_SECONDS: float = Field(default_factory=lambda: float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "2.0")))
    MISSED_HEARTBEATS_SUSPECT: int = Field(default_factory=lambda: int(os.getenv("MISSED_HEARTBEATS_SUSPECT", "3")))
    FAILURE_THRESHOLD_SECONDS: float = Field(default_factory=lambda: float(os.getenv("FAILURE_THRESHOLD_SECONDS", "10.0")))
    HEARTBEAT_TIMEOUT_SECONDS: float = Field(default_factory=lambda: float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "2.0")))

    # Leader Lease Settings
    LEASE_DURATION_SECONDS: float = Field(default_factory=lambda: float(os.getenv("LEASE_DURATION_SECONDS", "10.0")))
    LEASE_RENEW_INTERVAL_SECONDS: float = Field(default_factory=lambda: float(os.getenv("LEASE_RENEW_INTERVAL_SECONDS", "3.0")))

    # Comma-separated storage node registry "node-1:http://storage-node-1:8001,node-2:..."
    STORAGE_NODES: str = Field(
        default_factory=lambda: os.getenv(
            "STORAGE_NODES",
            "storage-node-1:http://127.0.0.1:8001,storage-node-2:http://127.0.0.1:8002,storage-node-3:http://127.0.0.1:8003,storage-node-4:http://127.0.0.1:8004,storage-node-5:http://127.0.0.1:8005"
        )
    )

    def get_parsed_nodes(self) -> List[Tuple[str, str]]:
        """Parses STORAGE_NODES string into a list of (node_id, url) tuples."""
        nodes: List[Tuple[str, str]] = []
        if not self.STORAGE_NODES:
            return nodes
        for part in self.STORAGE_NODES.split(","):
            part = part.strip()
            if not part:
                continue
            if ":" in part:
                # Format: node_id:http://host:port
                node_id, url = part.split(":", 1)
                nodes.append((node_id.strip(), url.strip()))
        return nodes

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = CoordinatorSettings()
