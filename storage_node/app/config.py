import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class StorageNodeSettings(BaseSettings):
    NODE_ID: str = Field(default_factory=lambda: os.getenv("NODE_ID", "storage-node-1"))
    NODE_NAME: str = Field(default_factory=lambda: os.getenv("NODE_NAME", "Storage Node 1"))
    HOST: str = Field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    PORT: int = Field(default_factory=lambda: int(os.getenv("PORT", "8001")))
    ADVERTISED_URL: str = Field(default_factory=lambda: os.getenv("ADVERTISED_URL", "http://localhost:8001"))
    
    # Persistent storage path for chunks
    STORAGE_DIR: str = Field(default_factory=lambda: os.getenv("STORAGE_DIR", "/data/chunks"))
    STORAGE_LIMIT_BYTES: int = Field(default_factory=lambda: int(os.getenv("STORAGE_LIMIT_BYTES", str(10 * 1024 * 1024 * 1024))))  # 10 GiB default

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = StorageNodeSettings()

# Ensure local fallback directory exists if running locally
def get_resolved_storage_path() -> Path:
    storage_path = Path(settings.STORAGE_DIR)
    if not storage_path.is_absolute():
        storage_path = Path.cwd() / storage_path
    storage_path.mkdir(parents=True, exist_ok=True)
    return storage_path
