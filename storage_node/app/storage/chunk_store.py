import os
import re
import json
import shutil
import time
import uuid
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

try:
    from storage_node.app.config import settings, get_resolved_storage_path
    from storage_node.app.integrity.checksum import compute_sha256, compute_file_sha256, verify_checksum
except (ImportError, AttributeError):
    from app.config import settings, get_resolved_storage_path
    from app.integrity.checksum import compute_sha256, compute_file_sha256, verify_checksum

# Valid chunk ID pattern: prevent directory traversal or malformed paths
CHUNK_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-]+$")


class ChunkStorageError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class ChunkStore:
    def __init__(self, storage_dir: Optional[Path] = None):
        self.storage_dir = storage_dir or get_resolved_storage_path()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        # Dedicated temp directory on same filesystem for atomic rename
        self.temp_dir = self.storage_dir / ".tmp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def validate_chunk_id(self, chunk_id: str) -> None:
        if not chunk_id or not CHUNK_ID_PATTERN.match(chunk_id):
            raise ChunkStorageError(
                f"Invalid chunk ID '{chunk_id}'. Only alphanumeric characters, hyphens, and underscores are allowed.",
                status_code=400,
            )

    def _chunk_file_path(self, chunk_id: str) -> Path:
        self.validate_chunk_id(chunk_id)
        return self.storage_dir / f"chunk_{chunk_id}.bin"

    def _meta_file_path(self, chunk_id: str) -> Path:
        self.validate_chunk_id(chunk_id)
        return self.storage_dir / f"chunk_{chunk_id}.meta.json"

    def get_storage_stats(self) -> Dict[str, Any]:
        """Calculates actual disk and stored chunk metrics."""
        total_chunks = 0
        total_bytes_used = 0

        for item in self.storage_dir.glob("chunk_*.bin"):
            try:
                stat = item.stat()
                total_chunks += 1
                total_bytes_used += stat.st_size
            except OSError:
                continue

        # Get filesystem stats
        statvfs = os.statvfs(self.storage_dir)
        disk_total = statvfs.f_frsize * statvfs.f_blocks
        disk_free = statvfs.f_frsize * statvfs.f_bavail
        disk_used = disk_total - disk_free
        usage_percent = (disk_used / disk_total * 100.0) if disk_total > 0 else 0.0

        free_bytes = max(0, settings.STORAGE_LIMIT_BYTES - total_bytes_used)

        return {
            "node_id": settings.NODE_ID,
            "total_chunks": total_chunks,
            "total_bytes_used": total_bytes_used,
            "storage_limit_bytes": settings.STORAGE_LIMIT_BYTES,
            "free_bytes": free_bytes,
            "disk_total_bytes": disk_total,
            "disk_free_bytes": disk_free,
            "disk_usage_percent": round(usage_percent, 2),
        }

    def list_chunks(self) -> List[str]:
        chunks: List[str] = []
        prefix = "chunk_"
        suffix = ".bin"
        for item in self.storage_dir.glob("chunk_*.bin"):
            name = item.name
            if name.startswith(prefix) and name.endswith(suffix):
                chunk_id = name[len(prefix) : -len(suffix)]
                chunks.append(chunk_id)
        return chunks

    def chunk_exists(self, chunk_id: str) -> bool:
        return self._chunk_file_path(chunk_id).is_file()

    def get_chunk_meta(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        meta_path = self._meta_file_path(chunk_id)
        if meta_path.is_file():
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None
        return None

    def read_chunk(self, chunk_id: str) -> Tuple[bytes, str]:
        """Reads chunk bytes and returns (data, checksum)."""
        chunk_path = self._chunk_file_path(chunk_id)
        if not chunk_path.is_file():
            raise ChunkStorageError(f"Chunk '{chunk_id}' not found", status_code=404)

        try:
            with open(chunk_path, "rb") as f:
                data = f.read()
        except OSError as e:
            raise ChunkStorageError(f"Failed to read chunk file: {e}", status_code=500)

        checksum = compute_sha256(data)
        return data, checksum

    def write_chunk(
        self,
        chunk_id: str,
        data: bytes,
        expected_checksum: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Durably writes chunk with checksum verification, atomic rename, and fsync.
        Supports idempotent retries safely.
        """
        self.validate_chunk_id(chunk_id)
        data_size = len(data)

        # Check storage limit
        current_stats = self.get_storage_stats()
        if current_stats["total_bytes_used"] + data_size > settings.STORAGE_LIMIT_BYTES:
            raise ChunkStorageError(
                f"Storage limit exceeded on node {settings.NODE_ID}",
                status_code=507,  # Insufficient Storage
            )

        calculated_checksum = compute_sha256(data)

        if expected_checksum and not verify_checksum(calculated_checksum, expected_checksum):
            raise ChunkStorageError(
                f"Checksum mismatch for chunk {chunk_id}: expected {expected_checksum}, calculated {calculated_checksum}",
                status_code=422,
            )

        target_file = self._chunk_file_path(chunk_id)
        target_meta = self._meta_file_path(chunk_id)

        # Idempotent write check: if chunk already exists and matches checksum, return existing
        if target_file.is_file():
            existing_checksum = compute_file_sha256(target_file)
            if verify_checksum(existing_checksum, calculated_checksum):
                return {
                    "chunk_id": chunk_id,
                    "size_bytes": data_size,
                    "checksum_sha256": calculated_checksum,
                    "node_id": settings.NODE_ID,
                    "is_new": False,
                    "status": "existing_valid",
                }
            else:
                # Existing corrupted chunk: allow safe overwrite
                pass

        # Atomic write via temporary file
        temp_file = self.temp_dir / f"tmp_{chunk_id}_{uuid.uuid4().hex}.bin"
        temp_meta = self.temp_dir / f"tmp_{chunk_id}_{uuid.uuid4().hex}.meta.json"

        meta_info = {
            "chunk_id": chunk_id,
            "node_id": settings.NODE_ID,
            "size_bytes": data_size,
            "checksum_sha256": calculated_checksum,
            "created_at": time.time(),
        }

        try:
            with open(temp_file, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())

            with open(temp_meta, "w", encoding="utf-8") as f:
                json.dump(meta_info, f)
                f.flush()
                os.fsync(f.fileno())

            # Atomic replace
            os.replace(temp_file, target_file)
            os.replace(temp_meta, target_meta)

        except Exception as e:
            # Cleanup temp files on failure
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)
            if temp_meta.exists():
                temp_meta.unlink(missing_ok=True)
            raise ChunkStorageError(f"Failed to durably write chunk {chunk_id}: {e}", status_code=500)

        return {
            "chunk_id": chunk_id,
            "size_bytes": data_size,
            "checksum_sha256": calculated_checksum,
            "node_id": settings.NODE_ID,
            "is_new": True,
            "status": "stored",
        }

    def delete_chunk(self, chunk_id: str) -> bool:
        """Safely and idempotently removes chunk and its metadata."""
        self.validate_chunk_id(chunk_id)
        target_file = self._chunk_file_path(chunk_id)
        target_meta = self._meta_file_path(chunk_id)

        existed = target_file.is_file() or target_meta.is_file()

        if target_file.exists():
            target_file.unlink(missing_ok=True)
        if target_meta.exists():
            target_meta.unlink(missing_ok=True)

        return existed

    def verify_chunk(self, chunk_id: str) -> Dict[str, Any]:
        """Calculates live on-disk checksum and compares against stored metadata."""
        self.validate_chunk_id(chunk_id)
        target_file = self._chunk_file_path(chunk_id)
        meta = self.get_chunk_meta(chunk_id)

        if not target_file.is_file():
            return {
                "chunk_id": chunk_id,
                "node_id": settings.NODE_ID,
                "status": "missing",
                "stored_checksum": meta.get("checksum_sha256") if meta else None,
                "actual_checksum": None,
                "size_bytes": None,
                "verified_at": time.time(),
            }

        actual_checksum = compute_file_sha256(target_file)
        actual_size = target_file.stat().st_size
        stored_checksum = meta.get("checksum_sha256") if meta else None

        if stored_checksum:
            status = "valid" if verify_checksum(actual_checksum, stored_checksum) else "corrupted"
        else:
            status = "valid"

        return {
            "chunk_id": chunk_id,
            "node_id": settings.NODE_ID,
            "status": status,
            "stored_checksum": stored_checksum,
            "actual_checksum": actual_checksum,
            "size_bytes": actual_size,
            "verified_at": time.time(),
        }
