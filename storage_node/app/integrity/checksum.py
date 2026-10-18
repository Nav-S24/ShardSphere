import hashlib
from pathlib import Path
from typing import Union


def compute_sha256(data: Union[bytes, bytearray, memoryview]) -> str:
    """Calculates SHA-256 hash of bytes and returns hex string."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def compute_file_sha256(file_path: Path, chunk_size: int = 65536) -> str:
    """Reads a file in stream chunks and calculates SHA-256."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def verify_checksum(calculated: str, expected: str) -> bool:
    """Constant-time compare for SHA-256 checksums."""
    import hmac
    return hmac.compare_digest(calculated.strip().lower(), expected.strip().lower())
