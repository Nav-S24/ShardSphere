import pytest
from pathlib import Path
import sys

# Add backend to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.storage.distributor import StorageDistributor
from app.integrity.checksum import compute_sha256


def test_chunk_splitting():
    dist = StorageDistributor()
    chunk_size = 1024  # 1 KB chunk size for test
    total_size = 2500  # 2 full chunks + 1 partial chunk
    data = os_bytes = bytes([i % 256 for i in range(total_size)])

    chunks = dist.split_into_chunks(data, chunk_size=chunk_size)
    assert len(chunks) == 3

    # Check sequence indices and sizes
    assert chunks[0][0] == 0
    assert len(chunks[0][1]) == 1024
    assert chunks[0][2] == compute_sha256(chunks[0][1])

    assert chunks[1][0] == 1
    assert len(chunks[1][1]) == 1024
    assert chunks[1][2] == compute_sha256(chunks[1][1])

    assert chunks[2][0] == 2
    assert len(chunks[2][1]) == 452
    assert chunks[2][2] == compute_sha256(chunks[2][1])

    # Reconstruct
    reconstructed = b"".join(c[1] for c in chunks)
    assert reconstructed == data


def test_empty_file_chunk_splitting():
    dist = StorageDistributor()
    empty_data = b""
    chunks = dist.split_into_chunks(empty_data, chunk_size=1024)
    assert len(chunks) == 1
    assert chunks[0][0] == 0
    assert chunks[0][1] == b""
    assert chunks[0][2] == compute_sha256(b"")
