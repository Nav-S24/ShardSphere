import tempfile
import pytest
from pathlib import Path
import sys
import os

# Insert storage_node directory to sys.path
storage_node_dir = Path(__file__).resolve().parent.parent
if str(storage_node_dir) not in sys.path:
    sys.path.insert(0, str(storage_node_dir))

try:
    from storage_node.app.storage.chunk_store import ChunkStore, ChunkStorageError
    from storage_node.app.integrity.checksum import compute_sha256, verify_checksum
except ImportError:
    from app.storage.chunk_store import ChunkStore, ChunkStorageError
    from app.integrity.checksum import compute_sha256, verify_checksum


def test_chunk_write_and_read():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = ChunkStore(storage_dir=Path(temp_dir))
        data = b"Hello, ShardSphere Distributed Storage Node!"
        sha256 = compute_sha256(data)
        chunk_id = "chunk-test-001"

        # Write chunk
        result = store.write_chunk(chunk_id, data, expected_checksum=sha256)
        assert result["status"] == "stored"
        assert result["size_bytes"] == len(data)
        assert result["checksum_sha256"] == sha256
        assert result["is_new"] is True

        # Idempotent write should not fail
        retry_result = store.write_chunk(chunk_id, data, expected_checksum=sha256)
        assert retry_result["is_new"] is False
        assert retry_result["status"] == "existing_valid"

        # Read back
        read_data, read_sha = store.read_chunk(chunk_id)
        assert read_data == data
        assert read_sha == sha256


def test_chunk_checksum_mismatch():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = ChunkStore(storage_dir=Path(temp_dir))
        data = b"Corrupted bytes"
        wrong_sha = "0000000000000000000000000000000000000000000000000000000000000000"

        with pytest.raises(ChunkStorageError) as exc_info:
            store.write_chunk("chunk-test-corrupt", data, expected_checksum=wrong_sha)
        assert exc_info.value.status_code == 422


def test_path_traversal_prevention():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = ChunkStore(storage_dir=Path(temp_dir))
        with pytest.raises(ChunkStorageError) as exc_info:
            store.write_chunk("../../../etc/passwd", b"malicious", None)
        assert exc_info.value.status_code == 400


def test_storage_stats_and_delete():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = ChunkStore(storage_dir=Path(temp_dir))
        data = b"Testing stats and deletion"
        chunk_id = "chunk-stat-01"

        store.write_chunk(chunk_id, data, compute_sha256(data))
        stats = store.get_storage_stats()
        assert stats["total_chunks"] == 1
        assert stats["total_bytes_used"] == len(data)

        # Verification
        v = store.verify_chunk(chunk_id)
        assert v["status"] == "valid"

        # Delete
        assert store.delete_chunk(chunk_id) is True
        assert store.chunk_exists(chunk_id) is False
        stats_after = store.get_storage_stats()
        assert stats_after["total_chunks"] == 0
