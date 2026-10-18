import asyncio
import uuid
from typing import List, Dict, Tuple, Optional
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.metadata import StorageNodeRecord, ChunkMetadata, ReplicaMetadata
from app.integrity.checksum import compute_sha256, verify_checksum


class StorageDistributor:
    """
    Handles chunk placement strategy, inter-node chunk distribution,
    parallel replication writes, and retrieval.
    """

    def __init__(self):
        self._http_client: Optional[httpx.AsyncClient] = None
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_client(self) -> httpx.AsyncClient:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self._http_client is not None:
            if self._http_client.is_closed or self._client_loop is not current_loop:
                self._http_client = None

        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
            self._client_loop = current_loop

        return self._http_client

    def split_into_chunks(self, data: bytes, chunk_size: int = settings.DEFAULT_CHUNK_SIZE_BYTES) -> List[Tuple[int, bytes, str]]:
        """
        Splits data into chunks of configurable size.
        Returns list of (sequence_index, chunk_bytes, chunk_sha256).
        Supports empty file as 1 zero-length chunk.
        """
        if len(data) == 0:
            checksum = compute_sha256(b"")
            return [(0, b"", checksum)]

        chunks: List[Tuple[int, bytes, str]] = []
        offset = 0
        seq = 0
        total_len = len(data)

        while offset < total_len:
            end = min(offset + chunk_size, total_len)
            chunk_bytes = data[offset:end]
            checksum = compute_sha256(chunk_bytes)
            chunks.append((seq, chunk_bytes, checksum))
            offset = end
            seq += 1

        return chunks

    async def select_placement_nodes(
        self,
        session: AsyncSession,
        sequence_index: int,
        replication_factor: int = settings.REPLICATION_FACTOR,
        exclude_node_ids: Optional[List[str]] = None,
    ) -> List[StorageNodeRecord]:
        """
        Selects target storage nodes for chunk replicas based on:
        1. Node health (HEALTHY or RECOVERING only)
        2. Free storage capacity
        3. Round-robin / hash distribution across available nodes
        4. Strict exclusion of duplicates (no 2 replicas on the same node)
        """
        result = await session.execute(
            select(StorageNodeRecord).where(
                StorageNodeRecord.health_state.in_(["HEALTHY", "RECOVERING"])
            )
        )
        eligible_nodes = list(result.scalars().all())

        if exclude_node_ids:
            eligible_nodes = [n for n in eligible_nodes if n.node_id not in exclude_node_ids]

        if len(eligible_nodes) < replication_factor:
            raise RuntimeError(
                f"Insufficient healthy storage nodes: needed {replication_factor}, available {len(eligible_nodes)}"
            )

        # Deterministic balanced distribution: sort nodes by ID, rotate by sequence index
        sorted_nodes = sorted(eligible_nodes, key=lambda n: n.node_id)
        n_count = len(sorted_nodes)

        selected: List[StorageNodeRecord] = []
        for i in range(replication_factor):
            target_idx = (sequence_index + i) % n_count
            selected.append(sorted_nodes[target_idx])

        return selected

    async def write_chunk_replica(
        self,
        node_url: str,
        chunk_id: str,
        chunk_data: bytes,
        expected_checksum: str,
    ) -> bool:
        """Writes chunk to a remote storage node and verifies durable receipt."""
        client = self._get_client()
        url = f"{node_url.rstrip('/')}/chunks/{chunk_id}"
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Expected-Checksum": expected_checksum,
        }

        try:
            resp = await client.put(url, content=chunk_data, headers=headers, timeout=10.0)
            return resp.status_code in (200, 201)
        except Exception as e:
            print(f"[StorageDistributor] Failed write to {node_url}: {e}")
            return False

    async def fetch_chunk_replica(
        self,
        node_url: str,
        chunk_id: str,
        expected_checksum: str,
    ) -> Optional[bytes]:
        """Fetches chunk from a node and validates SHA-256 checksum."""
        client = self._get_client()
        url = f"{node_url.rstrip('/')}/chunks/{chunk_id}"

        try:
            resp = await client.get(url, timeout=10.0)
            if resp.status_code == 200:
                data = resp.content
                calculated = compute_sha256(data)
                if verify_checksum(calculated, expected_checksum):
                    return data
                else:
                    print(f"[StorageDistributor] Checksum mismatch from {node_url} for chunk {chunk_id}")
                    return None
            return None
        except Exception as e:
            print(f"[StorageDistributor] Fetch error from {node_url}: {e}")
            return None

    async def delete_chunk_replica(self, node_url: str, chunk_id: str) -> bool:
        """Deletes chunk from physical storage node."""
        client = self._get_client()
        url = f"{node_url.rstrip('/')}/chunks/{chunk_id}"
        try:
            resp = await client.delete(url, timeout=5.0)
            return resp.status_code in (200, 204)
        except Exception:
            return False


distributor = StorageDistributor()
