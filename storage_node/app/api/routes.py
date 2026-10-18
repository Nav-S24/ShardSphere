import time
from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse

try:
    from storage_node.app.config import settings
    from storage_node.app.schemas import (
        NodeHealthResponse,
        NodeInfoResponse,
        StorageStatsResponse,
        ChunkWriteResponse,
        ChunkVerifyResponse,
        ChunkListResponse,
    )
    from storage_node.app.storage.chunk_store import ChunkStore, ChunkStorageError
except (ImportError, AttributeError):
    from app.config import settings
    from app.schemas import (
        NodeHealthResponse,
        NodeInfoResponse,
        StorageStatsResponse,
        ChunkWriteResponse,
        ChunkVerifyResponse,
        ChunkListResponse,
    )
    from app.storage.chunk_store import ChunkStore, ChunkStorageError

router = APIRouter()
chunk_store = ChunkStore()
START_TIME = time.time()


@router.get("/health", response_model=NodeHealthResponse, summary="Node Health Check")
async def health_check():
    """
    Returns the real-time operational health of this storage node.
    Used by the Coordinator's failure detector to register heartbeats.
    """
    return NodeHealthResponse(
        status="healthy",
        node_id=settings.NODE_ID,
        node_name=settings.NODE_NAME,
        uptime_seconds=round(time.time() - START_TIME, 2),
        timestamp=time.time(),
        version="1.0.0",
    )


@router.get("/node/info", response_model=NodeInfoResponse, summary="Node Identity & Configuration")
async def node_info():
    """Returns static node metadata, host, port, volume dir, and storage limits."""
    return NodeInfoResponse(
        node_id=settings.NODE_ID,
        node_name=settings.NODE_NAME,
        host=settings.HOST,
        port=settings.PORT,
        advertised_url=settings.ADVERTISED_URL,
        storage_dir=str(settings.STORAGE_DIR),
        storage_limit_bytes=settings.STORAGE_LIMIT_BYTES,
        version="1.0.0",
    )


@router.get("/storage/stats", response_model=StorageStatsResponse, summary="Physical Storage Usage")
async def storage_stats():
    """Returns actual filesystem disk and chunk metrics for this node's persistent volume."""
    try:
        stats = chunk_store.get_storage_stats()
        return StorageStatsResponse(**stats)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to calculate storage stats: {str(e)}")


@router.get("/chunks", response_model=ChunkListResponse, summary="List Stored Chunks")
async def list_chunks():
    """Lists all chunk IDs physically stored on this node."""
    chunks = chunk_store.list_chunks()
    return ChunkListResponse(
        node_id=settings.NODE_ID,
        total_chunks=len(chunks),
        chunks=chunks,
    )


@router.put(
    "/chunks/{chunk_id}",
    response_model=ChunkWriteResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Store Chunk",
)
async def store_chunk(
    chunk_id: str,
    request: Request,
    response: Response,
    x_expected_checksum: Optional[str] = Header(None, alias="X-Expected-Checksum"),
):
    """
    Receives raw chunk binary payload, verifies SHA-256 checksum, flushes to disk
    with fsync, and performs atomic rename to ensure durability.
    """
    try:
        data = await request.body()
        if not data:
            raise HTTPException(status_code=400, detail="Chunk body cannot be empty")

        result = chunk_store.write_chunk(
            chunk_id=chunk_id,
            data=data,
            expected_checksum=x_expected_checksum,
        )

        if not result["is_new"]:
            response.status_code = status.HTTP_200_OK

        return ChunkWriteResponse(**result)

    except ChunkStorageError as cse:
        raise HTTPException(status_code=cse.status_code, detail=cse.message)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error storing chunk: {str(e)}")


@router.get("/chunks/{chunk_id}", summary="Retrieve Chunk")
async def get_chunk(chunk_id: str):
    """
    Retrieves the raw binary content of a chunk.
    Includes the SHA-256 checksum in the ETag header.
    """
    try:
        data, checksum = chunk_store.read_chunk(chunk_id)
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={
                "ETag": f'"{checksum}"',
                "Content-Length": str(len(data)),
                "X-Chunk-ID": chunk_id,
                "X-Node-ID": settings.NODE_ID,
            },
        )
    except ChunkStorageError as cse:
        raise HTTPException(status_code=cse.status_code, detail=cse.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error reading chunk: {str(e)}")


@router.delete("/chunks/{chunk_id}", summary="Delete Chunk")
async def delete_chunk(chunk_id: str):
    """
    Safely and idempotently removes a chunk and its sidecar metadata from persistent volume.
    """
    try:
        existed = chunk_store.delete_chunk(chunk_id)
        return {
            "chunk_id": chunk_id,
            "node_id": settings.NODE_ID,
            "deleted": True,
            "existed_prior": existed,
        }
    except ChunkStorageError as cse:
        raise HTTPException(status_code=cse.status_code, detail=cse.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error deleting chunk: {str(e)}")


@router.get(
    "/chunks/{chunk_id}/verify",
    response_model=ChunkVerifyResponse,
    summary="Verify Chunk Integrity",
)
async def verify_chunk_integrity(chunk_id: str):
    """
    Calculates live SHA-256 hash of the on-disk file and verifies against sidecar metadata.
    Detects bit rot, truncation, or corruption.
    """
    try:
        result = chunk_store.verify_chunk(chunk_id)
        return ChunkVerifyResponse(**result)
    except ChunkStorageError as cse:
        raise HTTPException(status_code=cse.status_code, detail=cse.message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to verify chunk: {str(e)}")
