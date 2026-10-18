# ShardSphere — Architecture & System Design Specification

## 1. System Overview

**ShardSphere** is a high-reliability, self-healing distributed file storage engine designed for enterprise durability, partition tolerance, and automatic re-replication. Files are ingested, verified with SHA-256 cryptographic checksums, partitioned into 1 MiB chunks, and distributed across five independent storage nodes with 3x replication.

```
                              +---------------------------+
                              |      React Dashboard      |
                              |   (Monitoring & Explorer) |
                              +-------------+-------------+
                                            |
                                            v REST / JSON
                              +---------------------------+
                              |    FastAPI Coordinator    |
                              |  (Leader Leased via DB)   |
                              +------+--------------+-----+
                                     |              |
                      Heartbeats &   |              | Relational Schema
                      Placement HTTP |              | (Leases, Chunks, Files)
                                     |              v
            +------------------------+      +---------------------------+
            |                               |    PostgreSQL Metadata    |
            |                               |    (Strong Consistency)   |
            v                               +---------------------------+
  +-----------------------------------------------------------------------------+
  |                             Storage Nodes Cluster                            |
  |                                                                             |
  |  +----------------+  +----------------+  +----------------+  +------------+ |
  |  | Storage Node 1 |  | Storage Node 2 |  | Storage Node 3 |  | ... Node 5 | |
  |  |  (Port: 8001)  |  |  (Port: 8002)  |  |  (Port: 8003)  |  | (Port 8005)| |
  |  +-------+--------+  +-------+--------+  +-------+--------+  +-----+------+ |
  |          |                   |                   |                 |        |
  |  +-------v--------+  +-------v--------+  +-------v--------+  +-----v------+ |
  |  | Volume: node1  |  | Volume: node2  |  | Volume: node3  |  |Vol: node5  | |
  |  +----------------+  +----------------+  +----------------+  +------------+ |
  +-----------------------------------------------------------------------------+
```

---

## 2. Service Contracts & API Boundaries

### A. Coordinator Service (`/api/v1`)
- **`POST /api/v1/files`**: Multipart upload endpoint. Splits byte stream into 1 MiB chunks, calculates chunk SHA-256 checksums, places 3 distinct replicas per chunk on healthy nodes, writes in parallel, and commits file metadata upon durability confirmation.
- **`GET /api/v1/files`**: Paginated file inventory with status, size, metadata version, and chunk count.
- **`GET /api/v1/files/{file_id}`**: Detailed chunk placement topology, replica health states, node URLs, and sequence order.
- **`GET /api/v1/files/{file_id}/download`**: Reassembles chunk sequence into the original byte stream, verifying SHA-256 integrity on each chunk and on the full reconstructed file.
- **`DELETE /api/v1/files/{file_id}`**: Coordinated physical chunk deletion across nodes and transactional purge.
- **`GET /api/v1/cluster`**: Aggregate health of all 5 storage nodes, total chunks, storage utilization, and leader election lease status.
- **`GET /api/v1/leader`**: Current leader lease holder ID, fencing token, expiration timestamp, and remaining seconds.
- **`GET /api/v1/recovery/jobs`**: Active and completed self-healing re-replication jobs.
- **`POST /api/v1/integrity/verify`**: On-demand cluster-wide cryptographic audit of chunk replicas against disk.

### B. Independent Storage Node Service (`/`)
- **`GET /health`**: Operational heartbeat response reporting node identity, uptime, and status.
- **`GET /node/info`**: Node metadata, host, port, volume mount path, and storage limit.
- **`GET /storage/stats`**: Physical disk statistics (`statvfs`), total chunk files stored, and byte allocation.
- **`PUT /chunks/{chunk_id}`**: Stores binary chunk. Computes SHA-256, verifies against `X-Expected-Checksum`, calls `os.fsync`, and atomically replaces temporary file. Idempotent.
- **`GET /chunks/{chunk_id}`**: Reads binary chunk from persistent storage with `ETag` checksum header.
- **`DELETE /chunks/{chunk_id}`**: Purges chunk binary and metadata sidecar.
- **`GET /chunks/{chunk_id}/verify`**: Computes live on-disk hash and compares with stored metadata to detect bit rot.

---

## 3. Database Schema (PostgreSQL)

The metadata database enforces referential integrity, unique constraints, and atomic transactions:

1. **`files`**:
   - `file_id`: Primary Key (String 64)
   - `filename`, `content_type`, `size_bytes`, `checksum_sha256`
   - `chunk_count`, `chunk_size_bytes`, `replication_factor`, `metadata_version`
   - `status`: Lifecycle (`pending`, `committed`, `degraded`, `deleting`)
2. **`chunks`**:
   - `chunk_id`: Primary Key (String 64)
   - `file_id`: Foreign Key (`files.file_id` ON DELETE CASCADE)
   - `sequence_index`: 0-indexed integer ordering
   - `size_bytes`, `checksum_sha256`, `generation`
   - Unique Constraint: `(file_id, sequence_index)`
3. **`replicas`**:
   - `replica_id`: Primary Key
   - `chunk_id`: Foreign Key (`chunks.chunk_id` ON DELETE CASCADE)
   - `node_id`, `node_url`, `status`, `generation`, `last_verified_at`
   - Unique Constraint: `(chunk_id, node_id)`
4. **`storage_nodes`**:
   - `node_id`: Primary Key
   - `node_name`, `advertised_url`, `health_state`, `consecutive_failures`
   - `last_heartbeat_at`, `total_chunks`, `total_bytes_used`
5. **`leader_leases`**:
   - `lease_id`: Primary Key
   - `holder_id`: Active coordinator instance ID
   - `fencing_token`: Monotonically increasing 64-bit integer
   - `expires_at`: Epoch timestamp for lease expiration
6. **`recovery_jobs`**:
   - `job_id`: Primary Key
   - `chunk_id`, `file_id`, `source_node_id`, `target_node_id`
   - `status` (`pending`, `in_progress`, `completed`, `failed`), `attempts`
7. **`cluster_events`**:
   - `event_id`: Primary Key
   - `event_type`, `severity`, `node_id`, `file_id`, `message`, `timestamp`

---

## 4. Leader Election & Fencing Tokens

To prevent split-brain conditions across multiple coordinator instances:
- The coordinator acquires a time-bounded lease in the database (`leader_leases` row with row-level locking `FOR UPDATE`).
- Each new leadership acquisition monotonically increments the `fencing_token`.
- Every mutation checks that the current coordinator holds the lease. If a coordinator's lease expires, a standby coordinator takes over with an incremented fencing token, immediately invalidating any delayed writes from the stale coordinator.

---

## 5. Failure Detection & Health State Machine

- **Heartbeat Interval**: 2 seconds.
- Consecutive missed checks:
  - 1-2 missed: `HEALTHY` (transient network jitter allowance)
  - 3 missed: `SUSPECT`
  - Unresponsive for >= 10 seconds (`FAILURE_THRESHOLD_SECONDS`): `UNAVAILABLE`
- When a node resumes responding: Transitions to `RECOVERING` -> `HEALTHY`.
- Under-replicated chunks are automatically detected by the `ReplicationReconciler` and recovered from surviving replicas.
