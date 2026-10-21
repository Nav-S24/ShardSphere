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
   - `lamport_clock`: Logical clock value at event origin
   - `origin_node_id`: Participant that generated the event
   - `causal_parent_id`: Primary causal parent event ID
   - `causal_parent_ids`: JSON array of parent event IDs (supporting multi-parent causality)
   - `correlation_id`: Logical correlation identifier (e.g. `file_id`)
8. **`participant_clocks`**:
   - `participant_id`: Primary Key (Coordinator or storage node ID)
   - `clock_value`: Monotonically advancing logical clock integer (BIGINT)
   - `updated_at`: Wall-clock timestamp of last clock advancement

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

---

## 6. Distributed Lamport Logical Clocks & Causal Ordering (Phase 4)

### A. Participants Topology
The cluster consists of 6 discrete logical clock participants:
- **`coordinator-primary-1`**: The distributed coordinator holding the leader election lease.
- **`storage-node-1` through `storage-node-5`**: Five independent storage nodes maintaining isolated volume mounts and chunk stores.

Each participant maintains an independent, strictly monotonic scalar clock.

### B. Lamport Clock Update Rules
The `LamportClockEngine` enforces Leslie Lamport's 1978 logical clock specification:
- **Rule 1 (Local Event)**: Before generating an internal event on participant $i$, its clock advances:
  $$L_i = L_i + 1$$
- **Rule 2 (Send Event)**: When participant $i$ sends a message to participant $j$, it advances its clock and attaches the timestamp to the message:
  $$L_i = L_i + 1, \quad T_{\text{msg}} = L_i$$
- **Rule 3 (Receive Event)**: When participant $j$ receives a message with timestamp $T_{\text{msg}}$, it advances its clock past the maximum of its local clock and the incoming timestamp:
  $$L_j = \max(L_j, T_{\text{msg}}) + 1$$

### C. The `participant_clocks` Table & Crash Recovery
Clocks are persisted to SQLite via `participant_clocks` with atomic conditional upserts:
```sql
INSERT INTO participant_clocks (participant_id, clock_value, updated_at)
VALUES (?, ?, ?)
ON CONFLICT(participant_id) DO UPDATE SET
  clock_value = CASE
    WHEN excluded.clock_value > participant_clocks.clock_value THEN excluded.clock_value
    ELSE participant_clocks.clock_value
  END,
  updated_at = excluded.updated_at
WHERE excluded.clock_value > participant_clocks.clock_value;
```
**Crash Recovery (`hydrate()`)**:
Upon coordinator initialization:
1. Loads all existing clock values from `participant_clocks`.
2. Scans `cluster_events` for `max(lamport_clock)` grouped by `origin_node_id`, advancing any participant's clock to its historical high-water mark.
3. Queries global `max(lamport_clock)` from `cluster_events` to ensure the coordinator's clock is at least as high as any event recorded anywhere in cluster history.
4. Persists the recovered high-water mark back to SQLite.

### D. Causal Event Metadata & Multiple Causal Parents
Every cluster event is recorded inside an atomic SQLite transaction (`BEGIN IMMEDIATE` ... `COMMIT` / `ROLLBACK`) that updates both `participant_clocks` and `cluster_events`.

To support events resulting from concurrent actions (e.g., multi-node replication completions, returning-node reconciliation, or multi-replica convergence), the engine supports multiple causal parents:
- `causal_parent_id`: Primary parent event ID string (for backward compatibility).
- `causal_parent_ids`: Serialized JSON array of predecessor event IDs (e.g. `["evt_1", "evt_2"]`).
- `correlation_id`: Links related workflows across different lifecycle stages (e.g., `file_id`).

### E. Clock Endpoints
- **`GET /api/v1/clocks`** (alias: **`GET /api/v1/clock`**): Returns the full logical clock vector across all participants:
  ```json
  {
    "coordinator": { "id": "coordinator-primary-1", "clock": 591 },
    "nodes": [
      { "id": "storage-node-1", "clock": 595 },
      { "id": "storage-node-2", "clock": 599 },
      { "id": "storage-node-3", "clock": 603 },
      { "id": "storage-node-4", "clock": 580 },
      { "id": "storage-node-5", "clock": 580 }
    ],
    "highest_observed": 621,
    "timestamp": 1792582412.4
  }
  ```

### F. `X-Lamport-Clock` Header Handling
An Express middleware is mounted on `['/api', '/api/v1']`:
- **Inbound**: Inspects incoming `X-Lamport-Clock` headers using `processUntrustedClientClock()`.
  - Rejects non-numeric, negative, floating-point, NaN, or non-finite inputs.
  - Implements **Clock-Exhaustion DoS Defense**: if an untrusted client sends an absurdly large value (> current clock + 10,000 steps or near `Number.MAX_SAFE_INTEGER`), the coordinator advances its clock by a single local tick (`tickLocal`) without adopting the spoofed jump.
  - Valid client clocks advance the coordinator clock via Rule 3 (`tickReceive`).
- **Outbound**: Wraps `res.json` and `res.send` to attach the authoritative `X-Lamport-Clock: <coordinator_clock>` on all API responses.

### G. Deterministic Event Ordering & Limitations
The endpoint `GET /api/v1/events?order_by=lamport&direction=asc|desc` applies a three-level deterministic tie-breaker:
```sql
ORDER BY lamport_clock DESC, origin_node_id ASC, event_id ASC
```
**Theoretical Limitations**:
1. Lamport logical clocks define a **partial order** via the happens-before relation ($\to$). If event $A \to B$, then $L(A) < L(B)$.
2. The converse does **not** hold: $L(A) < L(B)$ does not necessarily mean $A \to B$; events on independent nodes can have arbitrary numerical relations if no causal message was exchanged.
3. The secondary sort keys (`origin_node_id`, `event_id`) produce a deterministic **total order** consistent with causality, but they do not prove that two concurrent events had a causal relationship.

### H. Architectural Boundary Distinction
It is critical to distinguish the role of logical clocks from the cluster's physical durability and consistency mechanisms:
- **Lamport Logical Clocks**: Provide causal lineage tracking, debugging telemetry, and chronological auditing across distributed participants. They **do not** enforce mutual exclusion or guarantee write consensus.
- **Leader Leases & Fencing Tokens**: Enforce mutual exclusion on metadata operations, preventing split-brain writes through monotonic fencing tokens (`#28`).
- **Replication Quorum (3x)**: Guarantees durability and availability across node failures by requiring three independent physical replicas before commit.
- **SHA-256 Checksums**: Ensure end-to-end cryptographic data integrity, detecting on-disk bit-rot, truncation, or transport tampering.

