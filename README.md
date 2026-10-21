# ShardSphere — Self-Healing Distributed File Storage Engine

[![Tests](https://img.shields.io/badge/pytest-8%20passed-brightgreen.svg)]()
[![Cluster](https://img.shields.io/badge/storage--nodes-5%20independent-blue.svg)]()
[![Replication](https://img.shields.io/badge/replication--factor-3x-purple.svg)]()

ShardSphere is a full-stack, distributed file storage system engineered for strong durability, partition tolerance, and automatic self-healing. Files uploaded to the cluster are cryptographically hashed with SHA-256, chunked into 1 MiB segments, placed across independent storage nodes with 3x redundancy, and continually monitored for node failure and replica degradation.

---

## Phase 1 Implementation Summary

Phase 1 provides the complete foundational architecture, service contracts, database models, Docker containers, multi-node configuration, and automated test suite:

- **Independent Storage Nodes**: 5 independent FastAPI storage services (`storage-node-1` through `storage-node-5`), each operating on dedicated ports (8001–8005) with isolated persistent volumes (`/data/chunks`).
- **Distributed Coordinator**: FastAPI backend managing cluster metadata, chunk placement algorithms, leader election leases, failure detection, and self-healing.
- **Relational Metadata Database**: PostgreSQL/SQLAlchemy schemas for files, chunks, replicas, storage node registry, leader leases, recovery jobs, and audit events.
- **Container Infrastructure**: Production Dockerfiles for backend, storage nodes, and frontend, plus a complete `docker-compose.yml` linking all services over a dedicated bridge network.
- **Service Verification**: Fully automated health verification script (`scripts/verify_health.sh`) confirming all 5 storage nodes and the coordinator.

---

## Repository Structure

```
shardsphere/
├── docker-compose.yml              # Complete 8-service deployment specification
├── .env.example                    # Production configuration template
├── README.md                       # Engineering documentation
├── docs/
│   └── architecture.md             # In-depth architectural design & failure semantics
├── scripts/
│   ├── start_cluster.sh            # Local cluster runner (starts 5 nodes + coordinator)
│   ├── verify_health.sh            # Live cluster health & statistics verification
│   └── stop_cluster.sh             # Graceful shutdown script
├── backend/                        # Coordinator Service
│   ├── Dockerfile                  # Container definition
│   ├── requirements.txt            # Python dependencies (FastAPI, SQLAlchemy, asyncpg)
│   ├── app/
│   │   ├── config.py               # Coordinator cluster settings
│   │   ├── main.py                 # FastAPI app entry with lifespan loops
│   │   ├── api/                    # Versioned REST router (/api/v1)
│   │   ├── coordinator/            # File upload, download & deletion pipelines
│   │   ├── election/               # PostgreSQL lease & fencing tokens
│   │   ├── failure_detection/      # Heartbeat monitoring & state machine
│   │   ├── integrity/              # SHA-256 calculation & validation
│   │   ├── metadata/               # Database engine & session maker
│   │   ├── models/                 # SQLAlchemy 2.0 relational schemas
│   │   ├── replication/            # Self-healing & auto re-replication
│   │   └── schemas/                # Pydantic request/response contracts
│   └── tests/                      # Pytest automated test suite
├── storage_node/                   # Independent Storage Node Service
│   ├── Dockerfile                  # Container definition
│   ├── requirements.txt            # Storage node dependencies
│   ├── app/
│   │   ├── config.py               # Node identity & volume configuration
│   │   ├── main.py                 # FastAPI application
│   │   ├── api/                    # Chunk endpoints (/health, /chunks/{id})
│   │   ├── integrity/              # Live checksum & bit-rot verification
│   │   ├── schemas.py              # Storage node Pydantic models
│   │   └── storage/                # Atomic chunk write with fsync
│   └── tests/                      # Storage node unit tests
└── frontend/                       # React Monitoring Dashboard
    ├── Dockerfile                  # Multi-stage container build
    ├── nginx.conf                  # Nginx proxy configuration
    └── package.json                # Frontend package configuration
```

---

## Service Contracts & API Summary

### Coordinator Endpoints (`/api/v1`)
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/files` | Upload file, chunk into 1 MiB segments, write 3 replicas, commit metadata |
| `GET` | `/api/v1/files` | List committed files with pagination |
| `GET` | `/api/v1/files/{file_id}` | Detailed file chunk layout and replica health status |
| `GET` | `/api/v1/files/{file_id}/download` | Reassemble chunks and download full verified file |
| `DELETE` | `/api/v1/files/{file_id}` | Coordinated physical deletion across all nodes with Lamport clock synchronization |
| `GET` | `/api/v1/cluster` | Real-time cluster overview, active nodes, and stored data stats |
| `GET` | `/api/v1/nodes` | List all 5 storage nodes with health states and disk stats |
| `GET` | `/api/v1/leader` | Current leader election lease and fencing token |
| `GET` | `/api/v1/recovery/jobs` | Self-healing re-replication job status |
| `POST` | `/api/v1/integrity/verify` | Trigger on-demand cryptographic audit of all stored chunks |
| `GET` | `/api/v1/clocks` (or `/api/v1/clock`) | Real-time scalar Lamport clock vector across coordinator and 5 storage nodes |
| `GET` | `/api/v1/events` | Audit event stream with deterministic Lamport causal ordering (`order_by=lamport`) |

### Storage Node Endpoints (`/`)
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Heartbeat health endpoint reporting uptime and node ID |
| `GET` | `/node/info` | Node configuration, volume mount, and storage capacity |
| `GET` | `/storage/stats` | Live disk statistics (`statvfs`) and byte allocation |
| `GET` | `/chunks` | List all chunk IDs stored on the node |
| `PUT` | `/chunks/{chunk_id}` | Atomic write with SHA-256 verification and `fsync` |
| `GET` | `/chunks/{chunk_id}` | Raw binary chunk retrieval with `ETag` checksum |
| `DELETE` | `/chunks/{chunk_id}` | Safe deletion of chunk binary and metadata sidecar |
| `GET` | `/chunks/{chunk_id}/verify` | On-disk hash computation against metadata |

---

## Phase 4: Distributed Lamport Logical Clocks & Causal Ordering

ShardSphere integrates Leslie Lamport's 1978 logical clock specification to establish happens-before causal ordering across distributed participants:

### 1. Participants Topology
The cluster defines 6 distinct participants:
- `coordinator-primary-1`: Coordinator service managing cluster leases, uploads, and health reconciliation.
- `storage-node-1` through `storage-node-5`: 5 independent storage nodes with isolated volumes (`/data/storage_node_<n>/chunks`).

### 2. Lamport Clock Rules
- **Rule 1 (Local Event)**: Internal participant action advances clock: $L_i = L_i + 1$.
- **Rule 2 (Send Event)**: Outgoing RPC/message advances sender clock and attaches timestamp: $L_i = L_i + 1, T_{\text{msg}} = L_i$.
- **Rule 3 (Receive Event)**: Incoming message updates receiver clock past message timestamp: $L_j = \max(L_j, T_{\text{msg}}) + 1$.

### 3. Persistence & Crash Recovery
Clocks are persisted in SQLite via `participant_clocks`:
```sql
CREATE TABLE IF NOT EXISTS participant_clocks (
  participant_id VARCHAR(64) PRIMARY KEY,
  clock_value BIGINT NOT NULL DEFAULT 0,
  updated_at FLOAT NOT NULL
);
```
Conditional upserts prevent clock regression during concurrent updates. On crash recovery, `LamportClockEngine.hydrate()` inspects both `participant_clocks` and historical `cluster_events` to restore each participant to its highest observed historical high-water mark.

### 4. Causal Event Metadata & Multiple Causal Parents
Every event logged to `cluster_events` contains:
- `lamport_clock`: Logical timestamp when event occurred.
- `origin_node_id`: Participant that generated the event.
- `causal_parent_id`: Primary predecessor event ID.
- `causal_parent_ids`: Serialized JSON array of predecessor event IDs supporting multi-parent causality (e.g., multi-node replication completions or quorum convergence).
- `correlation_id`: Lifecycle correlation tag (e.g., `file_id`).

### 5. `X-Lamport-Clock` Middleware & Security
- **Incoming**: Middleware intercepts `X-Lamport-Clock` headers on `/api` and `/api/v1`. Untrusted values are strictly validated (rejecting negative, non-numeric, or non-finite inputs). If an attacker sends a spoofed clock (> current clock + 10,000 steps or near `MAX_SAFE_INTEGER`), the coordinator defends against clock exhaustion by incrementing only a single local tick (`tickLocal`). Valid headers advance the coordinator clock via Rule 3.
- **Outgoing**: All responses include the authoritative `X-Lamport-Clock: <coordinator_clock>` header.

### 6. Deterministic Event Ordering & Limitations
Events can be fetched ordered by logical time:
```
GET /api/v1/events?order_by=lamport&direction=desc
```
Tie-breaking is strictly deterministic:
`ORDER BY lamport_clock DESC, origin_node_id ASC, event_id ASC`

**Limitations**:
Lamport clocks establish a **partial order** ($A \to B \implies L(A) < L(B)$). The converse ($L(A) < L(B) \implies A \to B$) does not strictly hold between independent nodes that never communicated. The secondary tie-breaking keys create a deterministic **total order** consistent with causality, but do not imply a direct causal connection between concurrent events.

### 7. Architectural Boundaries
- **Lamport Clocks**: Causal lineage and chronological debugging. They do **not** enforce mutual exclusion or data consensus.
- **Leader Leases & Fencing Tokens**: Mutual exclusion for metadata operations and split-brain prevention via monotonic tokens (`#28`).
- **Replication Quorum (3x)**: Physical durability guaranteeing availability across storage node failures.
- **SHA-256 Checksums**: Cryptographic data integrity detecting bit-rot and transmission errors.

---

## Deployment & Verification

### Running the Full-Stack Application

```bash
# Start server with Vite middleware on port 3000 (0.0.0.0)
npm run dev

# Production build
npm run build
npm start
```

---

## Automated Test Suites

The TypeScript test suite covers clock mechanics, workflow causal progression, cluster integration, and storage audit invariants:

| Test Script | Description |
|---|---|
| `npm run test:clock` | 12 focused unit tests for LamportClockEngine (Rules 1-3, concurrency, persistence, overflow) |
| `npm run test:workflow` | 6 integration tests for causal upload, multi-node replication, and self-healing |
| `npm run test:audit` | 6 storage consistency, inventory classification, and SHA-256 audit tests |
| `npm run test:coverage` | Bit-rot detection & in-place repair, orphan reclamation, and quota enforcement |
| `npm run test` | Full 8-stage end-to-end cluster integration suite |
| `npm run test:all` | Executes all 5 test suites consecutively |
| `npm run lint` | TypeScript type-checking (`tsc --noEmit`) |
| `npm run build` | Production client build verification |
