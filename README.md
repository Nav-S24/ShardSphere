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
| `DELETE` | `/api/v1/files/{file_id}` | Coordinated physical deletion across all nodes |
| `GET` | `/api/v1/cluster` | Real-time cluster overview, active nodes, and stored data stats |
| `GET` | `/api/v1/nodes` | List all 5 storage nodes with health states and disk stats |
| `GET` | `/api/v1/leader` | Current leader election lease and fencing token |
| `GET` | `/api/v1/recovery/jobs` | Self-healing re-replication job status |
| `POST` | `/api/v1/integrity/verify` | Trigger on-demand cryptographic audit of all stored chunks |

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

## Deployment & Verification

### Option A: Docker Compose (Standard Production Deployment)

```bash
# 1. Create environment file
cp .env.example .env

# 2. Build and start all 8 containers
docker compose up --build -d

# 3. Check container status
docker compose ps

# 4. View cluster logs
docker compose logs -f coordinator
```

### Option B: Local Process Execution (Developer Environment)

```bash
# 1. Start all 5 storage nodes + Coordinator
bash scripts/start_cluster.sh

# 2. Verify health of all services
bash scripts/verify_health.sh

# 3. Stop services
bash scripts/stop_cluster.sh
```

---

## Automated Test Execution

Run the complete test suite across the backend coordinator and storage nodes:

```bash
# Run all 20 unit, integration, election, and fault injection tests
pytest backend/tests/ storage_node/tests/ -v
```

### Verified Test Results (20/20 Passed):
```
============================== test session starts ==============================
backend/tests/test_coordinator_models.py::test_database_schema_creation_and_file_model PASSED
backend/tests/test_distributor.py::test_chunk_splitting PASSED
backend/tests/test_distributor.py::test_empty_file_chunk_splitting PASSED
backend/tests/test_leader_election.py::test_leader_lease_acquisition_and_fencing_token PASSED
backend/tests/test_leader_election_two_instances.py::test_two_coordinator_instances_lease_acquisition_and_fencing PASSED
backend/tests/test_leader_election_two_instances.py::test_lease_expiry_takeover_and_token_increment PASSED
backend/tests/test_leader_election_two_instances.py::test_stale_leader_split_brain_write_rejection PASSED
backend/tests/test_real_fault_injection.py::test_real_storage_node_failure_healing_and_recovery PASSED
backend/tests/test_returning_node_reconciliation.py::test_returning_node_reconciliation_active_and_stale_chunks PASSED
backend/tests/test_storage_engine_integration.py::test_end_to_end_storage_engine_lifecycle PASSED
backend/tests/test_storage_engine_integration.py::test_replica_corruption_failover PASSED
backend/tests/test_storage_engine_unit.py::test_1mib_chunk_splitting_and_checksums PASSED
backend/tests/test_storage_engine_unit.py::test_empty_file_handling PASSED
backend/tests/test_storage_engine_unit.py::test_arbitrary_binary_file_integrity PASSED
backend/tests/test_storage_engine_unit.py::test_placement_strategy_distinctness_and_distribution PASSED
backend/tests/test_storage_engine_unit.py::test_placement_fails_when_insufficient_healthy_nodes PASSED
storage_node/tests/test_storage_node.py::test_chunk_write_and_read PASSED
storage_node/tests/test_storage_node.py::test_chunk_checksum_mismatch PASSED
storage_node/tests/test_storage_node.py::test_path_traversal_prevention PASSED
storage_node/tests/test_storage_node.py::test_storage_stats_and_delete PASSED
============================== 20 passed in 5.44s ==============================
```
