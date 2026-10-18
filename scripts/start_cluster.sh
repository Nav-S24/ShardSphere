#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$DIR"

echo "=========================================================="
echo " Starting ShardSphere Services (Phase 1)"
echo "=========================================================="

mkdir -p logs
mkdir -p data/storage_node_1/chunks
mkdir -p data/storage_node_2/chunks
mkdir -p data/storage_node_3/chunks
mkdir -p data/storage_node_4/chunks
mkdir -p data/storage_node_5/chunks

# Kill any existing instances
pkill -f "storage_node.*800" 2>/dev/null || true
pkill -f "backend.app.main" 2>/dev/null || true
sleep 1

# 1. Start 5 Independent Storage Nodes
for i in {1..5}; do
    PORT=$((8000 + i))
    NODE_ID="storage-node-${i}"
    STORAGE_DIR="${DIR}/data/storage_node_${i}/chunks"
    
    echo "Starting ${NODE_ID} on port ${PORT} (dir: ${STORAGE_DIR})..."
    NODE_ID="${NODE_ID}" \
    NODE_NAME="Storage Node ${i}" \
    PORT="${PORT}" \
    HOST="0.0.0.0" \
    ADVERTISED_URL="http://127.0.0.1:${PORT}" \
    STORAGE_DIR="${STORAGE_DIR}" \
    PYTHONPATH="${DIR}/storage_node" \
    python3 -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT}" > "logs/${NODE_ID}.log" 2>&1 &
    
    echo $! > "logs/${NODE_ID}.pid"
done

# Wait for storage nodes to be responsive
sleep 2

# 2. Start Coordinator Service on port 8006 (or custom COORDINATOR_PORT)
COORD_PORT="${COORDINATOR_PORT:-8006}"
echo "Starting ShardSphere Coordinator on port ${COORD_PORT}..."
COORDINATOR_ID="coordinator-primary-1" \
PORT="${COORD_PORT}" \
HOST="0.0.0.0" \
DATABASE_URL="sqlite+aiosqlite:///${DIR}/data/shardsphere_metadata.db" \
STORAGE_NODES="storage-node-1:http://127.0.0.1:8001,storage-node-2:http://127.0.0.1:8002,storage-node-3:http://127.0.0.1:8003,storage-node-4:http://127.0.0.1:8004,storage-node-5:http://127.0.0.1:8005" \
REPLICATION_FACTOR="3" \
DEFAULT_CHUNK_SIZE_BYTES="1048576" \
HEARTBEAT_INTERVAL_SECONDS="2" \
LEASE_DURATION_SECONDS="10" \
PYTHONPATH="${DIR}/backend" \
python3 -m uvicorn app.main:app --host 0.0.0.0 --port "${COORD_PORT}" > "logs/coordinator.log" 2>&1 &

echo $! > "logs/coordinator.pid"

sleep 2
echo "Cluster startup completed. Running health verification..."
bash scripts/verify_health.sh
