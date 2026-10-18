#!/usr/bin/env bash
set -e

echo "=========================================================="
echo " ShardSphere Phase 1 Health Verification"
echo "=========================================================="

COORDINATOR_PORT="${COORDINATOR_PORT:-8006}"
STORAGE_PORTS=(8001 8002 8003 8004 8005)
ALL_HEALTHY=true

echo ""
echo "[1/2] Verifying 5 Storage Nodes..."
for idx in "${!STORAGE_PORTS[@]}"; do
    node_num=$((idx + 1))
    port="${STORAGE_PORTS[$idx]}"
    node_id="storage-node-${node_num}"
    
    echo -n "  Checking ${node_id} on http://127.0.0.1:${port}/health ... "
    HEALTH_RESP=$(curl -s -f "http://127.0.0.1:${port}/health" 2>/dev/null || echo "FAILED")
    
    if [[ "$HEALTH_RESP" == *"healthy"* ]]; then
        echo -e "\e[32m[PASS: HEALTHY]\e[0m"
        STATS_RESP=$(curl -s "http://127.0.0.1:${port}/storage/stats" 2>/dev/null)
        echo "    ↳ Stats: $(echo "$STATS_RESP" | python3 -c "import sys, json; d=json.load(sys.stdin); print(f'Total chunks: {d.get(\"total_chunks\", 0)}, Used bytes: {d.get(\"total_bytes_used\", 0)}, Free bytes: {d.get(\"free_bytes\", 0)}')")"
    else
        echo -e "\e[31m[FAILED: UNREACHABLE]\e[0m"
        ALL_HEALTHY=false
    fi
done

echo ""
echo "[2/2] Verifying Coordinator on http://127.0.0.1:${COORDINATOR_PORT}..."
COORD_RESP=$(curl -s -f "http://127.0.0.1:${COORDINATOR_PORT}/api/v1/health" 2>/dev/null || echo "FAILED")
if [[ "$COORD_RESP" == *"healthy"* ]]; then
    echo -e "  Coordinator Health: \e[32m[PASS: HEALTHY]\e[0m"
    LEADER_RESP=$(curl -s "http://127.0.0.1:${COORDINATOR_PORT}/api/v1/leader" 2>/dev/null)
    echo "  ↳ Leader Lease: $(echo "$LEADER_RESP" | python3 -c "import sys, json; d=json.load(sys.stdin); print(f'Holder: {d.get(\"current_leader_id\")}, Fencing Token: {d.get(\"fencing_token\")}, Is Leader: {d.get(\"is_leader\")}')")"
    
    CLUSTER_RESP=$(curl -s "http://127.0.0.1:${COORDINATOR_PORT}/api/v1/cluster" 2>/dev/null)
    echo "  ↳ Cluster Overview: $(echo "$CLUSTER_RESP" | python3 -c "import sys, json; d=json.load(sys.stdin); print(f'Healthy nodes: {d.get(\"healthy_nodes\")}/{d.get(\"total_nodes\")}, Stored files: {d.get(\"total_files\")}, Chunks: {d.get(\"total_chunks\")}')")"
else
    echo -e "  Coordinator Health: \e[31m[FAILED: UNREACHABLE]\e[0m"
    ALL_HEALTHY=false
fi

echo ""
echo "=========================================================="
if [ "$ALL_HEALTHY" = true ]; then
    echo -e "\e[32m✔ SUCCESS: All 5 storage nodes and Coordinator verified operational!\e[0m"
    exit 0
else
    echo -e "\e[31m✘ FAILURE: One or more services failed health verification.\e[0m"
    exit 1
fi
