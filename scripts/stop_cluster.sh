#!/usr/bin/env bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$DIR"

echo "Stopping ShardSphere cluster services..."

if [ -d "logs" ]; then
    for pid_file in logs/*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                echo "Terminating PID $pid ($(basename "$pid_file" .pid))..."
                kill -TERM "$pid" 2>/dev/null || true
            fi
            rm -f "$pid_file"
        fi
    done
fi

pkill -f "storage_node.*800" 2>/dev/null || true
pkill -f "backend.app.main" 2>/dev/null || true

echo "All services stopped."
