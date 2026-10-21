import http from 'http';
import express from 'express';
import type { Request, Response } from 'express';
import multer from 'multer';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createServer as createViteServer } from 'vite';
import { DatabaseSync } from 'node:sqlite';
import { LamportClockEngine, COORDINATOR_PARTICIPANT_ID, type CausalEventEnvelope } from './src/clock/index.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = 3000;
const HOST = '0.0.0.0';
const DEFAULT_CHUNK_SIZE = 1048576; // 1 MiB
const REPLICATION_FACTOR = 3;
const NODE_STORAGE_LIMIT = 10737418240; // 10 GiB (matching original schema)
const LEASE_DURATION = 10.0; // 10 seconds
const LEASE_RENEW_INTERVAL = 3.0; // Renew lease every 3 seconds to permit natural countdown

// ==========================================
// SQLite Database Initialization
// ==========================================
const dataDir = path.resolve(__dirname, 'data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const dbPath = path.join(dataDir, 'shardsphere_metadata.db');
const db = new DatabaseSync(dbPath);

// Ensure tables exist with matching schema
db.exec(`
  CREATE TABLE IF NOT EXISTS files (
    file_id VARCHAR(64) NOT NULL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    content_type VARCHAR(128),
    size_bytes BIGINT NOT NULL,
    checksum_sha256 VARCHAR(64) NOT NULL,
    chunk_count INTEGER NOT NULL,
    chunk_size_bytes INTEGER NOT NULL,
    replication_factor INTEGER NOT NULL,
    metadata_version INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at FLOAT NOT NULL,
    updated_at FLOAT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS chunks (
    chunk_id VARCHAR(64) NOT NULL PRIMARY KEY,
    file_id VARCHAR(64) NOT NULL,
    sequence_index INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    checksum_sha256 VARCHAR(64) NOT NULL,
    generation INTEGER NOT NULL,
    created_at FLOAT NOT NULL,
    CONSTRAINT uq_file_chunk_sequence UNIQUE (file_id, sequence_index),
    FOREIGN KEY(file_id) REFERENCES files (file_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS replicas (
    replica_id VARCHAR(64) NOT NULL PRIMARY KEY,
    chunk_id VARCHAR(64) NOT NULL,
    node_id VARCHAR(64) NOT NULL,
    node_url VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    generation INTEGER NOT NULL,
    last_verified_at FLOAT,
    created_at FLOAT NOT NULL,
    CONSTRAINT uq_chunk_node_replica UNIQUE (chunk_id, node_id),
    FOREIGN KEY(chunk_id) REFERENCES chunks (chunk_id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS storage_nodes (
    node_id VARCHAR(64) NOT NULL PRIMARY KEY,
    node_name VARCHAR(128) NOT NULL,
    advertised_url VARCHAR(255) NOT NULL,
    health_state VARCHAR(32) NOT NULL,
    consecutive_failures INTEGER NOT NULL,
    last_heartbeat_at FLOAT,
    last_failure_at FLOAT,
    total_chunks INTEGER NOT NULL,
    total_bytes_used BIGINT NOT NULL,
    storage_limit_bytes BIGINT NOT NULL,
    updated_at FLOAT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS leader_leases (
    lease_id VARCHAR(64) NOT NULL PRIMARY KEY,
    holder_id VARCHAR(64) NOT NULL,
    fencing_token BIGINT NOT NULL,
    acquired_at FLOAT NOT NULL,
    expires_at FLOAT NOT NULL,
    last_renewed_at FLOAT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS recovery_jobs (
    job_id VARCHAR(64) NOT NULL PRIMARY KEY,
    chunk_id VARCHAR(64) NOT NULL,
    file_id VARCHAR(64) NOT NULL,
    source_node_id VARCHAR(64) NOT NULL,
    target_node_id VARCHAR(64) NOT NULL,
    generation INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempts INTEGER NOT NULL,
    error_message TEXT,
    created_at FLOAT NOT NULL,
    completed_at FLOAT
  );

  CREATE TABLE IF NOT EXISTS cluster_events (
    event_id VARCHAR(64) NOT NULL PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    node_id VARCHAR(64),
    file_id VARCHAR(64),
    message TEXT NOT NULL,
    timestamp FLOAT NOT NULL
  );
`);

// ==========================================
// Lamport Logical Clock Engine Initialization
// ==========================================
export const clockEngine = new LamportClockEngine(db);

// Seed storage_nodes if table is empty
const existingNodesCount = (db.prepare('SELECT count(*) as count FROM storage_nodes').get() as any).count;
if (existingNodesCount === 0) {
  const now = Date.now() / 1000;
  for (let i = 1; i <= 5; i++) {
    const nodeId = `storage-node-${i}`;
    db.prepare(`
      INSERT INTO storage_nodes (node_id, node_name, advertised_url, health_state, consecutive_failures, last_heartbeat_at, last_failure_at, total_chunks, total_bytes_used, storage_limit_bytes, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      nodeId,
      `Storage Node ${i}`,
      `http://127.0.0.1:${8000 + i}`,
      'HEALTHY',
      0,
      now,
      null,
      0,
      0,
      NODE_STORAGE_LIMIT,
      now
    );
  }
}

// Seed leader lease if missing
const leaseCheck = db.prepare("SELECT * FROM leader_leases WHERE lease_id = 'coordinator_primary_lease'").get() as any;
const now = Date.now() / 1000;
if (!leaseCheck) {
  db.prepare(`
    INSERT INTO leader_leases (lease_id, holder_id, fencing_token, acquired_at, expires_at, last_renewed_at)
    VALUES ('coordinator_primary_lease', 'coordinator-primary-1', 1, ?, ?, ?)
  `).run(now, now + LEASE_DURATION, now);
}

// Ensure directory for each node exists
for (let i = 1; i <= 5; i++) {
  const dir = path.join(dataDir, `storage_node_${i}`, 'chunks');
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function getNodeChunksDir(nodeId: string): string {
  const num = nodeId.replace('storage-node-', '');
  return path.join(dataDir, `storage_node_${num}`, 'chunks');
}

function computeSha256(data: Buffer | Uint8Array): string {
  return crypto.createHash('sha256').update(data).digest('hex');
}

function addEvent(
  eventType: string,
  severity: 'INFO' | 'WARNING' | 'ERROR',
  message: string,
  nodeId: string | null = null,
  fileId: string | null = null,
  options?: {
    origin_node_id?: string;
    causal_parent_id?: string | null;
    causal_parent_ids?: string[] | null;
    correlation_id?: string | null;
    received_clock?: number;
  }
): CausalEventEnvelope {
  try {
    return clockEngine.recordCausalEvent({
      origin_node_id: options?.origin_node_id || COORDINATOR_PARTICIPANT_ID,
      event_type: eventType,
      severity,
      message,
      node_id: nodeId,
      file_id: fileId,
      causal_parent_id: options?.causal_parent_id,
      causal_parent_ids: options?.causal_parent_ids,
      correlation_id: options?.correlation_id || fileId,
      received_clock: options?.received_clock,
    });
  } catch (err) {
    console.error('Failed to record causal cluster event:', err);
    // Fallback envelope if recordCausalEvent encountered persistence issue
    const fallbackId = `evt_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    const now = Date.now() / 1000;
    return {
      event_id: fallbackId,
      event_type: eventType,
      origin_node_id: options?.origin_node_id || COORDINATOR_PARTICIPANT_ID,
      lamport_clock: clockEngine.getClock(COORDINATOR_PARTICIPANT_ID),
      causal_parent_id: options?.causal_parent_id,
      causal_parent_ids: options?.causal_parent_ids,
      correlation_id: options?.correlation_id || fileId,
      severity,
      node_id: nodeId,
      file_id: fileId,
      message,
      timestamp: now,
    };
  }
}

// Record Coordinator Start Event
addEvent(
  'COORDINATOR_STARTED',
  'INFO',
  `ShardSphere Coordinator operational on ${HOST}:${PORT}. Fencing lease and 5 independent storage node drivers initialized.`
);

// ==========================================
// Precise Physical vs Committed Inventory
// ==========================================
export type ChunkClassification = 'COMMITTED_REPLICA' | 'STALE_REPLICA' | 'ORPHANED_CHUNK' | 'UNTRACKED_DATA';

export interface ChunkInventoryItem {
  node_id: string;
  chunk_id: string;
  filename: string;
  size_bytes: number;
  sha256: string;
  sha_match: boolean;
  classification: ChunkClassification;
  file_id: string | null;
  file_name: string | null;
  status: string;
  generation: number;
}

export interface NodeInventory {
  node_id: string;
  // Authoritative committed replicas for files in metadata
  committed_chunks: number;
  committed_bytes_used: number;
  // Corrupted or older generation replicas
  stale_chunks: number;
  stale_bytes_used: number;
  // Physical chunk files with no file reference in SQLite
  orphaned_chunks: number;
  orphaned_bytes_used: number;
  // Valid chunks present on node but not registered in replicas table (e.g. excess replicas)
  untracked_chunks: number;
  untracked_bytes_used: number;
  // Physical totals on disk
  physical_chunks: number;
  physical_bytes_used: number;
  // Backwards compatibility aliases
  active_chunks: number;
  active_bytes_used: number;
  total_chunks: number; // Represents authoritative committed chunks for consistent accounting
  total_bytes_used: number; // Represents committed bytes
  items: ChunkInventoryItem[];
}

function getNodeInventory(nodeId: string): NodeInventory {
  const dir = getNodeChunksDir(nodeId);
  let committedChunks = 0;
  let committedBytes = 0;
  let staleChunks = 0;
  let staleBytes = 0;
  let orphanChunks = 0;
  let orphanBytes = 0;
  let untrackedChunks = 0;
  let untrackedBytes = 0;
  const items: ChunkInventoryItem[] = [];

  // Query metadata for all committed or degraded files
  const committedFiles = db.prepare("SELECT * FROM files WHERE status IN ('committed', 'degraded')").all() as any[];
  const fileMap = new Map<string, any>(committedFiles.map(f => [f.file_id, f]));

  // Query all chunks
  const allChunks = db.prepare('SELECT * FROM chunks').all() as any[];
  const chunkMap = new Map<string, any>(allChunks.map(c => [c.chunk_id, c]));

  // Query replicas specifically assigned to this node
  const nodeReplicas = db.prepare('SELECT * FROM replicas WHERE node_id = ?').all(nodeId) as any[];
  const replicaMap = new Map<string, any>(nodeReplicas.map(r => [r.chunk_id, r]));

  if (fs.existsSync(dir)) {
    const filesInDir = fs.readdirSync(dir);
    for (const f of filesInDir) {
      if (f.endsWith('.bin')) {
        const chunkId = f.replace(/^chunk_/, '').replace(/\.bin$/, '');
        const fpath = path.join(dir, f);
        let sz = 0;
        let actualSha = '';
        try {
          sz = fs.statSync(fpath).size;
          const data = fs.readFileSync(fpath);
          actualSha = computeSha256(data);
        } catch {
          continue;
        }

        const chunkRec = chunkMap.get(chunkId);
        const repRec = replicaMap.get(chunkId);

        let classification: ChunkClassification = 'ORPHANED_CHUNK';
        let shaMatch = false;
        let fileId: string | null = null;
        let fileName: string | null = null;
        let status = 'orphaned';
        let gen = 1;

        if (chunkRec) {
          fileId = chunkRec.file_id;
          const fRec = fileId ? fileMap.get(fileId) : null;
          fileName = fRec ? fRec.filename : null;
          shaMatch = (actualSha === chunkRec.checksum_sha256);
          gen = chunkRec.generation;

          if (repRec) {
            // Node is registered as replica holder in metadata
            if (repRec.status === 'active' && shaMatch && repRec.generation === chunkRec.generation) {
              classification = 'COMMITTED_REPLICA';
              status = 'active';
              committedChunks++;
              committedBytes += sz;
            } else {
              classification = 'STALE_REPLICA';
              status = repRec.status || 'stale';
              staleChunks++;
              staleBytes += sz;
            }
          } else {
            // Chunk belongs to an authoritative file, but node is not assigned as replica holder
            classification = 'UNTRACKED_DATA';
            status = 'untracked_replica';
            untrackedChunks++;
            untrackedBytes += sz;
          }
        } else {
          // No record of this chunk in chunks table
          classification = 'ORPHANED_CHUNK';
          status = 'orphaned';
          orphanChunks++;
          orphanBytes += sz;
        }

        items.push({
          node_id: nodeId,
          chunk_id: chunkId,
          filename: f,
          size_bytes: sz,
          sha256: actualSha,
          sha_match: shaMatch,
          classification,
          file_id: fileId,
          file_name: fileName,
          status,
          generation: gen,
        });
      }
    }
  }

  const physicalChunks = committedChunks + staleChunks + orphanChunks + untrackedChunks;
  const physicalBytes = committedBytes + staleBytes + orphanBytes + untrackedBytes;

  return {
    node_id: nodeId,
    committed_chunks: committedChunks,
    committed_bytes_used: committedBytes,
    stale_chunks: staleChunks,
    stale_bytes_used: staleBytes,
    orphaned_chunks: orphanChunks,
    orphaned_bytes_used: orphanBytes,
    untracked_chunks: untrackedChunks,
    untracked_bytes_used: untrackedBytes,
    physical_chunks: physicalChunks,
    physical_bytes_used: physicalBytes,
    active_chunks: committedChunks,
    active_bytes_used: committedBytes,
    total_chunks: committedChunks, // Consistent: committed chunks reflect logical 3x replication
    total_bytes_used: committedBytes,
    items,
  };
}

// Synchronize node table with authoritative metadata & storage
function refreshNodeMetrics() {
  const nodes = db.prepare('SELECT * FROM storage_nodes').all() as any[];
  const now = Date.now() / 1000;

  for (const n of nodes) {
    const inv = getNodeInventory(n.node_id);
    const lastHeartbeat = n.health_state === 'HEALTHY' ? now : n.last_heartbeat_at;

    db.prepare(`
      UPDATE storage_nodes
      SET total_chunks = ?, total_bytes_used = ?, last_heartbeat_at = ?, updated_at = ?
      WHERE node_id = ?
    `).run(inv.committed_chunks, inv.committed_bytes_used, lastHeartbeat, now, n.node_id);
  }
}

// Periodic Lease Renewal and Heartbeat
let isLeaderActive = true;
let lastLeaderRenewAt = Date.now() / 1000;

setInterval(() => {
  const now = Date.now() / 1000;
  if (isLeaderActive) {
    // Only renew every LEASE_RENEW_INTERVAL seconds to produce a realistic countdown
    if (now - lastLeaderRenewAt >= LEASE_RENEW_INTERVAL) {
      lastLeaderRenewAt = now;
      db.prepare(`
        UPDATE leader_leases
        SET expires_at = ?, last_renewed_at = ?
        WHERE lease_id = 'coordinator_primary_lease'
      `).run(now + LEASE_DURATION, now);
    }
  }
  db.prepare(`
    UPDATE storage_nodes
    SET last_heartbeat_at = ?
    WHERE health_state = 'HEALTHY'
  `).run(now);
}, 1000);

// Modular Orphan Reclamation function (reusable for isolated testing and cluster route)
export function reclaimOrphanChunks(
  database: any = db,
  getChunksDir: (nodeId: string) => string = getNodeChunksDir
): { total_purged_orphans: number; total_reclaimed_bytes: number } {
  let totalPurged = 0;
  let totalReclaimed = 0;
  const nodes = database.prepare('SELECT * FROM storage_nodes').all() as any[];

  for (const node of nodes) {
    const dir = getChunksDir(node.node_id);
    if (!fs.existsSync(dir)) continue;

    const diskFiles = fs.readdirSync(dir);
    for (const fname of diskFiles) {
      if (fname.endsWith('.bin')) {
        const chunkId = fname.replace(/^chunk_/, '').replace(/\.bin$/, '');
        const binPath = path.join(dir, fname);
        const metaPath = path.join(dir, `chunk_${chunkId}.meta.json`);

        const chunkRec = database.prepare(`
          SELECT c.*, f.status as file_status
          FROM chunks c
          JOIN files f ON c.file_id = f.file_id
          WHERE c.chunk_id = ?
        `).get(chunkId) as any;

        if (!chunkRec || (chunkRec.file_status !== 'committed' && chunkRec.file_status !== 'degraded')) {
          try {
            const sz = fs.statSync(binPath).size;
            fs.unlinkSync(binPath);
            if (fs.existsSync(metaPath)) fs.unlinkSync(metaPath);
            totalPurged++;
            totalReclaimed += sz;
          } catch {
            // ignore
          }
        }
      }
    }
  }

  return { total_purged_orphans: totalPurged, total_reclaimed_bytes: totalReclaimed };
}

// ==========================================
// Express Application & API Setup
// ==========================================
async function startServer() {
  const app = express();
  app.use(express.json());

  const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: 50 * 1024 * 1024 }, // 50MB
  });

  // Lamport Logical Clock Middleware:
  // Ingests untrusted external client clocks safely and ensures outgoing responses have X-Lamport-Clock header
  app.use(['/api', '/api/v1'], (req, res, next) => {
    const clientHeader = req.headers['x-lamport-clock'];
    if (clientHeader !== undefined) {
      clockEngine.processUntrustedClientClock(clientHeader);
    }
    const origJson = res.json;
    res.json = function (body: any) {
      res.setHeader('X-Lamport-Clock', clockEngine.getClock(COORDINATOR_PARTICIPANT_ID));
      return origJson.call(this, body);
    };
    const origSend = res.send;
    res.send = function (body: any) {
      res.setHeader('X-Lamport-Clock', clockEngine.getClock(COORDINATOR_PARTICIPANT_ID));
      return origSend.call(this, body);
    };
    next();
  });

  const apiRouter = express.Router();

  // Lamport Logical Clocks Vector Snapshot Endpoint (aliases /clocks and /clock)
  apiRouter.get(['/clocks', '/clock'], (_req, res) => {
    const participantClocks = clockEngine.getParticipantClocks();
    const highestObserved = Math.max(...Object.values(participantClocks));
    res.json({
      coordinator: {
        id: COORDINATOR_PARTICIPANT_ID,
        clock: participantClocks[COORDINATOR_PARTICIPANT_ID] ?? 0,
      },
      nodes: Object.entries(participantClocks)
        .filter(([id]) => id.startsWith('storage-node-'))
        .map(([id, clock]) => ({ id, clock })),
      highest_observed: highestObserved,
      timestamp: Date.now() / 1000,
    });
  });

  // Health & Ready
  apiRouter.get('/health', (_req, res) => {
    res.json({ status: 'healthy', service: 'coordinator', timestamp: Date.now() / 1000, lamport_clock: clockEngine.getClock(COORDINATOR_PARTICIPANT_ID) });
  });

  apiRouter.get('/ready', (_req, res) => {
    try {
      db.prepare('SELECT 1').get();
      res.json({ status: 'ready', database: 'connected', timestamp: Date.now() / 1000, lamport_clock: clockEngine.getClock(COORDINATOR_PARTICIPANT_ID) });
    } catch (err: any) {
      res.status(503).json({ status: 'unready', error: err.message });
    }
  });

  // Leader Status
  apiRouter.get('/leader', (_req, res) => {
    const lease = db.prepare("SELECT * FROM leader_leases WHERE lease_id = 'coordinator_primary_lease'").get() as any;
    const now = Date.now() / 1000;
    const remaining = lease ? Math.max(0, lease.expires_at - now) : 0;
    const isLead = isLeaderActive && remaining > 0;

    res.json({
      is_leader: isLead,
      current_leader_id: isLead && lease ? lease.holder_id : 'none',
      fencing_token: lease ? Number(lease.fencing_token) : 1,
      lease_expires_at: lease ? lease.expires_at : now,
      time_remaining_seconds: remaining,
      coordinator_instance_id: lease ? lease.holder_id : 'coordinator-primary-1',
      lamport_clock: clockEngine.getClock(COORDINATOR_PARTICIPANT_ID),
    });
  });

  apiRouter.post('/leader/step-down', (_req, res) => {
    const lease = db.prepare("SELECT * FROM leader_leases WHERE lease_id = 'coordinator_primary_lease'").get() as any;
    const prevHolder = lease ? lease.holder_id : 'coordinator-primary-1';
    const nextToken = (lease ? Number(lease.fencing_token) : 1) + 1;
    const now = Date.now() / 1000;

    isLeaderActive = false;
    db.prepare(`
      UPDATE leader_leases
      SET fencing_token = ?, expires_at = ?, last_renewed_at = ?
      WHERE lease_id = 'coordinator_primary_lease'
    `).run(nextToken, now - 1, now);

    addEvent(
      'LEADER_STEP_DOWN',
      'WARNING',
      `Leader '${prevHolder}' voluntarily stepped down (lease expired). Fencing token incremented to #${nextToken}.`
    );

    // Automatically re-elect after brief grace period (1.2s)
    setTimeout(() => {
      isLeaderActive = true;
      const reNow = Date.now() / 1000;
      lastLeaderRenewAt = reNow;
      db.prepare(`
        UPDATE leader_leases
        SET holder_id = 'coordinator-primary-1', expires_at = ?, last_renewed_at = ?
        WHERE lease_id = 'coordinator_primary_lease'
      `).run(reNow + LEASE_DURATION, reNow);

      addEvent('LEADER_ELECTED', 'INFO', `Coordinator 'coordinator-primary-1' acquired leader lease with fencing token #${nextToken}.`);
    }, 1200);

    res.json({
      status: 'stepped_down',
      previous_holder: prevHolder,
      fencing_token: nextToken,
    });
  });

  // Comprehensive Chunk Inventory & Classification Audit
  apiRouter.get('/inventory/audit', (_req, res) => {
    refreshNodeMetrics();
    const rawNodes = db.prepare('SELECT * FROM storage_nodes').all() as any[];
    const allItems: ChunkInventoryItem[] = [];
    let totalCommitted = 0;
    let totalCommittedBytes = 0;
    let totalStale = 0;
    let totalStaleBytes = 0;
    let totalOrphans = 0;
    let totalOrphanBytes = 0;
    let totalUntracked = 0;
    let totalUntrackedBytes = 0;

    const perNode: Record<string, any> = {};

    for (const n of rawNodes) {
      const inv = getNodeInventory(n.node_id);
      totalCommitted += inv.committed_chunks;
      totalCommittedBytes += inv.committed_bytes_used;
      totalStale += inv.stale_chunks;
      totalStaleBytes += inv.stale_bytes_used;
      totalOrphans += inv.orphaned_chunks;
      totalOrphanBytes += inv.orphaned_bytes_used;
      totalUntracked += inv.untracked_chunks;
      totalUntrackedBytes += inv.untracked_bytes_used;

      perNode[n.node_id] = {
        committed_chunks: inv.committed_chunks,
        committed_bytes_used: inv.committed_bytes_used,
        stale_chunks: inv.stale_chunks,
        stale_bytes_used: inv.stale_bytes_used,
        orphaned_chunks: inv.orphaned_chunks,
        orphaned_bytes_used: inv.orphaned_bytes_used,
        untracked_chunks: inv.untracked_chunks,
        untracked_bytes_used: inv.untracked_bytes_used,
        physical_chunks: inv.physical_chunks,
        physical_bytes_used: inv.physical_bytes_used,
      };

      allItems.push(...inv.items);
    }

    const fileAgg = db.prepare(`
      SELECT count(*) as total_files, coalesce(sum(size_bytes), 0) as total_bytes
      FROM files
      WHERE status IN ('committed', 'degraded')
    `).get() as any;

    const chunkAgg = db.prepare(`
      SELECT count(c.chunk_id) as total_chunks
      FROM chunks c
      JOIN files f ON c.file_id = f.file_id
      WHERE f.status IN ('committed', 'degraded')
    `).get() as any;

    res.json({
      summary: {
        total_files_committed: fileAgg.total_files,
        total_logical_chunks: chunkAgg.total_chunks,
        total_committed_bytes: fileAgg.total_bytes,
        configured_replication_factor: REPLICATION_FACTOR,
        expected_replicas: chunkAgg.total_chunks * REPLICATION_FACTOR,
        actual_committed_replicas: totalCommitted,
        actual_stale_replicas: totalStale,
        actual_orphaned_chunks: totalOrphans,
        actual_untracked_chunks: totalUntracked,
        total_physical_chunks: totalCommitted + totalStale + totalOrphans + totalUntracked,
        total_physical_bytes: totalCommittedBytes + totalStaleBytes + totalOrphanBytes + totalUntrackedBytes,
      },
      per_node: perNode,
      items: allItems,
    });
  });

  // Cluster Overview & Node Summaries
  apiRouter.get('/cluster', (_req, res) => {
    refreshNodeMetrics();
    const rawNodes = db.prepare('SELECT * FROM storage_nodes').all() as any[];

    let totalCommittedChunks = 0;
    let totalCommittedBytes = 0;
    let totalStaleChunks = 0;
    let totalStaleBytes = 0;
    let totalOrphanChunks = 0;
    let totalOrphanBytes = 0;
    let totalUntrackedChunks = 0;
    let totalUntrackedBytes = 0;

    const nodeList = rawNodes.map(n => {
      const inv = getNodeInventory(n.node_id);
      totalCommittedChunks += inv.committed_chunks;
      totalCommittedBytes += inv.committed_bytes_used;
      totalStaleChunks += inv.stale_chunks;
      totalStaleBytes += inv.stale_bytes_used;
      totalOrphanChunks += inv.orphaned_chunks;
      totalOrphanBytes += inv.orphaned_bytes_used;
      totalUntrackedChunks += inv.untracked_chunks;
      totalUntrackedBytes += inv.untracked_bytes_used;

      const diskPct = Number(((inv.physical_bytes_used / (n.storage_limit_bytes || 1)) * 100).toFixed(2));
      return {
        node_id: n.node_id,
        node_name: n.node_name,
        advertised_url: n.advertised_url,
        health_state: n.health_state,
        consecutive_failures: n.consecutive_failures,
        last_heartbeat_at: n.last_heartbeat_at,
        last_failure_at: n.last_failure_at,
        // Authoritative committed replicas (consistent with logical file replicas!)
        total_chunks: inv.committed_chunks,
        total_bytes_used: inv.committed_bytes_used,
        committed_chunks: inv.committed_chunks,
        committed_bytes_used: inv.committed_bytes_used,
        active_chunks: inv.committed_chunks,
        active_bytes_used: inv.committed_bytes_used,
        // Detailed classification metrics
        stale_chunks: inv.stale_chunks,
        stale_bytes_used: inv.stale_bytes_used,
        orphaned_chunks: inv.orphaned_chunks,
        orphaned_bytes_used: inv.orphaned_bytes_used,
        untracked_chunks: inv.untracked_chunks,
        untracked_bytes_used: inv.untracked_bytes_used,
        physical_chunks: inv.physical_chunks,
        physical_bytes_used: inv.physical_bytes_used,
        storage_limit_bytes: n.storage_limit_bytes,
        disk_usage_percent: diskPct,
        lamport_clock: clockEngine.getClock(n.node_id),
      };
    });

    const healthyCount = nodeList.filter(n => n.health_state === 'HEALTHY').length;
    const suspectCount = nodeList.filter(n => n.health_state === 'SUSPECT').length;
    const unavailableCount = nodeList.filter(n => n.health_state === 'UNAVAILABLE').length;

    const fileAgg = db.prepare(`
      SELECT count(*) as total_files, coalesce(sum(size_bytes), 0) as total_bytes
      FROM files
      WHERE status IN ('committed', 'degraded')
    `).get() as any;

    const chunkAgg = db.prepare(`
      SELECT count(c.chunk_id) as total_chunks
      FROM chunks c
      JOIN files f ON c.file_id = f.file_id
      WHERE f.status IN ('committed', 'degraded')
    `).get() as any;

    const activeJobs = (db.prepare("SELECT count(*) as count FROM recovery_jobs WHERE status IN ('pending', 'in_progress')").get() as any).count;
    const lease = db.prepare("SELECT * FROM leader_leases WHERE lease_id = 'coordinator_primary_lease'").get() as any;
    const now = Date.now() / 1000;
    const remaining = lease ? Math.max(0, lease.expires_at - now) : 0;

    res.json({
      cluster_name: 'ShardSphere Production Cluster',
      coordinator_id: lease ? lease.holder_id : 'coordinator-primary-1',
      coordinator_lamport_clock: clockEngine.getClock(COORDINATOR_PARTICIPANT_ID),
      lamport_clock: clockEngine.getClock(COORDINATOR_PARTICIPANT_ID),
      is_leader: isLeaderActive && remaining > 0,
      fencing_token: lease ? Number(lease.fencing_token) : 1,
      total_nodes: nodeList.length,
      healthy_nodes: healthyCount,
      suspect_nodes: suspectCount,
      unavailable_nodes: unavailableCount,
      // Authoritative logical numbers
      total_files: fileAgg.total_files,
      total_chunks: chunkAgg.total_chunks,
      total_bytes_stored: fileAgg.total_bytes,
      // Physical replica counts
      total_committed_replicas: totalCommittedChunks,
      total_committed_bytes: totalCommittedBytes,
      total_active_replicas: totalCommittedChunks,
      total_active_bytes: totalCommittedBytes,
      total_stale_replicas: totalStaleChunks,
      total_stale_bytes: totalStaleBytes,
      total_orphaned_chunks: totalOrphanChunks,
      total_orphaned_bytes: totalOrphanBytes,
      total_untracked_chunks: totalUntrackedChunks,
      total_untracked_bytes: totalUntrackedBytes,
      total_physical_chunks: totalCommittedChunks + totalStaleChunks + totalOrphanChunks + totalUntrackedChunks,
      total_physical_bytes: totalCommittedBytes + totalStaleBytes + totalOrphanBytes + totalUntrackedBytes,
      configured_replication_factor: REPLICATION_FACTOR,
      active_recovery_jobs: activeJobs,
      nodes: nodeList,
    });
  });

  apiRouter.get('/nodes', (_req, res) => {
    refreshNodeMetrics();
    const rawNodes = db.prepare('SELECT * FROM storage_nodes').all() as any[];
    const summaries = rawNodes.map(n => {
      const inv = getNodeInventory(n.node_id);
      return {
        node_id: n.node_id,
        node_name: n.node_name,
        advertised_url: n.advertised_url,
        health_state: n.health_state,
        consecutive_failures: n.consecutive_failures,
        last_heartbeat_at: n.last_heartbeat_at,
        last_failure_at: n.last_failure_at,
        // Committed replicas
        total_chunks: inv.committed_chunks,
        total_bytes_used: inv.committed_bytes_used,
        committed_chunks: inv.committed_chunks,
        committed_bytes_used: inv.committed_bytes_used,
        active_chunks: inv.committed_chunks,
        active_bytes_used: inv.committed_bytes_used,
        // Classification breakdown
        stale_chunks: inv.stale_chunks,
        stale_bytes_used: inv.stale_bytes_used,
        orphaned_chunks: inv.orphaned_chunks,
        orphaned_bytes_used: inv.orphaned_bytes_used,
        untracked_chunks: inv.untracked_chunks,
        untracked_bytes_used: inv.untracked_bytes_used,
        physical_chunks: inv.physical_chunks,
        physical_bytes_used: inv.physical_bytes_used,
        storage_limit_bytes: n.storage_limit_bytes,
        disk_usage_percent: Number(((inv.physical_bytes_used / (n.storage_limit_bytes || 1)) * 100).toFixed(2)),
        lamport_clock: clockEngine.getClock(n.node_id),
      };
    });
    res.json(summaries);
  });

  // Toggle Node State
  apiRouter.post('/nodes/:node_id/toggle-state', (req, res) => {
    const { node_id } = req.params;
    const node = db.prepare('SELECT * FROM storage_nodes WHERE node_id = ?').get(node_id) as any;
    if (!node) {
      return res.status(404).json({ detail: `Node ${node_id} not found` });
    }

    const oldState = node.health_state;
    const newState = oldState === 'HEALTHY' || oldState === 'RECOVERING' ? 'UNAVAILABLE' : 'HEALTHY';
    const now = Date.now() / 1000;
    const failures = newState === 'UNAVAILABLE' ? 5 : 0;
    const failAt = newState === 'UNAVAILABLE' ? now : null;

    const sendTs = clockEngine.tickSend(COORDINATOR_PARTICIPANT_ID);
    const nodeTs = clockEngine.tickReceive(node_id, sendTs);
    const ackTs = clockEngine.tickSend(node_id);
    clockEngine.tickReceive(COORDINATOR_PARTICIPANT_ID, ackTs);

    db.prepare(`
      UPDATE storage_nodes
      SET health_state = ?, consecutive_failures = ?, last_failure_at = ?, last_heartbeat_at = ?, updated_at = ?, lamport_clock = ?
      WHERE node_id = ?
    `).run(newState, failures, failAt, now, now, nodeTs, node_id);

    addEvent(
      'NODE_HEALTH_CHANGED',
      newState === 'UNAVAILABLE' ? 'WARNING' : 'INFO',
      `Operator manually toggled node '${node_id}' from ${oldState} to ${newState}.`,
      node_id,
      null,
      { origin_node_id: node_id, received_clock: sendTs }
    );

    res.json({
      node_id,
      previous_state: oldState,
      new_state: newState,
      lamport_clock: nodeTs,
    });
  });

  // Set Node Storage Limit (Quota configuration)
  apiRouter.post('/nodes/:node_id/limit', (req, res) => {
    const { node_id } = req.params;
    const { storage_limit_bytes } = req.body || {};
    if (typeof storage_limit_bytes !== 'number' || storage_limit_bytes < 0) {
      return res.status(400).json({ detail: 'storage_limit_bytes must be a non-negative number' });
    }
    const node = db.prepare('SELECT * FROM storage_nodes WHERE node_id = ?').get(node_id) as any;
    if (!node) {
      return res.status(404).json({ detail: `Node ${node_id} not found` });
    }

    db.prepare('UPDATE storage_nodes SET storage_limit_bytes = ?, updated_at = ? WHERE node_id = ?').run(
      storage_limit_bytes,
      Date.now() / 1000,
      node_id
    );
    refreshNodeMetrics();

    addEvent(
      'NODE_LIMIT_CHANGED',
      'INFO',
      `Operator updated storage limit on '${node_id}' to ${storage_limit_bytes} bytes.`,
      node_id
    );

    res.json({
      node_id,
      storage_limit_bytes,
      message: `Storage limit updated to ${storage_limit_bytes} bytes`,
    });
  });

  // Returning Node Reconciliation & Orphan Chunk Purge
  apiRouter.post('/nodes/:node_id/reconcile', (req, res) => {
    const { node_id } = req.params;
    const node = db.prepare('SELECT * FROM storage_nodes WHERE node_id = ?').get(node_id) as any;
    if (!node) {
      return res.status(404).json({ detail: `Node ${node_id} not found` });
    }

    const dir = getNodeChunksDir(node_id);
    let verifiedActive = 0;
    let purgedStale = 0;
    let reclaimedBytes = 0;
    const now = Date.now() / 1000;

    if (fs.existsSync(dir)) {
      const diskFiles = fs.readdirSync(dir);
      for (const fname of diskFiles) {
        if (fname.endsWith('.bin')) {
          const chunkId = fname.replace(/^chunk_/, '').replace(/\.bin$/, '');
          const binPath = path.join(dir, fname);
          const metaPath = path.join(dir, `chunk_${chunkId}.meta.json`);

          const chunkRec = db.prepare(`
            SELECT c.*, f.status as file_status
            FROM chunks c
            JOIN files f ON c.file_id = f.file_id
            WHERE c.chunk_id = ?
          `).get(chunkId) as any;

          if (!chunkRec || (chunkRec.file_status !== 'committed' && chunkRec.file_status !== 'degraded')) {
            // Orphaned / Stale chunk! Delete it from physical disk
            try {
              const sz = fs.statSync(binPath).size;
              fs.unlinkSync(binPath);
              if (fs.existsSync(metaPath)) fs.unlinkSync(metaPath);
              purgedStale++;
              reclaimedBytes += sz;
            } catch {
              // ignore
            }
          } else {
            // Valid chunk: verify SHA-256
            try {
              const data = fs.readFileSync(binPath);
              const hash = computeSha256(data);
              if (hash === chunkRec.checksum_sha256) {
                verifiedActive++;
                // Ensure replica is recorded in SQLite
                const rep = db.prepare('SELECT * FROM replicas WHERE chunk_id = ? AND node_id = ?').get(chunkId, node_id) as any;
                if (rep) {
                  db.prepare(`
                    UPDATE replicas SET status = 'active', last_verified_at = ? WHERE replica_id = ?
                  `).run(now, rep.replica_id);
                } else {
                  const repId = `rep_${crypto.randomBytes(6).toString('hex')}`;
                  db.prepare(`
                    INSERT INTO replicas (replica_id, chunk_id, node_id, node_url, status, generation, last_verified_at, created_at)
                    VALUES (?, ?, ?, ?, 'active', ?, ?, ?)
                  `).run(repId, chunkId, node_id, node.advertised_url, chunkRec.generation, now, now);
                }

                // Balance over-replicated chunk if returning node restores original quorum
                const fileRec = db.prepare('SELECT replication_factor FROM files WHERE file_id = ?').get(chunkRec.file_id) as any;
                const desiredRF = fileRec ? fileRec.replication_factor : REPLICATION_FACTOR;
                const allActiveReps = db.prepare("SELECT * FROM replicas WHERE chunk_id = ? AND status = 'active' ORDER BY created_at DESC").all(chunkId) as any[];

                if (allActiveReps.length > desiredRF) {
                  // Deregister excess failover replica from metadata, preserving physical file on disk as untracked data
                  const excessReps = allActiveReps.filter((r: any) => r.node_id !== node_id).slice(0, allActiveReps.length - desiredRF);
                  for (const excess of excessReps) {
                    db.prepare('DELETE FROM replicas WHERE replica_id = ?').run(excess.replica_id);
                  }
                }
              }
            } catch {
              // ignore
            }
          }
        }
      }
    }

    refreshNodeMetrics();

    // 1. Storage node records local reconciliation operation (Rule 1 Local Event via addEvent)
    const reconEvent = purgedStale > 0
      ? addEvent(
          'ORPHAN_CHUNKS_PURGED',
          'INFO',
          `Reconciliation on ${node_id} reclaimed ${(reclaimedBytes / 1024).toFixed(1)} KB by purging ${purgedStale} unreferenced chunk replica(s).`,
          node_id,
          null,
          { origin_node_id: node_id }
        )
      : addEvent(
          'NODE_RECONCILED',
          'INFO',
          `Reconciliation verified ${verifiedActive} active chunk replica(s) on ${node_id}.`,
          node_id,
          null,
          { origin_node_id: node_id }
        );

    // 2. Storage node sends reconciliation completion ACK to coordinator (Rule 2 Send)
    const nodeAckClock = clockEngine.tickSend(node_id);

    // 3. Coordinator receives storage node ACK (Rule 3 Receive)
    clockEngine.tickReceive(COORDINATOR_PARTICIPANT_ID, nodeAckClock);

    // 4. Update storage node persisted clock to reflect its latest active participant clock (nodeAckClock)
    db.prepare('UPDATE storage_nodes SET lamport_clock = ? WHERE node_id = ?').run(nodeAckClock, node_id);

    res.json({
      node_id,
      verified_active: verifiedActive,
      purged_stale: purgedStale,
      orphaned_reclaimed_bytes: reclaimedBytes,
      corrupted: 0,
      missing_updated: 0,
      lamport_clock: nodeAckClock,
    });
  });

  // Cluster-Wide Orphan Purge & Garbage Collection
  apiRouter.post('/cluster/reconcile-orphans', (_req, res) => {
    const { total_purged_orphans, total_reclaimed_bytes } = reclaimOrphanChunks(db, getNodeChunksDir);

    refreshNodeMetrics();

    addEvent(
      'CLUSTER_GARBAGE_COLLECTED',
      'INFO',
      `Cluster garbage collection pass reclaimed ${(total_reclaimed_bytes / 1024).toFixed(1)} KB across storage nodes (${total_purged_orphans} orphan chunks purged).`
    );

    res.json({
      total_purged_orphans,
      total_reclaimed_bytes,
      timestamp: Date.now() / 1000,
    });
  });

  // Self-Healing Reconcile
  apiRouter.post('/self-healing/reconcile', (_req, res) => {
    let recoveredCount = 0;
    const healthyNodes = db.prepare("SELECT * FROM storage_nodes WHERE health_state = 'HEALTHY'").all() as any[];

    if (healthyNodes.length === 0) {
      return res.json({ status: 'completed', recovered_chunks: 0, timestamp: Date.now() / 1000 });
    }

    const healthyNodeMap = new Map<string, any>(healthyNodes.map(n => [n.node_id, n]));
    const committedFiles = db.prepare("SELECT * FROM files WHERE status IN ('committed', 'degraded')").all() as any[];

    for (const fileRecord of committedFiles) {
      const chunks = db.prepare('SELECT * FROM chunks WHERE file_id = ?').all(fileRecord.file_id) as any[];

      for (const chunk of chunks) {
        const replicas = db.prepare('SELECT * FROM replicas WHERE chunk_id = ?').all(chunk.chunk_id) as any[];

        const healthyActiveReplicas = replicas.filter(r => {
          return r.status === 'active' && healthyNodeMap.has(r.node_id);
        });

        const desired = fileRecord.replication_factor;
        if (healthyActiveReplicas.length > desired) {
          const excess = healthyActiveReplicas.slice().sort((a, b) => b.created_at - a.created_at).slice(0, healthyActiveReplicas.length - desired);
          for (const ex of excess) {
            db.prepare('DELETE FROM replicas WHERE replica_id = ?').run(ex.replica_id);
          }
        } else if (healthyActiveReplicas.length < desired && healthyActiveReplicas.length > 0) {
          const sourceReplica = healthyActiveReplicas[0];
          const sourceNode = healthyNodeMap.get(sourceReplica.node_id)!;
          const sourceDir = getNodeChunksDir(sourceNode.node_id);
          const sourceBinPath = path.join(sourceDir, `chunk_${chunk.chunk_id}.bin`);

          if (!fs.existsSync(sourceBinPath)) continue;
          const chunkData = fs.readFileSync(sourceBinPath);

          // Check if there is a corrupted replica on a healthy node that can be repaired in place
          const corruptedOnHealthy = replicas.find(r => r.status === 'corrupted' && healthyNodeMap.has(r.node_id));

          if (corruptedOnHealthy) {
            const repairNode = healthyNodeMap.get(corruptedOnHealthy.node_id)!;
            const targetDir = getNodeChunksDir(repairNode.node_id);
            const targetBinPath = path.join(targetDir, `chunk_${chunk.chunk_id}.bin`);
            fs.writeFileSync(targetBinPath, chunkData);

            const sendTs = clockEngine.tickSend(sourceNode.node_id);
            const repairTs = clockEngine.tickReceive(repairNode.node_id, sendTs);
            const ackTs = clockEngine.tickSend(repairNode.node_id);
            clockEngine.tickReceive(COORDINATOR_PARTICIPANT_ID, ackTs);

            const now = Date.now() / 1000;
            db.prepare("UPDATE replicas SET status = 'active', last_verified_at = ?, lamport_clock = ? WHERE replica_id = ?").run(
              now,
              repairTs,
              corruptedOnHealthy.replica_id
            );

            const jobId = `job_${crypto.randomBytes(6).toString('hex')}`;
            db.prepare(`
              INSERT INTO recovery_jobs (job_id, chunk_id, file_id, source_node_id, target_node_id, generation, status, attempts, error_message, created_at, completed_at, lamport_clock)
              VALUES (?, ?, ?, ?, ?, ?, 'completed', 1, null, ?, ?, ?)
            `).run(jobId, chunk.chunk_id, fileRecord.file_id, sourceNode.node_id, repairNode.node_id, chunk.generation, now, now, repairTs);

            addEvent(
              'REPLICATION_RESTORED',
              'INFO',
              `Self-healing repaired corrupted replica of chunk ${chunk.chunk_id} on ${repairNode.node_id} (L:${repairTs}).`,
              repairNode.node_id,
              fileRecord.file_id,
              { origin_node_id: repairNode.node_id, correlation_id: fileRecord.file_id }
            );
            recoveredCount++;
            continue;
          }

          // Otherwise find eligible healthy target nodes that do not currently hold an active replica
          const currentNodesWithChunk = new Set(replicas.filter(r => r.status === 'active').map(r => r.node_id));
          const eligibleTargets = healthyNodes.filter(n => !currentNodesWithChunk.has(n.node_id));

          if (eligibleTargets.length > 0) {
            const targetNode = eligibleTargets[0];
            const targetDir = getNodeChunksDir(targetNode.node_id);
            const targetBinPath = path.join(targetDir, `chunk_${chunk.chunk_id}.bin`);
            const targetMetaPath = path.join(targetDir, `chunk_${chunk.chunk_id}.meta.json`);
            const now = Date.now() / 1000;

            const sendTs = clockEngine.tickSend(sourceNode.node_id);
            const targetTs = clockEngine.tickReceive(targetNode.node_id, sendTs);
            const ackTs = clockEngine.tickSend(targetNode.node_id);
            clockEngine.tickReceive(COORDINATOR_PARTICIPANT_ID, ackTs);

            fs.writeFileSync(targetBinPath, chunkData);
            fs.writeFileSync(
              targetMetaPath,
              JSON.stringify({
                chunk_id: chunk.chunk_id,
                node_id: targetNode.node_id,
                size_bytes: chunkData.length,
                checksum_sha256: chunk.checksum_sha256,
                created_at: now,
                lamport_clock: targetTs,
              })
            );

            const repId = `rep_${crypto.randomBytes(6).toString('hex')}`;
            db.prepare(`
              INSERT INTO replicas (replica_id, chunk_id, node_id, node_url, status, generation, last_verified_at, created_at, lamport_clock)
              VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?)
            `).run(repId, chunk.chunk_id, targetNode.node_id, targetNode.advertised_url, chunk.generation, now, now, targetTs);

            const jobId = `job_${crypto.randomBytes(6).toString('hex')}`;
            db.prepare(`
              INSERT INTO recovery_jobs (job_id, chunk_id, file_id, source_node_id, target_node_id, generation, status, attempts, error_message, created_at, completed_at, lamport_clock)
              VALUES (?, ?, ?, ?, ?, ?, 'completed', 1, null, ?, ?, ?)
            `).run(jobId, chunk.chunk_id, fileRecord.file_id, sourceNode.node_id, targetNode.node_id, chunk.generation, now, now, targetTs);

            addEvent(
              'REPLICATION_RESTORED',
              'INFO',
              `Self-healing successfully re-replicated chunk ${chunk.chunk_id} from ${sourceNode.node_id} to ${targetNode.node_id} (L:${targetTs}).`,
              targetNode.node_id,
              fileRecord.file_id,
              { origin_node_id: targetNode.node_id, correlation_id: fileRecord.file_id }
            );

            recoveredCount++;
          }
        }
      }
    }

    refreshNodeMetrics();
    res.json({
      status: 'completed',
      recovered_chunks: recoveredCount,
      timestamp: Date.now() / 1000,
      lamport_clock: clockEngine.getClock(COORDINATOR_PARTICIPANT_ID),
    });
  });

  // Recovery Jobs List
  apiRouter.get('/recovery/jobs', (_req, res) => {
    const jobs = db.prepare('SELECT * FROM recovery_jobs ORDER BY created_at DESC LIMIT 50').all();
    res.json(jobs);
  });

  // Cluster Events List with Deterministic Lamport Ordering & Causal Metadata
  apiRouter.get('/events', (req, res) => {
    const limit = Math.min(200, Math.max(1, parseInt(req.query.limit as string) || 50));
    const orderBy = ((req.query.order_by || req.query.sort) as string) || 'timestamp';
    const direction = (req.query.direction as string)?.toUpperCase() === 'ASC' ? 'ASC' : 'DESC';

    const conditions: string[] = [];
    const params: any[] = [];

    if (req.query.node_id) {
      conditions.push('(node_id = ? OR origin_node_id = ?)');
      params.push(req.query.node_id, req.query.node_id);
    }
    if (req.query.origin_node_id) {
      conditions.push('origin_node_id = ?');
      params.push(req.query.origin_node_id);
    }
    if (req.query.file_id) {
      conditions.push('(file_id = ? OR correlation_id = ?)');
      params.push(req.query.file_id, req.query.file_id);
    }
    if (req.query.correlation_id) {
      conditions.push('correlation_id = ?');
      params.push(req.query.correlation_id);
    }
    if (req.query.event_type) {
      conditions.push('event_type = ?');
      params.push(req.query.event_type);
    }
    if (req.query.severity) {
      conditions.push('severity = ?');
      params.push(req.query.severity);
    }
    if (req.query.min_clock !== undefined) {
      const minC = parseInt(req.query.min_clock as string);
      if (!isNaN(minC)) {
        conditions.push('lamport_clock >= ?');
        params.push(minC);
      }
    }
    if (req.query.max_clock !== undefined) {
      const maxC = parseInt(req.query.max_clock as string);
      if (!isNaN(maxC)) {
        conditions.push('lamport_clock <= ?');
        params.push(maxC);
      }
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    let orderClause: string;
    if (orderBy === 'lamport' || orderBy === 'deterministic' || orderBy === 'clock') {
      orderClause = `ORDER BY lamport_clock ${direction}, origin_node_id ASC, event_id ASC`;
    } else {
      orderClause = `ORDER BY timestamp ${direction}, event_id DESC`;
    }

    params.push(limit);
    const events = db.prepare(`SELECT * FROM cluster_events ${whereClause} ${orderClause} LIMIT ?`).all(...params) as any[];

    // Parse causal_parent_ids from JSON if needed
    const formatted = events.map(e => {
      let parents: string[] | null = null;
      if (e.causal_parent_ids) {
        try {
          parents = JSON.parse(e.causal_parent_ids);
        } catch {
          parents = [e.causal_parent_ids];
        }
      } else if (e.causal_parent_id) {
        parents = [e.causal_parent_id];
      }
      return {
        ...e,
        causal_parent_ids: parents,
      };
    });

    res.json(formatted);
  });

  // Cryptographic Integrity Verify
  apiRouter.post('/integrity/verify', (req, res) => {
    const { file_id, chunk_id } = req.body || {};
    let query = "SELECT * FROM files WHERE status IN ('committed', 'degraded')";
    const params: any[] = [];
    if (file_id) {
      query += ' AND file_id = ?';
      params.push(file_id);
    }

    const files = db.prepare(query).all(...params) as any[];
    let checkedFiles = 0;
    let checkedChunks = 0;
    let validReps = 0;
    let corruptReps = 0;
    let missingReps = 0;
    const details: any[] = [];

    for (const f of files) {
      checkedFiles++;
      let chunkQuery = 'SELECT * FROM chunks WHERE file_id = ?';
      const chunkParams: any[] = [f.file_id];
      if (chunk_id) {
        chunkQuery += ' AND chunk_id = ?';
        chunkParams.push(chunk_id);
      }
      const chunks = db.prepare(chunkQuery).all(...chunkParams) as any[];

      for (const c of chunks) {
        checkedChunks++;
        const replicas = db.prepare('SELECT * FROM replicas WHERE chunk_id = ?').all(c.chunk_id) as any[];

        for (const r of replicas) {
          const dir = getNodeChunksDir(r.node_id);
          const binPath = path.join(dir, `chunk_${c.chunk_id}.bin`);
          let statusVal = 'valid';

          if (!fs.existsSync(binPath)) {
            statusVal = 'missing';
            missingReps++;
            db.prepare("UPDATE replicas SET status = 'missing' WHERE replica_id = ?").run(r.replica_id);
          } else {
            try {
              const data = fs.readFileSync(binPath);
              const hash = computeSha256(data);
              const nodeClock = clockEngine.tickLocal(r.node_id);
              clockEngine.tickReceive(COORDINATOR_PARTICIPANT_ID, clockEngine.tickSend(r.node_id));

              if (hash === c.checksum_sha256) {
                statusVal = 'valid';
                validReps++;
                const now = Date.now() / 1000;
                db.prepare("UPDATE replicas SET status = 'active', last_verified_at = ?, lamport_clock = ? WHERE replica_id = ?").run(now, nodeClock, r.replica_id);
              } else {
                statusVal = 'corrupted';
                corruptReps++;
                db.prepare("UPDATE replicas SET status = 'corrupted', lamport_clock = ? WHERE replica_id = ?").run(nodeClock, r.replica_id);
              }
            } catch {
              statusVal = 'missing';
              missingReps++;
              db.prepare("UPDATE replicas SET status = 'missing' WHERE replica_id = ?").run(r.replica_id);
            }
          }

          details.push({
            chunk_id: c.chunk_id,
            node_id: r.node_id,
            status: statusVal,
            classification: statusVal === 'valid' ? 'COMMITTED_REPLICA' : 'STALE_REPLICA',
          });
        }
      }
    }

    const auditEvent = addEvent(
      'INTEGRITY_AUDIT_COMPLETED',
      corruptReps > 0 || missingReps > 0 ? 'WARNING' : 'INFO',
      `Cryptographic audit verified ${validReps} replica(s) across ${checkedChunks} chunk(s) (${corruptReps} corrupted, ${missingReps} missing).`,
      null,
      file_id || null,
      { origin_node_id: COORDINATOR_PARTICIPANT_ID }
    );

    res.json({
      checked_files: checkedFiles,
      checked_chunks: checkedChunks,
      valid_replicas: validReps,
      corrupted_replicas: corruptReps,
      missing_replicas: missingReps,
      details,
      lamport_clock: auditEvent.lamport_clock,
    });
  });

  // Inject Bit-Rot / Corruption for testing
  apiRouter.post('/chunks/:chunk_id/corrupt', (req, res) => {
    const { chunk_id } = req.params;
    const { node_id } = req.body || {};

    let targetNodeId = node_id;
    if (!targetNodeId) {
      const rep = db.prepare('SELECT * FROM replicas WHERE chunk_id = ? LIMIT 1').get(chunk_id) as any;
      if (rep) targetNodeId = rep.node_id;
    }

    if (!targetNodeId) {
      return res.status(404).json({ detail: 'Target node or chunk not found' });
    }

    const dir = getNodeChunksDir(targetNodeId);
    const binPath = path.join(dir, `chunk_${chunk_id}.bin`);
    if (!fs.existsSync(binPath)) {
      return res.status(404).json({ detail: 'Chunk binary file not found on node' });
    }

    try {
      const data = fs.readFileSync(binPath);
      if (data.length > 0) {
        data[0] = data[0] ^ 0xff;
        fs.writeFileSync(binPath, data);
      }

      const rotEvent = addEvent(
        'BIT_ROT_INJECTED',
        'ERROR',
        `Operator injected physical bit-rot into chunk ${chunk_id} on ${targetNodeId}.`,
        targetNodeId,
        null,
        { origin_node_id: targetNodeId }
      );

      db.prepare("UPDATE replicas SET status = 'corrupted', lamport_clock = ? WHERE chunk_id = ? AND node_id = ?").run(rotEvent.lamport_clock, chunk_id, targetNodeId);
      db.prepare('UPDATE storage_nodes SET lamport_clock = ? WHERE node_id = ?').run(rotEvent.lamport_clock, targetNodeId);

      res.json({
        chunk_id,
        node_id: targetNodeId,
        status: 'corrupted',
        message: 'Physical bit rot injected into replica binary.',
        lamport_clock: rotEvent.lamport_clock,
      });
    } catch (e: any) {
      res.status(500).json({ detail: e.message });
    }
  });

  // Files List
  apiRouter.get('/files', (req, res) => {
    const page = Math.max(1, parseInt(req.query.page as string) || 1);
    const pageSize = Math.min(100, Math.max(1, parseInt(req.query.page_size as string) || 20));
    const offset = (page - 1) * pageSize;

    const total = (db.prepare("SELECT count(*) as cnt FROM files WHERE status IN ('committed', 'degraded')").get() as any).cnt;
    const fileList = db.prepare(`
      SELECT file_id, filename, size_bytes, checksum_sha256, chunk_count, replication_factor, metadata_version, status, created_at, lamport_clock
      FROM files
      WHERE status IN ('committed', 'degraded')
      ORDER BY created_at DESC
      LIMIT ? OFFSET ?
    `).all(pageSize, offset) as any[];

    res.json({
      total_files: total,
      page,
      page_size: pageSize,
      files: fileList,
    });
  });

  // File Upload
  apiRouter.post(
    '/files',
    (req: Request, res: Response, next) => {
      (upload.single('file') as any)(req, res, (err: any) => {
        if (err) {
          if (err.code === 'LIMIT_FILE_SIZE') {
            return res.status(413).json({ detail: 'File exceeds maximum upload size limit (50 MB)' });
          }
          return res.status(400).json({ detail: err.message || 'File upload error' });
        }
        next();
      });
    },
    (req: Request, res: Response) => {
      if (!req.file) {
        return res.status(400).json({ detail: 'No file uploaded' });
      }

      const buffer = req.file.buffer;
      const filename = req.file.originalname || 'unnamed_file';
      const contentType = req.file.mimetype || 'application/octet-stream';
      const totalSize = buffer.length;
      const fileSha256 = computeSha256(buffer);
      const now = Date.now() / 1000;
      const fileId = `file_${crypto.randomBytes(16).toString('hex')}`;

      // Split into 1 MiB chunks
      const chunkTuples: { seq: number; data: Buffer; sha: string }[] = [];
      let seq = 0;
      for (let offset = 0; offset < totalSize; offset += DEFAULT_CHUNK_SIZE) {
        const slice = buffer.subarray(offset, Math.min(totalSize, offset + DEFAULT_CHUNK_SIZE));
        chunkTuples.push({
          seq,
          data: slice,
          sha: computeSha256(slice),
        });
        seq++;
      }

      const healthyNodes = db.prepare("SELECT * FROM storage_nodes WHERE health_state = 'HEALTHY'").all() as any[];

      if (healthyNodes.length < REPLICATION_FACTOR) {
        return res.status(500).json({
          detail: `Not enough healthy nodes: needed ${REPLICATION_FACTOR}, found ${healthyNodes.length}`,
        });
      }

      // Check storage quota across target nodes before writing any chunks or metadata
      const projectedUsage = new Map<string, number>();
      for (const node of healthyNodes) {
        const inv = getNodeInventory(node.node_id);
        projectedUsage.set(node.node_id, inv.physical_bytes_used);
      }

      for (let i = 0; i < chunkTuples.length; i++) {
        const ct = chunkTuples[i];
        for (let r = 0; r < REPLICATION_FACTOR; r++) {
          const node = healthyNodes[(i + r) % healthyNodes.length];
          const current = projectedUsage.get(node.node_id) || 0;
          const updated = current + ct.data.length;
          if (updated > node.storage_limit_bytes) {
            return res.status(507).json({
              detail: `Storage quota exceeded on node '${node.node_id}': projected ${updated} bytes exceeds limit of ${node.storage_limit_bytes} bytes.`,
            });
          }
          projectedUsage.set(node.node_id, updated);
        }
      }

      // 1. Coordinator records UPLOAD_INITIATED local event (advances Coordinator clock)
      const uploadInitEvent = addEvent(
        'UPLOAD_INITIATED',
        'INFO',
        `Client initiated upload of file '${filename}' (${totalSize} bytes, ${chunkTuples.length} chunk(s)).`,
        null,
        fileId,
        { origin_node_id: COORDINATOR_PARTICIPANT_ID, correlation_id: fileId }
      );

      // Insert file metadata
      db.prepare(`
        INSERT INTO files (file_id, filename, content_type, size_bytes, checksum_sha256, chunk_count, chunk_size_bytes, replication_factor, metadata_version, status, created_at, updated_at, lamport_clock)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'committed', ?, ?, ?)
      `).run(fileId, filename, contentType, totalSize, fileSha256, chunkTuples.length, DEFAULT_CHUNK_SIZE, REPLICATION_FACTOR, now, now, uploadInitEvent.lamport_clock);

      // 2. Distribute replicas across healthy nodes with explicit Lamport Send/Receive rules
      for (let i = 0; i < chunkTuples.length; i++) {
        const ct = chunkTuples[i];
        const chunkId = `chk_${crypto.randomBytes(8).toString('hex')}`;

        const chosenNodes: any[] = [];
        for (let r = 0; r < REPLICATION_FACTOR; r++) {
          const nodeIdx = (i + r) % healthyNodes.length;
          chosenNodes.push(healthyNodes[nodeIdx]);
        }

        // Insert chunk metadata first to satisfy FOREIGN KEY constraint from replicas
        db.prepare(`
          INSERT INTO chunks (chunk_id, file_id, sequence_index, size_bytes, checksum_sha256, generation, created_at, lamport_clock)
          VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        `).run(chunkId, fileId, ct.seq, ct.data.length, ct.sha, now, uploadInitEvent.lamport_clock);

        let chunkMaxClock = 0;

        for (const node of chosenNodes) {
          const dir = getNodeChunksDir(node.node_id);
          const binPath = path.join(dir, `chunk_${chunkId}.bin`);
          const metaPath = path.join(dir, `chunk_${chunkId}.meta.json`);

          // Rule 2 (Send): Coordinator increments its clock and sends placement message
          const sendClock = clockEngine.tickSend(COORDINATOR_PARTICIPANT_ID);

          // Rule 3 (Receive): Storage node receives chunk write message and advances clock
          const nodeRecvClock = clockEngine.tickReceive(node.node_id, sendClock);

          fs.writeFileSync(binPath, ct.data);
          fs.writeFileSync(
            metaPath,
            JSON.stringify({
              chunk_id: chunkId,
              node_id: node.node_id,
              size_bytes: ct.data.length,
              checksum_sha256: ct.sha,
              created_at: now,
              lamport_clock: nodeRecvClock,
            })
          );

          // Rule 2 (Send): Storage node sends write ACK back to coordinator
          const nodeAckClock = clockEngine.tickSend(node.node_id);

          // Rule 3 (Receive): Coordinator processes storage node ACK
          clockEngine.tickReceive(COORDINATOR_PARTICIPANT_ID, nodeAckClock);

          chunkMaxClock = Math.max(chunkMaxClock, nodeRecvClock);

          // Record replica with storage node's logical clock
          const repId = `rep_${crypto.randomBytes(6).toString('hex')}`;
          db.prepare(`
            INSERT INTO replicas (replica_id, chunk_id, node_id, node_url, status, generation, last_verified_at, created_at, lamport_clock)
            VALUES (?, ?, ?, ?, 'active', 1, ?, ?, ?)
          `).run(repId, chunkId, node.node_id, node.advertised_url, now, now, nodeRecvClock);

          db.prepare('UPDATE storage_nodes SET lamport_clock = ? WHERE node_id = ?').run(nodeAckClock, node.node_id);
        }

        db.prepare('UPDATE chunks SET lamport_clock = ? WHERE chunk_id = ?').run(chunkMaxClock, chunkId);
      }

      refreshNodeMetrics();

      const lease = db.prepare("SELECT fencing_token FROM leader_leases WHERE lease_id = 'coordinator_primary_lease'").get() as any;
      const token = lease ? lease.fencing_token : 1;

      // 3. Coordinator records FILE_COMMITTED event linked causally to UPLOAD_INITIATED
      const commitEvent = addEvent(
        'FILE_COMMITTED',
        'INFO',
        `File '${filename}' (${totalSize} bytes, ${chunkTuples.length} chunks, ${REPLICATION_FACTOR}x replication) successfully committed with Fencing Token #${token}.`,
        null,
        fileId,
        {
          origin_node_id: COORDINATOR_PARTICIPANT_ID,
          correlation_id: fileId,
          causal_parent_id: uploadInitEvent.event_id,
          causal_parent_ids: [uploadInitEvent.event_id],
        }
      );

      db.prepare('UPDATE files SET lamport_clock = ? WHERE file_id = ?').run(commitEvent.lamport_clock, fileId);

      res.status(201).json({
        file_id: fileId,
        filename,
        size_bytes: totalSize,
        checksum_sha256: fileSha256,
        chunk_count: chunkTuples.length,
        replication_factor: REPLICATION_FACTOR,
        status: 'committed',
        metadata_version: 1,
        created_at: now,
        lamport_clock: commitEvent.lamport_clock,
      });
  });

  // File Details
  apiRouter.get('/files/:file_id', (req, res) => {
    const { file_id } = req.params;
    const fileRecord = db.prepare('SELECT * FROM files WHERE file_id = ?').get(file_id) as any;
    if (!fileRecord) {
      return res.status(404).json({ detail: 'File not found' });
    }

    const healthyNodes = db.prepare("SELECT node_id FROM storage_nodes WHERE health_state = 'HEALTHY'").all() as any[];
    const healthySet = new Set(healthyNodes.map(n => n.node_id));

    const chunks = db.prepare('SELECT * FROM chunks WHERE file_id = ? ORDER BY sequence_index ASC').all(file_id) as any[];
    const chunkDetails = chunks.map(c => {
      const reps = db.prepare('SELECT * FROM replicas WHERE chunk_id = ?').all(c.chunk_id) as any[];
      const healthyCount = reps.filter(r => r.status === 'active' && healthySet.has(r.node_id)).length;

      return {
        chunk_id: c.chunk_id,
        sequence_index: c.sequence_index,
        size_bytes: c.size_bytes,
        checksum_sha256: c.checksum_sha256,
        generation: c.generation,
        lamport_clock: c.lamport_clock ?? 0,
        replicas: reps.map(r => ({
          replica_id: r.replica_id,
          node_id: r.node_id,
          node_url: r.node_url,
          status: r.status,
          generation: r.generation,
          last_verified_at: r.last_verified_at,
          lamport_clock: r.lamport_clock ?? 0,
        })),
        healthy_replicas_count: healthyCount,
      };
    });

    res.json({
      file_id: fileRecord.file_id,
      filename: fileRecord.filename,
      content_type: fileRecord.content_type,
      size_bytes: fileRecord.size_bytes,
      checksum_sha256: fileRecord.checksum_sha256,
      chunk_count: fileRecord.chunk_count,
      chunk_size_bytes: fileRecord.chunk_size_bytes,
      replication_factor: fileRecord.replication_factor,
      metadata_version: fileRecord.metadata_version,
      status: fileRecord.status,
      created_at: fileRecord.created_at,
      updated_at: fileRecord.updated_at,
      lamport_clock: fileRecord.lamport_clock ?? 0,
      chunks: chunkDetails,
    });
  });

  // File Download
  apiRouter.get('/files/:file_id/download', (req, res) => {
    const { file_id } = req.params;
    const fileRecord = db.prepare('SELECT * FROM files WHERE file_id = ?').get(file_id) as any;
    if (!fileRecord) {
      return res.status(404).json({ detail: 'File not found' });
    }

    const healthyNodes = db.prepare("SELECT node_id FROM storage_nodes WHERE health_state = 'HEALTHY'").all() as any[];
    const healthySet = new Set(healthyNodes.map(n => n.node_id));

    const chunks = db.prepare('SELECT * FROM chunks WHERE file_id = ? ORDER BY sequence_index ASC').all(file_id) as any[];
    const assembledChunks: Buffer[] = [];

    for (const chunk of chunks) {
      let chunkData: Buffer | null = null;
      const replicas = db.prepare('SELECT * FROM replicas WHERE chunk_id = ?').all(chunk.chunk_id) as any[];

      // Try active replicas on healthy nodes first
      const candidateReplicas = replicas.slice().sort((a, b) => {
        const aHealthy = a.status === 'active' && healthySet.has(a.node_id);
        const bHealthy = b.status === 'active' && healthySet.has(b.node_id);
        if (aHealthy && !bHealthy) return -1;
        if (!aHealthy && bHealthy) return 1;
        return 0;
      });

      for (const rep of candidateReplicas) {
        if (!healthySet.has(rep.node_id)) continue;
        if (rep.status !== 'active') continue;

        const dir = getNodeChunksDir(rep.node_id);
        const binPath = path.join(dir, `chunk_${chunk.chunk_id}.bin`);
        if (fs.existsSync(binPath)) {
          try {
            const data = fs.readFileSync(binPath);
            if (computeSha256(data) === chunk.checksum_sha256) {
              chunkData = data;
              break;
            }
          } catch {
            // continue
          }
        }
      }

      if (!chunkData) {
        return res.status(500).json({
          detail: `Recoverability Error: Chunk index ${chunk.sequence_index} (${chunk.chunk_id}) could not be retrieved from any replica!`,
        });
      }

      assembledChunks.push(chunkData);
    }

    const fullFile = Buffer.concat(assembledChunks);
    if (computeSha256(fullFile) !== fileRecord.checksum_sha256) {
      return res.status(500).json({
        detail: 'Corrupted download: final file checksum did not match authoritative metadata!',
      });
    }

    res.setHeader('Content-Disposition', `attachment; filename="${fileRecord.filename}"`);
    res.setHeader('Content-Type', fileRecord.content_type || 'application/octet-stream');
    res.setHeader('Content-Length', fullFile.length);
    res.send(fullFile);
  });

  // File Delete with Lamport logical clock synchronization across storage participants
  apiRouter.delete('/files/:file_id', (req, res) => {
    const { file_id } = req.params;
    const fileRecord = db.prepare('SELECT * FROM files WHERE file_id = ?').get(file_id) as any;
    if (!fileRecord) {
      return res.status(404).json({ detail: 'File not found' });
    }

    const chunks = db.prepare('SELECT * FROM chunks WHERE file_id = ?').all(file_id) as any[];

    // Lamport Rule 2 (Send): Coordinator sends delete instruction to storage nodes
    const deleteSendClock = clockEngine.tickSend(COORDINATOR_PARTICIPANT_ID);

    // Delete chunks physically from disk across all 5 storage nodes
    for (let i = 1; i <= 5; i++) {
      const nid = `storage-node-${i}`;
      // Storage node processes deletion message
      const nodeRecvClock = clockEngine.tickReceive(nid, deleteSendClock);

      const dir = getNodeChunksDir(nid);
      for (const chunk of chunks) {
        const binPath = path.join(dir, `chunk_${chunk.chunk_id}.bin`);
        const metaPath = path.join(dir, `chunk_${chunk.chunk_id}.meta.json`);
        try {
          if (fs.existsSync(binPath)) fs.unlinkSync(binPath);
          if (fs.existsSync(metaPath)) fs.unlinkSync(metaPath);
        } catch {
          // ignore
        }
      }

      // Storage node sends deletion ACK back
      const nodeAckClock = clockEngine.tickSend(nid);
      clockEngine.tickReceive(COORDINATOR_PARTICIPANT_ID, nodeAckClock);
      db.prepare('UPDATE storage_nodes SET lamport_clock = ? WHERE node_id = ?').run(nodeAckClock, nid);
    }

    // Cascade delete in SQLite
    db.prepare('DELETE FROM files WHERE file_id = ?').run(file_id);
    refreshNodeMetrics();

    const delEvent = addEvent(
      'FILE_DELETED',
      'INFO',
      `File '${fileRecord.filename}' (${file_id}) deleted and storage reclaimed across storage nodes.`,
      null,
      file_id,
      { origin_node_id: COORDINATOR_PARTICIPANT_ID, correlation_id: file_id }
    );

    res.json({
      file_id,
      deleted: true,
      lamport_clock: delEvent.lamport_clock,
    });
  });

  // Mount API router
  app.use('/api/v1', apiRouter);
  app.use('/api', apiRouter);

  // Catch unmatched API routes and return JSON 404 so Vite never returns index.html for API requests
  app.all(['/api', '/api/*', '/api/v1/*'], (_req, res) => {
    res.status(404).json({ error: 'Endpoint not found' });
  });

  // API error handler returning JSON instead of HTML
  app.use((err: any, req: Request, res: Response, next: any) => {
    if (req.path.startsWith('/api')) {
      console.error('API Error:', err);
      return res.status(500).json({ error: err?.message || 'Internal Server Error' });
    }
    next(err);
  });

  const httpServer = http.createServer(app);

  // Mount Vite or static dist in production
  if (process.env.NODE_ENV === 'production') {
    const distPath = path.resolve(__dirname, 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  } else {
    const isHmrDisabled = process.env.DISABLE_HMR === 'true';
    const vite = await createViteServer({
      server: {
        middlewareMode: true,
        ws: isHmrDisabled ? false : { server: httpServer },
      },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  }

  httpServer.listen(PORT, HOST, () => {
    console.log(`ShardSphere Server listening on http://${HOST}:${PORT}`);
  });
}

startServer().catch(err => {
  console.error('Failed to start ShardSphere server:', err);
  process.exit(1);
});
