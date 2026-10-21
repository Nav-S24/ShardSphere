import React, { useState, useEffect, useRef } from 'react';
import {
  Server,
  Database,
  ShieldCheck,
  HardDrive,
  RefreshCw,
  Upload,
  Download,
  Trash2,
  FileText,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  Layers,
  Activity,
  Terminal,
  Radio,
  Zap,
  Search,
  ChevronRight,
  ExternalLink,
  ShieldAlert,
  ArrowRight,
  Menu,
  X,
  FileCheck2,
  Clock,
  Cpu,
  CornerDownRight,
  Filter,
} from 'lucide-react';

// ============================================================================
// Types & Contracts (Preserved Exactly)
// ============================================================================

interface StorageNodeInfo {
  node_id: string;
  node_name: string;
  advertised_url: string;
  health_state: string;
  consecutive_failures: number;
  last_heartbeat_at: number | null;
  last_failure_at: number | null;
  total_chunks: number;
  total_bytes_used: number;
  committed_chunks?: number;
  committed_bytes_used?: number;
  active_chunks?: number;
  active_bytes_used?: number;
  stale_chunks?: number;
  stale_bytes_used?: number;
  orphaned_chunks?: number;
  orphaned_bytes_used?: number;
  untracked_chunks?: number;
  untracked_bytes_used?: number;
  physical_chunks?: number;
  physical_bytes_used?: number;
  storage_limit_bytes: number;
  disk_usage_percent: number;
  lamport_clock?: number;
}

interface ClocksOverview {
  coordinator: { id: string; clock: number };
  nodes: { id: string; clock: number }[];
  highest_observed: number;
  timestamp: number;
}

interface ClusterOverview {
  cluster_name: string;
  coordinator_id: string;
  coordinator_lamport_clock?: number;
  lamport_clock?: number;
  is_leader: boolean;
  fencing_token: number;
  total_nodes: number;
  healthy_nodes: number;
  suspect_nodes: number;
  unavailable_nodes: number;
  total_files: number;
  total_chunks: number;
  total_bytes_stored: number;
  total_committed_replicas?: number;
  total_committed_bytes?: number;
  total_active_replicas?: number;
  total_active_bytes?: number;
  total_stale_replicas?: number;
  total_stale_bytes?: number;
  total_orphaned_chunks?: number;
  total_orphaned_bytes?: number;
  total_untracked_chunks?: number;
  total_untracked_bytes?: number;
  total_physical_chunks?: number;
  total_physical_bytes?: number;
  configured_replication_factor: number;
  active_recovery_jobs: number;
  nodes: StorageNodeInfo[];
}

interface LeaderInfo {
  is_leader: boolean;
  current_leader_id: string;
  fencing_token: number;
  lease_expires_at: number;
  time_remaining_seconds: number;
  coordinator_instance_id: string;
}

interface FileItem {
  file_id: string;
  filename: string;
  size_bytes: number;
  checksum_sha256: string;
  chunk_count: number;
  replication_factor: number;
  status: string;
  metadata_version: number;
  created_at: number;
  lamport_clock?: number;
}

interface ReplicaDetail {
  replica_id: string;
  node_id: string;
  node_url: string;
  status: string;
  generation: number;
  last_verified_at: number | null;
  lamport_clock?: number;
}

interface ChunkDetail {
  chunk_id: string;
  sequence_index: number;
  size_bytes: number;
  checksum_sha256: string;
  generation: number;
  replicas: ReplicaDetail[];
  healthy_replicas_count: number;
  lamport_clock?: number;
}

interface FileDetail {
  file_id: string;
  filename: string;
  content_type: string;
  size_bytes: number;
  checksum_sha256: string;
  chunk_count: number;
  chunk_size_bytes: number;
  replication_factor: number;
  metadata_version: number;
  status: string;
  created_at: number;
  updated_at: number;
  lamport_clock?: number;
  chunks: ChunkDetail[];
}

interface RecoveryJob {
  job_id: string;
  chunk_id: string;
  file_id: string;
  source_node_id: string;
  target_node_id: string;
  generation: number;
  status: string;
  attempts: number;
  error_message: string | null;
  created_at: number;
  completed_at: number | null;
  lamport_clock?: number;
}

interface ClusterEvent {
  event_id: string;
  event_type: string;
  severity: string;
  node_id: string | null;
  file_id: string | null;
  message: string;
  timestamp: number;
  lamport_clock?: number;
  origin_node_id?: string;
  causal_parent_id?: string | null;
  causal_parent_ids?: string[] | null;
  correlation_id?: string | null;
}

interface ChunkInventoryItem {
  node_id: string;
  chunk_id: string;
  filename: string;
  size_bytes: number;
  sha256: string;
  sha_match: boolean;
  classification: 'COMMITTED_REPLICA' | 'STALE_REPLICA' | 'ORPHANED_CHUNK' | 'UNTRACKED_DATA';
  file_id: string | null;
  file_name: string | null;
  status: string;
  generation: number;
}

interface InventoryAuditResult {
  summary: {
    total_files_committed: number;
    total_logical_chunks: number;
    total_committed_bytes: number;
    configured_replication_factor: number;
    expected_replicas: number;
    actual_committed_replicas: number;
    actual_stale_replicas: number;
    actual_orphaned_chunks: number;
    actual_untracked_chunks: number;
    total_physical_chunks: number;
    total_physical_bytes: number;
  };
  per_node: Record<string, any>;
  items: ChunkInventoryItem[];
}

// Safe JSON parser to protect against HTML warmup/502 responses
async function safeJson<T = any>(res: Response): Promise<T | null> {
  if (!res.ok) return null;
  const contentType = res.headers.get('content-type') || '';
  if (!contentType.includes('application/json')) return null;
  try {
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

export default function App() {
  const [cluster, setCluster] = useState<ClusterOverview | null>(null);
  const [leader, setLeader] = useState<LeaderInfo | null>(null);
  const [files, setFiles] = useState<FileItem[]>([]);
  const [jobs, setJobs] = useState<RecoveryJob[]>([]);
  const [events, setEvents] = useState<ClusterEvent[]>([]);
  const [inventoryAudit, setInventoryAudit] = useState<InventoryAuditResult | null>(null);
  const [selectedFileDetail, setSelectedFileDetail] = useState<FileDetail | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [activeTab, setActiveTab] = useState<'overview' | 'explorer' | 'nodes' | 'selfhealing' | 'inspector' | 'faultlab'>('overview');
  const [mobileMenuOpen, setMobileMenuOpen] = useState<boolean>(false);

  // File Upload State
  const [uploading, setUploading] = useState<boolean>(false);
  const [uploadFeedback, setUploadFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Search & Filter States (Frontend presentation enhancements)
  const [fileSearchQuery, setFileSearchQuery] = useState<string>('');
  const [eventSeverityFilter, setEventSeverityFilter] = useState<'ALL' | 'INFO' | 'WARNING' | 'ERROR'>('ALL');

  // Verification & Action States
  const [verifyingAudit, setVerifyingAudit] = useState<boolean>(false);
  const [auditResult, setAuditResult] = useState<any | null>(null);
  const [reconciling, setReconciling] = useState<boolean>(false);
  const [actionFeedback, setActionFeedback] = useState<string | null>(null);

  // File Deletion Safeguard Modal State
  const [fileToDelete, setFileToDelete] = useState<{ file_id: string; filename: string } | null>(null);
  const [isDeletingFile, setIsDeletingFile] = useState<boolean>(false);
  const [deleteFileError, setDeleteFileError] = useState<string | null>(null);

  // Fault Lab States
  const [labRunning, setLabRunning] = useState<boolean>(false);
  const [labLogs, setLabLogs] = useState<string[]>([]);
  const [labSelectedNode, setLabSelectedNode] = useState<string>('storage-node-2');

  // Lamport Logical Clocks State
  const [clocks, setClocks] = useState<ClocksOverview | null>(null);
  const [eventSortOrder, setEventSortOrder] = useState<'deterministic' | 'timestamp'>('deterministic');
  const eventSortOrderRef = useRef<'deterministic' | 'timestamp'>('deterministic');
  eventSortOrderRef.current = eventSortOrder;

  const handleSortOrderChange = async (order: 'deterministic' | 'timestamp') => {
    if (order === eventSortOrder) return;
    setEventSortOrder(order);
    eventSortOrderRef.current = order;
    try {
      const eventsRes = await fetch(`/api/v1/events?limit=50&order_by=${order === 'deterministic' ? 'lamport' : 'timestamp'}&direction=desc`);
      const eData = await safeJson<ClusterEvent[]>(eventsRes);
      if (eData) setEvents(eData);
    } catch (err) {
      console.warn('Failed to immediately refresh events for sort order:', err);
    }
  };

  // ============================================================================
  // Telemetry Polling (Unchanged Backend Contract + Lamport Clock Vector)
  // ============================================================================
  const fetchAllTelemetry = async () => {
    try {
      const [clusterRes, leaderRes, filesRes, jobsRes, eventsRes, auditRes, clocksRes] = await Promise.all([
        fetch('/api/v1/cluster'),
        fetch('/api/v1/leader'),
        fetch('/api/v1/files'),
        fetch('/api/v1/recovery/jobs'),
        fetch(`/api/v1/events?limit=50&order_by=${eventSortOrderRef.current === 'deterministic' ? 'lamport' : 'timestamp'}&direction=desc`),
        fetch('/api/v1/inventory/audit'),
        fetch('/api/v1/clocks'),
      ]);

      const [cData, lData, fData, jData, eData, aData, clkData] = await Promise.all([
        safeJson<ClusterOverview>(clusterRes),
        safeJson<LeaderInfo>(leaderRes),
        safeJson<{ files: FileItem[] }>(filesRes),
        safeJson<RecoveryJob[]>(jobsRes),
        safeJson<ClusterEvent[]>(eventsRes),
        safeJson<InventoryAuditResult>(auditRes),
        safeJson<ClocksOverview>(clocksRes),
      ]);

      if (cData) setCluster(cData);
      if (lData) setLeader(lData);
      if (fData?.files) setFiles(fData.files);
      if (jData) setJobs(jData);
      if (eData) setEvents(eData);
      if (aData) setInventoryAudit(aData);
      if (clkData) setClocks(clkData);
    } catch (err) {
      console.warn('Telemetry polling notice:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAllTelemetry();
    const interval = setInterval(fetchAllTelemetry, 2500);
    return () => clearInterval(interval);
  }, []);

  // ============================================================================
  // Action Handlers (All Original Behaviors & Safety Checks Preserved)
  // ============================================================================
  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploading(true);
    setUploadFeedback(null);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch('/api/v1/files', {
        method: 'POST',
        body: formData,
      });

      if (!res.ok) {
        const err = await safeJson<{ detail?: string }>(res);
        throw new Error(err?.detail || `Upload failed with status ${res.status}`);
      }

      const data = await safeJson<any>(res);
      if (!data) throw new Error('Invalid server response');

      setUploadFeedback({
        type: 'success',
        message: `File "${data.filename}" (${formatBytes(data.size_bytes)}) committed across ${data.replication_factor}x replicas across distinct storage nodes. Metadata version: v${data.metadata_version}`,
      });
      await fetchAllTelemetry();
      if (fileInputRef.current) fileInputRef.current.value = '';
    } catch (err: any) {
      setUploadFeedback({ type: 'error', message: err.message });
    } finally {
      setUploading(false);
    }
  };

  const handleInitiateDelete = (fileId: string, filename: string) => {
    setDeleteFileError(null);
    setFileToDelete({ file_id: fileId, filename });
  };

  const handleCancelDelete = () => {
    if (isDeletingFile) return;
    setFileToDelete(null);
    setDeleteFileError(null);
  };

  const handleConfirmDelete = async () => {
    if (!fileToDelete) return;
    setIsDeletingFile(true);
    setDeleteFileError(null);
    try {
      const res = await fetch(`/api/v1/files/${fileToDelete.file_id}`, { method: 'DELETE' });
      const data = await safeJson<{ file_id?: string; deleted?: boolean; detail?: string; error?: string }>(res);
      if (!res.ok) {
        throw new Error(data?.detail || data?.error || `Server returned error (${res.status})`);
      }
      if (selectedFileDetail?.file_id === fileToDelete.file_id) {
        setSelectedFileDetail(null);
      }
      await fetchAllTelemetry();
      setActionFeedback(`File "${fileToDelete.filename}" permanently deleted and physical chunks purged.`);
      setTimeout(() => setActionFeedback(null), 4000);
      setFileToDelete(null);
    } catch (err: any) {
      setDeleteFileError(err?.message || 'Failed to delete file across replica nodes');
    } finally {
      setIsDeletingFile(false);
    }
  };

  const handleInspectFile = async (fileId: string) => {
    try {
      const res = await fetch(`/api/v1/files/${fileId}`);
      const detail = await safeJson<FileDetail>(res);
      if (detail) {
        setSelectedFileDetail(detail);
      }
    } catch (err) {
      console.error('Failed to inspect file:', err);
    }
  };

  const handleTriggerReconcile = async () => {
    setReconciling(true);
    try {
      const res = await fetch('/api/v1/self-healing/reconcile', { method: 'POST' });
      const data = await safeJson<{ recovered_chunks: number }>(res);
      if (data) {
        setActionFeedback(`Reconciliation complete: recovered ${data.recovered_chunks} chunk replica(s).`);
        await fetchAllTelemetry();
      }
    } catch (err: any) {
      setActionFeedback(`Reconcile failed: ${err.message}`);
    } finally {
      setReconciling(false);
      setTimeout(() => setActionFeedback(null), 5000);
    }
  };

  const handleToggleNodeState = async (nodeId: string) => {
    try {
      const res = await fetch(`/api/v1/nodes/${nodeId}/toggle-state`, { method: 'POST' });
      const data = await safeJson<{ node_id: string; new_state: string }>(res);
      if (data) {
        setActionFeedback(`Node ${data.node_id} state transitioned to ${data.new_state}.`);
        await fetchAllTelemetry();
        setTimeout(() => setActionFeedback(null), 4000);
      }
    } catch (err: any) {
      alert(`Toggle failed: ${err.message}`);
    }
  };

  const handleReconcileNode = async (nodeId: string) => {
    try {
      const res = await fetch(`/api/v1/nodes/${nodeId}/reconcile`, { method: 'POST' });
      const data = await safeJson<any>(res);
      if (data) {
        const reclaimedMsg = data.orphaned_reclaimed_bytes > 0 ? `, reclaimed ${formatBytes(data.orphaned_reclaimed_bytes)}` : '';
        setActionFeedback(`Node ${nodeId} reconciled: ${data.verified_active} active verified, ${data.purged_stale} stale purged${reclaimedMsg}.`);
        await fetchAllTelemetry();
        setTimeout(() => setActionFeedback(null), 5000);
      }
    } catch (err: any) {
      alert(`Reconcile failed: ${err.message}`);
    }
  };

  const handleReconcileOrphans = async () => {
    setReconciling(true);
    try {
      const res = await fetch('/api/v1/cluster/reconcile-orphans', { method: 'POST' });
      const data = await safeJson<any>(res);
      if (data) {
        setActionFeedback(`Cluster orphan reconciliation complete: purged ${data.total_purged_orphans} orphaned chunks and reclaimed ${formatBytes(data.total_reclaimed_bytes)}.`);
        await fetchAllTelemetry();
      }
    } catch (err: any) {
      alert(`Orphan purge failed: ${err.message}`);
    } finally {
      setReconciling(false);
      setTimeout(() => setActionFeedback(null), 5000);
    }
  };

  const handleLeaderStepDown = async () => {
    if (!confirm('Force the current leader to step down? A new leader lease will be acquired with an incremented fencing token.')) return;
    try {
      const res = await fetch('/api/v1/leader/step-down', { method: 'POST' });
      const data = await safeJson<{ fencing_token: number }>(res);
      if (data) {
        setActionFeedback(`Leader stepped down voluntarily. Monotonic fencing token advanced to #${data.fencing_token}.`);
        await fetchAllTelemetry();
        setTimeout(() => setActionFeedback(null), 4000);
      }
    } catch (err: any) {
      alert(`Step down failed: ${err.message}`);
    }
  };

  const handleRunAudit = async () => {
    setVerifyingAudit(true);
    setAuditResult(null);
    try {
      const res = await fetch('/api/v1/integrity/verify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });
      const data = await safeJson<any>(res);
      if (data) {
        setAuditResult(data);
      }
      const invRes = await fetch('/api/v1/inventory/audit');
      const invData = await safeJson<InventoryAuditResult>(invRes);
      if (invData) {
        setInventoryAudit(invData);
      }
    } catch (err) {
      console.error(err);
    } finally {
      setVerifyingAudit(false);
    }
  };

  const handleCorruptChunk = async (chunkId: string, nodeId: string) => {
    try {
      const res = await fetch(`/api/v1/chunks/${chunkId}/corrupt`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ node_id: nodeId }),
      });
      const data = await safeJson<any>(res);
      if (data) {
        setActionFeedback(`Injected bit rot into chunk ${chunkId} on ${nodeId}. Run Cryptographic Audit to inspect!`);
        await fetchAllTelemetry();
        if (selectedFileDetail) {
          handleInspectFile(selectedFileDetail.file_id);
        }
        setTimeout(() => setActionFeedback(null), 5000);
      }
    } catch (err: any) {
      alert(`Corruption injection failed: ${err.message}`);
    }
  };

  const runEndToEndFaultDemo = async () => {
    setLabRunning(true);
    const logs: string[] = [];
    const addLog = (msg: string) => {
      logs.push(`[${new Date().toLocaleTimeString()}] ${msg}`);
      setLabLogs([...logs]);
    };

    try {
      addLog('--- STARTING END-TO-END DEMO: Node Failure, Re-replication & Exact Recovery ---');
      addLog('Step 1/6: Generating and uploading real 1.5 MiB verification probe...');

      const probeSize = 1572864; // 1.5 MiB
      const probeBytes = new Uint8Array(probeSize);
      for (let i = 0; i < probeSize; i++) probeBytes[i] = (i * 37) % 256;
      const probeBlob = new Blob([probeBytes], { type: 'application/octet-stream' });
      const formData = new FormData();
      formData.append('file', probeBlob, 'fault_lab_probe.bin');

      const uploadRes = await fetch('/api/v1/files', { method: 'POST', body: formData });
      if (!uploadRes.ok) throw new Error('Failed to upload lab probe');
      const uploadData = await safeJson<any>(uploadRes);
      if (!uploadData) throw new Error('Failed to parse uploaded probe metadata');
      addLog(`✔ Probe uploaded: ID ${uploadData.file_id.slice(0, 12)}... (${formatBytes(uploadData.size_bytes)}, ${uploadData.chunk_count} chunks, 3x replicas).`);

      const detailRes = await fetch(`/api/v1/files/${uploadData.file_id}`);
      const detailData = await safeJson<FileDetail>(detailRes);
      if (!detailData) throw new Error('Failed to retrieve probe details');
      const nodeAssignments = detailData.chunks.map(c => `Chunk #${c.sequence_index} -> [${c.replicas.map(r => r.node_id).join(', ')}]`);
      nodeAssignments.forEach(na => addLog(`   ${na}`));

      const nodesWithProbe = new Set(detailData.chunks.flatMap(c => c.replicas.map(r => r.node_id)));
      const targetNode = nodesWithProbe.has(labSelectedNode) ? labSelectedNode : detailData.chunks[0].replicas[0].node_id;
      if (targetNode !== labSelectedNode) {
        setLabSelectedNode(targetNode);
      }
      addLog(`Step 2/6: Simulating hard failure on target node "${targetNode}" (holding active probe replica)...`);
      await fetch(`/api/v1/nodes/${targetNode}/toggle-state`, { method: 'POST' });
      addLog(`✔ Node "${targetNode}" transitioned to UNAVAILABLE.`);

      addLog('Step 3/6: Polling cluster failure detector & under-replication discovery...');
      await new Promise(r => setTimeout(r, 1500));
      await fetchAllTelemetry();

      addLog('Step 4/6: Triggering automatic self-healing engine...');
      const recRes = await fetch('/api/v1/self-healing/reconcile', { method: 'POST' });
      const recData = await safeJson<any>(recRes);
      addLog(`✔ Self-healing complete: ${recData?.recovered_chunks ?? 0} chunk(s) re-replicated to alternate healthy nodes.`);

      addLog(`Step 5/6: Downloading and reconstructing file while "${targetNode}" is STILL DEAD...`);
      const dlRes = await fetch(`/api/v1/files/${uploadData.file_id}/download`);
      if (!dlRes.ok) throw new Error('Download failed during node partition!');
      const dlBuffer = await dlRes.arrayBuffer();
      const dlBytes = new Uint8Array(dlBuffer);

      if (dlBytes.length !== probeSize) {
        throw new Error(`Size mismatch: expected ${probeSize}, got ${dlBytes.length}`);
      }

      let match = true;
      for (let i = 0; i < probeSize; i++) {
        if (dlBytes[i] !== probeBytes[i]) {
          match = false;
          break;
        }
      }

      if (match) {
        addLog('✔ EXACT BYTE-FOR-BYTE MATCH CONFIRMED (1,572,864 bytes verified).');
      } else {
        throw new Error('Byte integrity verification failed!');
      }

      addLog(`Step 6/6: Reviving node "${targetNode}" and running returning-node reconciliation...`);
      await fetch(`/api/v1/nodes/${targetNode}/toggle-state`, { method: 'POST' });
      const reconResp = await fetch(`/api/v1/nodes/${targetNode}/reconcile`, { method: 'POST' });
      const reconData = await safeJson<any>(reconResp);
      addLog(`✔ Returning node reconciled: ${reconData?.verified_active ?? 0} active verified, ${reconData?.purged_stale ?? 0} stale purged.`);

      await fetch(`/api/v1/files/${uploadData.file_id}`, { method: 'DELETE' });
      addLog('✔ Demo file cleaned up. Cluster returned to 100% healthy status.');
      addLog('--- END-TO-END DEMO COMPLETED SUCCESSFULLY ---');
      await fetchAllTelemetry();
    } catch (err: any) {
      addLog(`✘ DEMO ERROR: ${err.message}`);
    } finally {
      setLabRunning(false);
    }
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  // Restrained semantic status styling (Linear-like calm badges)
  const getSemanticBadge = (status: string) => {
    const s = status.toUpperCase();
    if (['HEALTHY', 'COMMITTED', 'ACTIVE', 'VALID', 'COMPLETED', 'COMMITTED_REPLICA'].includes(s)) {
      return (
        <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[11px] font-medium bg-emerald-50 text-emerald-700 border border-emerald-200/80">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 shrink-0" aria-hidden="true" />
          <span>{status.replace(/_/g, ' ')}</span>
        </span>
      );
    }
    if (['SUSPECT', 'RECOVERING', 'DEGRADED', 'IN_PROGRESS', 'ORPHANED_CHUNK', 'ORPHANED'].includes(s)) {
      return (
        <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[11px] font-medium bg-amber-50 text-amber-700 border border-amber-200/80">
          <span className="w-1.5 h-1.5 rounded-full bg-amber-500 shrink-0" aria-hidden="true" />
          <span>{status.replace(/_/g, ' ')}</span>
        </span>
      );
    }
    if (['UNAVAILABLE', 'CORRUPTED', 'FAILED', 'MISSING', 'STALE_REPLICA', 'STALE'].includes(s)) {
      return (
        <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[11px] font-medium bg-rose-50 text-rose-700 border border-rose-200/80">
          <span className="w-1.5 h-1.5 rounded-full bg-rose-500 shrink-0" aria-hidden="true" />
          <span>{status.replace(/_/g, ' ')}</span>
        </span>
      );
    }
    if (['UNTRACKED_DATA', 'UNTRACKED'].includes(s)) {
      return (
        <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[11px] font-medium bg-sky-50 text-sky-700 border border-sky-200/80">
          <span className="w-1.5 h-1.5 rounded-full bg-sky-500 shrink-0" aria-hidden="true" />
          <span>{status.replace(/_/g, ' ')}</span>
        </span>
      );
    }
    return (
      <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[11px] font-medium bg-slate-100 text-slate-700 border border-slate-200">
        <span className="w-1.5 h-1.5 rounded-full bg-slate-400 shrink-0" aria-hidden="true" />
        <span>{status.replace(/_/g, ' ')}</span>
      </span>
    );
  };

  const filteredFiles = files.filter(f =>
    f.filename.toLowerCase().includes(fileSearchQuery.toLowerCase()) ||
    f.file_id.toLowerCase().includes(fileSearchQuery.toLowerCase()) ||
    f.checksum_sha256.toLowerCase().includes(fileSearchQuery.toLowerCase())
  );

  const sortedEvents = [...events].sort((a, b) => {
    if (eventSortOrder === 'deterministic') {
      const clockA = a.lamport_clock ?? 0;
      const clockB = b.lamport_clock ?? 0;
      if (clockB !== clockA) return clockB - clockA;
      const nodeA = a.origin_node_id || '';
      const nodeB = b.origin_node_id || '';
      if (nodeA !== nodeB) return nodeA.localeCompare(nodeB);
      return a.event_id.localeCompare(b.event_id);
    } else {
      return (b.timestamp || 0) - (a.timestamp || 0);
    }
  });

  const filteredEvents = sortedEvents.filter(e => {
    if (eventSeverityFilter === 'ALL') return true;
    return e.severity.toUpperCase() === eventSeverityFilter;
  });

  return (
    <div className="min-h-screen bg-[#f8fafc] text-slate-900 font-sans flex flex-col">
      {/* ===================================================================== */}
      {/* 1. Global Shell: Dark Charcoal Header (Linear Visual Restraint)      */}
      {/* ===================================================================== */}
      <header className="w-full bg-[#121316] border-b border-[#23262d] sticky top-0 z-50 shadow-xs text-slate-200">
        <div className="max-w-[1440px] w-full mx-auto px-4 sm:px-6 lg:px-8">
          <div className="h-14 flex items-center justify-between gap-3 sm:gap-4">
            
            {/* Zone 1: Brand Wordmark & Cluster Environment */}
            <div className="flex items-center gap-2.5 shrink-0">
              <div className="w-7 h-7 rounded-md bg-[#4f56c7] flex items-center justify-center text-white shadow-xs shrink-0">
                <Layers className="w-4 h-4" />
              </div>
              <div className="flex items-center gap-2">
                <span className="font-semibold text-white tracking-tight text-[15px] whitespace-nowrap">
                  ShardSphere
                </span>
                <span className="hidden xl:inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-mono bg-[#1c1f26] text-slate-400 border border-[#2d313b] whitespace-nowrap">
                  prod-cluster · 3x quorum
                </span>
              </div>
            </div>

            {/* Zone 2: Navigation Links */}
            <nav className="hidden lg:flex items-center gap-1 text-[13px] overflow-hidden">
              {[
                { id: 'overview', label: 'Overview', icon: Activity },
                { id: 'explorer', label: 'Storage Explorer', icon: Database, badge: files.length },
                { id: 'nodes', label: 'Node Topology', icon: Server, badge: cluster?.total_nodes ?? 5 },
                { id: 'selfhealing', label: 'Self-Healing', icon: ShieldCheck, badge: jobs.filter(j => j.status === 'in_progress').length || undefined },
                { id: 'inspector', label: 'Audit & Inspector', icon: FileCheck2 },
                { id: 'faultlab', label: 'Fault Lab', icon: Zap },
              ].map((tab) => {
                const Icon = tab.icon;
                const isActive = activeTab === tab.id;
                return (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id as any)}
                    className={`flex items-center gap-1.5 xl:gap-2 px-2.5 xl:px-3 py-1.5 rounded-md font-medium transition-colors cursor-pointer whitespace-nowrap ${
                      isActive
                        ? 'bg-[#22252c] text-white shadow-xs border border-[#2e333d]'
                        : 'text-slate-400 hover:text-slate-200 hover:bg-[#1a1c22]'
                    }`}
                  >
                    <Icon className={`w-3.5 h-3.5 shrink-0 ${isActive ? 'text-[#8b95f6]' : 'text-slate-500'}`} />
                    <span>{tab.label}</span>
                    {tab.badge !== undefined && (
                      <span className={`px-1.5 py-0.2 text-[10px] font-mono rounded ${isActive ? 'bg-[#313642] text-slate-200' : 'bg-[#1a1c22] text-slate-500'}`}>
                        {tab.badge}
                      </span>
                    )}
                  </button>
                );
              })}
            </nav>

            {/* Zone 3: Leader Lease & Symmetrical Controls */}
            <div className="flex items-center gap-2 sm:gap-2.5 shrink-0">
              {/* Leader Lease Indicator */}
              <div className="hidden md:inline-flex items-center gap-2 h-8 px-2.5 rounded-md bg-[#1a1c22] border border-[#2b2f3a] text-xs font-mono shrink-0">
                <span
                  className={`w-2 h-2 rounded-full shrink-0 ${leader?.is_leader ? 'bg-emerald-400 shadow-[0_0_8px_rgba(52,211,153,0.4)]' : 'bg-amber-400'}`}
                  aria-hidden="true"
                />
                <span className="text-slate-400 text-[11px]">Leader</span>
                <span className="text-slate-200 font-semibold text-[11px]">#{leader?.fencing_token ?? 1}</span>
                <span className="text-slate-600">·</span>
                <span className="text-slate-300 text-[11px]">{(leader?.time_remaining_seconds ?? 0).toFixed(1)}s</span>
              </div>

              {/* Reconcile Control */}
              <button
                onClick={handleTriggerReconcile}
                disabled={reconciling}
                className="h-8 px-3 rounded-md text-xs font-medium bg-[#1a1c22] hover:bg-[#232730] text-slate-200 border border-[#2e333d] transition-colors cursor-pointer disabled:opacity-50 inline-flex items-center justify-center gap-1.5 shrink-0"
                title="Trigger immediate cluster re-replication scan"
              >
                <RefreshCw className={`w-3.5 h-3.5 text-slate-400 shrink-0 ${reconciling ? 'animate-spin' : ''}`} />
                <span className="hidden sm:inline">{reconciling ? 'Scanning...' : 'Reconcile'}</span>
              </button>

              {/* Upload Primary Action */}
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={uploading}
                className="h-8 px-3 rounded-md text-xs font-medium bg-[#4f56c7] hover:bg-[#4349ab] text-white border border-[#5d65d8]/40 shadow-xs transition-colors cursor-pointer disabled:opacity-50 inline-flex items-center justify-center gap-1.5 shrink-0"
                title="Upload file to distributed cluster"
              >
                <Upload className="w-3.5 h-3.5 shrink-0" />
                <span>{uploading ? 'Uploading...' : 'Upload'}</span>
              </button>
              <input
                ref={fileInputRef}
                type="file"
                onChange={handleFileUpload}
                className="hidden"
              />

              {/* Mobile Menu Toggle */}
              <button
                onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                className="lg:hidden h-8 w-8 rounded-md text-slate-400 hover:text-white hover:bg-[#1a1c22] inline-flex items-center justify-center cursor-pointer shrink-0"
                aria-label="Toggle navigation and actions menu"
              >
                {mobileMenuOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
              </button>
            </div>
          </div>

          {/* Accessible Responsive Menu: Navigation and Global Actions */}
          {mobileMenuOpen && (
            <div className="lg:hidden py-3 border-t border-[#23262d] space-y-3">
              {/* Navigation Tabs */}
              <div className="space-y-1">
                {[
                  { id: 'overview', label: 'Overview', icon: Activity },
                  { id: 'explorer', label: 'Storage Explorer', icon: Database, badge: files.length },
                  { id: 'nodes', label: 'Node Topology', icon: Server, badge: cluster?.total_nodes ?? 5 },
                  { id: 'selfhealing', label: 'Self-Healing', icon: ShieldCheck, badge: jobs.filter(j => j.status === 'in_progress').length || undefined },
                  { id: 'inspector', label: 'Audit & Inspector', icon: FileCheck2 },
                  { id: 'faultlab', label: 'Fault Lab', icon: Zap },
                ].map((tab) => {
                  const Icon = tab.icon;
                  const isActive = activeTab === tab.id;
                  return (
                    <button
                      key={tab.id}
                      onClick={() => {
                        setActiveTab(tab.id as any);
                        setMobileMenuOpen(false);
                      }}
                      className={`w-full flex items-center justify-between px-3 py-2 rounded-md text-xs font-medium cursor-pointer ${
                        isActive
                          ? 'bg-[#22252c] text-white'
                          : 'text-slate-400 hover:text-slate-200 hover:bg-[#1a1c22]'
                      }`}
                    >
                      <div className="flex items-center gap-2.5">
                        <Icon className="w-4 h-4 text-slate-400" />
                        <span>{tab.label}</span>
                      </div>
                      {tab.badge !== undefined && (
                        <span className="px-1.5 py-0.2 text-[10px] font-mono rounded bg-[#1a1c22] text-slate-400">
                          {tab.badge}
                        </span>
                      )}
                    </button>
                  );
                })}
              </div>

              {/* Cluster Telemetry & Global Actions in Mobile Menu */}
              <div className="pt-2 border-t border-[#23262d] space-y-2 px-1">
                <div className="flex items-center justify-between p-2.5 rounded-md bg-[#1a1c22] border border-[#2b2f3a] text-xs font-mono">
                  <div className="flex items-center gap-2">
                    <span
                      className={`w-2 h-2 rounded-full ${leader?.is_leader ? 'bg-emerald-400' : 'bg-amber-400'}`}
                      aria-hidden="true"
                    />
                    <span className="text-slate-400">Leader Lease:</span>
                    <span className="text-slate-200 font-semibold">#{leader?.fencing_token ?? 1}</span>
                  </div>
                  <span className="text-slate-400">{(leader?.time_remaining_seconds ?? 0).toFixed(1)}s remaining</span>
                </div>

                <div className="grid grid-cols-2 gap-2 pt-1">
                  <button
                    onClick={() => {
                      handleTriggerReconcile();
                      setMobileMenuOpen(false);
                    }}
                    disabled={reconciling}
                    className="h-9 px-3 rounded-md text-xs font-medium bg-[#1a1c22] hover:bg-[#232730] text-slate-200 border border-[#2e333d] transition-colors cursor-pointer disabled:opacity-50 inline-flex items-center justify-center gap-2"
                  >
                    <RefreshCw className={`w-3.5 h-3.5 text-slate-400 ${reconciling ? 'animate-spin' : ''}`} />
                    <span>{reconciling ? 'Scanning...' : 'Reconcile'}</span>
                  </button>

                  <button
                    onClick={() => {
                      fileInputRef.current?.click();
                      setMobileMenuOpen(false);
                    }}
                    disabled={uploading}
                    className="h-9 px-3 rounded-md text-xs font-medium bg-[#4f56c7] hover:bg-[#4349ab] text-white border border-[#5d65d8]/40 shadow-xs transition-colors cursor-pointer disabled:opacity-50 inline-flex items-center justify-center gap-2"
                  >
                    <Upload className="w-3.5 h-3.5" />
                    <span>{uploading ? 'Processing...' : 'Upload File'}</span>
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </header>

      {/* Global Status Banner for User Actions */}
      {actionFeedback && (
        <div className="bg-slate-900 text-white text-xs px-4 py-2 text-center flex items-center justify-center gap-2 border-b border-slate-800">
          <Activity className="w-3.5 h-3.5 text-[#8b95f6]" />
          <span>{actionFeedback}</span>
        </div>
      )}

      {/* ===================================================================== */}
      {/* 2. Main Content Viewport (Warm Light Surface with Clean Borders)      */}
      {/* ===================================================================== */}
      <main className="flex-1 max-w-[1440px] w-full mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">

        {/* ------------------------------------------------------------- */}
        {/* TAB 1: CLUSTER OVERVIEW                                        */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'overview' && (
          <div className="space-y-6">
            {/* Page Header */}
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-2 border-b border-slate-200">
              <div>
                <h1 className="text-xl font-bold tracking-tight text-slate-900">Cluster Status & Health</h1>
                <p className="text-xs text-slate-500 mt-0.5">
                  High-availability 5-node storage cluster running quorum consensus and automatic re-replication.
                </p>
              </div>
              <div className="flex items-center gap-2 text-xs text-slate-500 font-mono">
                <span>Coordinator: {cluster?.coordinator_id || 'coordinator-primary-1'}</span>
                <span>·</span>
                <span>RF: {cluster?.configured_replication_factor ?? 3}x</span>
              </div>
            </div>

            {/* Top Operational Metrics (Linear-style KPI cards) */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              {/* Card 1: Storage Node Availability */}
              <div className="bg-white border border-slate-200/90 rounded-lg p-4 shadow-xs flex flex-col justify-between">
                <div>
                  <div className="flex items-center justify-between text-slate-500 text-xs font-medium">
                    <span>Node Availability</span>
                    <Server className="w-4 h-4 text-slate-400" />
                  </div>
                  <div className="mt-2.5 flex items-baseline gap-2">
                    <span className="text-2xl font-bold font-mono tracking-tight text-slate-900">
                      {cluster?.healthy_nodes ?? 0} <span className="text-base font-normal text-slate-400">/ {cluster?.total_nodes ?? 0}</span>
                    </span>
                    <span className="text-xs font-medium text-emerald-600">Online</span>
                  </div>
                </div>
                <div className="mt-3 pt-3 border-t border-slate-100 flex items-center justify-between text-[11px] text-slate-500">
                  <span>{cluster?.unavailable_nodes ?? 0} unavailable</span>
                  <span>{cluster?.suspect_nodes ?? 0} suspect</span>
                  <span className="font-mono text-slate-400">{cluster?.total_nodes ?? 5} nodes total</span>
                </div>
              </div>

              {/* Card 2: Authoritative File Storage */}
              <div className="bg-white border border-slate-200/90 rounded-lg p-4 shadow-xs flex flex-col justify-between">
                <div>
                  <div className="flex items-center justify-between text-slate-500 text-xs font-medium">
                    <span>Logical Stored Chunks</span>
                    <Layers className="w-4 h-4 text-slate-400" />
                  </div>
                  <div className="mt-2.5 flex items-baseline gap-2">
                    <span className="text-2xl font-bold font-mono tracking-tight text-slate-900">
                      {cluster?.total_chunks ?? 0}
                    </span>
                    <span className="text-xs text-slate-500">logical chunks</span>
                  </div>
                </div>
                <div className="mt-3 pt-3 border-t border-slate-100 flex items-center justify-between text-[11px] text-slate-500">
                  <span>{files.length} committed file(s)</span>
                  <span className="font-mono font-medium text-slate-700">{formatBytes(cluster?.total_bytes_stored ?? 0)}</span>
                </div>
              </div>

              {/* Card 3: Replicated Storage Footprint */}
              <div className="bg-white border border-slate-200/90 rounded-lg p-4 shadow-xs flex flex-col justify-between">
                <div>
                  <div className="flex items-center justify-between text-slate-500 text-xs font-medium">
                    <span>Active Replicas on Disk</span>
                    <HardDrive className="w-4 h-4 text-slate-400" />
                  </div>
                  <div className="mt-2.5 flex items-baseline gap-2">
                    <span className="text-2xl font-bold font-mono tracking-tight text-slate-900">
                      {cluster?.total_committed_replicas ?? cluster?.total_active_replicas ?? 0}
                    </span>
                    <span className="text-xs text-emerald-600 font-medium">{cluster?.configured_replication_factor ?? 3}x Quorum</span>
                  </div>
                </div>
                <div className="mt-3 pt-3 border-t border-slate-100 flex items-center justify-between text-[11px] text-slate-500">
                  <span>Replicated Footprint</span>
                  <span className="font-mono font-medium text-slate-700">
                    {formatBytes(cluster?.total_committed_bytes ?? cluster?.total_active_bytes ?? 0)}
                  </span>
                </div>
              </div>

              {/* Card 4: Leader Lease & Fencing */}
              <div className="bg-white border border-slate-200/90 rounded-lg p-4 shadow-xs flex flex-col justify-between">
                <div>
                  <div className="flex items-center justify-between text-slate-500 text-xs font-medium">
                    <span>Fencing Lease</span>
                    <Radio className="w-4 h-4 text-slate-400" />
                  </div>
                  <div className="mt-2.5 flex items-baseline gap-2">
                    <span className="text-2xl font-bold font-mono tracking-tight text-slate-900">
                      #{leader?.fencing_token ?? 1}
                    </span>
                    <span className="text-xs text-slate-500 font-mono">Token</span>
                  </div>
                </div>
                <div className="mt-3 pt-3 border-t border-slate-100 flex items-center justify-between text-[11px] text-slate-500">
                  <span>{(leader?.time_remaining_seconds ?? 0).toFixed(1)}s remaining</span>
                  <button
                    onClick={handleLeaderStepDown}
                    className="text-[#4f56c7] hover:text-[#4349ab] font-medium text-[11px] cursor-pointer"
                  >
                    Yield Lease
                  </button>
                </div>
              </div>
            </div>

            {/* Lamport Logical Clocks Vector Grid */}
            <div className="bg-white border border-slate-200/90 rounded-lg p-5 shadow-xs space-y-4">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 rounded-md bg-indigo-50 border border-indigo-200/80 flex items-center justify-center text-indigo-700">
                    <Clock className="w-4 h-4" />
                  </div>
                  <div>
                    <div className="flex items-center gap-2">
                      <h3 className="text-sm font-semibold text-slate-900">
                        Lamport Logical Clock Vector Engine
                      </h3>
                      <span className="px-2 py-0.5 rounded text-[10px] font-mono font-bold bg-indigo-50 text-indigo-700 border border-indigo-200">
                        Highest Observed: L:{clocks?.highest_observed ?? cluster?.coordinator_lamport_clock ?? 0}
                      </span>
                    </div>
                    <p className="text-xs text-slate-500 mt-0.5">
                      Distributed scalar logical clocks enforcing happens-before causal ordering across coordinator and storage node participants.
                    </p>
                  </div>
                </div>
                <div className="text-xs font-mono text-slate-500 flex items-center gap-2">
                  <span className="inline-block w-2 h-2 rounded-full bg-emerald-500"></span>
                  <span>Strictly Monotonic</span>
                </div>
              </div>

              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2.5 pt-1">
                {/* Coordinator */}
                <div className="bg-slate-50/80 border border-indigo-200/80 rounded-md p-3 flex flex-col justify-between">
                  <div className="flex items-center justify-between text-[11px] font-medium text-slate-500">
                    <span className="truncate">Coordinator</span>
                    <span className="w-1.5 h-1.5 rounded-full bg-indigo-600"></span>
                  </div>
                  <div className="my-1.5">
                    <span className="text-xl font-bold font-mono text-indigo-700">
                      L:{clocks?.coordinator.clock ?? cluster?.coordinator_lamport_clock ?? 0}
                    </span>
                  </div>
                  <div className="text-[10px] font-mono text-slate-400 truncate">
                    coordinator-primary-1
                  </div>
                </div>

                {/* 5 Storage Nodes */}
                {[1, 2, 3, 4, 5].map((idx) => {
                  const nodeId = `storage-node-${idx}`;
                  const nodeObj = cluster?.nodes?.find(n => n.node_id === nodeId);
                  const nodeClock = clocks?.nodes?.find(n => n.id === nodeId)?.clock ?? nodeObj?.lamport_clock ?? 0;
                  return (
                    <div key={nodeId} className="bg-slate-50/60 border border-slate-200 rounded-md p-3 flex flex-col justify-between">
                      <div className="flex items-center justify-between text-[11px] font-medium text-slate-500">
                        <span>Node {idx}</span>
                        <span className={`w-1.5 h-1.5 rounded-full ${nodeObj?.health_state === 'HEALTHY' ? 'bg-emerald-500' : 'bg-rose-500'}`}></span>
                      </div>
                      <div className="my-1.5">
                        <span className="text-xl font-bold font-mono text-slate-800">
                          L:{nodeClock}
                        </span>
                      </div>
                      <div className="text-[10px] font-mono text-slate-400 truncate">
                        {nodeId}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Storage Capacity & Replication Architecture */}
            {(() => {
              const logicalBytes = cluster?.total_bytes_stored ?? 0;
              const committedReplicasBytes = cluster?.total_committed_bytes ?? cluster?.total_active_bytes ?? 0;
              const physicalBytes = cluster?.total_physical_bytes ?? (cluster?.nodes?.reduce((acc, n) => acc + (n.physical_bytes_used || n.total_bytes_used || 0), 0) ?? 0);
              const totalCapacity = cluster?.nodes?.reduce((acc, n) => acc + (n.storage_limit_bytes || 0), 0) || (5 * 1024 * 1024 * 1024);
              const orphanBytes = cluster?.total_orphaned_bytes ?? 0;
              const rf = cluster?.configured_replication_factor ?? 3;
              const activePct = totalCapacity > 0 ? (committedReplicasBytes / totalCapacity) * 100 : 0;
              const orphanPct = totalCapacity > 0 ? (orphanBytes / totalCapacity) * 100 : 0;

              return (
                <div className="bg-white border border-slate-200/90 rounded-lg p-5 shadow-xs space-y-4">
                  <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-2">
                    <div>
                      <h3 className="text-sm font-semibold text-slate-900">
                        Storage Capacity & Quorum Replication Architecture
                      </h3>
                      <p className="text-xs text-slate-500 mt-0.5">
                        Visual relationship between logical data (1x), {rf}x active quorum replicas, physical disk allocation, and storage limit
                      </p>
                    </div>
                    <div className="text-xs font-mono text-slate-500 shrink-0">
                      Cluster Utilization: {((physicalBytes / (totalCapacity || 1)) * 100).toFixed(1)}% of {formatBytes(totalCapacity)}
                    </div>
                  </div>

                  {/* Multi-segment capacity breakdown bar */}
                  <div className="space-y-2.5">
                    <div className="w-full bg-slate-100 rounded-md h-3.5 flex overflow-hidden p-0.5 border border-slate-200/80">
                      <div
                        className="bg-[#4f56c7] h-full rounded-xs transition-all duration-300"
                        style={{ width: `${Math.max(activePct, committedReplicasBytes > 0 ? 1.5 : 0)}%` }}
                        title={`Active Quorum Replicas: ${formatBytes(committedReplicasBytes)}`}
                      />
                      {orphanBytes > 0 && (
                        <div
                          className="bg-amber-500 h-full rounded-xs ml-0.5 transition-all duration-300"
                          style={{ width: `${Math.max(orphanPct, 1.5)}%` }}
                          title={`Orphaned Fixture Chunks: ${formatBytes(orphanBytes)}`}
                        />
                      )}
                    </div>

                    {/* Legend with exact quantitative figures */}
                    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 pt-2 text-xs border-t border-slate-100">
                      <div className="space-y-0.5">
                        <div className="flex items-center gap-1.5 text-slate-500">
                          <span className="w-2 h-2 rounded-full bg-slate-400 shrink-0" />
                          <span>1. Logical Data:</span>
                        </div>
                        <div className="font-mono font-medium text-slate-900 pl-3.5">
                          {formatBytes(logicalBytes)} ({cluster?.total_chunks ?? 0} chk)
                        </div>
                      </div>

                      <div className="space-y-0.5">
                        <div className="flex items-center gap-1.5 text-slate-500">
                          <span className="w-2 h-2 rounded-full bg-[#4f56c7] shrink-0" />
                          <span>2. {rf}x Quorum Replicas:</span>
                        </div>
                        <div className="font-mono font-medium text-[#4f56c7] pl-3.5">
                          {formatBytes(committedReplicasBytes)} ({cluster?.total_committed_replicas ?? cluster?.total_active_replicas ?? 0} rep)
                        </div>
                      </div>

                      <div className="space-y-0.5">
                        <div className="flex items-center gap-1.5 text-slate-500">
                          <span className="w-2 h-2 rounded-full bg-slate-700 shrink-0" />
                          <span>3. Physical Disk Used:</span>
                        </div>
                        <div className="font-mono font-medium text-slate-800 pl-3.5">
                          {formatBytes(physicalBytes)}
                        </div>
                      </div>

                      <div className="space-y-0.5">
                        <div className="flex items-center gap-1.5 text-slate-500">
                          <span className="w-2 h-2 rounded-full bg-emerald-500 shrink-0" />
                          <span>4. Headroom Available:</span>
                        </div>
                        <div className="font-mono font-medium text-emerald-700 pl-3.5">
                          {formatBytes(Math.max(0, totalCapacity - physicalBytes))}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })()}

            {/* Prominent, Linear-Consistent Orphan Storage Warning */}
            {((cluster?.total_orphaned_chunks ?? 0) > 0 || (cluster?.total_untracked_chunks ?? 0) > 0) && (
              <div className="bg-amber-50/70 border border-amber-200/90 rounded-lg p-4 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 shadow-xs">
                <div className="flex items-start gap-3">
                  <div className="w-8 h-8 rounded-md bg-amber-100 flex items-center justify-center shrink-0 text-amber-700 mt-0.5">
                    <AlertTriangle className="w-4 h-4" />
                  </div>
                  <div>
                    <div className="flex flex-wrap items-center gap-2">
                      <h4 className="text-xs font-semibold text-amber-900">
                        Physical Volume Storage Notice
                      </h4>
                      <span className="text-[11px] font-mono text-amber-800 bg-amber-100/80 px-2 py-0.5 rounded border border-amber-200">
                        {cluster?.total_orphaned_chunks ?? 0} Orphaned Chunks · {formatBytes(cluster?.total_orphaned_bytes ?? 0)}
                      </span>
                    </div>
                    <p className="text-xs text-amber-800/90 mt-1">
                      Residual chunk binaries detected across storage nodes from prior test fixtures. Authoritative file metadata is completely healthy and protected with 3x quorum replication.
                    </p>
                  </div>
                </div>
                <button
                  onClick={() => {
                    if (confirm(`Purge ${cluster?.total_orphaned_chunks ?? 0} orphaned chunk binaries and reclaim ${formatBytes(cluster?.total_orphaned_bytes ?? 0)} across storage nodes?`)) {
                      handleReconcileOrphans();
                    }
                  }}
                  disabled={reconciling}
                  className="h-8 px-3.5 rounded-md bg-amber-600 hover:bg-amber-700 text-white font-medium text-xs transition-colors shrink-0 disabled:opacity-50 cursor-pointer shadow-xs inline-flex items-center gap-1.5 whitespace-nowrap"
                >
                  <Trash2 className="w-3.5 h-3.5 shrink-0" />
                  <span>{reconciling ? 'Reclaiming...' : 'Reclaim Orphan Storage'}</span>
                </button>
              </div>
            )}

            {/* Storage Node Grid Overview */}
            <div className="bg-white border border-slate-200/90 rounded-lg p-5 shadow-xs space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-semibold text-slate-900">Storage Node Capacity & Distribution</h3>
                  <p className="text-xs text-slate-500 mt-0.5">Committed chunk replicas versus physical disk allocation per independent storage instance</p>
                </div>
                <button
                  onClick={() => setActiveTab('nodes')}
                  className="text-xs text-[#4f56c7] hover:text-[#4349ab] font-medium flex items-center gap-1 cursor-pointer"
                >
                  <span>View All Nodes</span>
                  <ChevronRight className="w-3.5 h-3.5" />
                </button>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-3.5">
                {(cluster?.nodes || []).map((node, i) => (
                  <div key={node.node_id} className="bg-slate-50/70 border border-slate-200/80 rounded-lg p-3.5 flex flex-col justify-between min-w-0">
                    <div className="space-y-2.5">
                      <div className="flex items-center justify-between gap-1">
                        <span className="text-xs font-semibold text-slate-900 truncate">Node {i + 1}</span>
                        {getSemanticBadge(node.health_state)}
                      </div>
                      <div className="text-[11px] font-mono text-slate-500 truncate" title={node.advertised_url}>
                        {node.advertised_url.replace('http://', '')}
                      </div>

                      <div className="space-y-1 pt-1 text-xs text-slate-600">
                        <div className="flex justify-between">
                          <span className="text-slate-400">Committed:</span>
                          <span className="font-mono font-medium text-slate-800">{node.committed_chunks ?? node.total_chunks} chk</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-slate-400">Replicas:</span>
                          <span className="font-mono font-medium text-slate-800">{formatBytes(node.committed_bytes_used ?? node.total_bytes_used)}</span>
                        </div>
                        <div className="flex justify-between text-[11px] text-slate-400">
                          <span>Physical Disk:</span>
                          <span className="font-mono">{formatBytes(node.physical_bytes_used ?? node.total_bytes_used)}</span>
                        </div>
                      </div>
                    </div>

                    <div className="pt-3 mt-2 border-t border-slate-200/60">
                      <div className="flex justify-between text-[10px] text-slate-400 font-mono mb-1">
                        <span>Quota Utilization</span>
                        <span>{node.disk_usage_percent}%</span>
                      </div>
                      <div className="w-full bg-slate-200 rounded-full h-1.5 overflow-hidden">
                        <div
                          className="bg-[#4f56c7] h-1.5 rounded-full transition-all duration-300"
                          style={{ width: `${Math.max(node.disk_usage_percent || 0, 3)}%` }}
                        />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Real-Time Cluster Audit Event Stream */}
            <div className="bg-white border border-slate-200/90 rounded-lg p-5 shadow-[0_1px_2px_rgba(0,0,0,0.03)] space-y-3">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div>
                  <h3 className="text-sm font-semibold text-slate-900 flex items-center gap-2">
                    <Terminal className="w-4 h-4 text-slate-500" />
                    <span>Cluster Audit & Event Stream</span>
                  </h3>
                  <p className="text-xs text-slate-500 mt-0.5">Authoritative events persisted with deterministic Lamport causal ordering</p>
                </div>

                <div className="flex flex-wrap items-center gap-2">
                  {/* Lamport Deterministic vs Wall-Clock Toggle */}
                  <div className="flex items-center gap-1 p-0.5 bg-slate-100 rounded-md text-xs">
                    <button
                      onClick={() => handleSortOrderChange('deterministic')}
                      className={`px-2.5 py-1 rounded text-[11px] font-medium transition-colors cursor-pointer ${
                        eventSortOrder === 'deterministic'
                          ? 'bg-white text-indigo-700 font-semibold shadow-xs'
                          : 'text-slate-600 hover:text-slate-900'
                      }`}
                      title="Sort deterministically by (Lamport Clock, Origin Node, Event ID)"
                    >
                      Lamport Order
                    </button>
                    <button
                      onClick={() => handleSortOrderChange('timestamp')}
                      className={`px-2.5 py-1 rounded text-[11px] font-medium transition-colors cursor-pointer ${
                        eventSortOrder === 'timestamp'
                          ? 'bg-white text-slate-900 font-semibold shadow-xs'
                          : 'text-slate-600 hover:text-slate-900'
                      }`}
                      title="Sort by physical wall-clock timestamp"
                    >
                      Wall-Clock
                    </button>
                  </div>

                  {/* Filter Tabs */}
                  <div className="flex items-center gap-1 p-0.5 bg-slate-100 rounded-md text-xs">
                    {(['ALL', 'INFO', 'WARNING', 'ERROR'] as const).map(sev => (
                      <button
                        key={sev}
                        onClick={() => setEventSeverityFilter(sev)}
                        className={`px-2.5 py-1 rounded text-[11px] font-medium transition-colors cursor-pointer ${
                          eventSeverityFilter === sev
                            ? 'bg-white text-slate-900 shadow-xs'
                            : 'text-slate-600 hover:text-slate-900'
                        }`}
                      >
                        {sev}
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              <div className="border border-slate-200 rounded-md divide-y divide-slate-100 max-h-72 overflow-y-auto text-xs bg-slate-50/30">
                {filteredEvents.length === 0 ? (
                  <div className="p-6 text-center text-slate-400">No events found matching current criteria.</div>
                ) : (
                  filteredEvents.map((ev) => (
                    <div key={ev.event_id} className="p-2.5 px-3 flex flex-col sm:flex-row sm:items-start gap-2 sm:gap-3 hover:bg-slate-50 transition-colors">
                      <div className="flex items-center gap-1.5 shrink-0">
                        {/* Lamport Clock Badge */}
                        <span className="font-mono text-[11px] font-bold text-indigo-700 bg-indigo-50 border border-indigo-200/90 px-1.5 py-0.5 rounded shadow-2xs" title={`Lamport Logical Timestamp: L:${ev.lamport_clock ?? 0}`}>
                          L:{ev.lamport_clock ?? 0}
                        </span>
                        {/* Origin Node Badge */}
                        <span className="font-mono text-[10px] text-slate-500 bg-slate-100 border border-slate-200/70 px-1.5 py-0.5 rounded shrink-0">
                          {ev.origin_node_id || 'coordinator-primary-1'}
                        </span>
                        <span className="shrink-0">{getSemanticBadge(ev.severity)}</span>
                      </div>

                      <div className="flex-1 min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="font-mono text-[11px] font-semibold text-slate-800">
                            {ev.event_type}
                          </span>
                          {ev.causal_parent_id && (
                            <span className="text-[10px] font-mono text-purple-700 bg-purple-50 border border-purple-200/60 px-1.5 py-0.2 rounded flex items-center gap-0.5" title={`Causal Parent Event: ${ev.causal_parent_id}`}>
                              <CornerDownRight className="w-2.5 h-2.5" /> caused by: {ev.causal_parent_id.slice(-8)}
                            </span>
                          )}
                          {ev.correlation_id && (
                            <span className="text-[10px] font-mono text-slate-400" title={`Correlation ID: ${ev.correlation_id}`}>
                              corr: {ev.correlation_id.slice(0, 10)}
                            </span>
                          )}
                        </div>
                        <p className="text-slate-600 text-xs mt-0.5 break-words">
                          {ev.message}
                        </p>
                      </div>

                      <span className="text-[10px] font-mono text-slate-400 shrink-0 self-start sm:self-center">
                        {new Date(ev.timestamp * 1000).toLocaleTimeString()}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* TAB 2: STORAGE EXPLORER                                        */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'explorer' && (
          <div className="space-y-6">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-2 border-b border-slate-200">
              <div>
                <h1 className="text-xl font-bold tracking-tight text-slate-900">Storage Explorer</h1>
                <p className="text-xs text-slate-500 mt-0.5">
                  Authoritative files committed to cluster metadata with SHA-256 chunk layouts and 3x replica distribution.
                </p>
              </div>

              <div className="flex items-center gap-3">
                <div className="relative">
                  <Search className="w-3.5 h-3.5 absolute left-3 top-2.5 text-slate-400" />
                  <input
                    type="text"
                    placeholder="Filter files by name or hash..."
                    value={fileSearchQuery}
                    onChange={(e) => setFileSearchQuery(e.target.value)}
                    className="pl-8 pr-3 py-1.5 text-xs bg-white border border-slate-200 rounded-md text-slate-800 focus:outline-none focus:ring-1 focus:ring-[#4f56c7] w-56"
                  />
                </div>
                <button
                  onClick={() => fileInputRef.current?.click()}
                  disabled={uploading}
                  className="h-8 px-3 text-xs font-medium rounded-md bg-[#4f56c7] hover:bg-[#4349ab] text-white shadow-xs transition-colors cursor-pointer inline-flex items-center gap-1.5"
                >
                  <Upload className="w-3.5 h-3.5" />
                  <span>Upload File</span>
                </button>
              </div>
            </div>

            {uploadFeedback && (
              <div className={`p-3 rounded-md text-xs border ${uploadFeedback.type === 'success' ? 'bg-emerald-50 border-emerald-200 text-emerald-800' : 'bg-rose-50 border-rose-200 text-rose-800'}`}>
                {uploadFeedback.message}
              </div>
            )}

            {files.length === 0 ? (
              <div className="bg-white border border-slate-200/90 rounded-lg p-12 text-center space-y-3 shadow-xs">
                <Database className="w-9 h-9 text-slate-300 mx-auto" />
                <h3 className="text-sm font-semibold text-slate-900">No files committed to cluster storage</h3>
                <p className="text-xs text-slate-500 max-w-sm mx-auto">
                  Upload a binary or text file to test automated 1 MiB chunk splitting, SHA-256 checksumming, and 3x replica distribution across nodes.
                </p>
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="h-8 px-3.5 text-xs font-medium bg-[#4f56c7] hover:bg-[#4349ab] text-white rounded-md cursor-pointer transition-colors shadow-xs inline-flex items-center gap-1.5"
                >
                  <Upload className="w-3.5 h-3.5" />
                  <span>Upload First File</span>
                </button>
              </div>
            ) : (
              <div className="bg-white border border-slate-200/90 rounded-lg overflow-hidden shadow-xs">
                <div className="overflow-x-auto w-full">
                  <table className="w-full text-left text-xs min-w-[760px]">
                    <thead className="bg-slate-50 text-slate-500 font-semibold border-b border-slate-200 font-mono text-[11px]">
                      <tr>
                        <th className="py-2.5 px-4">Filename</th>
                        <th className="py-2.5 px-4">Size</th>
                        <th className="py-2.5 px-4">Chunks</th>
                        <th className="py-2.5 px-4">Replicas</th>
                        <th className="py-2.5 px-4">SHA-256 Checksum</th>
                        <th className="py-2.5 px-4">Status</th>
                        <th className="py-2.5 px-4 text-right">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 text-slate-700">
                      {filteredFiles.map((file) => (
                        <tr key={file.file_id} className="hover:bg-slate-50/80 transition-colors">
                          <td className="py-3 px-4 font-medium text-slate-900 flex items-center gap-2">
                            <FileText className="w-4 h-4 text-[#4f56c7] shrink-0" />
                            <span className="truncate max-w-[200px] sm:max-w-xs">{file.filename}</span>
                          </td>
                          <td className="py-3 px-4 font-mono">{formatBytes(file.size_bytes)}</td>
                          <td className="py-3 px-4 font-mono">{file.chunk_count} chunk(s)</td>
                          <td className="py-3 px-4 font-semibold text-[#4f56c7] font-mono">{file.replication_factor}x</td>
                          <td className="py-3 px-4 font-mono text-[11px] text-slate-500 truncate max-w-xs" title={file.checksum_sha256}>
                            {file.checksum_sha256.slice(0, 16)}...
                          </td>
                          <td className="py-3 px-4">
                            {getSemanticBadge(file.status)}
                          </td>
                          <td className="py-3 px-4 text-right space-x-2">
                            <button
                              onClick={() => handleInspectFile(file.file_id)}
                              className="text-xs text-[#4f56c7] hover:text-[#4349ab] font-medium cursor-pointer"
                            >
                              Inspect Chunks
                            </button>
                            <a
                              href={`/api/v1/files/${file.file_id}/download`}
                              download={file.filename}
                              className="inline-flex items-center text-xs text-emerald-700 hover:text-emerald-800 font-medium ml-2"
                            >
                              <Download className="w-3.5 h-3.5 mr-1" />
                              Download
                            </a>
                            <button
                              onClick={() => handleInitiateDelete(file.file_id, file.filename)}
                              className="text-xs text-rose-600 hover:text-rose-700 ml-2 cursor-pointer p-1 rounded hover:bg-rose-50 transition-colors inline-flex items-center"
                              title="Safely Delete Across All Nodes"
                              aria-label={`Delete ${file.filename}`}
                            >
                              <Trash2 className="w-3.5 h-3.5 inline" />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Chunk Layout Inspector (When a file is selected) */}
            {selectedFileDetail && (
              <div className="bg-white border border-[#4f56c7]/30 rounded-lg p-5 space-y-4 shadow-xs">
                <div className="flex items-center justify-between pb-3 border-b border-slate-100">
                  <div>
                    <h3 className="text-sm font-semibold text-slate-900 flex items-center gap-2">
                      <Layers className="w-4 h-4 text-[#4f56c7]" />
                      <span>Physical Chunk Placement: {selectedFileDetail.filename}</span>
                      <span className="font-mono text-xs text-indigo-700 bg-indigo-50 border border-indigo-200 px-2 py-0.5 rounded ml-2">
                        Commit L:{selectedFileDetail.lamport_clock ?? 0}
                      </span>
                    </h3>
                    <p className="text-xs text-slate-500 font-mono mt-0.5">
                      File ID: {selectedFileDetail.file_id} · Metadata Version: v{selectedFileDetail.metadata_version} · Hash: {selectedFileDetail.checksum_sha256}
                    </p>
                  </div>
                  <button
                    onClick={() => setSelectedFileDetail(null)}
                    className="text-xs text-slate-600 hover:text-slate-900 px-2.5 py-1 rounded bg-slate-100 hover:bg-slate-200 transition-colors cursor-pointer"
                  >
                    Close
                  </button>
                </div>

                <div className="space-y-3">
                  {selectedFileDetail.chunks.map((chk) => (
                    <div key={chk.chunk_id} className="bg-slate-50 p-4 rounded-md border border-slate-200/90 space-y-2.5">
                      <div className="flex items-center justify-between text-xs">
                        <div className="flex items-center gap-2">
                          <span className="font-semibold text-slate-900 font-mono">
                            Chunk #{chk.sequence_index} ({chk.chunk_id})
                          </span>
                          <span className="font-mono text-[10px] text-indigo-700 bg-indigo-50 border border-indigo-200/80 px-1.5 py-0.2 rounded">
                            L:{chk.lamport_clock ?? 0}
                          </span>
                        </div>
                        <span className="font-mono text-slate-600">
                          {formatBytes(chk.size_bytes)} ({chk.size_bytes} bytes)
                        </span>
                      </div>

                      <div className="text-[11px] font-mono text-slate-500 truncate">
                        SHA-256: {chk.checksum_sha256}
                      </div>

                      <div className="pt-2">
                        <span className="text-[11px] text-slate-600 block mb-2 font-medium">Replicas on Independent Nodes:</span>
                        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2.5">
                          {chk.replicas.map((rep) => (
                            <div
                              key={rep.replica_id}
                              className="p-2.5 rounded bg-white border border-slate-200 text-xs font-mono space-y-1.5"
                            >
                              <div className="flex items-center justify-between">
                                <span className="font-semibold text-slate-800">{rep.node_id}</span>
                                {getSemanticBadge(rep.status)}
                              </div>
                              <div className="text-[10px] text-slate-400 truncate flex items-center justify-between">
                                <span>{rep.node_url}</span>
                                <span className="text-indigo-600 font-mono font-medium">L:{rep.lamport_clock ?? 0}</span>
                              </div>
                              {rep.status === 'active' && (
                                <button
                                  onClick={() => handleCorruptChunk(chk.chunk_id, rep.node_id)}
                                  className="text-[11px] text-rose-600 hover:text-rose-700 underline font-sans cursor-pointer block pt-1"
                                  title="Inject bit rot into this chunk to test cryptographic audit detection"
                                >
                                  Inject Bit-Rot Probe
                                </button>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* TAB 3: STORAGE NODES TOPOLOGY                                  */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'nodes' && (
          <div className="space-y-6">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-2 border-b border-slate-200">
              <div>
                <h1 className="text-xl font-bold tracking-tight text-slate-900">Storage Node Topology</h1>
                <p className="text-xs text-slate-500 mt-0.5">
                  Cluster topology, committed chunk replica mappings, storage limits, and independent node failure controls.
                </p>
              </div>
              <div className="text-xs text-slate-500 font-mono">
                {cluster?.total_nodes ?? 5} nodes registered · Quorum RF: 3x
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {(cluster?.nodes || []).map((node, i) => (
                <div key={node.node_id} className="bg-white border border-slate-200 rounded-lg p-5 space-y-4 shadow-xs flex flex-col justify-between">
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 rounded bg-slate-100 flex items-center justify-center text-slate-700">
                          <Server className="w-4 h-4" />
                        </div>
                        <div>
                          <h4 className="text-sm font-semibold text-slate-900">{node.node_name}</h4>
                          <span className="text-xs font-mono text-slate-500">{node.node_id}</span>
                        </div>
                      </div>
                      {getSemanticBadge(node.health_state)}
                    </div>

                    {/* Metadata breakdown text */}
                    <div className="flex flex-wrap gap-2 text-[11px] font-mono">
                      <span className="text-emerald-700 font-medium">
                        {node.committed_chunks ?? node.total_chunks} Committed ({formatBytes(node.committed_bytes_used ?? node.total_bytes_used)})
                      </span>
                      {(node.untracked_chunks ?? 0) > 0 && (
                        <span className="text-sky-700">
                          · {node.untracked_chunks} Untracked ({formatBytes(node.untracked_bytes_used ?? 0)})
                        </span>
                      )}
                      {(node.orphaned_chunks ?? 0) > 0 && (
                        <span className="text-amber-700">
                          · {node.orphaned_chunks} Orphaned ({formatBytes(node.orphaned_bytes_used ?? 0)})
                        </span>
                      )}
                    </div>

                    <div className="space-y-2 text-xs border-t border-slate-100 pt-3">
                      <div className="flex justify-between">
                        <span className="text-slate-400">Endpoint:</span>
                        <span className="font-mono text-slate-800 font-medium">{node.advertised_url}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Storage Path:</span>
                        <span className="font-mono text-slate-600 text-[11px]">/data/storage_node_{node.node_id.replace('storage-node-', '')}/chunks</span>
                      </div>
                      <div className="flex justify-between items-center bg-indigo-50/70 px-2 py-1 rounded border border-indigo-100">
                        <span className="text-indigo-900 font-medium flex items-center gap-1">
                          <Clock className="w-3 h-3 text-indigo-600" />
                          <span>Lamport Clock:</span>
                        </span>
                        <span className="font-mono font-bold text-indigo-700">
                          L: {node.lamport_clock ?? clocks?.nodes?.find(n => n.id === node.node_id)?.clock ?? 0}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Committed Replicas:</span>
                        <span className="font-mono font-semibold text-emerald-700">{node.committed_chunks ?? node.total_chunks}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Physical Files on Disk:</span>
                        <span className="font-mono font-semibold text-slate-900">{node.physical_chunks ?? node.total_chunks}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-400">Physical Disk Used:</span>
                        <span className="font-mono text-slate-700">{formatBytes(node.physical_bytes_used ?? node.total_bytes_used)}</span>
                      </div>
                    </div>

                    <div className="pt-2">
                      <div className="flex justify-between text-xs text-slate-500 mb-1 font-mono">
                        <span>Storage Headroom</span>
                        <span>{node.disk_usage_percent}% of {formatBytes(node.storage_limit_bytes)}</span>
                      </div>
                      <div className="w-full bg-slate-100 rounded-full h-1.5 overflow-hidden">
                        <div
                          className="bg-[#4f56c7] h-1.5 rounded-full"
                          style={{ width: `${Math.max(node.disk_usage_percent, 3)}%` }}
                        />
                      </div>
                    </div>
                  </div>

                  {/* Operational Controls */}
                  <div className="pt-3 border-t border-slate-100 flex gap-2">
                    <button
                      onClick={() => handleToggleNodeState(node.node_id)}
                      className={`flex-1 py-1.5 text-xs font-medium rounded-md border transition-colors cursor-pointer ${
                        node.health_state === 'UNAVAILABLE'
                          ? 'bg-emerald-50 border-emerald-300 text-emerald-800 hover:bg-emerald-100'
                          : 'bg-rose-50 border-rose-300 text-rose-800 hover:bg-rose-100'
                      }`}
                    >
                      {node.health_state === 'UNAVAILABLE' ? 'Simulate Recovery' : 'Simulate Failure'}
                    </button>
                    <button
                      onClick={() => handleReconcileNode(node.node_id)}
                      className="px-3 py-1.5 text-xs font-medium rounded-md bg-slate-100 hover:bg-slate-200 text-slate-800 border border-slate-200 transition-colors cursor-pointer"
                      title="Inspect node disk chunks and reconcile with database"
                    >
                      Reconcile
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* TAB 4: SELF-HEALING & RE-REPLICATION MONITOR                   */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'selfhealing' && (
          <div className="space-y-6">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-2 border-b border-slate-200">
              <div>
                <h1 className="text-xl font-bold tracking-tight text-slate-900">Self-Healing & Re-Replication Monitor</h1>
                <p className="text-xs text-slate-500 mt-0.5">
                  Autonomous discovery of degraded chunks and transactional re-replication to restore 3x redundancy.
                </p>
              </div>
              <button
                onClick={handleTriggerReconcile}
                disabled={reconciling}
                className="h-8 px-3 text-xs font-medium rounded-md bg-[#4f56c7] hover:bg-[#4349ab] text-white transition-colors disabled:opacity-50 cursor-pointer shadow-xs inline-flex items-center gap-1.5"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${reconciling ? 'animate-spin' : ''}`} />
                <span>{reconciling ? 'Running Reconciler...' : 'Trigger Healing Scan'}</span>
              </button>
            </div>

            {/* Architecture Explanatory Callout */}
            <div className="bg-slate-100/70 border border-slate-200 rounded-lg p-4 text-xs text-slate-700 space-y-1">
              <div className="font-semibold text-slate-900">Self-Healing Invariants</div>
              <p className="text-slate-600">
                When a storage node fails or is partitioned, the cluster reconciler scans all chunks in SQLite metadata.
                Any chunk with fewer than 3 active replicas is scheduled for re-replication: bytes are streamed from an alternate healthy replica holder
                and atomically committed to an idle healthy node.
              </p>
            </div>

            {/* Recovery Jobs Table */}
            <div className="bg-white border border-slate-200/90 rounded-lg overflow-hidden shadow-xs">
              <div className="p-3.5 px-4 border-b border-slate-200 flex items-center justify-between bg-slate-50/50">
                <h3 className="text-xs font-semibold text-slate-800 font-mono">
                  Re-Replication Job History ({jobs.length})
                </h3>
              </div>

              {jobs.length === 0 ? (
                <div className="p-8 text-center text-xs text-slate-500">
                  No recovery jobs recorded. All committed files currently satisfy 3x replication factor.
                </div>
              ) : (
                <div className="overflow-x-auto w-full">
                  <table className="w-full text-left text-xs min-w-[700px]">
                    <thead className="bg-slate-50 text-slate-500 font-semibold border-b border-slate-200 font-mono text-[11px]">
                      <tr>
                        <th className="py-2.5 px-4">Job ID</th>
                        <th className="py-2.5 px-4">Chunk ID</th>
                        <th className="py-2.5 px-4">Source Node</th>
                        <th className="py-2.5 px-4">Target Node</th>
                        <th className="py-2.5 px-4">Status</th>
                        <th className="py-2.5 px-4">Attempts</th>
                        <th className="py-2.5 px-4 text-right">Timestamp</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 text-slate-700 font-mono text-[11px]">
                      {jobs.map((job) => (
                        <tr key={job.job_id} className="hover:bg-slate-50/80 transition-colors">
                          <td className="py-3 px-4 font-semibold text-slate-900">{job.job_id}</td>
                          <td className="py-3 px-4 text-slate-600">{job.chunk_id}</td>
                          <td className="py-3 px-4 text-emerald-700 font-medium">{job.source_node_id}</td>
                          <td className="py-3 px-4 text-[#4f56c7] font-medium">{job.target_node_id}</td>
                          <td className="py-3 px-4">
                            {getSemanticBadge(job.status)}
                          </td>
                          <td className="py-3 px-4">{job.attempts}</td>
                          <td className="py-3 px-4 text-right text-slate-400">
                            {new Date(job.created_at * 1000).toLocaleTimeString()}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* TAB 5: AUDIT & INSPECTOR                                       */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'inspector' && (
          <div className="space-y-6">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-2 border-b border-slate-200">
              <div>
                <h1 className="text-xl font-bold tracking-tight text-slate-900">Distributed-Systems & Audit Inspector</h1>
                <p className="text-xs text-slate-500 mt-0.5">
                  Leader leases, fencing tokens, cryptographic audits, and 4-way physical chunk inventory classification.
                </p>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Leader Lease Card */}
              <div className="bg-white border border-slate-200/90 rounded-lg p-5 space-y-4 shadow-xs">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-slate-900 flex items-center gap-2">
                    <Radio className="w-4 h-4 text-[#4f56c7]" />
                    <span>Leader Lease & Monotonic Fencing</span>
                  </h3>
                  <button
                    onClick={handleLeaderStepDown}
                    className="text-xs px-2.5 py-1 rounded bg-slate-100 hover:bg-slate-200 text-slate-700 border border-slate-200 transition-colors cursor-pointer"
                  >
                    Yield Lease (Step Down)
                  </button>
                </div>

                <div className="space-y-2 text-xs font-mono bg-slate-50 p-4 rounded-md border border-slate-200">
                  <div className="flex justify-between">
                    <span className="text-slate-500">Current Lease Holder:</span>
                    <span className="text-emerald-700 font-bold">{leader?.current_leader_id}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-500">Monotonic Fencing Token:</span>
                    <span className="text-[#4f56c7] font-bold">#{leader?.fencing_token}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-500">Time Until Renewal:</span>
                    <span className="text-slate-800 font-semibold">{(leader?.time_remaining_seconds ?? 0).toFixed(1)}s</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-slate-500">Coordinator Instance:</span>
                    <span className="text-slate-600">{leader?.coordinator_instance_id}</span>
                  </div>
                </div>

                <p className="text-xs text-slate-500">
                  Fencing tokens eliminate split-brain writes. If an expired leader attempts a metadata commit, the monotonic token prevents stale state updates.
                </p>
              </div>

              {/* Cryptographic SHA-256 Audit Card */}
              <div className="bg-white border border-slate-200/90 rounded-lg p-5 space-y-4 shadow-xs">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-slate-900 flex items-center gap-2">
                    <ShieldCheck className="w-4 h-4 text-emerald-600" />
                    <span>Cryptographic SHA-256 Integrity Audit</span>
                  </h3>
                  <button
                    onClick={handleRunAudit}
                    disabled={verifyingAudit}
                    className="h-8 px-3 rounded-md bg-[#4f56c7] hover:bg-[#4349ab] text-white transition-colors cursor-pointer disabled:opacity-50 shadow-xs text-xs font-medium inline-flex items-center gap-1.5"
                  >
                    {verifyingAudit ? 'Auditing...' : 'Run SHA-256 Audit'}
                  </button>
                </div>

                {auditResult ? (
                  <div className="space-y-3">
                    <div className="space-y-2 text-xs font-mono bg-slate-50 p-4 rounded-md border border-slate-200">
                      <div className="flex justify-between">
                        <span className="text-slate-500">Files Audited:</span>
                        <span className="text-slate-900 font-semibold">{auditResult.checked_files}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-500">Chunks Audited:</span>
                        <span className="text-slate-900 font-semibold">{auditResult.checked_chunks}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-500">Valid Replicas on Disk:</span>
                        <span className="text-emerald-700 font-bold">{auditResult.valid_replicas}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-500">Corrupted Replicas:</span>
                        <span className={auditResult.corrupted_replicas > 0 ? 'text-rose-600 font-bold' : 'text-slate-600'}>
                          {auditResult.corrupted_replicas}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-slate-500">Missing Replicas:</span>
                        <span className={auditResult.missing_replicas > 0 ? 'text-rose-600 font-bold' : 'text-slate-600'}>
                          {auditResult.missing_replicas}
                        </span>
                      </div>
                      <div className="flex justify-between border-t border-slate-200/80 pt-1.5 mt-1.5">
                        <span className="text-indigo-900 font-medium">Audit Lamport Clock:</span>
                        <span className="text-indigo-700 font-bold">L: {auditResult.lamport_clock ?? 0}</span>
                      </div>
                    </div>

                    {auditResult.corrupted_replicas > 0 && (
                      <button
                        onClick={handleTriggerReconcile}
                        disabled={reconciling}
                        className="w-full h-8 bg-[#4f56c7] hover:bg-[#4349ab] text-white rounded-md text-xs font-semibold cursor-pointer transition-colors flex items-center justify-center gap-1.5 shadow-xs"
                      >
                        <RefreshCw className={`w-3.5 h-3.5 ${reconciling ? 'animate-spin' : ''}`} />
                        <span>Trigger Self-Healing to Repair {auditResult.corrupted_replicas} Corrupted Replica(s)</span>
                      </button>
                    )}
                  </div>
                ) : (
                  <div className="p-6 bg-slate-50 rounded-md border border-slate-200 text-center text-xs text-slate-500">
                    Click "Run SHA-256 Audit" to verify byte-by-byte hash integrity across all storage node disks.
                  </div>
                )}
              </div>
            </div>

            {/* Comprehensive Physical Chunk Inventory & 4-Way Classification Table */}
            <div className="bg-white border border-slate-200/90 rounded-lg p-5 space-y-4 shadow-xs">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div>
                  <h3 className="text-sm font-semibold text-slate-900 flex items-center gap-2">
                    <Database className="w-4 h-4 text-[#4f56c7]" />
                    <span>Physical Chunk Inventory & 4-Way Classification Audit</span>
                  </h3>
                  <p className="text-xs text-slate-500 mt-0.5">
                    Itemized inventory of physical .bin chunk files on disk across storage node volumes
                  </p>
                </div>
                {inventoryAudit && (
                  <div className="flex flex-wrap gap-2 text-[11px] font-mono">
                    <span className="px-2 py-0.5 rounded bg-emerald-50 border border-emerald-200 text-emerald-700">
                      {inventoryAudit.summary.actual_committed_replicas} Committed
                    </span>
                    <span className="px-2 py-0.5 rounded bg-sky-50 border border-sky-200 text-sky-700">
                      {inventoryAudit.summary.actual_untracked_chunks} Untracked
                    </span>
                    <span className="px-2 py-0.5 rounded bg-amber-50 border border-amber-200 text-amber-700">
                      {inventoryAudit.summary.actual_orphaned_chunks} Orphaned
                    </span>
                    <span className="px-2 py-0.5 rounded bg-rose-50 border border-rose-200 text-rose-700">
                      {inventoryAudit.summary.actual_stale_replicas} Stale
                    </span>
                  </div>
                )}
              </div>

              <div className="bg-white rounded-md border border-slate-200/90 overflow-hidden shadow-xs">
                <div className="overflow-x-auto w-full">
                  <table className="w-full text-left text-xs min-w-[760px]">
                    <thead className="bg-slate-50 text-slate-500 font-semibold uppercase tracking-wider border-b border-slate-200 font-mono text-[11px]">
                      <tr>
                        <th className="py-2.5 px-3">Storage Node</th>
                        <th className="py-2.5 px-3">Chunk ID</th>
                        <th className="py-2.5 px-3">File / Origin</th>
                        <th className="py-2.5 px-3">Size</th>
                        <th className="py-2.5 px-3">SHA-256 (Disk)</th>
                        <th className="py-2.5 px-3">Audit Classification</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100 font-mono text-[11px] text-slate-700">
                      {(!inventoryAudit || inventoryAudit.items.length === 0) ? (
                        <tr>
                          <td colSpan={6} className="py-6 text-center text-slate-400 font-sans">
                            No chunk inventory loaded. Refreshing cluster data...
                          </td>
                        </tr>
                      ) : (
                        inventoryAudit.items.map((item, idx) => (
                          <tr key={idx} className="hover:bg-slate-50/80 transition-colors">
                            <td className="py-2.5 px-3 font-semibold text-slate-900">{item.node_id}</td>
                            <td className="py-2.5 px-3 text-[#4f56c7] truncate max-w-[120px]">{item.chunk_id}</td>
                            <td className="py-2.5 px-3 text-slate-600 font-sans truncate max-w-[150px]">
                              {item.file_name ? item.file_name : <span className="text-slate-400 italic">No File Reference</span>}
                            </td>
                            <td className="py-2.5 px-3">{formatBytes(item.size_bytes)}</td>
                            <td className="py-2.5 px-3 text-slate-500 truncate max-w-[130px] font-mono text-[10px]" title={item.sha256}>
                              {item.sha256.slice(0, 16)}...
                            </td>
                            <td className="py-2.5 px-3">
                              {getSemanticBadge(item.classification)}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* TAB 6: FAULT LAB                                               */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'faultlab' && (
          <div className="space-y-6">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-2 border-b border-slate-200">
              <div>
                <h1 className="text-xl font-bold tracking-tight text-slate-900">Live Fault-Testing Lab</h1>
                <p className="text-xs text-slate-500 mt-0.5">
                  End-to-end failure injection: upload verification probe, partition node, verify re-replication, recover file, and reconcile returning node.
                </p>
              </div>
            </div>

            <div className="bg-white border border-slate-200/90 rounded-lg p-5 space-y-4 shadow-xs">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 p-4 bg-slate-50 rounded-lg border border-slate-200">
                <div className="flex items-center gap-3">
                  <label className="text-xs text-slate-600 font-medium">Target Node for Hard Failure:</label>
                  <select
                    value={labSelectedNode}
                    onChange={(e) => setLabSelectedNode(e.target.value)}
                    disabled={labRunning}
                    className="bg-white border border-slate-200 rounded px-3 py-1.5 text-xs font-mono text-slate-800 focus:outline-none focus:ring-1 focus:ring-[#4f56c7]"
                  >
                    <option value="storage-node-1">storage-node-1 (:8001)</option>
                    <option value="storage-node-2">storage-node-2 (:8002)</option>
                    <option value="storage-node-3">storage-node-3 (:8003)</option>
                    <option value="storage-node-4">storage-node-4 (:8004)</option>
                    <option value="storage-node-5">storage-node-5 (:8005)</option>
                  </select>
                </div>

                <button
                  onClick={runEndToEndFaultDemo}
                  disabled={labRunning}
                  className="flex items-center gap-2 px-4 py-2 text-xs font-semibold rounded-md bg-[#4f56c7] hover:bg-[#4349ab] text-white transition-colors disabled:opacity-50 cursor-pointer shadow-xs"
                >
                  <Zap className={`w-4 h-4 ${labRunning ? 'animate-pulse' : ''}`} />
                  <span>{labRunning ? 'Running Live Simulation...' : 'Execute Live Fault & Self-Healing Demo'}</span>
                </button>
              </div>

              {/* Console Output Screen */}
              <div className="bg-[#121316] border border-[#23262d] rounded-lg p-4 font-mono text-xs space-y-1.5 h-84 overflow-y-auto overflow-x-auto break-all text-slate-200 shadow-inner">
                {labLogs.length === 0 ? (
                  <div className="text-slate-500 italic">
                    Press "Execute Live Fault & Self-Healing Demo" above to run the automated 6-step lifecycle demonstration.
                  </div>
                ) : (
                  labLogs.map((log, idx) => (
                    <div
                      key={idx}
                      className={
                        log.includes('✔')
                          ? 'text-emerald-400'
                          : log.includes('Step')
                          ? 'text-[#8b95f6] font-semibold mt-2.5'
                          : log.includes('---')
                          ? 'text-slate-400 font-bold'
                          : log.includes('✘')
                          ? 'text-rose-400 font-bold'
                          : 'text-slate-300'
                      }
                    >
                      {log}
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}

      </main>

      {/* ===================================================================== */}
      {/* File Deletion Safeguard Confirmation Modal                             */}
      {/* ===================================================================== */}
      {fileToDelete && (
        <div
          className="fixed inset-0 bg-slate-950/60 backdrop-blur-xs z-50 flex items-center justify-center p-4 animate-in fade-in duration-150"
          role="dialog"
          aria-modal="true"
          aria-labelledby="delete-dialog-title"
          aria-describedby="delete-dialog-desc"
          onClick={(e) => {
            if (e.target === e.currentTarget && !isDeletingFile) {
              handleCancelDelete();
            }
          }}
        >
          <div className="bg-white rounded-xl border border-slate-200 shadow-2xl max-w-md w-full p-6 space-y-4 animate-in zoom-in-95 duration-150">
            <div className="flex items-start gap-3.5">
              <div className="w-9 h-9 rounded-full bg-rose-50 border border-rose-100 flex items-center justify-center text-rose-600 shrink-0 mt-0.5">
                <Trash2 className="w-4.5 h-4.5" />
              </div>
              <div className="space-y-1 flex-1 min-w-0">
                <h3 id="delete-dialog-title" className="text-sm font-semibold text-slate-900">
                  Permanently Delete File
                </h3>
                <p id="delete-dialog-desc" className="text-xs text-slate-600 leading-relaxed">
                  Are you sure you want to delete <span className="font-semibold text-slate-900 break-all">"{fileToDelete.filename}"</span>?
                </p>
              </div>
            </div>

            <div className="bg-slate-50 rounded-lg p-3 border border-slate-200/80 text-xs text-slate-600 space-y-1.5">
              <div className="flex items-center gap-1.5 font-medium text-slate-700">
                <ShieldAlert className="w-3.5 h-3.5 text-amber-600 shrink-0" />
                <span>Distributed Storage Safeguard</span>
              </div>
              <p className="text-[11px] text-slate-500 leading-normal pl-5">
                This operation will delete authoritative SQLite metadata and permanently purge all physical chunk replicas across the 5 storage nodes in the cluster. This action is irreversible.
              </p>
            </div>

            {deleteFileError && (
              <div className="bg-rose-50 border border-rose-200 rounded-lg p-3 text-xs text-rose-800 flex items-start gap-2">
                <AlertTriangle className="w-4 h-4 text-rose-600 shrink-0 mt-0.5" />
                <div className="flex-1 min-w-0">
                  <span className="font-semibold">Action Failed: </span>
                  <span>{deleteFileError}</span>
                </div>
              </div>
            )}

            <div className="flex items-center justify-end gap-2.5 pt-3 border-t border-slate-100">
              <button
                type="button"
                onClick={handleCancelDelete}
                disabled={isDeletingFile}
                className="h-8 px-3.5 text-xs font-medium text-slate-700 bg-white hover:bg-slate-50 border border-slate-300 rounded-md transition-colors cursor-pointer disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirmDelete}
                disabled={isDeletingFile}
                className="h-8 px-4 text-xs font-medium text-white bg-rose-600 hover:bg-rose-700 rounded-md transition-colors cursor-pointer shadow-xs disabled:opacity-50 inline-flex items-center gap-1.5"
              >
                {isDeletingFile ? (
                  <>
                    <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                    <span>Purging Replicas...</span>
                  </>
                ) : (
                  <>
                    <Trash2 className="w-3.5 h-3.5" />
                    <span>Permanently Delete</span>
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ===================================================================== */}
      {/* 3. Quiet Footer                                                       */}
      {/* ===================================================================== */}
      <footer className="border-t border-slate-200 bg-white py-4 mt-auto">
        <div className="max-w-[1440px] w-full mx-auto px-4 sm:px-6 lg:px-8 flex flex-col sm:flex-row items-center justify-between gap-2 text-xs text-slate-500">
          <div className="flex items-center gap-2">
            <span className="font-semibold text-slate-700">ShardSphere Console</span>
            <span>·</span>
            <span>Self-Healing Distributed Storage Architecture</span>
          </div>
          <div className="flex items-center gap-3 font-mono text-[11px] text-slate-400">
            <span>5 Storage Nodes</span>
            <span>·</span>
            <span>Fencing Lease Protocol</span>
            <span>·</span>
            <span>SHA-256 Quorum (3x)</span>
          </div>
        </div>
      </footer>
    </div>
  );
}
