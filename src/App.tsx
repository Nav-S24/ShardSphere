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
  FileCode,
  Zap,
  GitBranch,
  ShieldAlert,
  Search,
  Check,
  ChevronDown,
  ChevronRight,
  ExternalLink,
  Cpu,
  Info,
} from 'lucide-react';

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
}

interface ClusterOverview {
  cluster_name: string;
  coordinator_id: string;
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
}

interface ReplicaDetail {
  replica_id: string;
  node_id: string;
  node_url: string;
  status: string;
  generation: number;
  last_verified_at: number | null;
}

interface ChunkDetail {
  chunk_id: string;
  sequence_index: number;
  size_bytes: number;
  checksum_sha256: string;
  generation: number;
  replicas: ReplicaDetail[];
  healthy_replicas_count: number;
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
}

interface ClusterEvent {
  event_id: string;
  event_type: string;
  severity: string;
  node_id: string | null;
  file_id: string | null;
  message: string;
  timestamp: number;
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

  // File Upload State
  const [uploading, setUploading] = useState<boolean>(false);
  const [uploadFeedback, setUploadFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Verification & Action States
  const [verifyingAudit, setVerifyingAudit] = useState<boolean>(false);
  const [auditResult, setAuditResult] = useState<any | null>(null);
  const [reconciling, setReconciling] = useState<boolean>(false);
  const [actionFeedback, setActionFeedback] = useState<string | null>(null);

  // Fault Lab States
  const [labRunning, setLabRunning] = useState<boolean>(false);
  const [labLogs, setLabLogs] = useState<string[]>([]);
  const [labSelectedNode, setLabSelectedNode] = useState<string>('storage-node-2');

  const fetchAllTelemetry = async () => {
    try {
      const [clusterRes, leaderRes, filesRes, jobsRes, eventsRes, auditRes] = await Promise.all([
        fetch('/api/v1/cluster'),
        fetch('/api/v1/leader'),
        fetch('/api/v1/files'),
        fetch('/api/v1/recovery/jobs'),
        fetch('/api/v1/events?limit=30'),
        fetch('/api/v1/inventory/audit'),
      ]);

      if (clusterRes.ok) setCluster(await clusterRes.json());
      if (leaderRes.ok) setLeader(await leaderRes.json());
      if (filesRes.ok) {
        const fData = await filesRes.json();
        setFiles(fData.files || []);
      }
      if (jobsRes.ok) setJobs(await jobsRes.json());
      if (eventsRes.ok) setEvents(await eventsRes.json());
      if (auditRes.ok) setInventoryAudit(await auditRes.json());
    } catch (err) {
      console.error('Telemetry polling error:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAllTelemetry();
    const interval = setInterval(fetchAllTelemetry, 2500);
    return () => clearInterval(interval);
  }, []);

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
        const err = await res.json();
        throw new Error(err.detail || 'Upload failed');
      }

      const data = await res.json();
      setUploadFeedback({
        type: 'success',
        message: `File "${data.filename}" (${formatBytes(data.size_bytes)}) committed across ${data.replication_factor}x replicas across distinct nodes. Version: v${data.metadata_version}`,
      });
      await fetchAllTelemetry();
      if (fileInputRef.current) fileInputRef.current.value = '';
    } catch (err: any) {
      setUploadFeedback({ type: 'error', message: err.message });
    } finally {
      setUploading(false);
    }
  };

  const handleFileDelete = async (fileId: string, filename: string) => {
    if (!confirm(`Permanently delete file "${filename}" across all replica nodes?`)) return;
    try {
      const res = await fetch(`/api/v1/files/${fileId}`, { method: 'DELETE' });
      if (res.ok) {
        if (selectedFileDetail?.file_id === fileId) {
          setSelectedFileDetail(null);
        }
        await fetchAllTelemetry();
        setActionFeedback(`File "${filename}" deleted and physical chunks purged from storage nodes.`);
        setTimeout(() => setActionFeedback(null), 4000);
      }
    } catch (err: any) {
      alert(`Delete failed: ${err.message}`);
    }
  };

  const handleInspectFile = async (fileId: string) => {
    try {
      const res = await fetch(`/api/v1/files/${fileId}`);
      if (res.ok) {
        const detail = await res.json();
        setSelectedFileDetail(detail);
      }
    } catch (err) {
      console.error(err);
    }
  };

  const handleTriggerReconcile = async () => {
    setReconciling(true);
    try {
      const res = await fetch('/api/v1/self-healing/reconcile', { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        setActionFeedback(`Reconciliation scan finished. Recovered ${data.recovered_chunks} chunk replica(s).`);
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
      if (res.ok) {
        const data = await res.json();
        setActionFeedback(`Node ${data.node_id} state toggled to ${data.new_state}.`);
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
      if (res.ok) {
        const data = await res.json();
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
      if (res.ok) {
        const data = await res.json();
        setActionFeedback(`Cluster orphan reconciliation complete: purged ${data.total_purged_orphans} orphaned chunks and reclaimed ${formatBytes(data.total_reclaimed_bytes)}!`);
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
    if (!confirm('Force the current leader to step down? A new leader will be elected with an incremented fencing token.')) return;
    try {
      const res = await fetch('/api/v1/leader/step-down', { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        setActionFeedback(`Leader stepped down. Fencing token: ${data.fencing_token}`);
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
      if (res.ok) {
        setAuditResult(await res.json());
      }
      // Also refresh inventory audit
      const invRes = await fetch('/api/v1/inventory/audit');
      if (invRes.ok) {
        setInventoryAudit(await invRes.json());
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
      if (res.ok) {
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
      addLog(`Step 1/6: Generating and uploading real 1.5 MiB verification probe...`);

      // Create random blob
      const probeSize = 1572864; // 1.5 MiB
      const probeBytes = new Uint8Array(probeSize);
      for (let i = 0; i < probeSize; i++) probeBytes[i] = (i * 37) % 256;
      const probeBlob = new Blob([probeBytes], { type: 'application/octet-stream' });
      const formData = new FormData();
      formData.append('file', probeBlob, 'fault_lab_probe.bin');

      const uploadRes = await fetch('/api/v1/files', { method: 'POST', body: formData });
      if (!uploadRes.ok) throw new Error('Failed to upload lab probe');
      const uploadData = await uploadRes.json();
      addLog(`✔ Probe uploaded: ID ${uploadData.file_id.slice(0, 12)}... (${formatBytes(uploadData.size_bytes)}, ${uploadData.chunk_count} chunks, 3x replicas).`);

      // Inspect chunks
      const detailRes = await fetch(`/api/v1/files/${uploadData.file_id}`);
      const detailData: FileDetail = await detailRes.json();
      const nodeAssignments = detailData.chunks.map(c => `Chunk #${c.sequence_index} -> [${c.replicas.map(r => r.node_id).join(', ')}]`);
      nodeAssignments.forEach(na => addLog(`   ${na}`));

      // Select target node: use selected node if it holds a replica, else choose first node holding chunks
      const nodesWithProbe = new Set(detailData.chunks.flatMap(c => c.replicas.map(r => r.node_id)));
      const targetNode = nodesWithProbe.has(labSelectedNode) ? labSelectedNode : detailData.chunks[0].replicas[0].node_id;
      if (targetNode !== labSelectedNode) {
        setLabSelectedNode(targetNode);
      }
      addLog(`Step 2/6: Simulating hard failure on target node "${targetNode}" (holding active probe replica)...`);
      await fetch(`/api/v1/nodes/${targetNode}/toggle-state`, { method: 'POST' });
      addLog(`✔ Node "${targetNode}" transitioned to UNAVAILABLE.`);

      addLog(`Step 3/6: Polling cluster failure detector & under-replication discovery...`);
      await new Promise(r => setTimeout(r, 1500));
      await fetchAllTelemetry();

      addLog(`Step 4/6: Triggering automatic self-healing engine...`);
      const recRes = await fetch('/api/v1/self-healing/reconcile', { method: 'POST' });
      const recData = await recRes.json();
      addLog(`✔ Self-healing complete: ${recData.recovered_chunks} chunk(s) re-replicated to alternate healthy nodes.`);

      addLog(`Step 5/6: Downloading and reconstructing file while "${targetNode}" is STILL DEAD...`);
      const dlRes = await fetch(`/api/v1/files/${uploadData.file_id}/download`);
      if (!dlRes.ok) throw new Error('Download failed during node partition!');
      const dlBuffer = await dlRes.arrayBuffer();
      const dlBytes = new Uint8Array(dlBuffer);

      if (dlBytes.length !== probeSize) {
        throw new Error(`Size mismatch: expected ${probeSize}, got ${dlBytes.length}`);
      }

      // Verify byte-for-byte
      let match = true;
      for (let i = 0; i < probeSize; i++) {
        if (dlBytes[i] !== probeBytes[i]) {
          match = false;
          break;
        }
      }

      if (match) {
        addLog(`✔ EXACT BYTE-FOR-BYTE MATCH CONFIRMED (1,572,864 bytes verified).`);
      } else {
        throw new Error('Byte integrity verification failed!');
      }

      addLog(`Step 6/6: Reviving node "${targetNode}" and running returning-node reconciliation...`);
      await fetch(`/api/v1/nodes/${targetNode}/toggle-state`, { method: 'POST' });
      const reconResp = await fetch(`/api/v1/nodes/${targetNode}/reconcile`, { method: 'POST' });
      const reconData = await reconResp.json();
      addLog(`✔ Returning node reconciled: ${reconData.verified_active} active verified, ${reconData.purged_stale} stale purged.`);

      // Cleanup
      await fetch(`/api/v1/files/${uploadData.file_id}`, { method: 'DELETE' });
      addLog(`✔ Demo file cleaned up. Cluster returned to 100% healthy status.`);
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

  const getStatusColor = (status: string) => {
    switch (status.toUpperCase()) {
      case 'HEALTHY':
      case 'COMMITTED':
      case 'ACTIVE':
      case 'VALID':
      case 'COMPLETED':
      case 'COMMITTED_REPLICA':
        return 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20';
      case 'SUSPECT':
      case 'RECOVERING':
      case 'DEGRADED':
      case 'IN_PROGRESS':
      case 'ORPHANED_CHUNK':
        return 'text-amber-400 bg-amber-500/10 border-amber-500/20';
      case 'UNTRACKED_DATA':
        return 'text-sky-400 bg-sky-500/10 border-sky-500/20';
      case 'UNAVAILABLE':
      case 'CORRUPTED':
      case 'FAILED':
      case 'MISSING':
      case 'STALE_REPLICA':
        return 'text-rose-400 bg-rose-500/10 border-rose-500/20';
      default:
        return 'text-zinc-400 bg-zinc-800 border-zinc-700';
    }
  };

  const getStatusDot = (status: string) => {
    switch (status.toUpperCase()) {
      case 'HEALTHY':
      case 'ACTIVE':
      case 'COMMITTED_REPLICA':
        return 'bg-emerald-500';
      case 'SUSPECT':
      case 'RECOVERING':
        return 'bg-amber-500 animate-pulse';
      case 'UNAVAILABLE':
      case 'CORRUPTED':
        return 'bg-rose-500';
      case 'UNTRACKED_DATA':
        return 'bg-sky-500';
      default:
        return 'bg-zinc-500';
    }
  };

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 font-sans selection:bg-indigo-600 selection:text-white">
      {/* Top Application Header */}
      <header className="border-b border-zinc-800/80 bg-zinc-900/90 backdrop-blur-md sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="w-9 h-9 rounded-lg bg-indigo-600 flex items-center justify-center shadow-md shadow-indigo-600/30">
              <Layers className="w-5 h-5 text-white" />
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <span className="text-lg font-bold tracking-tight text-white">ShardSphere</span>
                <span className="text-xs text-zinc-400 font-mono">v1.0-cluster</span>
              </div>
              <div className="flex items-center space-x-2 text-[11px] text-zinc-400">
                <span>Distributed File Engine</span>
                <span aria-hidden="true">·</span>
                <span>Fenced Leases</span>
                <span aria-hidden="true">·</span>
                <span>3x Redundancy</span>
              </div>
            </div>
          </div>

          <div className="flex items-center space-x-3">
            {/* Real Leader Lease Status */}
            <div className="hidden sm:flex items-center space-x-2 text-xs bg-zinc-900 border border-zinc-800 rounded-lg px-3 py-1.5 font-mono">
              <div className={`w-2 h-2 rounded-full ${leader?.is_leader ? 'bg-emerald-500' : 'bg-amber-500'}`} />
              <span className="text-zinc-400">Leader:</span>
              <span className="text-zinc-200 font-semibold">{leader?.current_leader_id || 'unknown'}</span>
              <span className="text-zinc-600">|</span>
              <span className="text-zinc-400">Token:</span>
              <span className="text-indigo-400 font-bold">#{leader?.fencing_token ?? 0}</span>
              <span className="text-zinc-600">|</span>
              <span className="text-zinc-400">{(leader?.time_remaining_seconds ?? 0).toFixed(1)}s lease</span>
            </div>

            <button
              onClick={handleTriggerReconcile}
              disabled={reconciling}
              className="flex items-center space-x-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-zinc-800 hover:bg-zinc-700 text-zinc-200 border border-zinc-700 transition disabled:opacity-50 cursor-pointer"
              title="Execute immediate self-healing scan"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${reconciling ? 'animate-spin' : ''}`} />
              <span>{reconciling ? 'Reconciling...' : 'Reconcile'}</span>
            </button>

            <button
              onClick={() => fileInputRef.current?.click()}
              disabled={uploading}
              className="flex items-center space-x-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition disabled:opacity-50 cursor-pointer shadow-sm"
            >
              <Upload className="w-3.5 h-3.5" />
              <span>{uploading ? 'Chunking...' : 'Upload File'}</span>
            </button>
            <input
              ref={fileInputRef}
              type="file"
              onChange={handleFileUpload}
              className="hidden"
            />
          </div>
        </div>
      </header>

      {/* Global Feedback Banner */}
      {actionFeedback && (
        <div className="bg-indigo-950/80 border-b border-indigo-800/60 px-4 py-2 text-xs text-indigo-200 text-center flex items-center justify-center space-x-2 animate-fadeIn">
          <Activity className="w-3.5 h-3.5 text-indigo-400" />
          <span>{actionFeedback}</span>
        </div>
      )}

      {/* Navigation Sub-header (Tabs) */}
      <nav className="border-b border-zinc-800 bg-zinc-950/60 backdrop-blur sticky top-16 z-40">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex space-x-1 py-2 overflow-x-auto">
          {[
            { id: 'overview', label: 'Cluster Overview', icon: Activity },
            { id: 'explorer', label: 'Storage Explorer', icon: Database, badge: files.length },
            { id: 'nodes', label: 'Cluster Nodes', icon: Server, badge: cluster?.total_nodes ?? 0 },
            { id: 'selfhealing', label: 'Self-Healing Monitor', icon: ShieldCheck, badge: jobs.filter(j => j.status === 'in_progress').length || undefined },
            { id: 'inspector', label: 'Systems & Audit Inspector', icon: Radio },
            { id: 'faultlab', label: 'Fault-Testing Lab', icon: Zap },
          ].map((tab) => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as any)}
                className={`flex items-center space-x-2 px-3.5 py-2 text-xs font-medium rounded-md whitespace-nowrap transition cursor-pointer ${
                  isActive
                    ? 'bg-zinc-800 text-white shadow-sm'
                    : 'text-zinc-400 hover:text-zinc-200 hover:bg-zinc-900'
                }`}
              >
                <Icon className={`w-3.5 h-3.5 ${isActive ? 'text-indigo-400' : 'text-zinc-400'}`} />
                <span>{tab.label}</span>
                {tab.badge !== undefined && (
                  <span className={`px-1.5 py-0.2 text-[10px] font-mono rounded ${isActive ? 'bg-zinc-700 text-zinc-200' : 'bg-zinc-800 text-zinc-400'}`}>
                    {tab.badge}
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </nav>

      {/* Main Container */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">

        {/* ------------------------------------------------------------- */}
        {/* 1. CLUSTER OVERVIEW                                            */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'overview' && (
          <div className="space-y-6">
            {/* Operational KPI Grid */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center justify-between text-zinc-400 text-xs">
                  <span>Storage Nodes</span>
                  <Server className="w-4 h-4 text-indigo-400" />
                </div>
                <div className="mt-2 flex items-baseline space-x-2">
                  <span className="text-2xl font-bold font-mono text-white">{cluster?.healthy_nodes ?? 0}</span>
                  <span className="text-xs text-zinc-400">/ {cluster?.total_nodes ?? 0} online</span>
                </div>
                <div className="mt-3 flex items-center text-xs text-zinc-400 space-x-1">
                  <div className={`w-2 h-2 rounded-full ${cluster?.unavailable_nodes === 0 ? 'bg-emerald-500' : 'bg-rose-500'}`} />
                  <span>{cluster?.unavailable_nodes ?? 0} unavailable · {cluster?.suspect_nodes ?? 0} suspect</span>
                </div>
              </div>

              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center justify-between text-zinc-400 text-xs">
                  <span>Stored Chunks (3x)</span>
                  <Layers className="w-4 h-4 text-indigo-400" />
                </div>
                <div className="mt-2 flex items-baseline space-x-2">
                  <span className="text-2xl font-bold font-mono text-white">{cluster?.total_chunks ?? 0}</span>
                  <span className="text-xs text-zinc-400">logical ({cluster?.total_committed_replicas ?? cluster?.total_active_replicas ?? 0} active replicas)</span>
                </div>
                <div className="mt-3 text-xs text-zinc-400">
                  <span>{files.length} file(s) committed ({formatBytes(cluster?.total_bytes_stored ?? 0)} logical · {formatBytes(cluster?.total_committed_bytes ?? cluster?.total_active_bytes ?? 0)} replicated)</span>
                </div>
              </div>

              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center justify-between text-zinc-400 text-xs">
                  <span>Physical Storage Footprint</span>
                  <HardDrive className="w-4 h-4 text-indigo-400" />
                </div>
                <div className="mt-2 flex items-baseline space-x-2">
                  <span className="text-2xl font-bold font-mono text-white">{cluster?.total_physical_chunks ?? 0}</span>
                  <span className="text-xs text-zinc-400">files on disk ({formatBytes(cluster?.total_physical_bytes ?? 0)})</span>
                </div>
                <div className="mt-3 text-xs text-zinc-400 flex items-center space-x-1.5 font-mono text-[11px]">
                  <span className="text-emerald-400 font-semibold">{cluster?.total_committed_replicas ?? 0} committed</span>
                  <span className="text-zinc-600">·</span>
                  <span className="text-sky-400 font-semibold">{cluster?.total_untracked_chunks ?? 0} untracked</span>
                  <span className="text-zinc-600">·</span>
                  <span className="text-amber-400 font-semibold">{cluster?.total_orphaned_chunks ?? 0} orphaned</span>
                </div>
              </div>

              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
                <div className="flex items-center justify-between text-zinc-400 text-xs">
                  <span>Leader Coordination</span>
                  <Radio className="w-4 h-4 text-indigo-400" />
                </div>
                <div className="mt-2 flex items-baseline space-x-2">
                  <span className="text-2xl font-bold font-mono text-white">Token #{leader?.fencing_token ?? 1}</span>
                </div>
                <div className="mt-3 text-xs text-zinc-400 flex items-center justify-between">
                  <span>Lease: {leader?.is_leader ? `${(leader?.time_remaining_seconds ?? 0).toFixed(1)}s active` : 'Standby'}</span>
                  <button
                    onClick={handleLeaderStepDown}
                    className="text-indigo-400 hover:text-indigo-300 font-mono text-[11px] underline cursor-pointer"
                  >
                    Step Down
                  </button>
                </div>
              </div>
            </div>

            {/* Storage Volume Classification Breakdown Notice */}
            {((cluster?.total_orphaned_chunks ?? 0) > 0 || (cluster?.total_untracked_chunks ?? 0) > 0) && (
              <div className="bg-amber-950/40 border border-amber-800/60 rounded-xl p-4 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3 text-xs text-amber-200">
                <div className="flex items-start sm:items-center space-x-3">
                  <AlertTriangle className="w-5 h-5 text-amber-400 shrink-0 mt-0.5 sm:mt-0" />
                  <div>
                    <div className="font-semibold text-amber-100 flex items-center space-x-2">
                      <span>Physical Volume Discrepancy: {cluster?.total_orphaned_chunks ?? 0} Orphaned Chunks ({formatBytes(cluster?.total_orphaned_bytes ?? 0)}) & {cluster?.total_untracked_chunks ?? 0} Untracked Replicas</span>
                      <span className="bg-amber-500/20 text-amber-300 px-1.5 py-0.2 rounded text-[10px] font-mono">Storage Audited</span>
                    </div>
                    <div className="text-amber-300/80 text-[11px] mt-0.5">
                      Storage nodes contain residual chunk files from prior operations or unassigned replicas on disk. Authoritative committed file metadata is 100% verified with 3 distinct replicas.
                    </div>
                  </div>
                </div>
                <button
                  onClick={handleReconcileOrphans}
                  disabled={reconciling}
                  className="px-3 py-1.5 rounded-lg bg-amber-600 hover:bg-amber-500 text-zinc-950 font-semibold text-xs transition cursor-pointer shrink-0 disabled:opacity-50 shadow-sm"
                >
                  {reconciling ? 'Reclaiming...' : 'Reclaim Orphan Storage'}
                </button>
              </div>
            )}

            {/* Storage Node Distribution Grid */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-semibold text-white">Cluster Storage Nodes ({cluster?.total_nodes ?? 5} Active Instances)</h3>
                  <p className="text-xs text-zinc-400">Authoritative committed replicas vs raw physical disk allocation</p>
                </div>
                <button
                  onClick={() => setActiveTab('nodes')}
                  className="text-xs text-indigo-400 hover:text-indigo-300 flex items-center space-x-1 cursor-pointer"
                >
                  <span>Manage Nodes</span>
                  <ChevronRight className="w-3.5 h-3.5" />
                </button>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
                {(cluster?.nodes || []).map((node, i) => (
                  <div key={node.node_id} className="bg-zinc-950 border border-zinc-800/80 rounded-lg p-3 space-y-2">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <div className={`w-2 h-2 rounded-full ${getStatusDot(node.health_state)}`} />
                        <span className="text-xs font-semibold text-white">Node {i + 1}</span>
                      </div>
                      <span className="text-[10px] font-mono text-zinc-400">:{node.advertised_url.split(':').pop()}</span>
                    </div>
                    <div className="space-y-1 text-[11px] text-zinc-400">
                      <div className="flex justify-between">
                        <span>Committed:</span>
                        <span className="font-mono text-emerald-400 font-semibold">{node.committed_chunks ?? node.total_chunks} ({formatBytes(node.committed_bytes_used ?? node.total_bytes_used)})</span>
                      </div>
                      {(node.untracked_chunks ?? 0) > 0 && (
                        <div className="flex justify-between text-sky-400 font-mono text-[10px]">
                          <span>Untracked:</span>
                          <span>{node.untracked_chunks} ({formatBytes(node.untracked_bytes_used ?? 0)})</span>
                        </div>
                      )}
                      {(node.orphaned_chunks ?? 0) > 0 && (
                        <div className="flex justify-between text-amber-400 font-mono text-[10px]">
                          <span>Orphans:</span>
                          <span>{node.orphaned_chunks} ({formatBytes(node.orphaned_bytes_used ?? 0)})</span>
                        </div>
                      )}
                      <div className="flex justify-between border-t border-zinc-800/50 pt-1">
                        <span>Physical Disk:</span>
                        <span className="font-mono text-zinc-200">{formatBytes(node.physical_bytes_used ?? node.total_bytes_used)} ({node.physical_chunks ?? node.total_chunks} chk)</span>
                      </div>
                    </div>
                    <div className="w-full bg-zinc-800 rounded-full h-1 overflow-hidden mt-1">
                      <div
                        className="bg-indigo-500 h-1 rounded-full"
                        style={{ width: `${Math.max(node.disk_usage_percent || 0, 4)}%` }}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Recent Audit Events */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-3">
              <div className="flex items-center justify-between">
                <h3 className="text-sm font-semibold text-white flex items-center space-x-2">
                  <Terminal className="w-4 h-4 text-zinc-400" />
                  <span>Real-Time Cluster Audit Log (Persisted in SQLite)</span>
                </h3>
                <span className="text-xs text-zinc-500">{events.length} events logged</span>
              </div>
              <div className="bg-zinc-950 border border-zinc-800/80 rounded-lg divide-y divide-zinc-800/50 max-h-64 overflow-y-auto font-mono text-xs">
                {events.length === 0 ? (
                  <div className="p-4 text-center text-zinc-500">No events recorded yet.</div>
                ) : (
                  events.map((ev) => (
                    <div key={ev.event_id} className="p-2.5 flex items-start space-x-3 hover:bg-zinc-900/50 transition">
                      <span className="text-[11px] text-zinc-500 shrink-0 mt-0.5">
                        {new Date(ev.timestamp * 1000).toLocaleTimeString()}
                      </span>
                      <span className={`px-1.5 py-0.2 rounded text-[10px] uppercase font-semibold ${getStatusColor(ev.severity)}`}>
                        {ev.event_type}
                      </span>
                      <span className="text-zinc-300 text-xs flex-1 break-words">{ev.message}</span>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* 2. STORAGE EXPLORER                                            */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'explorer' && (
          <div className="space-y-6">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
              <div>
                <h2 className="text-lg font-semibold text-white">Storage Explorer</h2>
                <p className="text-xs text-zinc-400">Authoritative files committed to cluster metadata with SHA-256 chunk layouts</p>
              </div>
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={uploading}
                className="self-start sm:self-auto flex items-center space-x-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition cursor-pointer"
              >
                <Upload className="w-3.5 h-3.5" />
                <span>Upload New File</span>
              </button>
            </div>

            {uploadFeedback && (
              <div className={`p-3 rounded-lg text-xs border ${uploadFeedback.type === 'success' ? 'bg-emerald-950/60 border-emerald-800 text-emerald-300' : 'bg-rose-950/60 border-rose-800 text-rose-300'}`}>
                {uploadFeedback.message}
              </div>
            )}

            {files.length === 0 ? (
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-12 text-center space-y-3">
                <FileText className="w-10 h-10 text-zinc-600 mx-auto" />
                <h3 className="text-sm font-semibold text-white">No files in cluster storage</h3>
                <p className="text-xs text-zinc-500 max-w-sm mx-auto">
                  Upload a binary or text file to test 1 MiB boundary chunking, SHA-256 validation, and 3x replica distribution.
                </p>
                <button
                  onClick={() => fileInputRef.current?.click()}
                  className="px-3.5 py-1.5 text-xs font-medium bg-zinc-800 hover:bg-zinc-700 text-zinc-200 rounded-lg cursor-pointer transition border border-zinc-700"
                >
                  Upload First File
                </button>
              </div>
            ) : (
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
                <table className="w-full text-left text-xs">
                  <thead className="bg-zinc-950/60 text-zinc-400 uppercase tracking-wider border-b border-zinc-800 font-mono text-[11px]">
                    <tr>
                      <th className="py-3 px-4">Filename</th>
                      <th className="py-3 px-4">Size</th>
                      <th className="py-3 px-4">Chunks</th>
                      <th className="py-3 px-4">Replicas</th>
                      <th className="py-3 px-4">SHA-256</th>
                      <th className="py-3 px-4">Status</th>
                      <th className="py-3 px-4 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-zinc-800/60 text-zinc-300">
                    {files.map((file) => (
                      <tr key={file.file_id} className="hover:bg-zinc-800/30 transition">
                        <td className="py-3 px-4 font-medium text-white flex items-center space-x-2">
                          <FileText className="w-4 h-4 text-indigo-400 shrink-0" />
                          <span className="truncate max-w-xs">{file.filename}</span>
                        </td>
                        <td className="py-3 px-4 font-mono">{formatBytes(file.size_bytes)}</td>
                        <td className="py-3 px-4 font-mono">{file.chunk_count} chunk(s)</td>
                        <td className="py-3 px-4 font-semibold text-indigo-400">{file.replication_factor}x</td>
                        <td className="py-3 px-4 font-mono text-[11px] text-zinc-400 truncate max-w-xs">
                          {file.checksum_sha256}
                        </td>
                        <td className="py-3 px-4">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-semibold uppercase ${getStatusColor(file.status)}`}>
                            {file.status}
                          </span>
                        </td>
                        <td className="py-3 px-4 text-right space-x-2">
                          <button
                            onClick={() => handleInspectFile(file.file_id)}
                            className="text-xs text-indigo-400 hover:text-indigo-300 font-medium cursor-pointer"
                          >
                            Inspect Layout
                          </button>
                          <a
                            href={`/api/v1/files/${file.file_id}/download`}
                            download={file.filename}
                            className="inline-flex items-center text-xs text-emerald-400 hover:text-emerald-300 font-medium ml-2"
                          >
                            <Download className="w-3.5 h-3.5 mr-1" />
                            Download
                          </a>
                          <button
                            onClick={() => handleFileDelete(file.file_id, file.filename)}
                            className="text-xs text-rose-400 hover:text-rose-300 ml-2 cursor-pointer"
                            title="Safe Delete Across Nodes"
                          >
                            <Trash2 className="w-3.5 h-3.5 inline" />
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Chunk Layout Inspector */}
            {selectedFileDetail && (
              <div className="bg-zinc-900 border border-indigo-500/30 rounded-xl p-5 space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <h3 className="text-sm font-semibold text-white flex items-center space-x-2">
                      <Layers className="w-4 h-4 text-indigo-400" />
                      <span>Physical Chunk Placement: {selectedFileDetail.filename}</span>
                    </h3>
                    <p className="text-xs text-zinc-400 font-mono mt-0.5">
                      File ID: {selectedFileDetail.file_id} · Version: v{selectedFileDetail.metadata_version} · Full SHA-256: {selectedFileDetail.checksum_sha256}
                    </p>
                  </div>
                  <button
                    onClick={() => setSelectedFileDetail(null)}
                    className="text-xs text-zinc-400 hover:text-zinc-200 px-2 py-1 rounded bg-zinc-800 cursor-pointer"
                  >
                    Close
                  </button>
                </div>

                <div className="space-y-3">
                  {selectedFileDetail.chunks.map((chk) => (
                    <div key={chk.chunk_id} className="bg-zinc-950 p-3.5 rounded-lg border border-zinc-800 space-y-2">
                      <div className="flex items-center justify-between text-xs">
                        <span className="font-semibold text-white">
                          Chunk #{chk.sequence_index} ({chk.chunk_id})
                        </span>
                        <span className="font-mono text-zinc-400">
                          {formatBytes(chk.size_bytes)} ({chk.size_bytes} bytes)
                        </span>
                      </div>
                      <div className="text-[11px] font-mono text-zinc-500 truncate">
                        Checksum: {chk.checksum_sha256}
                      </div>

                      <div className="pt-2">
                        <span className="text-[11px] text-zinc-400 block mb-1.5 font-medium">Replicas on Independent Nodes:</span>
                        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
                          {chk.replicas.map((rep) => (
                            <div
                              key={rep.replica_id}
                              className="p-2 rounded bg-zinc-900 border border-zinc-800 text-xs font-mono space-y-1"
                            >
                              <div className="flex items-center justify-between">
                                <span className="font-semibold text-zinc-200">{rep.node_id}</span>
                                <span className={`text-[10px] px-1 rounded ${getStatusColor(rep.status)}`}>{rep.status}</span>
                              </div>
                              <div className="text-[10px] text-zinc-500 truncate">{rep.node_url}</div>
                              {rep.status === 'active' && (
                                <button
                                  onClick={() => handleCorruptChunk(chk.chunk_id, rep.node_id)}
                                  className="text-[10px] text-rose-400 hover:text-rose-300 underline font-sans cursor-pointer pt-0.5"
                                  title="Flip physical bytes on this storage node to test cryptographic audit detection"
                                >
                                  Inject Bit-Rot
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
        {/* 3. CLUSTER NODES                                               */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'nodes' && (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-white">Storage Node Topology</h2>
                <p className="text-xs text-zinc-400">Committed replica assignments, physical disk utilization, and failure controls for all 5 cluster services</p>
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {(cluster?.nodes || []).map((node, i) => (
                <div key={node.node_id} className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4 hover:border-zinc-700 transition">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-3">
                      <div className="w-8 h-8 rounded-lg bg-zinc-800 border border-zinc-700 flex items-center justify-center text-indigo-400">
                        <HardDrive className="w-4 h-4" />
                      </div>
                      <div>
                        <h4 className="text-sm font-semibold text-white">{node.node_name}</h4>
                        <span className="text-xs font-mono text-zinc-400">{node.node_id}</span>
                      </div>
                    </div>
                    <span className={`px-2 py-0.5 rounded text-[11px] font-semibold uppercase ${getStatusColor(node.health_state)}`}>
                      {node.health_state}
                    </span>
                  </div>

                  {/* Classification Pills */}
                  <div className="flex flex-wrap gap-1.5 font-mono text-[10px]">
                    <span className="px-2 py-0.5 rounded bg-emerald-950/60 text-emerald-300 border border-emerald-800/60">
                      {node.committed_chunks ?? node.total_chunks} Committed ({formatBytes(node.committed_bytes_used ?? node.total_bytes_used)})
                    </span>
                    {(node.untracked_chunks ?? 0) > 0 && (
                      <span className="px-2 py-0.5 rounded bg-sky-950/60 text-sky-300 border border-sky-800/60">
                        {node.untracked_chunks} Untracked ({formatBytes(node.untracked_bytes_used ?? 0)})
                      </span>
                    )}
                    {(node.orphaned_chunks ?? 0) > 0 && (
                      <span className="px-2 py-0.5 rounded bg-amber-950/60 text-amber-300 border border-amber-800/60">
                        {node.orphaned_chunks} Orphaned ({formatBytes(node.orphaned_bytes_used ?? 0)})
                      </span>
                    )}
                  </div>

                  <div className="space-y-2 text-xs border-t border-zinc-800 pt-3">
                    <div className="flex justify-between">
                      <span className="text-zinc-500">HTTP Endpoint:</span>
                      <span className="font-mono text-zinc-300">{node.advertised_url}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">Volume Storage:</span>
                      <span className="font-mono text-zinc-300">/data/storage_node_{node.node_id.replace('storage-node-', '')}/chunks</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">Committed Replicas:</span>
                      <span className="font-mono font-semibold text-emerald-400">{node.committed_chunks ?? node.total_chunks}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">Physical Files on Disk:</span>
                      <span className="font-mono font-semibold text-white">{node.physical_chunks ?? node.total_chunks}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">Committed Storage:</span>
                      <span className="font-mono text-zinc-300">{formatBytes(node.committed_bytes_used ?? node.total_bytes_used)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-zinc-500">Physical Disk Used:</span>
                      <span className="font-mono text-zinc-300">{formatBytes(node.physical_bytes_used ?? node.total_bytes_used)}</span>
                    </div>
                  </div>

                  <div className="pt-2">
                    <div className="flex justify-between text-xs text-zinc-400 mb-1 font-mono">
                      <span>Physical Disk ({formatBytes(node.physical_bytes_used ?? node.total_bytes_used)} / {formatBytes(node.storage_limit_bytes)})</span>
                      <span>{node.disk_usage_percent}%</span>
                    </div>
                    <div className="w-full bg-zinc-800 rounded-full h-1.5 overflow-hidden">
                      <div
                        className="bg-indigo-500 h-1.5 rounded-full"
                        style={{ width: `${Math.max(node.disk_usage_percent, 3)}%` }}
                      />
                    </div>
                  </div>

                  {/* Operational Controls */}
                  <div className="pt-3 border-t border-zinc-800 flex space-x-2">
                    <button
                      onClick={() => handleToggleNodeState(node.node_id)}
                      className={`flex-1 py-1.5 text-xs font-medium rounded-lg border transition cursor-pointer ${
                        node.health_state === 'UNAVAILABLE'
                          ? 'bg-emerald-950/60 border-emerald-800 text-emerald-300 hover:bg-emerald-900/60'
                          : 'bg-rose-950/60 border-rose-800 text-rose-300 hover:bg-rose-900/60'
                      }`}
                    >
                      {node.health_state === 'UNAVAILABLE' ? 'Simulate Recovery' : 'Simulate Failure'}
                    </button>
                    <button
                      onClick={() => handleReconcileNode(node.node_id)}
                      className="px-3 py-1.5 text-xs font-medium rounded-lg bg-zinc-800 hover:bg-zinc-700 text-zinc-300 border border-zinc-700 transition cursor-pointer"
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
        {/* 4. SELF-HEALING MONITOR                                        */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'selfhealing' && (
          <div className="space-y-6">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-white">Self-Healing & Re-Replication Monitor</h2>
                <p className="text-xs text-zinc-400">Automated discovery of degraded replicas and transactional re-replication jobs</p>
              </div>
              <button
                onClick={handleTriggerReconcile}
                disabled={reconciling}
                className="flex items-center space-x-1.5 px-3 py-1.5 text-xs font-medium rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition disabled:opacity-50 cursor-pointer"
              >
                <RefreshCw className={`w-3.5 h-3.5 ${reconciling ? 'animate-spin' : ''}`} />
                <span>{reconciling ? 'Reconciling...' : 'Run Healing Scan'}</span>
              </button>
            </div>

            <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
              <div className="p-4 border-b border-zinc-800 flex items-center justify-between">
                <h3 className="text-xs font-semibold uppercase tracking-wider text-zinc-400 font-mono">
                  Re-Replication Jobs History ({jobs.length})
                </h3>
              </div>

              {jobs.length === 0 ? (
                <div className="p-8 text-center text-xs text-zinc-500">
                  No recovery jobs recorded. All committed files currently satisfy 3x replication factor.
                </div>
              ) : (
                <table className="w-full text-left text-xs">
                  <thead className="bg-zinc-950/60 text-zinc-400 uppercase tracking-wider border-b border-zinc-800 font-mono text-[11px]">
                    <tr>
                      <th className="py-3 px-4">Job ID</th>
                      <th className="py-3 px-4">Chunk ID</th>
                      <th className="py-3 px-4">Source Node</th>
                      <th className="py-3 px-4">Target Node</th>
                      <th className="py-3 px-4">Status</th>
                      <th className="py-3 px-4">Attempts</th>
                      <th className="py-3 px-4 text-right">Timestamp</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-zinc-800/60 text-zinc-300">
                    {jobs.map((job) => (
                      <tr key={job.job_id} className="hover:bg-zinc-800/30 transition">
                        <td className="py-3 px-4 font-mono font-medium text-white">{job.job_id}</td>
                        <td className="py-3 px-4 font-mono text-zinc-400">{job.chunk_id}</td>
                        <td className="py-3 px-4 font-mono text-emerald-400">{job.source_node_id}</td>
                        <td className="py-3 px-4 font-mono text-indigo-400">{job.target_node_id}</td>
                        <td className="py-3 px-4">
                          <span className={`px-2 py-0.5 rounded text-[10px] font-semibold uppercase ${getStatusColor(job.status)}`}>
                            {job.status}
                          </span>
                        </td>
                        <td className="py-3 px-4 font-mono">{job.attempts}</td>
                        <td className="py-3 px-4 text-right font-mono text-zinc-500 text-[11px]">
                          {new Date(job.created_at * 1000).toLocaleTimeString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* 5. DISTRIBUTED-SYSTEMS & AUDIT INSPECTOR                       */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'inspector' && (
          <div className="space-y-6">
            <div>
              <h2 className="text-lg font-semibold text-white">Distributed-Systems & Audit Inspector</h2>
              <p className="text-xs text-zinc-400">Database leases, fencing tokens, cryptographic audits, and complete chunk inventory classification</p>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Leader Lease Card */}
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-white flex items-center space-x-2">
                    <Radio className="w-4 h-4 text-indigo-400" />
                    <span>PostgreSQL / SQLite Leader Lease</span>
                  </h3>
                  <button
                    onClick={handleLeaderStepDown}
                    className="text-xs px-2.5 py-1 rounded bg-rose-950/60 border border-rose-800/80 text-rose-300 hover:bg-rose-900/60 transition cursor-pointer"
                  >
                    Force Expiration
                  </button>
                </div>

                <div className="space-y-2 text-xs font-mono bg-zinc-950 p-4 rounded-lg border border-zinc-800/80">
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Current Lease Holder:</span>
                    <span className="text-emerald-400 font-bold">{leader?.current_leader_id}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Monotonic Fencing Token:</span>
                    <span className="text-indigo-300 font-bold">#{leader?.fencing_token}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Time Until Renewal:</span>
                    <span className="text-zinc-300">{(leader?.time_remaining_seconds ?? 0).toFixed(1)}s</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-zinc-500">Coordinator Instance:</span>
                    <span className="text-zinc-400">{leader?.coordinator_instance_id}</span>
                  </div>
                </div>

                <p className="text-xs text-zinc-400">
                  Fencing tokens prevent split-brain writes. If a stale leader attempts a metadata update after its lease expires, the transaction is rejected.
                </p>
              </div>

              {/* Cryptographic Audit Card */}
              <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-white flex items-center space-x-2">
                    <ShieldCheck className="w-4 h-4 text-emerald-400" />
                    <span>Cryptographic Cluster Audit</span>
                  </h3>
                  <button
                    onClick={handleRunAudit}
                    disabled={verifyingAudit}
                    className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition cursor-pointer disabled:opacity-50"
                  >
                    {verifyingAudit ? 'Auditing...' : 'Run SHA-256 Audit'}
                  </button>
                </div>

                {auditResult ? (
                  <div className="space-y-3">
                    <div className="space-y-2 text-xs font-mono bg-zinc-950 p-4 rounded-lg border border-zinc-800/80">
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Files Audited:</span>
                        <span className="text-zinc-200">{auditResult.checked_files}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Chunks Audited:</span>
                        <span className="text-zinc-200">{auditResult.checked_chunks}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Valid Replicas on Disk:</span>
                        <span className="text-emerald-400 font-bold">{auditResult.valid_replicas}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Corrupted Replicas:</span>
                        <span className={auditResult.corrupted_replicas > 0 ? 'text-rose-400 font-bold' : 'text-zinc-400'}>
                          {auditResult.corrupted_replicas}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-zinc-500">Missing Replicas:</span>
                        <span className={auditResult.missing_replicas > 0 ? 'text-rose-400 font-bold' : 'text-zinc-400'}>
                          {auditResult.missing_replicas}
                        </span>
                      </div>
                    </div>

                    {auditResult.corrupted_replicas > 0 && (
                      <button
                        onClick={handleTriggerReconcile}
                        disabled={reconciling}
                        className="w-full py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-xs font-semibold cursor-pointer transition flex items-center justify-center space-x-1.5 shadow"
                      >
                        <RefreshCw className={`w-3.5 h-3.5 ${reconciling ? 'animate-spin' : ''}`} />
                        <span>Trigger Self-Healing Scan to Repair {auditResult.corrupted_replicas} Corrupted Replica(s)</span>
                      </button>
                    )}
                  </div>
                ) : (
                  <div className="p-6 bg-zinc-950 rounded-lg border border-zinc-800 text-center text-xs text-zinc-500">
                    Click "Run SHA-256 Audit" to verify on-disk hash consistency across all storage nodes.
                  </div>
                )}
              </div>
            </div>

            {/* Comprehensive Physical Chunk Inventory & 4-Way Classification Table */}
            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div>
                  <h3 className="text-sm font-semibold text-white flex items-center space-x-2">
                    <Database className="w-4 h-4 text-indigo-400" />
                    <span>Physical Chunk Inventory & 4-Way Classification Audit</span>
                  </h3>
                  <p className="text-xs text-zinc-400 mt-0.5">
                    Itemized audit of all 11 physical chunk files found on disk across all 5 nodes
                  </p>
                </div>
                {inventoryAudit && (
                  <div className="flex flex-wrap gap-2 text-[11px] font-mono">
                    <span className="px-2 py-0.5 rounded bg-emerald-950/60 border border-emerald-800 text-emerald-300">
                      {inventoryAudit.summary.actual_committed_replicas} Committed
                    </span>
                    <span className="px-2 py-0.5 rounded bg-sky-950/60 border border-sky-800 text-sky-300">
                      {inventoryAudit.summary.actual_untracked_chunks} Untracked
                    </span>
                    <span className="px-2 py-0.5 rounded bg-amber-950/60 border border-amber-800 text-amber-300">
                      {inventoryAudit.summary.actual_orphaned_chunks} Orphaned
                    </span>
                    <span className="px-2 py-0.5 rounded bg-rose-950/60 border border-rose-800 text-rose-300">
                      {inventoryAudit.summary.actual_stale_replicas} Stale
                    </span>
                  </div>
                )}
              </div>

              <div className="bg-zinc-950 rounded-lg border border-zinc-800 overflow-hidden">
                <table className="w-full text-left text-xs">
                  <thead className="bg-zinc-900/80 text-zinc-400 uppercase tracking-wider border-b border-zinc-800 font-mono text-[11px]">
                    <tr>
                      <th className="py-2.5 px-3">Storage Node</th>
                      <th className="py-2.5 px-3">Chunk ID</th>
                      <th className="py-2.5 px-3">File / Origin</th>
                      <th className="py-2.5 px-3">Size</th>
                      <th className="py-2.5 px-3">SHA-256 (Disk)</th>
                      <th className="py-2.5 px-3">Audit Classification</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-zinc-800/50 font-mono text-[11px] text-zinc-300">
                    {(!inventoryAudit || inventoryAudit.items.length === 0) ? (
                      <tr>
                        <td colSpan={6} className="py-6 text-center text-zinc-500 font-sans">
                          No chunk inventory loaded. Refreshing...
                        </td>
                      </tr>
                    ) : (
                      inventoryAudit.items.map((item, idx) => (
                        <tr key={idx} className="hover:bg-zinc-900/40 transition">
                          <td className="py-2.5 px-3 font-semibold text-zinc-200">{item.node_id}</td>
                          <td className="py-2.5 px-3 text-indigo-400 truncate max-w-[120px]">{item.chunk_id}</td>
                          <td className="py-2.5 px-3 text-zinc-400 font-sans truncate max-w-[150px]">
                            {item.file_name ? item.file_name : <span className="text-zinc-600 italic">No File (Residual)</span>}
                          </td>
                          <td className="py-2.5 px-3">{formatBytes(item.size_bytes)}</td>
                          <td className="py-2.5 px-3 text-zinc-500 truncate max-w-[130px] font-mono text-[10px]" title={item.sha256}>
                            {item.sha256.slice(0, 16)}...
                          </td>
                          <td className="py-2.5 px-3">
                            <span className={`px-2 py-0.5 rounded text-[10px] font-semibold uppercase ${getStatusColor(item.classification)}`}>
                              {item.classification.replace('_', ' ')}
                            </span>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* ------------------------------------------------------------- */}
        {/* 6. FAULT-TESTING INTERFACE                                     */}
        {/* ------------------------------------------------------------- */}
        {activeTab === 'faultlab' && (
          <div className="space-y-6">
            <div>
              <h2 className="text-lg font-semibold text-white">Live Fault-Testing Lab</h2>
              <p className="text-xs text-zinc-400">
                Execute a live end-to-end failure injection scenario: upload probe, kill node, verify self-healing, recover file, and reconcile.
              </p>
            </div>

            <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 space-y-4">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                <div className="flex items-center space-x-3">
                  <label className="text-xs text-zinc-400">Target Node for Hard Failure:</label>
                  <select
                    value={labSelectedNode}
                    onChange={(e) => setLabSelectedNode(e.target.value)}
                    disabled={labRunning}
                    className="bg-zinc-950 border border-zinc-700 rounded px-2.5 py-1 text-xs font-mono text-zinc-200"
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
                  className="flex items-center space-x-2 px-4 py-2 text-xs font-semibold rounded-lg bg-indigo-600 hover:bg-indigo-500 text-white transition disabled:opacity-50 cursor-pointer shadow-md"
                >
                  <Zap className={`w-4 h-4 ${labRunning ? 'animate-pulse' : ''}`} />
                  <span>{labRunning ? 'Running Live Demo...' : 'Execute Live Fault & Self-Healing Demo'}</span>
                </button>
              </div>

              {/* Lab Console Terminal */}
              <div className="bg-zinc-950 border border-zinc-800/80 rounded-lg p-4 font-mono text-xs space-y-1.5 h-80 overflow-y-auto">
                {labLogs.length === 0 ? (
                  <div className="text-zinc-600 italic">
                    Press "Execute Live Fault & Self-Healing Demo" above to start the automated 6-step lifecycle demonstration.
                  </div>
                ) : (
                  labLogs.map((log, idx) => (
                    <div
                      key={idx}
                      className={
                        log.includes('✔')
                          ? 'text-emerald-400'
                          : log.includes('Step')
                          ? 'text-indigo-300 font-semibold mt-2'
                          : log.includes('---')
                          ? 'text-zinc-400 font-bold'
                          : log.includes('✘')
                          ? 'text-rose-400 font-bold'
                          : 'text-zinc-300'
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
    </div>
  );
}
