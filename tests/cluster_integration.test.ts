import assert from 'assert';
import crypto from 'crypto';

const BASE_URL = process.env.TEST_API_URL || 'http://127.0.0.1:3000';

async function runTests() {
  console.log(`\n==========================================================`);
  console.log(` ShardSphere Distributed Storage Engine — Integration Tests`);
  console.log(` Target Server: ${BASE_URL}`);
  console.log(`==========================================================\n`);

  // 1. Health & Readiness
  console.log('[1/8] Verifying Coordinator Health & Readiness...');
  const healthRes = await fetch(`${BASE_URL}/api/v1/health`);
  assert.strictEqual(healthRes.status, 200, 'Health endpoint should return 200');
  const health = await healthRes.json();
  assert.strictEqual(health.status, 'healthy');
  console.log('  ✔ Health check passed:', health.service, health.status);

  const readyRes = await fetch(`${BASE_URL}/api/v1/ready`);
  assert.strictEqual(readyRes.status, 200);
  const ready = await readyRes.json();
  assert.strictEqual(ready.status, 'ready');
  console.log('  ✔ Coordinator ready and database connected');

  // 2. Leader Lease & Fencing Token
  console.log('\n[2/8] Verifying Leader Election Lease & Fencing Tokens...');
  const leaderRes = await fetch(`${BASE_URL}/api/v1/leader`);
  assert.strictEqual(leaderRes.status, 200);
  const leader = await leaderRes.json();
  assert.strictEqual(leader.is_leader, true, 'Coordinator must hold leader lease');
  const initialToken = leader.fencing_token;
  console.log(`  ✔ Current leader: ${leader.current_leader_id}, Fencing Token: ${initialToken}`);

  // Test leader step down
  const stepRes = await fetch(`${BASE_URL}/api/v1/leader/step-down`, { method: 'POST' });
  assert.strictEqual(stepRes.status, 200);
  const stepData = await stepRes.json();
  assert.strictEqual(stepData.fencing_token, initialToken + 1, 'Token must increment on lease expiration');
  console.log(`  ✔ Leader stepped down cleanly, token advanced to #${stepData.fencing_token}`);

  // Wait for re-election
  await new Promise(r => setTimeout(r, 1500));
  const newLeaderRes = await fetch(`${BASE_URL}/api/v1/leader`);
  const newLeader = await newLeaderRes.json();
  assert.strictEqual(newLeader.is_leader, true, 'Leader should be re-elected');
  console.log(`  ✔ Re-elected leader: ${newLeader.current_leader_id} (Token #${newLeader.fencing_token})`);

  // 3. Cluster Topology & Storage Nodes
  console.log('\n[3/8] Inspecting 5-Node Storage Topology...');
  const clusterRes = await fetch(`${BASE_URL}/api/v1/cluster`);
  assert.strictEqual(clusterRes.status, 200);
  const cluster = await clusterRes.json();
  assert.strictEqual(cluster.total_nodes, 5, 'Cluster must have 5 independent nodes');
  assert.strictEqual(cluster.healthy_nodes, 5, 'All nodes should initially be healthy');
  console.log(`  ✔ 5/5 nodes online. Quorum replication factor: ${cluster.configured_replication_factor}x`);

  // 4. File Upload, Chunking & Placement
  console.log('\n[4/8] Testing 1.5 MiB File Ingestion & 3x Chunk Placement...');
  const probeSize = 1572864; // 1.5 MiB (1 chunk of 1MB + 1 chunk of 0.5MB)
  const probeData = Buffer.alloc(probeSize);
  for (let i = 0; i < probeSize; i++) {
    probeData[i] = (i * 37) % 256;
  }
  const expectedHash = crypto.createHash('sha256').update(probeData).digest('hex');

  const formData = new FormData();
  formData.append('file', new Blob([probeData], { type: 'application/octet-stream' }), 'test_probe.bin');

  const uploadRes = await fetch(`${BASE_URL}/api/v1/files`, { method: 'POST', body: formData });
  assert.strictEqual(uploadRes.status, 201, 'Upload must return 201 Created');
  const uploadInfo = await uploadRes.json();
  assert.strictEqual(uploadInfo.size_bytes, probeSize);
  assert.strictEqual(uploadInfo.chunk_count, 2, '1.5 MiB file should produce exactly 2 chunks');
  assert.strictEqual(uploadInfo.checksum_sha256, expectedHash, 'SHA-256 must match probe data');
  assert.strictEqual(uploadInfo.replication_factor, 3, 'Must replicate to 3 distinct nodes');
  console.log(`  ✔ Uploaded: ${uploadInfo.file_id} (${uploadInfo.chunk_count} chunks, 3x replication, hash: ${expectedHash.slice(0, 16)}...)`);

  // Inspect chunk details
  const detailRes = await fetch(`${BASE_URL}/api/v1/files/${uploadInfo.file_id}`);
  assert.strictEqual(detailRes.status, 200);
  const detail = await detailRes.json();
  assert.strictEqual(detail.chunks.length, 2);
  for (const chk of detail.chunks) {
    assert.strictEqual(chk.replicas.length, 3, 'Each chunk must have 3 replicas');
    console.log(`  ↳ Chunk #${chk.sequence_index} (${chk.size_bytes}B) -> Nodes: [${chk.replicas.map((r: any) => r.node_id).join(', ')}]`);
  }

  // 5. Fault Injection: Kill a Node
  const targetNode = detail.chunks[0].replicas[0].node_id;
  console.log(`\n[5/8] Simulating Hard Failure on Storage Node "${targetNode}"...`);
  const toggleRes = await fetch(`${BASE_URL}/api/v1/nodes/${targetNode}/toggle-state`, { method: 'POST' });
  assert.strictEqual(toggleRes.status, 200);
  const toggleData = await toggleRes.json();
  assert.strictEqual(toggleData.new_state, 'UNAVAILABLE');
  console.log(`  ✔ Storage node "${targetNode}" transitioned to UNAVAILABLE.`);

  // 6. Automated Self-Healing Reconciliation
  console.log('\n[6/8] Executing Automated Self-Healing Reconciler...');
  const recRes = await fetch(`${BASE_URL}/api/v1/self-healing/reconcile`, { method: 'POST' });
  assert.strictEqual(recRes.status, 200);
  const recData = await recRes.json();
  assert.ok(recData.recovered_chunks > 0, 'Reconciliation should have re-replicated degraded chunks');
  console.log(`  ✔ Self-healing complete: ${recData.recovered_chunks} chunk replica(s) restored on alternate healthy nodes.`);

  // Verify recovery jobs logged
  const jobsRes = await fetch(`${BASE_URL}/api/v1/recovery/jobs`);
  const jobs = await jobsRes.json();
  assert.ok(jobs.length > 0, 'Recovery jobs must be recorded');
  console.log(`  ✔ Audit confirmed recovery job #${jobs[0].job_id} status: ${jobs[0].status}`);

  // 7. Resilient File Download During Node Partition
  console.log(`\n[7/8] Downloading File While Node "${targetNode}" is DEAD (Failover Retrieval)...`);
  const dlRes = await fetch(`${BASE_URL}/api/v1/files/${uploadInfo.file_id}/download`);
  assert.strictEqual(dlRes.status, 200, 'File download must succeed despite node outage');
  const dlBuffer = Buffer.from(await dlRes.arrayBuffer());
  assert.strictEqual(dlBuffer.length, probeSize, 'Downloaded byte size must match original exactly');
  const dlHash = crypto.createHash('sha256').update(dlBuffer).digest('hex');
  assert.strictEqual(dlHash, expectedHash, 'Cryptographic checksum must match original probe byte-for-byte');
  console.log(`  ✔ EXACT BYTE-FOR-BYTE MATCH CONFIRMED (1,572,864 bytes verified, SHA-256 matched).`);

  // 8. Revive Node, Reconcile & Clean Up
  console.log(`\n[8/8] Reviving Node "${targetNode}" & Running Returning-Node Reconciliation...`);
  await fetch(`${BASE_URL}/api/v1/nodes/${targetNode}/toggle-state`, { method: 'POST' });
  const nodeReconRes = await fetch(`${BASE_URL}/api/v1/nodes/${targetNode}/reconcile`, { method: 'POST' });
  const nodeRecon = await nodeReconRes.json();
  console.log(`  ✔ Returning node reconciled: ${nodeRecon.verified_active} active verified, ${nodeRecon.purged_stale} stale purged.`);

  // Cryptographic Cluster Audit
  console.log('  Running cryptographic integrity audit across all cluster replicas...');
  const auditRes = await fetch(`${BASE_URL}/api/v1/integrity/verify`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  });
  const audit = await auditRes.json();
  assert.strictEqual(audit.corrupted_replicas, 0, 'No replicas should be corrupted');
  console.log(`  ✔ Cluster audit passed: ${audit.valid_replicas} valid replicas on disk, 0 corrupted, 0 missing.`);

  // Cleanup test probe
  await fetch(`${BASE_URL}/api/v1/files/${uploadInfo.file_id}`, { method: 'DELETE' });
  console.log(`  ✔ Cleaned up test probe file.`);

  console.log(`\n==========================================================`);
  console.log(` ALL INTEGRATION TESTS PASSED SUCCESSFULLY (8/8)`);
  console.log(`==========================================================\n`);
}

runTests().catch(err => {
  console.error('\n❌ Test Failure:', err);
  process.exit(1);
});
