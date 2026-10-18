import assert from 'assert';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BASE_URL = process.env.TEST_API_URL || 'http://127.0.0.1:3000';

async function runRegressionAuditTests() {
  console.log(`\n======================================================================`);
  console.log(` ShardSphere Storage Consistency, Inventory & Audit Regression Tests`);
  console.log(` Target Server: ${BASE_URL}`);
  console.log(`======================================================================\n`);

  // -------------------------------------------------------------------------
  // 1. Logical Chunk Counts vs Physical Replica Counts
  // -------------------------------------------------------------------------
  console.log('[Test 1/6] Auditing Logical File Metadata vs Authoritative Replicas...');
  const filesRes = await fetch(`${BASE_URL}/api/v1/files`);
  assert.strictEqual(filesRes.status, 200);
  const filesData = await filesRes.json();

  assert.strictEqual(filesData.total_files, 1, 'Exactly 1 committed file should exist');
  const committedFile = filesData.files[0];
  assert.strictEqual(committedFile.filename, 'testfile.txt');
  assert.strictEqual(committedFile.size_bytes, 53, 'File size must be 53 bytes');
  assert.strictEqual(committedFile.chunk_count, 1, 'File must have exactly 1 logical chunk');
  assert.strictEqual(committedFile.replication_factor, 3, 'Replication factor must be 3x');
  assert.strictEqual(
    committedFile.checksum_sha256,
    '67225f351d08e285a7e28f51f6828356250663450881742186258f51e07b6814'
  );
  console.log(`  ✔ Logical metadata verified: "${committedFile.filename}" (${committedFile.size_bytes}B, 1 chunk, 3x RF).`);

  const fileDetailRes = await fetch(`${BASE_URL}/api/v1/files/${committedFile.file_id}`);
  assert.strictEqual(fileDetailRes.status, 200);
  const fileDetail = await fileDetailRes.json();
  assert.strictEqual(fileDetail.chunks.length, 1);
  const chunk0 = fileDetail.chunks[0];
  assert.strictEqual(chunk0.chunk_id, 'chk_715c3733d5ba411e');
  assert.strictEqual(chunk0.size_bytes, 53);
  assert.strictEqual(chunk0.replicas.length, 3, 'Must have exactly 3 registered replicas');

  const registeredNodeIds = chunk0.replicas.map((r: any) => r.node_id).sort();
  assert.deepStrictEqual(
    registeredNodeIds,
    ['storage-node-1', 'storage-node-2', 'storage-node-3'],
    'Replicas must reside on 3 distinct storage nodes (1, 2, 3)'
  );
  console.log(`  ✔ Chunk ${chunk0.chunk_id} replicas confirmed on distinct nodes: [${registeredNodeIds.join(', ')}]`);

  // -------------------------------------------------------------------------
  // 2. Physical Replica File Integrity & SHA-256 Checksums
  // -------------------------------------------------------------------------
  console.log('\n[Test 2/6] Verifying Physical On-Disk Chunks & Cryptographic Checksums...');
  const expectedSha = '67225f351d08e285a7e28f51f6828356250663450881742186258f51e07b6814';
  const expectedContent = 'Distributed Self-Healing Storage Engine Test Content\n';

  for (let nodeNum = 1; nodeNum <= 3; nodeNum++) {
    const chunkPath = path.resolve(__dirname, `../data/storage_node_${nodeNum}/chunks/chunk_${chunk0.chunk_id}.bin`);
    assert.ok(fs.existsSync(chunkPath), `Physical chunk file must exist at ${chunkPath}`);
    const content = fs.readFileSync(chunkPath);
    assert.strictEqual(content.length, 53, `Node ${nodeNum} chunk size must be 53 bytes`);
    assert.strictEqual(content.toString('utf-8'), expectedContent, `Node ${nodeNum} content mismatch`);
    const actualSha = crypto.createHash('sha256').update(content).digest('hex');
    assert.strictEqual(actualSha, expectedSha, `Node ${nodeNum} SHA-256 checksum must match`);
    console.log(`  ✔ Storage Node ${nodeNum}: physical chunk exists, 53 bytes, SHA-256 validated.`);
  }

  // -------------------------------------------------------------------------
  // 3. Storage Byte Accounting Consistency
  // -------------------------------------------------------------------------
  console.log('\n[Test 3/6] Auditing Byte Accounting Consistency (Committed vs Physical)...');
  const clusterRes = await fetch(`${BASE_URL}/api/v1/cluster`);
  assert.strictEqual(clusterRes.status, 200);
  const cluster = await clusterRes.json();

  // Logical totals
  assert.strictEqual(cluster.total_files, 1);
  assert.strictEqual(cluster.total_chunks, 1);
  assert.strictEqual(cluster.total_bytes_stored, 53, 'Logical stored bytes must be 53');

  // Committed physical replicas
  assert.strictEqual(cluster.total_committed_replicas, 3, 'Must report exactly 3 committed replicas');
  assert.strictEqual(cluster.total_committed_bytes, 159, '3 committed replicas * 53B = 159B');

  // Per-node accounting
  const nodes = cluster.nodes;
  assert.strictEqual(nodes.length, 5);
  const node1 = nodes.find((n: any) => n.node_id === 'storage-node-1');
  const node2 = nodes.find((n: any) => n.node_id === 'storage-node-2');
  const node3 = nodes.find((n: any) => n.node_id === 'storage-node-3');
  const node4 = nodes.find((n: any) => n.node_id === 'storage-node-4');
  const node5 = nodes.find((n: any) => n.node_id === 'storage-node-5');

  assert.strictEqual(node1.committed_chunks, 1);
  assert.strictEqual(node1.committed_bytes_used, 53);
  assert.strictEqual(node2.committed_chunks, 1);
  assert.strictEqual(node2.committed_bytes_used, 53);
  assert.strictEqual(node3.committed_chunks, 1);
  assert.strictEqual(node3.committed_bytes_used, 53);
  assert.strictEqual(node4.committed_chunks, 0);
  assert.strictEqual(node4.committed_bytes_used, 0);
  assert.strictEqual(node5.committed_chunks, 0);
  assert.strictEqual(node5.committed_bytes_used, 0);

  const totalCommittedSum = nodes.reduce((sum: number, n: any) => sum + n.committed_chunks, 0);
  const totalCommittedBytesSum = nodes.reduce((sum: number, n: any) => sum + n.committed_bytes_used, 0);
  assert.strictEqual(totalCommittedSum, 3, 'Node card committed chunks sum must equal 3');
  assert.strictEqual(totalCommittedBytesSum, 159, 'Node card committed bytes sum must equal 159');
  console.log(`  ✔ Node card committed replicas sum: ${totalCommittedSum} chunks, ${totalCommittedBytesSum} bytes.`);

  // -------------------------------------------------------------------------
  // 4. Physical Chunk Inventory & 4-Way Classification Audit
  // -------------------------------------------------------------------------
  console.log('\n[Test 4/6] Executing 4-Way Physical Chunk Classification Audit...');
  const auditRes = await fetch(`${BASE_URL}/api/v1/inventory/audit`);
  assert.strictEqual(auditRes.status, 200);
  const invAudit = await auditRes.json();
  const summary = invAudit.summary;

  assert.strictEqual(summary.actual_committed_replicas, 3, 'Must classify 3 committed replicas');
  assert.strictEqual(summary.actual_untracked_chunks, 1, 'Must classify 1 untracked chunk (node 4)');
  assert.strictEqual(summary.actual_orphaned_chunks, 7, 'Must classify 7 orphaned chunks (nodes 2, 3, 4)');
  assert.strictEqual(summary.actual_stale_replicas, 0, 'No corrupted/stale replicas');
  assert.strictEqual(summary.total_physical_chunks, 11, 'Total physical chunk files on disk must be 11');
  assert.strictEqual(summary.total_physical_bytes, 6341696, 'Total physical bytes must match disk inventory (~6.05 MB)');

  // Verify untracked chunk on Node 4
  const untrackedItem = invAudit.items.find((item: any) => item.classification === 'UNTRACKED_DATA');
  assert.ok(untrackedItem, 'Untracked chunk item must be present');
  assert.strictEqual(untrackedItem.node_id, 'storage-node-4');
  assert.strictEqual(untrackedItem.chunk_id, 'chk_715c3733d5ba411e');
  assert.strictEqual(untrackedItem.size_bytes, 53);
  assert.strictEqual(untrackedItem.sha_match, true, 'Untracked chunk has valid SHA-256 for testfile.txt');
  console.log(`  ✔ Untracked chunk on storage-node-4 safely classified and preserved: ${untrackedItem.chunk_id} (53B, SHA match: true)`);

  // Verify orphaned chunks
  const orphanedItems = invAudit.items.filter((item: any) => item.classification === 'ORPHANED_CHUNK');
  assert.strictEqual(orphanedItems.length, 7, 'Must detect exactly 7 orphaned chunks');
  const orphanChunkIds = new Set(orphanedItems.map((i: any) => i.chunk_id));
  assert.ok(orphanChunkIds.has('chk_2b7169e7eaf7492b'));
  assert.ok(orphanChunkIds.has('chk_9561403cee03491f'));
  assert.ok(orphanChunkIds.has('chk_c4ddf53fbca741f0'));
  console.log(`  ✔ 7 orphaned chunks detected across nodes 2, 3, 4: ${Array.from(orphanChunkIds).join(', ')}`);

  // -------------------------------------------------------------------------
  // 5. Leader Lease & Monotonic Fencing Token Monitored
  // -------------------------------------------------------------------------
  console.log('\n[Test 5/6] Verifying Leader Lease Countdown & Fencing Tokens...');
  const leaderRes = await fetch(`${BASE_URL}/api/v1/leader`);
  assert.strictEqual(leaderRes.status, 200);
  const leaderData = await leaderRes.json();

  assert.strictEqual(leaderData.is_leader, true);
  assert.ok(leaderData.time_remaining_seconds > 0 && leaderData.time_remaining_seconds <= 10.0);
  const currentToken = leaderData.fencing_token;
  console.log(`  ✔ Leader: ${leaderData.current_leader_id}, lease expires in ${leaderData.time_remaining_seconds.toFixed(2)}s, Fencing Token #${currentToken}`);

  // -------------------------------------------------------------------------
  // 6. Audit Log Generation & SQLite Persistence
  // -------------------------------------------------------------------------
  console.log('\n[Test 6/6] Verifying Persistent Audit Event Generation...');
  // Trigger cryptographic integrity audit to generate event
  const verifyRes = await fetch(`${BASE_URL}/api/v1/integrity/verify`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  });
  assert.strictEqual(verifyRes.status, 200);
  const verifyData = await verifyRes.json();
  assert.strictEqual(verifyData.valid_replicas, 3);
  assert.strictEqual(verifyData.corrupted_replicas, 0);

  // Check audit events
  const eventsRes = await fetch(`${BASE_URL}/api/v1/events?limit=20`);
  assert.strictEqual(eventsRes.status, 200);
  const events = await eventsRes.json();
  assert.ok(events.length > 0, 'Audit events must be recorded in SQLite');

  const auditEvent = events.find((e: any) => e.event_type === 'INTEGRITY_AUDIT_COMPLETED');
  assert.ok(auditEvent, 'INTEGRITY_AUDIT_COMPLETED event must be recorded');
  const bootEvent = events.find((e: any) => e.event_type === 'COORDINATOR_STARTED' || e.event_type === 'SYSTEM_BOOT');
  assert.ok(bootEvent, 'System startup event must be recorded');
  console.log(`  ✔ Audit events confirmed in persistent SQLite log (found ${events.length} events, latest: ${events[0].event_type}).`);

  console.log(`\n======================================================================`);
  console.log(` ALL 6/6 REGRESSION & CONSISTENCY AUDIT TESTS PASSED!`);
  console.log(`======================================================================\n`);
}

runRegressionAuditTests().catch(err => {
  console.error('\n❌ Regression Test Failure:', err);
  process.exit(1);
});
