import assert from 'assert';
import crypto from 'crypto';

const BASE_URL = process.env.TEST_API_URL || 'http://127.0.0.1:3000';

async function runWorkflowTests() {
  console.log(`\n======================================================================`);
  console.log(` ShardSphere Lamport Workflow Integration Suite (Phase 4)`);
  console.log(` Target Server: ${BASE_URL}`);
  console.log(`======================================================================\n`);

  // [Test 1] Verify Clock Snapshot API and Response Headers
  console.log('[Test 1/6] Verifying Clock Vector Snapshot API & X-Lamport-Clock Headers...');
  const clocksRes = await fetch(`${BASE_URL}/api/v1/clocks`);
  assert.strictEqual(clocksRes.status, 200, '/api/v1/clocks should return 200');
  const clockHeader = clocksRes.headers.get('x-lamport-clock');
  assert.ok(clockHeader, 'Response must include X-Lamport-Clock header');
  const clocksData = await clocksRes.json();
  assert.strictEqual(clocksData.coordinator.id, 'coordinator-primary-1');
  assert.ok(typeof clocksData.coordinator.clock === 'number', 'Coordinator clock must be a number');
  assert.strictEqual(clocksData.nodes.length, 5, 'Must report all 5 storage nodes');
  for (const n of clocksData.nodes) {
    assert.ok(n.id.startsWith('storage-node-'), 'Storage node ID must match convention');
    assert.ok(typeof n.clock === 'number', 'Node clock must be a number');
  }
  const clockAliasRes = await fetch(`${BASE_URL}/api/v1/clock`);
  assert.strictEqual(clockAliasRes.status, 200, '/api/v1/clock alias should return 200');
  console.log(`  ✔ Clock vector verified: coordinator L:${clocksData.coordinator.clock}, highest observed L:${clocksData.highest_observed}`);

  // [Test 2] Verify File Upload & Multi-Node Replication Causal Progression
  console.log('\n[Test 2/6] Verifying File Upload, Chunking & Replication Lamport Progression...');
  const probeData = Buffer.from('Lamport Causal Storage Invariant Test Payload ' + Date.now());
  const expectedHash = crypto.createHash('sha256').update(probeData).digest('hex');

  const formData = new FormData();
  formData.append('file', new Blob([probeData], { type: 'text/plain' }), 'causal_probe.txt');

  const uploadRes = await fetch(`${BASE_URL}/api/v1/files`, {
    method: 'POST',
    body: formData,
    headers: {
      'X-Lamport-Clock': String(clocksData.coordinator.clock),
    },
  });
  assert.strictEqual(uploadRes.status, 201, 'Upload should succeed with 201');
  const uploadInfo = await uploadRes.json();
  assert.ok(uploadInfo.lamport_clock > clocksData.coordinator.clock, 'Commit clock must advance coordinator clock');
  console.log(`  ✔ File committed: ${uploadInfo.file_id} with Lamport Clock L:${uploadInfo.lamport_clock}`);

  // Verify file details contain logical clocks
  const detailRes = await fetch(`${BASE_URL}/api/v1/files/${uploadInfo.file_id}`);
  assert.strictEqual(detailRes.status, 200);
  const detail = await detailRes.json();
  assert.strictEqual(detail.lamport_clock, uploadInfo.lamport_clock);
  assert.strictEqual(detail.chunks.length, 1);
  const chunk = detail.chunks[0];
  assert.ok(chunk.lamport_clock > 0, 'Chunk must have positive logical clock');
  assert.strictEqual(chunk.replicas.length, 3, 'Must have 3 replicas');
  for (const rep of chunk.replicas) {
    assert.ok(rep.lamport_clock > 0, `Replica on ${rep.node_id} must have positive logical clock`);
    assert.ok(rep.lamport_clock <= uploadInfo.lamport_clock, 'Replica logical clock must precede or equal commit clock');
  }
  console.log(`  ✔ Chunk & replica clocks validated: Chunk L:${chunk.lamport_clock}, Replicas: [${chunk.replicas.map((r: any) => `${r.node_id}:L${r.lamport_clock}`).join(', ')}]`);

  // [Test 3] Verify Events Causality & Lineage (UPLOAD_INITIATED -> FILE_COMMITTED)
  console.log('\n[Test 3/6] Verifying Event Causality Lineage & Correlation Linking...');
  const fileEventsRes = await fetch(`${BASE_URL}/api/v1/events?file_id=${uploadInfo.file_id}&order_by=lamport&direction=asc`);
  assert.strictEqual(fileEventsRes.status, 200);
  const fileEvents = await fileEventsRes.json();
  assert.ok(fileEvents.length >= 2, 'Must have at least UPLOAD_INITIATED and FILE_COMMITTED events');

  const initEvent = fileEvents.find((e: any) => e.event_type === 'UPLOAD_INITIATED');
  const commitEvent = fileEvents.find((e: any) => e.event_type === 'FILE_COMMITTED');

  assert.ok(initEvent, 'UPLOAD_INITIATED event must exist');
  assert.ok(commitEvent, 'FILE_COMMITTED event must exist');
  assert.ok(initEvent.lamport_clock < commitEvent.lamport_clock, `Happens-before invariant: init clock (${initEvent.lamport_clock}) < commit clock (${commitEvent.lamport_clock})`);
  assert.strictEqual(commitEvent.causal_parent_id, initEvent.event_id, 'FILE_COMMITTED must cite UPLOAD_INITIATED as causal parent');
  assert.strictEqual(initEvent.correlation_id, uploadInfo.file_id, 'Correlation ID must match file_id');
  assert.strictEqual(commitEvent.correlation_id, uploadInfo.file_id, 'Correlation ID must match file_id');
  console.log(`  ✔ Causal lineage verified: ${initEvent.event_id} (L:${initEvent.lamport_clock}) -> ${commitEvent.event_id} (L:${commitEvent.lamport_clock})`);

  // [Test 4] Verify Deterministic Sorting Order
  console.log('\n[Test 4/6] Verifying Deterministic Event Sorting by (Lamport Clock, Origin Node, Event ID)...');
  const sortedEventsRes = await fetch(`${BASE_URL}/api/v1/events?order_by=lamport&direction=desc&limit=50`);
  assert.strictEqual(sortedEventsRes.status, 200);
  const sortedEvents = await sortedEventsRes.json();
  assert.ok(sortedEvents.length > 0, 'Should return events');

  for (let i = 0; i < sortedEvents.length - 1; i++) {
    const cur = sortedEvents[i];
    const nxt = sortedEvents[i + 1];
    if (cur.lamport_clock === nxt.lamport_clock) {
      if (cur.origin_node_id === nxt.origin_node_id) {
        assert.ok(cur.event_id <= nxt.event_id, `Tie breaker failed for event_id: ${cur.event_id} should <= ${nxt.event_id}`);
      } else {
        assert.ok(cur.origin_node_id <= nxt.origin_node_id, `Tie breaker failed for origin_node_id: ${cur.origin_node_id} should <= ${nxt.origin_node_id}`);
      }
    } else {
      assert.ok(cur.lamport_clock > nxt.lamport_clock, `Descending order violation: ${cur.lamport_clock} should be > ${nxt.lamport_clock}`);
    }
  }
  console.log(`  ✔ Deterministic sorting key (lamport_clock, origin_node_id, event_id) verified across ${sortedEvents.length} events.`);

  // [Test 5] Verify Fault Injection, Self-Healing, and Causal Restoration
  console.log('\n[Test 5/6] Verifying Node Failure, Bit-Rot Repair & Self-Healing Causal Flow...');
  const targetReplica = chunk.replicas[0];
  const targetNodeId = targetReplica.node_id;

  // Inject bit rot into replica
  const corruptRes = await fetch(`${BASE_URL}/api/v1/chunks/${chunk.chunk_id}/corrupt`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ node_id: targetNodeId }),
  });
  assert.strictEqual(corruptRes.status, 200);
  const corruptData = await corruptRes.json();
  assert.ok(corruptData.lamport_clock > 0, 'Bit rot injection must advance node clock');

  // Verify cryptographic audit detects it
  const verifyRes = await fetch(`${BASE_URL}/api/v1/integrity/verify`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ file_id: uploadInfo.file_id }),
  });
  assert.strictEqual(verifyRes.status, 200);
  const verifyData = await verifyRes.json();
  assert.ok(verifyData.corrupted_replicas >= 1, 'Audit must detect corrupted replica');
  assert.ok(verifyData.lamport_clock > 0, 'Audit must record logical completion clock');

  // Trigger self-healing
  const healRes = await fetch(`${BASE_URL}/api/v1/self-healing/reconcile`, { method: 'POST' });
  assert.strictEqual(healRes.status, 200);
  const healData = await healRes.json();
  assert.ok(healData.recovered_chunks >= 1, 'Self healing must repair degraded replica');
  assert.ok(healData.lamport_clock > 0, 'Self healing must return coordinator lamport clock');

  // Verify REPLICATION_RESTORED event
  const restoredEventsRes = await fetch(`${BASE_URL}/api/v1/events?event_type=REPLICATION_RESTORED&limit=5`);
  const restoredEvents = await restoredEventsRes.json();
  assert.ok(restoredEvents.length > 0, 'REPLICATION_RESTORED event must be recorded');
  console.log(`  ✔ Self-healing causal recovery verified (Restored event L:${restoredEvents[0].lamport_clock} on node ${restoredEvents[0].origin_node_id}).`);

  // [Test 6] Verify File Deletion & Multi-Node Reclamation
  console.log('\n[Test 6/6] Verifying File Deletion & Multi-Node Reclamation Clock Synchronization...');
  const deleteRes = await fetch(`${BASE_URL}/api/v1/files/${uploadInfo.file_id}`, { method: 'DELETE' });
  assert.strictEqual(deleteRes.status, 200);
  const deleteData = await deleteRes.json();
  assert.strictEqual(deleteData.deleted, true);
  assert.ok(deleteData.lamport_clock > 0, 'Delete must advance Lamport clock');

  // Verify FILE_DELETED event
  const delEventRes = await fetch(`${BASE_URL}/api/v1/events?event_type=FILE_DELETED&file_id=${uploadInfo.file_id}`);
  const delEvents = await delEventRes.json();
  assert.ok(delEvents.length > 0, 'FILE_DELETED event must be present in SQLite');
  assert.strictEqual(delEvents[0].origin_node_id, 'coordinator-primary-1');
  assert.strictEqual(delEvents[0].correlation_id, uploadInfo.file_id);
  console.log(`  ✔ File deletion event verified: ${delEvents[0].event_id} (L:${delEvents[0].lamport_clock}).`);

  console.log(`\n======================================================================`);
  console.log(` ALL 6/6 LAMPORT WORKFLOW INTEGRATION TESTS PASSED!`);
  console.log(`======================================================================\n`);
}

runWorkflowTests().catch((err) => {
  console.error('\n❌ Workflow Integration Test Failed:', err);
  process.exit(1);
});
