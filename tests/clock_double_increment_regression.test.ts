import assert from 'assert';

const BASE_URL = process.env.TEST_API_URL || 'http://127.0.0.1:3000';

async function runDoubleIncrementRegressionTests() {
  console.log(`\n======================================================================`);
  console.log(` ShardSphere Lamport Clock Double Increment Regression Suite`);
  console.log(` Target Server: ${BASE_URL}`);
  console.log(`======================================================================\n`);

  // -------------------------------------------------------------------------
  // 1. Integrity Audit Completion: No redundant tick, response matches event
  // -------------------------------------------------------------------------
  console.log('[Test 1/4] Verifying Integrity Audit Completion Clock Accuracy...');
  const verifyRes = await fetch(`${BASE_URL}/api/v1/integrity/verify`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  });
  assert.strictEqual(verifyRes.status, 200, 'Integrity audit must return 200 OK');
  const verifyData = await verifyRes.json();
  assert.ok(typeof verifyData.lamport_clock === 'number', 'Response must include lamport_clock');

  // Fetch the latest INTEGRITY_AUDIT_COMPLETED event recorded by this audit
  const auditEventsRes = await fetch(`${BASE_URL}/api/v1/events?event_type=INTEGRITY_AUDIT_COMPLETED&limit=1&order_by=timestamp&direction=desc`);
  assert.strictEqual(auditEventsRes.status, 200);
  const auditEvents = await auditEventsRes.json();
  assert.ok(auditEvents.length > 0, 'Audit event must exist');
  const latestAuditEvent = auditEvents[0];

  assert.strictEqual(
    verifyData.lamport_clock,
    latestAuditEvent.lamport_clock,
    `Audit response clock (${verifyData.lamport_clock}) must match recorded event clock (${latestAuditEvent.lamport_clock}) exactly with zero redundant ticks`
  );

  // Check coordinator clock matches the event clock
  const clocksRes1 = await fetch(`${BASE_URL}/api/v1/clocks`);
  const clocks1 = await clocksRes1.json();
  assert.strictEqual(
    clocks1.coordinator.clock,
    latestAuditEvent.lamport_clock,
    `Coordinator clock (${clocks1.coordinator.clock}) must equal audit event clock (${latestAuditEvent.lamport_clock}) without extra trailing ticks`
  );
  console.log(`  ✔ Integrity audit completion verified: Event L:${latestAuditEvent.lamport_clock} == Response L:${verifyData.lamport_clock} == Coordinator L:${clocks1.coordinator.clock}`);

  // -------------------------------------------------------------------------
  // 2. Bit-Rot Injection: Consistent recorded timestamp, no double increment
  // -------------------------------------------------------------------------
  console.log('\n[Test 2/4] Verifying Bit-Rot Injection Timestamp Consistency...');
  // Find an active chunk
  const filesRes = await fetch(`${BASE_URL}/api/v1/files`);
  const filesData = await filesRes.json();
  assert.ok(filesData.files.length > 0, 'At least 1 file must exist');
  const fileId = filesData.files[0].file_id;

  const fileDetailRes = await fetch(`${BASE_URL}/api/v1/files/${fileId}`);
  const fileDetail = await fileDetailRes.json();
  const testChunk = fileDetail.chunks[0];
  const targetNodeId = testChunk.replicas[0].node_id;

  // Record node clock before injection
  const nodePreClock = clocks1.nodes.find((n: any) => n.id === targetNodeId)!.clock;

  // Corrupt replica
  const corruptRes = await fetch(`${BASE_URL}/api/v1/chunks/${testChunk.chunk_id}/corrupt`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ node_id: targetNodeId }),
  });
  assert.strictEqual(corruptRes.status, 200);
  const corruptData = await corruptRes.json();

  // Fetch the latest BIT_ROT_INJECTED event
  const rotEventsRes = await fetch(`${BASE_URL}/api/v1/events?event_type=BIT_ROT_INJECTED&limit=1&order_by=timestamp&direction=desc`);
  const rotEvents = await rotEventsRes.json();
  assert.ok(rotEvents.length > 0, 'Bit rot event must be logged');
  const latestRotEvent = rotEvents[0];

  assert.strictEqual(
    corruptData.lamport_clock,
    latestRotEvent.lamport_clock,
    `Corrupt response clock (${corruptData.lamport_clock}) must match recorded event clock (${latestRotEvent.lamport_clock})`
  );

  // Storage node clock must have advanced by exactly 1 local event (not 2)
  assert.strictEqual(
    latestRotEvent.lamport_clock,
    nodePreClock + 1,
    `Bit rot event clock (${latestRotEvent.lamport_clock}) must advance node clock (${nodePreClock}) by exactly 1`
  );
  console.log(`  ✔ Bit-rot injection clock verified: Node pre-clock L:${nodePreClock} -> Event/Response L:${latestRotEvent.lamport_clock} (advance = exactly 1)`);

  // Heal replica back so test leaves cluster clean
  const healRes = await fetch(`${BASE_URL}/api/v1/self-healing/reconcile`, { method: 'POST' });
  assert.strictEqual(healRes.status, 200);
  console.log(`  ✔ Healed corrupted replica back to healthy state`);

  // -------------------------------------------------------------------------
  // 3. Node Reconciliation: Legitimate Event & ACK preserved, no redundant tick
  // -------------------------------------------------------------------------
  console.log('\n[Test 3/4] Verifying Node Reconciliation Clock Sequence...');
  const reconNodeId = 'storage-node-5';
  const preReconClocksRes = await fetch(`${BASE_URL}/api/v1/clocks`);
  const preReconClocks = await preReconClocksRes.json();
  const nodeStartClock = preReconClocks.nodes.find((n: any) => n.id === reconNodeId)!.clock;

  const nodeReconRes = await fetch(`${BASE_URL}/api/v1/nodes/${reconNodeId}/reconcile`, { method: 'POST' });
  assert.strictEqual(nodeReconRes.status, 200);
  const nodeReconData = await nodeReconRes.json();

  // Fetch latest NODE_RECONCILED event for this node
  const nodeEventsRes = await fetch(`${BASE_URL}/api/v1/events?event_type=NODE_RECONCILED&limit=1&order_by=timestamp&direction=desc`);
  const nodeEvents = await nodeEventsRes.json();
  assert.ok(nodeEvents.length > 0, 'NODE_RECONCILED event must be recorded');
  const latestReconEvent = nodeEvents[0];

  // 1. The local reconciliation event should advance node clock by exactly 1
  assert.strictEqual(
    latestReconEvent.lamport_clock,
    nodeStartClock + 1,
    `Reconciliation local event clock (${latestReconEvent.lamport_clock}) should be startClock (${nodeStartClock}) + 1`
  );

  // 2. The ACK sent to coordinator should advance node clock by another 1 (Rule 2 Send)
  const expectedAckClock = latestReconEvent.lamport_clock + 1;
  assert.strictEqual(
    nodeReconData.lamport_clock,
    expectedAckClock,
    `Returned node clock (${nodeReconData.lamport_clock}) must equal ACK clock (${expectedAckClock})`
  );

  // 3. Storage node clock on server must match the ACK clock (persisted clock)
  const postReconClocksRes = await fetch(`${BASE_URL}/api/v1/clocks`);
  const postReconClocks = await postReconClocksRes.json();
  const nodeFinalClock = postReconClocks.nodes.find((n: any) => n.id === reconNodeId)!.clock;
  assert.strictEqual(
    nodeFinalClock,
    expectedAckClock,
    `Persisted node clock (${nodeFinalClock}) must match returned ACK clock (${expectedAckClock})`
  );

  // Total advance on storage node must be exactly +2 (1 local event + 1 send ACK), NOT +3
  assert.strictEqual(
    nodeFinalClock - nodeStartClock,
    2,
    `Total node clock advance must be exactly 2 ticks (local event + send ACK), got ${nodeFinalClock - nodeStartClock}`
  );
  console.log(`  ✔ Node reconciliation clock progression verified: Start L:${nodeStartClock} -> Recon Event L:${latestReconEvent.lamport_clock} -> ACK/Persisted L:${nodeFinalClock} (total = exactly 2 ticks)`);

  // -------------------------------------------------------------------------
  // 4. Monotonicity & Causal Ordering Invariants
  // -------------------------------------------------------------------------
  console.log('\n[Test 4/4] Verifying Cluster-Wide Monotonicity & Causal Lineage...');
  const allEventsRes = await fetch(`${BASE_URL}/api/v1/events?limit=100&order_by=timestamp&direction=desc`);
  const allEvents = await allEventsRes.json();
  assert.ok(allEvents.length > 0);

  // Verify all events have positive integer Lamport clocks
  for (const ev of allEvents) {
    assert.ok(Number.isInteger(ev.lamport_clock) && ev.lamport_clock > 0, `Event ${ev.event_id} must have positive integer clock`);
  }

  // Verify causal parent happens-before: if event has causal_parent_id, child clock > parent clock
  const eventMap = new Map<string, any>(allEvents.map((e: any) => [e.event_id, e]));
  let parentChecks = 0;
  for (const ev of allEvents) {
    if (ev.causal_parent_id && eventMap.has(ev.causal_parent_id)) {
      const parent = eventMap.get(ev.causal_parent_id)!;
      assert.ok(
        ev.lamport_clock > parent.lamport_clock,
        `Causal ordering violation: Child ${ev.event_id} (L:${ev.lamport_clock}) must be > Parent ${parent.event_id} (L:${parent.lamport_clock})`
      );
      parentChecks++;
    }
  }
  console.log(`  ✔ Monotonicity verified across ${allEvents.length} events (${parentChecks} causal parent links checked).`);

  console.log(`\n======================================================================`);
  console.log(` ALL 4/4 DOUBLE INCREMENT REGRESSION TESTS PASSED!`);
  console.log(`======================================================================\n`);
}

runDoubleIncrementRegressionTests().catch((err) => {
  console.error('\n❌ Regression Test Failed:', err);
  process.exit(1);
});
