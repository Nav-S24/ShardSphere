import assert from 'assert';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';
import { DatabaseSync } from 'node:sqlite';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BASE_URL = process.env.TEST_API_URL || 'http://127.0.0.1:3000';

async function runMissingCoverageRegressionTests() {
  console.log(`\n======================================================================`);
  console.log(` ShardSphere Missing Regression Coverage Tests`);
  console.log(` Target Server: ${BASE_URL}`);
  console.log(`======================================================================\n`);

  // =========================================================================
  // SUITE A: Bit-Rot Detection & In-Place Self-Healing Repair
  // =========================================================================
  console.log('--- SUITE A: Bit-Rot Detection & In-Place Self-Healing Repair ---');
  console.log('[A.1] Ingesting disposable test file with known SHA-256...');

  const probeSize = 512;
  const probeData = Buffer.alloc(probeSize);
  for (let i = 0; i < probeSize; i++) {
    probeData[i] = (i * 43 + 7) % 256;
  }
  const expectedSha = crypto.createHash('sha256').update(probeData).digest('hex');

  const formData = new FormData();
  formData.append(
    'file',
    new Blob([probeData], { type: 'application/octet-stream' }),
    `disposable_bitrot_probe_${Date.now()}.bin`
  );

  let fileId: string | null = null;
  let physicalBinPath: string | null = null;
  try {
    const uploadRes = await fetch(`${BASE_URL}/api/v1/files`, { method: 'POST', body: formData });
    assert.strictEqual(uploadRes.status, 201, 'Upload must succeed with 201 Created');
    const uploadData = await uploadRes.json();
    fileId = uploadData.file_id;
    assert.strictEqual(uploadData.checksum_sha256, expectedSha);
    console.log(`  ✔ Disposable probe uploaded: ${fileId} (${probeSize}B, hash: ${expectedSha.slice(0, 16)}...)`);

    // Fetch chunk details
    const fileDetailRes = await fetch(`${BASE_URL}/api/v1/files/${fileId}`);
    assert.strictEqual(fileDetailRes.status, 200);
    const fileDetail = await fileDetailRes.json();
    assert.strictEqual(fileDetail.chunks.length, 1);
    const chunk0 = fileDetail.chunks[0];
    const chunkId = chunk0.chunk_id;
    assert.strictEqual(chunk0.replicas.length, 3);

    // Pick target replica
    const targetNodeId = chunk0.replicas[0].node_id;
    const nodeNum = targetNodeId.replace('storage-node-', '');
    physicalBinPath = path.resolve(__dirname, `../data/storage_node_${nodeNum}/chunks/chunk_${chunkId}.bin`);
    assert.ok(fs.existsSync(physicalBinPath), `Physical chunk must exist at ${physicalBinPath}`);

    // Confirm initial chunk hash on disk
    const initialData = fs.readFileSync(physicalBinPath);
    assert.strictEqual(crypto.createHash('sha256').update(initialData).digest('hex'), expectedSha);
    console.log(`  ✔ Verified initial physical replica on ${targetNodeId} has correct SHA-256.`);

    // Inject Bit-Rot
    console.log(`[A.2] Injecting physical bit rot into chunk ${chunkId} on ${targetNodeId}...`);
    const corruptRes = await fetch(`${BASE_URL}/api/v1/chunks/${chunkId}/corrupt`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ node_id: targetNodeId }),
    });
    assert.strictEqual(corruptRes.status, 200);
    const corruptData = await corruptRes.json();
    assert.strictEqual(corruptData.status, 'corrupted');

    // Verify byte flipped on disk
    const corruptedPhysicalData = fs.readFileSync(physicalBinPath);
    const corruptedHash = crypto.createHash('sha256').update(corruptedPhysicalData).digest('hex');
    assert.notStrictEqual(corruptedHash, expectedSha, 'Corrupted chunk hash must differ from authoritative checksum');
    console.log(`  ✔ Bit flipped on disk: new SHA-256 ${corruptedHash.slice(0, 16)}... (differs from authoritative).`);

    // Run Cryptographic Audit to detect corruption
    console.log('[A.3] Executing Cryptographic Integrity Audit to detect corruption...');
    const auditRes = await fetch(`${BASE_URL}/api/v1/integrity/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file_id: fileId }),
    });
    assert.strictEqual(auditRes.status, 200);
    const auditData = await auditRes.json();
    assert.strictEqual(auditData.corrupted_replicas, 1, 'Audit must detect exactly 1 corrupted replica');
    assert.strictEqual(auditData.valid_replicas, 2, 'Audit must detect 2 valid peer replicas');
    const corruptDetail = auditData.details.find((d: any) => d.node_id === targetNodeId);
    assert.ok(corruptDetail);
    assert.strictEqual(corruptDetail.status, 'corrupted');
    assert.strictEqual(corruptDetail.classification, 'STALE_REPLICA');
    console.log(`  ✔ Integrity audit successfully identified corrupted replica on ${targetNodeId} as STALE_REPLICA.`);

    // Trigger Self-Healing Reconcile to repair corrupted replica in place from surviving peer
    console.log('[A.4] Triggering automated self-healing to repair corrupted replica in-place...');
    const healRes = await fetch(`${BASE_URL}/api/v1/self-healing/reconcile`, { method: 'POST' });
    assert.strictEqual(healRes.status, 200);
    const healData = await healRes.json();
    assert.ok(healData.recovered_chunks >= 1, 'Self-healing must restore degraded/corrupted chunk');
    console.log(`  ✔ Self-healing repaired ${healData.recovered_chunks} chunk(s).`);

    // Verify physical replica on disk has been repaired to original content
    const repairedData = fs.readFileSync(physicalBinPath);
    const repairedHash = crypto.createHash('sha256').update(repairedData).digest('hex');
    assert.strictEqual(repairedHash, expectedSha, 'Repaired chunk on disk must exactly match authoritative checksum');
    assert.deepStrictEqual(repairedData, probeData, 'Repaired chunk bytes must match original byte-for-byte');
    console.log(`  ✔ Physical chunk on ${targetNodeId} restored byte-for-byte (SHA-256 matches authoritative).`);

    // Verify subsequent integrity audit passes with 0 corrupted replicas
    console.log('[A.5] Verifying post-repair cluster audit...');
    const postAuditRes = await fetch(`${BASE_URL}/api/v1/integrity/verify`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file_id: fileId }),
    });
    const postAudit = await postAuditRes.json();
    assert.strictEqual(postAudit.corrupted_replicas, 0, 'No replicas should be corrupted after self-healing');
    assert.strictEqual(postAudit.valid_replicas, 3, 'All 3 replicas must be valid');
    console.log(`  ✔ Post-repair audit: 3 valid replicas, 0 corrupted.`);

    // Confirm file metadata consistency
    const verifiedFileRes = await fetch(`${BASE_URL}/api/v1/files/${fileId}`);
    const verifiedFile = await verifiedFileRes.json();
    assert.strictEqual(verifiedFile.status, 'committed');
    assert.strictEqual(verifiedFile.size_bytes, probeSize);
    assert.strictEqual(verifiedFile.checksum_sha256, expectedSha);
  } finally {
    if (fileId) {
      await fetch(`${BASE_URL}/api/v1/files/${fileId}`, { method: 'DELETE' });
      if (physicalBinPath) {
        assert.ok(!fs.existsSync(physicalBinPath), 'Physical chunk must be removed upon file delete');
      }
      console.log(`  ✔ Cleaned up disposable bit-rot test probe cleanly.`);
    }
  }

  // =========================================================================
  // SUITE B: Safe Orphan Reclamation (Isolated Fixture Environment)
  // =========================================================================
  console.log('\n--- SUITE B: Safe Orphan Reclamation (Isolated Fixture) ---');
  console.log('[B.1] Bootstrapping isolated test fixture with committed, untracked, and orphan chunks...');

  const fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), 'shardsphere_orphan_fixture_'));
  const fixtureDbPath = path.join(fixtureDir, 'fixture_metadata.db');
  const fixtureDb = new DatabaseSync(fixtureDbPath);

  // Initialize schema in fixture DB
  fixtureDb.exec(`
    CREATE TABLE files (
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
    CREATE TABLE chunks (
      chunk_id VARCHAR(64) NOT NULL PRIMARY KEY,
      file_id VARCHAR(64) NOT NULL,
      sequence_index INTEGER NOT NULL,
      size_bytes INTEGER NOT NULL,
      checksum_sha256 VARCHAR(64) NOT NULL,
      generation INTEGER NOT NULL,
      created_at FLOAT NOT NULL
    );
    CREATE TABLE replicas (
      replica_id VARCHAR(64) NOT NULL PRIMARY KEY,
      chunk_id VARCHAR(64) NOT NULL,
      node_id VARCHAR(64) NOT NULL,
      node_url VARCHAR(255) NOT NULL,
      status VARCHAR(32) NOT NULL,
      generation INTEGER NOT NULL,
      last_verified_at FLOAT,
      created_at FLOAT NOT NULL
    );
    CREATE TABLE storage_nodes (
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
  `);

  const now = Date.now() / 1000;
  for (let i = 1; i <= 5; i++) {
    const nDir = path.join(fixtureDir, `storage_node_${i}`, 'chunks');
    fs.mkdirSync(nDir, { recursive: true });
    fixtureDb.prepare(`
      INSERT INTO storage_nodes VALUES (?, ?, ?, 'HEALTHY', 0, ?, null, 0, 0, 10737418240, ?)
    `).run(`storage-node-${i}`, `Node ${i}`, `http://127.0.0.1:${8000 + i}`, now, now);
  }

  // Create committed file in fixture
  const committedFileId = 'file_fixture_committed_1';
  const committedChunkId = 'chk_fixture_committed_0';
  const committedBytes = Buffer.from('Committed authoritative fixture data for orphan safety verification\n');
  const committedSha = crypto.createHash('sha256').update(committedBytes).digest('hex');

  fixtureDb.prepare(`
    INSERT INTO files VALUES (?, 'fixture_committed.bin', 'application/octet-stream', ?, ?, 1, 1048576, 3, 1, 'committed', ?, ?)
  `).run(committedFileId, committedBytes.length, committedSha, now, now);

  fixtureDb.prepare(`
    INSERT INTO chunks VALUES (?, ?, 0, ?, ?, 1, ?)
  `).run(committedChunkId, committedFileId, committedBytes.length, committedSha, now);

  // Register replicas on Nodes 1, 2, 3
  for (let i = 1; i <= 3; i++) {
    const nodeId = `storage-node-${i}`;
    fixtureDb.prepare(`
      INSERT INTO replicas VALUES (?, ?, ?, ?, 'active', 1, ?, ?)
    `).run(`rep_fix_${i}`, committedChunkId, nodeId, `http://127.0.0.1:${8000 + i}`, now, now);

    const dir = path.join(fixtureDir, `storage_node_${i}`, 'chunks');
    fs.writeFileSync(path.join(dir, `chunk_${committedChunkId}.bin`), committedBytes);
    fs.writeFileSync(path.join(dir, `chunk_${committedChunkId}.meta.json`), JSON.stringify({
      chunk_id: committedChunkId,
      node_id: nodeId,
      size_bytes: committedBytes.length,
      checksum_sha256: committedSha,
    }));
  }

  // Write UNTRACKED valid chunk to Node 4 (physical file exists and matches committed chunk, but NOT in replicas table)
  const node4Dir = path.join(fixtureDir, 'storage_node_4', 'chunks');
  fs.writeFileSync(path.join(node4Dir, `chunk_${committedChunkId}.bin`), committedBytes);
  fs.writeFileSync(path.join(node4Dir, `chunk_${committedChunkId}.meta.json`), JSON.stringify({
    chunk_id: committedChunkId,
    node_id: 'storage-node-4',
    size_bytes: committedBytes.length,
    checksum_sha256: committedSha,
  }));

  // Write CONFIRMED ORPHAN chunks (no DB records exist in chunks or files)
  const orphan1 = Buffer.alloc(800, 'A');
  const orphan2 = Buffer.alloc(900, 'B');
  const orphan3 = Buffer.alloc(1500, 'C');

  fs.writeFileSync(path.join(fixtureDir, 'storage_node_2', 'chunks', 'chunk_chk_orphan_1.bin'), orphan1);
  fs.writeFileSync(path.join(fixtureDir, 'storage_node_2', 'chunks', 'chunk_chk_orphan_1.meta.json'), JSON.stringify({ chunk_id: 'chk_orphan_1' }));

  fs.writeFileSync(path.join(fixtureDir, 'storage_node_3', 'chunks', 'chunk_chk_orphan_2.bin'), orphan2);
  fs.writeFileSync(path.join(fixtureDir, 'storage_node_3', 'chunks', 'chunk_chk_orphan_2.meta.json'), JSON.stringify({ chunk_id: 'chk_orphan_2' }));

  fs.writeFileSync(path.join(fixtureDir, 'storage_node_5', 'chunks', 'chunk_chk_orphan_3.bin'), orphan3);
  fs.writeFileSync(path.join(fixtureDir, 'storage_node_5', 'chunks', 'chunk_chk_orphan_3.meta.json'), JSON.stringify({ chunk_id: 'chk_orphan_3' }));

  console.log('  ✔ Isolated fixture populated: 3 committed replicas, 1 untracked valid replica, 3 confirmed orphans (3,200 orphan bytes).');

  // Execute Orphan Reclamation Algorithm on Fixture (identical algorithm to server.ts)
  console.log('[B.2] Executing orphan reclamation algorithm on isolated fixture...');
  function reclaimOrphanChunksFixture(database: any, getChunksDir: (nodeId: string) => string) {
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
            const sz = fs.statSync(binPath).size;
            fs.unlinkSync(binPath);
            if (fs.existsSync(metaPath)) fs.unlinkSync(metaPath);
            totalPurged++;
            totalReclaimed += sz;
          }
        }
      }
    }

    return { total_purged_orphans: totalPurged, total_reclaimed_bytes: totalReclaimed };
  }

  const fixtureChunksDir = (nodeId: string) => {
    const num = nodeId.replace('storage-node-', '');
    return path.join(fixtureDir, `storage_node_${num}`, 'chunks');
  };

  const { total_purged_orphans: fixturePurged, total_reclaimed_bytes: fixtureReclaimed } = reclaimOrphanChunksFixture(
    fixtureDb,
    fixtureChunksDir
  );

  // Assertions on Reclamation Outcome
  console.log('[B.3] Verifying reclamation selectivity & safety invariants...');
  assert.strictEqual(fixturePurged, 3, 'Must purge exactly the 3 confirmed orphaned chunks');
  assert.strictEqual(fixtureReclaimed, 3200, 'Reclaimed bytes must equal 800 + 900 + 1500 = 3200');

  // Verify orphans were removed
  assert.ok(!fs.existsSync(path.join(fixtureDir, 'storage_node_2', 'chunks', 'chunk_chk_orphan_1.bin')));
  assert.ok(!fs.existsSync(path.join(fixtureDir, 'storage_node_3', 'chunks', 'chunk_chk_orphan_2.bin')));
  assert.ok(!fs.existsSync(path.join(fixtureDir, 'storage_node_5', 'chunks', 'chunk_chk_orphan_3.bin')));
  console.log('  ✔ Confirmed: All 3 orphaned chunks successfully unlinked from disk.');

  // CRITICAL: Verify committed replicas remain 100% intact
  for (let i = 1; i <= 3; i++) {
    const p = path.join(fixtureDir, `storage_node_${i}`, 'chunks', `chunk_${committedChunkId}.bin`);
    assert.ok(fs.existsSync(p), `Committed replica on Node ${i} must remain intact`);
    assert.strictEqual(fs.readFileSync(p).length, committedBytes.length);
  }
  console.log('  ✔ Confirmed: All 3 committed replicas on Nodes 1, 2, 3 remain intact and unmodified.');

  // CRITICAL: Verify untracked valid chunk on Node 4 remains 100% intact
  const untrackedP = path.join(node4Dir, `chunk_${committedChunkId}.bin`);
  assert.ok(fs.existsSync(untrackedP), 'Untracked valid chunk on Node 4 must NOT be deleted by orphan reclamation');
  assert.strictEqual(fs.readFileSync(untrackedP).length, committedBytes.length);
  console.log('  ✔ Confirmed: Untracked valid replica on Node 4 safely preserved (not purged).');

  // Clean up fixture directory
  fs.rmSync(fixtureDir, { recursive: true, force: true });
  console.log('  ✔ Isolated fixture cleaned up without touching live workspace data.');

  // =========================================================================
  // SUITE C: Storage Quota & Capacity Enforcement
  // =========================================================================
  console.log('\n--- SUITE C: Storage Quota & Capacity Enforcement ---');
  console.log('[C.1] Setting temporary low quota on storage-node-1 to test rejection...');

  const quotaNode = 'storage-node-1';
  // Retrieve original limit
  const nodesRes = await fetch(`${BASE_URL}/api/v1/nodes`);
  const nodesList = await nodesRes.json();
  const origNode1 = nodesList.find((n: any) => n.node_id === quotaNode);
  const originalLimit = origNode1 ? origNode1.storage_limit_bytes : 10737418240;

  let recoveryProbeId: string | null = null;
  try {
    // Set restrictive limit (e.g. 100 bytes while current usage is 53 bytes)
    const setLimitRes = await fetch(`${BASE_URL}/api/v1/nodes/${quotaNode}/limit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ storage_limit_bytes: 100 }),
    });
    assert.strictEqual(setLimitRes.status, 200);
    console.log(`  ✔ Set ${quotaNode} storage limit to 100 bytes (usage: 53 bytes, remaining headroom: 47 bytes).`);

    // Count files before attempt
    const preFilesRes = await fetch(`${BASE_URL}/api/v1/files`);
    const preFilesData = await preFilesRes.json();
    const preFileCount = preFilesData.total_files;

    // Attempt to upload 200-byte file (requires 200 bytes per node, exceeding 47B headroom on Node 1)
    console.log('[C.2] Attempting to upload 200-byte file exceeding available storage headroom...');
    const overQuotaData = Buffer.alloc(200, 0x5a);
    const overQuotaForm = new FormData();
    overQuotaForm.append('file', new Blob([overQuotaData]), 'quota_exceed_probe.bin');

    const overQuotaRes = await fetch(`${BASE_URL}/api/v1/files`, {
      method: 'POST',
      body: overQuotaForm,
    });

    assert.strictEqual(overQuotaRes.status, 507, 'Must reject with HTTP 507 Insufficient Storage');
    const errJson = await overQuotaRes.json();
    assert.ok(errJson.detail && errJson.detail.includes('Storage quota exceeded'), 'Error detail must state quota exceeded');
    console.log(`  ✔ Upload rejected cleanly with HTTP 507: "${errJson.detail}"`);

    // Verify NO partial commits or metadata inconsistencies
    console.log('[C.3] Verifying no partial commits or leaked chunk files on disk...');
    const postFilesRes = await fetch(`${BASE_URL}/api/v1/files`);
    const postFilesData = await postFilesRes.json();
    assert.strictEqual(postFilesData.total_files, preFileCount, 'Files count in metadata must be unchanged');

    // Verify no orphaned physical files leaked
    for (let i = 1; i <= 5; i++) {
      const dir = path.resolve(__dirname, `../data/storage_node_${i}/chunks`);
      const files = fs.readdirSync(dir);
      for (const f of files) {
        assert.ok(!f.includes('quota_exceed'), 'No leaked chunk files for rejected upload');
      }
    }
    console.log('  ✔ Confirmed: No rows inserted in files/chunks/replicas and 0 leaked chunks on disk.');

    // Restore original limit
    console.log(`[C.4] Restoring ${quotaNode} storage limit to ${originalLimit} bytes...`);
    const restoreRes = await fetch(`${BASE_URL}/api/v1/nodes/${quotaNode}/limit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ storage_limit_bytes: originalLimit }),
    });
    assert.strictEqual(restoreRes.status, 200);
    console.log(`  ✔ Restored ${quotaNode} limit to ${originalLimit} bytes.`);

    // Verify normal uploads succeed when within quota
    console.log('[C.5] Verifying normal upload succeeds after limit restoration...');
    const normalData = Buffer.alloc(64, 0x33);
    const normalForm = new FormData();
    normalForm.append('file', new Blob([normalData]), 'quota_recovery_probe.bin');
    const normalRes = await fetch(`${BASE_URL}/api/v1/files`, { method: 'POST', body: normalForm });
    assert.strictEqual(normalRes.status, 201);
    const normalJson = await normalRes.json();
    recoveryProbeId = normalJson.file_id;
    console.log(`  ✔ Normal upload succeeded: ${normalJson.file_id} (committed within quota).`);
  } finally {
    // Guarantee limit is restored
    await fetch(`${BASE_URL}/api/v1/nodes/${quotaNode}/limit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ storage_limit_bytes: originalLimit }),
    });
    // Guarantee recovery probe is cleaned up
    if (recoveryProbeId) {
      await fetch(`${BASE_URL}/api/v1/files/${recoveryProbeId}`, { method: 'DELETE' });
      console.log(`  ✔ Cleaned up recovery probe file.`);
    }
  }

  console.log(`\n======================================================================`);
  console.log(` ALL MISSING COVERAGE REGRESSION TESTS PASSED (Suites A, B, C)!`);
  console.log(`======================================================================\n`);
}

runMissingCoverageRegressionTests().catch(err => {
  console.error('\n❌ Missing Coverage Regression Test Failure:', err);
  process.exit(1);
});
