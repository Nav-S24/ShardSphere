import assert from 'assert';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';
import { DatabaseSync } from 'node:sqlite';
import {
  LamportClockEngine,
  COORDINATOR_PARTICIPANT_ID,
  STORAGE_NODE_PARTICIPANT_IDS,
  InvalidParticipantError,
  InvalidClockValueError,
  ClockOverflowError,
  ClockPersistenceError,
} from '../src/clock/index.ts';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function runLamportClockEngineTests() {
  console.log(`\n======================================================================`);
  console.log(` ShardSphere Lamport Clock Engine — Focused Test Suite (Phase 3)`);
  console.log(`======================================================================\n`);

  // Helper to create an isolated temporary SQLite database for testing
  function createTestDb(): { db: DatabaseSync; dir: string; dbPath: string; cleanup: () => void } {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'shardsphere_clock_test_'));
    const dbPath = path.join(dir, 'test_metadata.db');
    const db = new DatabaseSync(dbPath);
    
    // Seed baseline tables
    db.exec(`
      CREATE TABLE IF NOT EXISTS cluster_events (
        event_id VARCHAR(64) PRIMARY KEY,
        event_type VARCHAR(64) NOT NULL,
        severity VARCHAR(16) NOT NULL,
        node_id VARCHAR(64),
        file_id VARCHAR(64),
        message TEXT NOT NULL,
        timestamp FLOAT NOT NULL
      );
    `);

    return {
      db,
      dir,
      dbPath,
      cleanup: () => {
        try {
          fs.rmSync(dir, { recursive: true, force: true });
        } catch {
          // ignore
        }
      },
    };
  }

  // -------------------------------------------------------------------------
  // Test 1: Local Clock Increment
  // -------------------------------------------------------------------------
  console.log('[Test 1/12] Verifying Local Clock Increment (Rule 1)...');
  {
    const engine = new LamportClockEngine();
    assert.strictEqual(engine.getClock(COORDINATOR_PARTICIPANT_ID), 0, 'Initial clock should be 0');
    
    const tick1 = engine.tickLocal(COORDINATOR_PARTICIPANT_ID);
    assert.strictEqual(tick1, 1, 'Local tick should increment from 0 to 1');
    assert.strictEqual(engine.getClock(COORDINATOR_PARTICIPANT_ID), 1);

    const tick2 = engine.tickLocal(COORDINATOR_PARTICIPANT_ID);
    assert.strictEqual(tick2, 2, 'Local tick should increment from 1 to 2');
    assert.strictEqual(engine.getClock(COORDINATOR_PARTICIPANT_ID), 2);
    console.log('  ✔ Local clock increment verified: 0 -> 1 -> 2');
  }

  // -------------------------------------------------------------------------
  // Test 2: Send Timestamp Generation
  // -------------------------------------------------------------------------
  console.log('\n[Test 2/12] Verifying Send Timestamp Generation (Rule 2)...');
  {
    const engine = new LamportClockEngine();
    const sendTimestamp1 = engine.tickSend(COORDINATOR_PARTICIPANT_ID);
    assert.strictEqual(sendTimestamp1, 1, 'Send tick should advance clock to 1 and return 1');
    assert.strictEqual(engine.getClock(COORDINATOR_PARTICIPANT_ID), 1);

    const sendTimestamp2 = engine.tickSend(COORDINATOR_PARTICIPANT_ID);
    assert.strictEqual(sendTimestamp2, 2, 'Send tick should advance clock to 2 and return 2');
    assert.strictEqual(engine.getClock(COORDINATOR_PARTICIPANT_ID), 2);
    console.log('  ✔ Send timestamp generation verified: outbound messages receive incremented clock');
  }

  // -------------------------------------------------------------------------
  // Test 3: Receive with Lower Incoming Timestamp
  // -------------------------------------------------------------------------
  console.log('\n[Test 3/12] Verifying Receive with Lower Incoming Timestamp (Rule 3)...');
  {
    const engine = new LamportClockEngine();
    // Advance local clock to 10
    for (let i = 0; i < 10; i++) {
      engine.tickLocal('storage-node-1');
    }
    assert.strictEqual(engine.getClock('storage-node-1'), 10);

    // Receive message with clock 4 (lower than local clock 10)
    const newClock = engine.tickReceive('storage-node-1', 4);
    assert.strictEqual(newClock, 11, 'Receive with lower timestamp (4 < 10) should advance to max(10, 4) + 1 = 11');
    assert.strictEqual(engine.getClock('storage-node-1'), 11);
    console.log('  ✔ Receive with lower timestamp verified: max(10, 4) + 1 = 11');
  }

  // -------------------------------------------------------------------------
  // Test 4: Receive with Higher Incoming Timestamp
  // -------------------------------------------------------------------------
  console.log('\n[Test 4/12] Verifying Receive with Higher Incoming Timestamp (Rule 3)...');
  {
    const engine = new LamportClockEngine();
    // Local clock is 3
    engine.tickLocal('storage-node-2');
    engine.tickLocal('storage-node-2');
    engine.tickLocal('storage-node-2');
    assert.strictEqual(engine.getClock('storage-node-2'), 3);

    // Receive message with clock 25 (higher than local clock 3)
    const newClock = engine.tickReceive('storage-node-2', 25);
    assert.strictEqual(newClock, 26, 'Receive with higher timestamp (25 > 3) should advance to max(3, 25) + 1 = 26');
    assert.strictEqual(engine.getClock('storage-node-2'), 26);
    console.log('  ✔ Receive with higher timestamp verified: max(3, 25) + 1 = 26');
  }

  // -------------------------------------------------------------------------
  // Test 5: Receive with Equal Incoming Timestamp
  // -------------------------------------------------------------------------
  console.log('\n[Test 5/12] Verifying Receive with Equal Incoming Timestamp (Rule 3)...');
  {
    const engine = new LamportClockEngine();
    // Advance local clock to 7
    for (let i = 0; i < 7; i++) {
      engine.tickLocal('storage-node-3');
    }
    assert.strictEqual(engine.getClock('storage-node-3'), 7);

    // Receive message with clock 7 (equal to local clock)
    const newClock = engine.tickReceive('storage-node-3', 7);
    assert.strictEqual(newClock, 8, 'Receive with equal timestamp (7 == 7) should advance to max(7, 7) + 1 = 8');
    assert.strictEqual(engine.getClock('storage-node-3'), 8);
    console.log('  ✔ Receive with equal timestamp verified: max(7, 7) + 1 = 8');
  }

  // -------------------------------------------------------------------------
  // Test 6: Independent Clocks for Different Participants
  // -------------------------------------------------------------------------
  console.log('\n[Test 6/12] Verifying Clock Independence Across All Participants...');
  {
    const engine = new LamportClockEngine();
    
    // Advance coordinator 5 times
    for (let i = 0; i < 5; i++) engine.tickLocal(COORDINATOR_PARTICIPANT_ID);
    
    // Advance node-1 2 times
    engine.tickLocal('storage-node-1');
    engine.tickLocal('storage-node-1');

    // Advance node-4 12 times
    for (let i = 0; i < 12; i++) engine.tickLocal('storage-node-4');

    const clocks = engine.getParticipantClocks();
    assert.strictEqual(clocks[COORDINATOR_PARTICIPANT_ID], 5);
    assert.strictEqual(clocks['storage-node-1'], 2);
    assert.strictEqual(clocks['storage-node-2'], 0, 'Untouched node-2 should remain 0');
    assert.strictEqual(clocks['storage-node-3'], 0, 'Untouched node-3 should remain 0');
    assert.strictEqual(clocks['storage-node-4'], 12);
    assert.strictEqual(clocks['storage-node-5'], 0, 'Untouched node-5 should remain 0');

    console.log('  ✔ Confirmed: All participants maintain separate, independent scalar clock state:');
    for (const [id, val] of Object.entries(clocks)) {
      console.log(`     ↳ ${id}: ${val}`);
    }
  }

  // -------------------------------------------------------------------------
  // Test 7: Invalid Participant IDs and Timestamps
  // -------------------------------------------------------------------------
  console.log('\n[Test 7/12] Verifying Rejection of Invalid Participant IDs and Clock Values...');
  {
    const engine = new LamportClockEngine();

    // 1. Invalid participant IDs
    assert.throws(
      () => engine.getClock(''),
      InvalidParticipantError,
      'Empty string participant ID must be rejected'
    );
    assert.throws(
      () => engine.tickLocal('unknown-random-node'),
      InvalidParticipantError,
      'Unrecognized participant ID must be rejected'
    );
    assert.throws(
      () => engine.tickLocal(null as any),
      InvalidParticipantError,
      'Null participant ID must be rejected'
    );

    // 2. Invalid timestamps for tickReceive
    assert.throws(
      () => engine.tickReceive(COORDINATOR_PARTICIPANT_ID, -1),
      InvalidClockValueError,
      'Negative clock value must be rejected'
    );
    assert.throws(
      () => engine.tickReceive(COORDINATOR_PARTICIPANT_ID, 3.1415),
      InvalidClockValueError,
      'Floating point clock value must be rejected'
    );
    assert.throws(
      () => engine.tickReceive(COORDINATOR_PARTICIPANT_ID, NaN),
      InvalidClockValueError,
      'NaN clock value must be rejected'
    );
    assert.throws(
      () => engine.tickReceive(COORDINATOR_PARTICIPANT_ID, Infinity),
      InvalidClockValueError,
      'Infinity clock value must be rejected'
    );
    assert.throws(
      () => engine.tickReceive(COORDINATOR_PARTICIPANT_ID, '10' as any),
      InvalidClockValueError,
      'String clock value must be rejected'
    );
    assert.throws(
      () => engine.tickReceive(COORDINATOR_PARTICIPANT_ID, Number.MAX_SAFE_INTEGER + 10),
      InvalidClockValueError,
      'Unsafe integer above MAX_SAFE_INTEGER must be rejected'
    );

    console.log('  ✔ Confirmed: All invalid participant IDs, negative, float, NaN, and unsafe values rejected.');
  }

  // -------------------------------------------------------------------------
  // Test 8: Overflow Handling
  // -------------------------------------------------------------------------
  console.log('\n[Test 8/12] Verifying Explicit Overflow Handling...');
  {
    const engine = new LamportClockEngine();
    // Simulate clock reaching MAX_SAFE_INTEGER
    (engine as any).clocks.set('storage-node-5', Number.MAX_SAFE_INTEGER);

    assert.throws(
      () => engine.tickLocal('storage-node-5'),
      ClockOverflowError,
      'Ticking at Number.MAX_SAFE_INTEGER must throw ClockOverflowError'
    );

    assert.throws(
      () => engine.tickSend('storage-node-5'),
      ClockOverflowError,
      'Sending at Number.MAX_SAFE_INTEGER must throw ClockOverflowError'
    );

    console.log('  ✔ Confirmed: ClockOverflowError thrown when advancing beyond Number.MAX_SAFE_INTEGER.');
  }

  // -------------------------------------------------------------------------
  // Test 9: Concurrent Clock Increments
  // -------------------------------------------------------------------------
  console.log('\n[Test 9/12] Verifying Concurrent In-Process Increments...');
  {
    const engine = new LamportClockEngine();
    const target = 'storage-node-1';
    const iterations = 500;

    // Concurrently trigger 500 local ticks
    const promises: Promise<number>[] = [];
    for (let i = 0; i < iterations; i++) {
      promises.push(
        new Promise<number>(resolve => {
          setTimeout(() => {
            resolve(engine.tickLocal(target));
          }, Math.floor(Math.random() * 5));
        })
      );
    }

    const results = await Promise.all(promises);
    assert.strictEqual(engine.getClock(target), iterations, `Final clock must equal exactly ${iterations}`);
    
    // Verify each result is unique and within range [1..500]
    const uniqueResults = new Set(results);
    assert.strictEqual(uniqueResults.size, iterations, 'All increment values must be distinct');
    console.log(`  ✔ Confirmed: 500 concurrent operations produced strictly monotonic increments (final: ${engine.getClock(target)}).`);
  }

  // -------------------------------------------------------------------------
  // Test 10: Persistence Across Engine Restart
  // -------------------------------------------------------------------------
  console.log('\n[Test 10/12] Verifying Persistence Across Simulated Engine Restart...');
  {
    const testDb = createTestDb();
    try {
      // 1. First engine lifecycle: advance clocks and record events
      const engine1 = new LamportClockEngine(testDb.db);
      engine1.tickLocal(COORDINATOR_PARTICIPANT_ID); // 1
      engine1.tickLocal(COORDINATOR_PARTICIPANT_ID); // 2
      engine1.tickReceive('storage-node-2', 15);     // 16
      engine1.tickLocal('storage-node-3');           // 1

      // Record causal event with transaction
      engine1.recordCausalEvent({
        origin_node_id: COORDINATOR_PARTICIPANT_ID,
        event_type: 'FILE_COMMITTED',
        severity: 'INFO',
        message: 'File committed in test',
        correlation_id: 'op_test_1',
      }); // coordinator becomes 3

      assert.strictEqual(engine1.getClock(COORDINATOR_PARTICIPANT_ID), 3);
      assert.strictEqual(engine1.getClock('storage-node-2'), 16);
      assert.strictEqual(engine1.getClock('storage-node-3'), 1);

      // 2. Simulate complete engine shutdown & restart on same SQLite database
      const engine2 = new LamportClockEngine(testDb.db);
      assert.strictEqual(
        engine2.getClock(COORDINATOR_PARTICIPANT_ID),
        3,
        'Coordinator clock must be restored to 3 after restart'
      );
      assert.strictEqual(
        engine2.getClock('storage-node-2'),
        16,
        'Storage node 2 clock must be restored to 16 after restart'
      );
      assert.strictEqual(
        engine2.getClock('storage-node-3'),
        1,
        'Storage node 3 clock must be restored to 1 after restart'
      );
      assert.strictEqual(
        engine2.getClock('storage-node-1'),
        0,
        'Storage node 1 clock should be preserved as 0'
      );

      // Continue ticking on restored engine
      const tickPostRestart = engine2.tickLocal(COORDINATOR_PARTICIPANT_ID);
      assert.strictEqual(tickPostRestart, 4, 'Clock must continue from 3 -> 4 after restart');

      console.log('  ✔ Confirmed: All participant clocks accurately restored and advanced post-restart.');
    } finally {
      testDb.cleanup();
    }
  }

  // -------------------------------------------------------------------------
  // Test 11: Recovery When Clock Table Missing But Event History Exists
  // -------------------------------------------------------------------------
  console.log('\n[Test 11/12] Verifying High-Water Mark Recovery from Event History...');
  {
    const testDb = createTestDb();
    try {
      // Initialize schema
      const engineInit = new LamportClockEngine(testDb.db);

      // Insert high-value events directly into cluster_events (as if written by earlier run)
      testDb.db.prepare(`
        INSERT INTO cluster_events (event_id, event_type, severity, message, timestamp, lamport_clock, origin_node_id)
        VALUES 
          ('evt_hist_1', 'CHUNK_PLACED', 'INFO', 'Chunk written', 1700000000, 42, 'storage-node-4'),
          ('evt_hist_2', 'FILE_COMMITTED', 'INFO', 'File committed', 1700000001, 88, 'coordinator-primary-1')
      `).run();

      // Corrupt/truncate participant_clocks to simulate missing table state
      testDb.db.exec('DELETE FROM participant_clocks;');

      // Create a fresh engine instance
      const recoveredEngine = new LamportClockEngine(testDb.db);

      // Verify engine hydrated from cluster_events high-water marks!
      assert.strictEqual(
        recoveredEngine.getClock('storage-node-4'),
        42,
        'storage-node-4 must be restored to 42 from event history'
      );
      assert.strictEqual(
        recoveredEngine.getClock(COORDINATOR_PARTICIPANT_ID),
        88,
        'coordinator must be restored to 88 from event history'
      );

      // Verify continuing operations advance from the recovered high-water mark
      const nextCoord = recoveredEngine.tickLocal(COORDINATOR_PARTICIPANT_ID);
      assert.strictEqual(nextCoord, 89, 'Next coordinator tick must be 89 (never reset to 0 or 1)');

      console.log('  ✔ Confirmed: Missing participant_clocks successfully recovered from cluster_events (storage-node-4: 42, coordinator: 88 -> 89).');
    } finally {
      testDb.cleanup();
    }
  }

  // -------------------------------------------------------------------------
  // Test 12: Persistence Failure Handling & Transaction Consistency
  // -------------------------------------------------------------------------
  console.log('\n[Test 12/12] Verifying Transaction Consistency and Persistence Failure Handling...');
  {
    const testDb = createTestDb();
    try {
      const engine = new LamportClockEngine(testDb.db);
      const initialCoordClock = engine.getClock(COORDINATOR_PARTICIPANT_ID);

      // Deliberately drop cluster_events to force transaction failure on recordCausalEvent
      testDb.db.exec('DROP TABLE cluster_events;');

      assert.throws(
        () => {
          engine.recordCausalEvent({
            origin_node_id: COORDINATOR_PARTICIPANT_ID,
            event_type: 'FAULTY_EVENT',
            severity: 'ERROR',
            message: 'This should fail transaction',
          });
        },
        ClockPersistenceError,
        'Database write failure must throw ClockPersistenceError'
      );

      // Verify that in-memory clock was NOT committed when transaction rolled back
      assert.strictEqual(
        engine.getClock(COORDINATOR_PARTICIPANT_ID),
        initialCoordClock,
        'In-memory clock must NOT advance if database transaction failed'
      );

      console.log('  ✔ Confirmed: ClockPersistenceError raised and clock rolled back safely on transaction error.');
    } finally {
      testDb.cleanup();
    }
  }

  console.log(`\n======================================================================`);
  console.log(` ALL 12/12 LAMPORT CLOCK ENGINE TESTS PASSED!`);
  console.log(`======================================================================\n`);
}

runLamportClockEngineTests().catch(err => {
  console.error('\n❌ Lamport Clock Engine Test Failure:', err);
  process.exit(1);
});
