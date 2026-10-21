import crypto from 'crypto';
import {
  COORDINATOR_PARTICIPANT_ID,
  KNOWN_PARTICIPANT_IDS,
  InvalidParticipantError,
  InvalidClockValueError,
  ClockOverflowError,
  ClockPersistenceError,
  type CausalEventEnvelope,
  type ParticipantId,
} from './types.ts';

export interface ClockEngineOptions {
  allowedParticipants?: string[];
  autoHydrate?: boolean;
}

/**
 * Dedicated, production-grade Lamport Logical Clock Engine for ShardSphere.
 * Enforces Lamport rules:
 * - Local Event: L_i = L_i + 1
 * - Send Event: L_i = L_i + 1 (attached to message)
 * - Receive Event: L_j = max(L_j, L_msg) + 1
 * 
 * Provides crash-resilient SQLite persistence, event envelope management,
 * and high-water mark history recovery.
 */
export class LamportClockEngine {
  private db: any | null;
  private clocks: Map<string, number> = new Map();
  private allowedParticipants: Set<string>;

  constructor(database: any | null = null, options?: ClockEngineOptions) {
    this.db = database;
    this.allowedParticipants = new Set<string>(
      options?.allowedParticipants ?? (KNOWN_PARTICIPANT_IDS as unknown as string[])
    );

    // Initialize all allowed participants with 0
    for (const p of this.allowedParticipants) {
      this.clocks.set(p, 0);
    }

    if (this.db && options?.autoHydrate !== false) {
      this.applySchemaMigrations(this.db);
      this.hydrate();
    }
  }

  // ==========================================================================
  // Validation Helpers
  // ==========================================================================

  public validateParticipantId(participantId: unknown): asserts participantId is string {
    if (typeof participantId !== 'string' || !participantId.trim()) {
      throw new InvalidParticipantError(participantId);
    }
    const trimmed = participantId.trim();
    if (!this.allowedParticipants.has(trimmed)) {
      throw new InvalidParticipantError(participantId);
    }
  }

  public validateClockValue(clockValue: unknown): asserts clockValue is number {
    if (typeof clockValue !== 'number' || !Number.isFinite(clockValue)) {
      throw new InvalidClockValueError(clockValue, 'value must be a finite number');
    }
    if (!Number.isInteger(clockValue)) {
      throw new InvalidClockValueError(clockValue, 'value must be an integer');
    }
    if (clockValue < 0) {
      throw new InvalidClockValueError(clockValue, 'value must be non-negative (>= 0)');
    }
    if (clockValue > Number.MAX_SAFE_INTEGER) {
      throw new InvalidClockValueError(clockValue, `value exceeds Number.MAX_SAFE_INTEGER (${Number.MAX_SAFE_INTEGER})`);
    }
  }

  private checkIncrementOverflow(participantId: string, currentVal: number): void {
    if (currentVal >= Number.MAX_SAFE_INTEGER) {
      throw new ClockOverflowError(participantId, currentVal);
    }
  }

  // ==========================================================================
  // Core Lamport Clock Operations
  // ==========================================================================

  /**
   * Retrieves current logical clock for the given participant.
   */
  public getClock(participantId: string): number {
    this.validateParticipantId(participantId);
    return this.clocks.get(participantId) ?? 0;
  }

  /**
   * Rule 1: Local event. Advances local participant clock by 1.
   */
  public tickLocal(participantId: string): number {
    this.validateParticipantId(participantId);
    const current = this.getClock(participantId);
    this.checkIncrementOverflow(participantId, current);

    const next = current + 1;
    this.clocks.set(participantId, next);
    this.persistClock(participantId, next);
    return next;
  }

  /**
   * Rule 2: Send event. Advances local sender clock by 1 and returns timestamp for message.
   */
  public tickSend(participantId: string): number {
    return this.tickLocal(participantId);
  }

  /**
   * Rule 3: Receive event. Sets receiver clock to max(localClock, receivedClock) + 1.
   */
  public tickReceive(participantId: string, receivedClock: number): number {
    this.validateParticipantId(participantId);
    this.validateClockValue(receivedClock);

    const current = this.getClock(participantId);
    const baseline = Math.max(current, receivedClock);
    this.checkIncrementOverflow(participantId, baseline);

    const next = baseline + 1;
    this.clocks.set(participantId, next);
    this.persistClock(participantId, next);
    return next;
  }

  /**
   * Returns a snapshot of all participant clocks.
   */
  public getParticipantClocks(): Record<string, number> {
    const res: Record<string, number> = {};
    for (const [id, val] of this.clocks.entries()) {
      res[id] = val;
    }
    return res;
  }

  // ==========================================================================
  // Persistence & Transaction Consistency
  // ==========================================================================

  /**
   * Persists participant clock to SQLite with concurrency conflict protection.
   * Ensures an older clock never overwrites a newer clock value in SQLite.
   */
  public persistClock(participantId: string, clockValue: number): void {
    if (!this.db) return;

    const now = Date.now() / 1000;
    try {
      this.db.prepare(`
        INSERT INTO participant_clocks (participant_id, clock_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(participant_id) DO UPDATE SET
          clock_value = CASE
            WHEN excluded.clock_value > participant_clocks.clock_value THEN excluded.clock_value
            ELSE participant_clocks.clock_value
          END,
          updated_at = excluded.updated_at
        WHERE excluded.clock_value > participant_clocks.clock_value
      `).run(participantId, clockValue, now);
    } catch (err: any) {
      throw new ClockPersistenceError(participantId, clockValue, err);
    }
  }

  /**
   * Safely inspects untrusted external client-supplied X-Lamport-Clock header.
   * Never blindly trusts client clocks. Guards against malicious clock-exhaustion DoS
   * (e.g. client sending Number.MAX_SAFE_INTEGER - 1) or malformed values.
   */
  public processUntrustedClientClock(clientHeader: unknown): number | null {
    if (clientHeader === undefined || clientHeader === null || clientHeader === '') {
      return null;
    }

    let parsed: number;
    if (typeof clientHeader === 'number') {
      parsed = clientHeader;
    } else if (typeof clientHeader === 'string') {
      const trimmed = clientHeader.trim();
      if (!/^\d+$/.test(trimmed)) {
        return null; // Malformed string rejected
      }
      parsed = Number(trimmed);
    } else {
      return null;
    }

    if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 0) {
      return null;
    }

    const current = this.getClock(COORDINATOR_PARTICIPANT_ID);

    // Defense against clock-exhaustion DoS:
    // If the client clock is wildly higher than current coordinator clock (> +10,000 steps),
    // do not allow it to force coordinator clock to near-overflow.
    const MAX_ALLOWED_FORWARD_JUMP = 10000;
    if (parsed > current + MAX_ALLOWED_FORWARD_JUMP || parsed > Number.MAX_SAFE_INTEGER - 1000) {
      // Untrusted jump detected: advance coordinator by safe single tick without adopting spoofed value
      return this.tickLocal(COORDINATOR_PARTICIPANT_ID);
    }

    return this.tickReceive(COORDINATOR_PARTICIPANT_ID, parsed);
  }

  /**
   * Atomically advances the participant clock and records a Causal Event Envelope
   * inside a single SQLite transaction.
   */
  public recordCausalEvent(params: {
    origin_node_id: string;
    event_type: string;
    severity: 'INFO' | 'WARNING' | 'ERROR';
    message: string;
    node_id?: string | null;
    file_id?: string | null;
    causal_parent_id?: string | null;
    causal_parent_ids?: string[] | null;
    correlation_id?: string | null;
    received_clock?: number;
  }): CausalEventEnvelope {
    const origin = params.origin_node_id;
    this.validateParticipantId(origin);

    // Calculate next clock
    let nextClock: number;
    if (params.received_clock !== undefined && params.received_clock !== null) {
      this.validateClockValue(params.received_clock);
      const current = this.getClock(origin);
      const baseline = Math.max(current, params.received_clock);
      this.checkIncrementOverflow(origin, baseline);
      nextClock = baseline + 1;
    } else {
      const current = this.getClock(origin);
      this.checkIncrementOverflow(origin, current);
      nextClock = current + 1;
    }

    const eventId = `evt_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
    const now = Date.now() / 1000;

    let primaryParent = params.causal_parent_id ?? null;
    let parentsList: string[] | null = null;
    if (params.causal_parent_ids && Array.isArray(params.causal_parent_ids) && params.causal_parent_ids.length > 0) {
      parentsList = params.causal_parent_ids.filter(p => typeof p === 'string' && p.trim().length > 0);
      if (!primaryParent && parentsList.length > 0) {
        primaryParent = parentsList[0];
      }
    } else if (primaryParent) {
      parentsList = [primaryParent];
    }

    const envelope: CausalEventEnvelope = {
      event_id: eventId,
      event_type: params.event_type,
      origin_node_id: origin,
      lamport_clock: nextClock,
      causal_parent_id: primaryParent,
      causal_parent_ids: parentsList,
      correlation_id: params.correlation_id ?? null,
      severity: params.severity,
      node_id: params.node_id ?? null,
      file_id: params.file_id ?? null,
      message: params.message,
      timestamp: now,
    };

    if (this.db) {
      try {
        this.db.exec('BEGIN IMMEDIATE');

        // 1. Update participant_clocks
        this.db.prepare(`
          INSERT INTO participant_clocks (participant_id, clock_value, updated_at)
          VALUES (?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            clock_value = CASE
              WHEN excluded.clock_value > participant_clocks.clock_value THEN excluded.clock_value
              ELSE participant_clocks.clock_value
            END,
            updated_at = excluded.updated_at
          WHERE excluded.clock_value > participant_clocks.clock_value
        `).run(origin, nextClock, now);

        // 2. Insert into cluster_events
        const serializedParents = parentsList ? JSON.stringify(parentsList) : null;
        this.db.prepare(`
          INSERT INTO cluster_events (event_id, event_type, severity, node_id, file_id, message, timestamp, lamport_clock, origin_node_id, causal_parent_id, causal_parent_ids, correlation_id)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
          envelope.event_id,
          envelope.event_type,
          envelope.severity,
          envelope.node_id,
          envelope.file_id,
          envelope.message,
          envelope.timestamp,
          envelope.lamport_clock,
          envelope.origin_node_id,
          envelope.causal_parent_id,
          serializedParents,
          envelope.correlation_id
        );

        this.db.exec('COMMIT');
      } catch (err: any) {
        try {
          this.db.exec('ROLLBACK');
        } catch {
          // ignore rollback failure
        }
        throw new ClockPersistenceError(origin, nextClock, err);
      }
    }

    // Update in-memory state only after successful transaction/persistence
    this.clocks.set(origin, nextClock);
    return envelope;
  }

  // ==========================================================================
  // Recovery & Hydration
  // ==========================================================================

  /**
   * Recovers participant clocks from SQLite persistent state and event history.
   * Invariant: Never silently reset an existing participant's clock to zero
   * when higher valid history exists in participant_clocks or cluster_events.
   */
  public hydrate(): void {
    if (!this.db) return;

    // 1. Read from participant_clocks table
    try {
      const rows = this.db.prepare('SELECT participant_id, clock_value FROM participant_clocks').all() as any[];
      for (const row of rows) {
        if (this.allowedParticipants.has(row.participant_id)) {
          const val = Number(row.clock_value);
          if (Number.isInteger(val) && val >= 0) {
            const current = this.clocks.get(row.participant_id) ?? 0;
            this.clocks.set(row.participant_id, Math.max(current, val));
          }
        }
      }
    } catch {
      // Table may not exist yet or be empty
    }

    // 2. Scan cluster_events history for highest observed lamport_clock per participant
    try {
      const eventRows = this.db.prepare(`
        SELECT coalesce(origin_node_id, 'coordinator-primary-1') as p_id, coalesce(max(lamport_clock), 0) as max_c
        FROM cluster_events
        GROUP BY origin_node_id
      `).all() as any[];

      for (const row of eventRows) {
        const pId = row.p_id;
        const maxC = Number(row.max_c);
        if (this.allowedParticipants.has(pId) && Number.isInteger(maxC) && maxC > 0) {
          const current = this.clocks.get(pId) ?? 0;
          if (maxC > current) {
            this.clocks.set(pId, maxC);
            this.persistClock(pId, maxC);
          }
        }
      }

      // Ensure coordinator clock is at least as high as any event recorded anywhere in cluster_events
      const globalMax = (this.db.prepare(`
        SELECT coalesce(max(lamport_clock), 0) as gm FROM cluster_events
      `).get() as any)?.gm;
      const globalMaxVal = Number(globalMax);
      if (Number.isInteger(globalMaxVal) && globalMaxVal > 0) {
        const coordClock = this.clocks.get(COORDINATOR_PARTICIPANT_ID) ?? 0;
        if (globalMaxVal > coordClock) {
          this.clocks.set(COORDINATOR_PARTICIPANT_ID, globalMaxVal);
          this.persistClock(COORDINATOR_PARTICIPANT_ID, globalMaxVal);
        }
      }
    } catch {
      // cluster_events may not have lamport_clock column yet
    }
  }

  // ==========================================================================
  // Non-Destructive Schema Migrations
  // ==========================================================================

  /**
   * Applies additive, non-destructive schema migrations to the given SQLite database.
   */
  public applySchemaMigrations(database: any = this.db): void {
    if (!database) return;

    // 1. Create participant_clocks table
    database.exec(`
      CREATE TABLE IF NOT EXISTS participant_clocks (
        participant_id VARCHAR(64) PRIMARY KEY,
        clock_value BIGINT NOT NULL DEFAULT 0,
        updated_at FLOAT NOT NULL
      );
    `);

    // Helper to add column idempotently
    const addColumnIfNotExists = (tableName: string, columnName: string, columnDef: string) => {
      try {
        const cols = database.prepare(`PRAGMA table_info(${tableName})`).all() as any[];
        const hasCol = cols.some((c: any) => c.name === columnName);
        if (!hasCol) {
          database.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDef};`);
        }
      } catch (err: any) {
        // Table might not exist yet in test environments
      }
    };

    // 2. Additive columns for cluster_events
    addColumnIfNotExists('cluster_events', 'lamport_clock', 'BIGINT DEFAULT 0');
    addColumnIfNotExists('cluster_events', 'origin_node_id', "VARCHAR(64) DEFAULT 'coordinator-primary-1'");
    addColumnIfNotExists('cluster_events', 'causal_parent_id', 'VARCHAR(64) DEFAULT NULL');
    addColumnIfNotExists('cluster_events', 'causal_parent_ids', 'TEXT DEFAULT NULL');
    addColumnIfNotExists('cluster_events', 'correlation_id', 'VARCHAR(64) DEFAULT NULL');

    // 3. Additive columns for storage_nodes, files, chunks, replicas, recovery_jobs
    addColumnIfNotExists('storage_nodes', 'lamport_clock', 'BIGINT DEFAULT 0');
    addColumnIfNotExists('files', 'lamport_clock', 'BIGINT DEFAULT 0');
    addColumnIfNotExists('chunks', 'lamport_clock', 'BIGINT DEFAULT 0');
    addColumnIfNotExists('replicas', 'lamport_clock', 'BIGINT DEFAULT 0');
    addColumnIfNotExists('recovery_jobs', 'lamport_clock', 'BIGINT DEFAULT 0');
  }
}
