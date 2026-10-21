/**
 * ShardSphere — Lamport Logical Clock Types & Contracts
 */

export const COORDINATOR_PARTICIPANT_ID = 'coordinator-primary-1';

export const STORAGE_NODE_PARTICIPANT_IDS = [
  'storage-node-1',
  'storage-node-2',
  'storage-node-3',
  'storage-node-4',
  'storage-node-5',
] as const;

export const KNOWN_PARTICIPANT_IDS = [
  COORDINATOR_PARTICIPANT_ID,
  ...STORAGE_NODE_PARTICIPANT_IDS,
] as const;

export type ParticipantId = (typeof KNOWN_PARTICIPANT_IDS)[number] | string;

export class InvalidParticipantError extends Error {
  constructor(public participantId: unknown) {
    super(`Invalid participant ID: ${JSON.stringify(participantId)}. Must be a recognized non-empty string identifier.`);
    this.name = 'InvalidParticipantError';
  }
}

export class InvalidClockValueError extends Error {
  constructor(public clockValue: unknown, reason: string) {
    super(`Invalid Lamport clock value ${JSON.stringify(clockValue)}: ${reason}`);
    this.name = 'InvalidClockValueError';
  }
}

export class ClockOverflowError extends Error {
  constructor(public participantId: string, public currentClock: number) {
    super(`Lamport clock overflow on participant "${participantId}": current clock is ${currentClock}, cannot advance beyond Number.MAX_SAFE_INTEGER.`);
    this.name = 'ClockOverflowError';
  }
}

export class ClockPersistenceError extends Error {
  constructor(public participantId: string, public attemptedClock: number, public originalError: Error) {
    super(`Failed to persist Lamport clock ${attemptedClock} for participant "${participantId}": ${originalError.message}`);
    this.name = 'ClockPersistenceError';
    (this as any).cause = originalError;
  }
}

/**
 * Strongly typed Causal Event Envelope supporting multi-parent causality
 */
export interface CausalEventEnvelope {
  event_id: string;
  event_type: string;
  origin_node_id: string;
  lamport_clock: number;
  causal_parent_id?: string | null;
  causal_parent_ids?: string[] | null;
  correlation_id?: string | null;
  severity: 'INFO' | 'WARNING' | 'ERROR';
  node_id?: string | null;
  file_id?: string | null;
  message: string;
  timestamp: number; // Physical wall-clock seconds
}

export interface ParticipantClockRecord {
  participant_id: string;
  clock_value: number;
  updated_at: number;
}
