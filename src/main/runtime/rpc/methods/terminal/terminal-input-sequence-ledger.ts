import { randomUUID } from 'node:crypto'
import type { OrcaRuntimeService } from '../../../orca-runtime'

// Why bounded: one entry per (PTY, client input session); a session evicted here has been idle
// behind thousands of newer ones, so it holds no input still awaiting acknowledgement.
export const TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS = 4096
const DELIVERY_UNKNOWN_SEQS_PER_SESSION = 64

/** What one write did: `refused` wrote nothing, `delivery-unknown` threw after it may have. */
export type TerminalInputWriteOutcome = 'applied' | 'refused' | 'delivery-unknown'

/**
 * What the client is told. `resend` acks nothing: the session needs every input from `fromSeq`
 * again, in order, before it writes anything newer.
 */
export type TerminalInputAdmission =
  | { kind: 'applied' }
  | { kind: 'delivery-unknown' }
  | { kind: 'resend'; fromSeq: number }

const APPLIED: TerminalInputAdmission = { kind: 'applied' }
const DELIVERY_UNKNOWN: TerminalInputAdmission = { kind: 'delivery-unknown' }

type SessionEntry = {
  // Highest sequence admitted; every sequence at or below it is written or queued to be.
  appliedSeq: number
  // Bumped when a refused write revokes the admissions queued behind it.
  epoch: number
  writeTail: Promise<void>
  deliveryUnknownSeqs: number[]
}

/**
 * Last input sequence each client session applied to a PTY. It outlives any one multiplex
 * connection, so input a client replays after a silent outage is written exactly once even when
 * the dead connection's copy arrives too.
 */
export class TerminalInputSequenceLedger {
  // Why: published to clients; a PTY that outlives this ledger (runtime restart) gets a new id, so
  // clients drop input of unknown delivery instead of replaying it into a ledger that forgot it.
  readonly id = randomUUID()
  private readonly sessions = new Map<string, SessionEntry>()

  /**
   * Records `seq` and runs `write` after every earlier admitted write of the session, whichever
   * connection brought it. A session admits only the next sequence: a gap (a refused write rolled
   * back, or a stale frame from a dead connection racing the replay) is answered with `resend`, so
   * nothing is ever written ahead of earlier input. `duplicate` is set when `seq` was already
   * admitted; `settled` then reports the original write.
   */
  admit(
    ptyId: string,
    inputSessionId: string,
    seq: number,
    write: () => Promise<TerminalInputWriteOutcome>
  ): { duplicate: boolean; settled: Promise<TerminalInputAdmission> } {
    const key = `${ptyId}\u0000${inputSessionId}`
    // Why a new session starts at any sequence: the client numbers input per pane, not per PTY.
    const entry = this.sessions.get(key) ?? {
      appliedSeq: seq - 1,
      epoch: 0,
      writeTail: Promise.resolve(),
      deliveryUnknownSeqs: []
    }
    this.sessions.delete(key)
    this.sessions.set(key, entry)
    this.evictOverflow()
    if (seq <= entry.appliedSeq) {
      return {
        duplicate: true,
        settled: entry.writeTail.then(() => admissionOf(entry, seq))
      }
    }
    if (seq > entry.appliedSeq + 1) {
      return { duplicate: false, settled: Promise.resolve(resendFrom(entry)) }
    }
    entry.appliedSeq = seq
    const epoch = entry.epoch
    const run = entry.writeTail.then(async (): Promise<TerminalInputAdmission> => {
      if (entry.epoch !== epoch) {
        return resendFrom(entry)
      }
      const outcome = await write().catch((): TerminalInputWriteOutcome => 'delivery-unknown')
      if (outcome === 'refused') {
        // Why roll back: the client keeps refused input, and its resend must be written, not deduped.
        entry.epoch += 1
        entry.appliedSeq = seq - 1
        return resendFrom(entry)
      }
      if (outcome === 'delivery-unknown') {
        entry.deliveryUnknownSeqs.push(seq)
        entry.deliveryUnknownSeqs.splice(
          0,
          entry.deliveryUnknownSeqs.length - DELIVERY_UNKNOWN_SEQS_PER_SESSION
        )
        return DELIVERY_UNKNOWN
      }
      return APPLIED
    })
    entry.writeTail = run.then(() => undefined)
    return { duplicate: false, settled: run }
  }

  private evictOverflow(): void {
    while (this.sessions.size > TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS) {
      const oldest = this.sessions.keys().next()
      if (oldest.done) {
        return
      }
      this.sessions.delete(oldest.value)
    }
  }
}

function resendFrom(entry: SessionEntry): TerminalInputAdmission {
  return { kind: 'resend', fromSeq: entry.appliedSeq + 1 }
}

function admissionOf(entry: SessionEntry, seq: number): TerminalInputAdmission {
  if (seq > entry.appliedSeq) {
    return resendFrom(entry)
  }
  return entry.deliveryUnknownSeqs.includes(seq) ? DELIVERY_UNKNOWN : APPLIED
}

const ledgers = new WeakMap<OrcaRuntimeService, TerminalInputSequenceLedger>()

export function getTerminalInputSequenceLedger(
  runtime: OrcaRuntimeService
): TerminalInputSequenceLedger {
  let ledger = ledgers.get(runtime)
  if (!ledger) {
    ledger = new TerminalInputSequenceLedger()
    ledgers.set(runtime, ledger)
  }
  return ledger
}
