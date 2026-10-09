import { randomUUID } from 'node:crypto'
import type { OrcaRuntimeService } from '../../../orca-runtime'

// Why bounded: one entry per (PTY, client input session); a session evicted here has been idle
// behind thousands of newer ones, so it holds no input still awaiting acknowledgement.
export const TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS = 4096

/**
 * What one write did. `refused` proved nothing reached the terminal; `delivery-unknown` may have
 * written, so it is never retried (a duplicate keystroke can run a command twice).
 */
export type TerminalInputWriteOutcome = 'applied' | 'refused' | 'delivery-unknown'

/**
 * The cumulative reply for one frame: every sequence through `appliedSeq` is settled. `resend`
 * (a refusal or a gap) asks the client for everything after `appliedSeq` again, in order.
 */
export type TerminalInputAdmission = {
  kind: 'applied' | 'delivery-unknown' | 'resend'
  appliedSeq: number
}

type Session = { appliedSeq: number; tail: Promise<void> }

/**
 * Go-back-N receiver for client input sequences, per (PTY, client input session). It outlives any
 * one multiplex connection, so input replayed after an outage is written once even when the dead
 * connection's copy arrives too. Frames run one at a time in arrival order against the last
 * confirmed write, so nothing is ever written ahead of earlier input and no state blocks later input.
 */
export class TerminalInputSequenceLedger {
  // Why: published to clients; a PTY that outlives this ledger (runtime restart) gets a new id, so
  // clients drop input of unknown delivery instead of replaying it into a ledger that forgot it.
  readonly id = randomUUID()
  private readonly sessions = new Map<string, Session>()

  admit(
    ptyId: string,
    inputSessionId: string,
    seq: number,
    write: () => Promise<TerminalInputWriteOutcome>
  ): Promise<TerminalInputAdmission> {
    const key = `${ptyId}\u0000${inputSessionId}`
    // Why a new session starts at any sequence: the client numbers input per pane, not per PTY.
    const session = this.sessions.get(key) ?? { appliedSeq: seq - 1, tail: Promise.resolve() }
    this.sessions.delete(key)
    this.sessions.set(key, session)
    this.evictOverflow()
    const run = session.tail.then(async (): Promise<TerminalInputAdmission> => {
      if (seq <= session.appliedSeq) {
        return { kind: 'applied', appliedSeq: session.appliedSeq }
      }
      if (seq > session.appliedSeq + 1) {
        return { kind: 'resend', appliedSeq: session.appliedSeq }
      }
      const outcome = await write().catch((): TerminalInputWriteOutcome => 'delivery-unknown')
      if (outcome === 'refused') {
        return { kind: 'resend', appliedSeq: session.appliedSeq }
      }
      session.appliedSeq = seq
      return { kind: outcome, appliedSeq: seq }
    })
    session.tail = run.then(() => undefined)
    return run
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
