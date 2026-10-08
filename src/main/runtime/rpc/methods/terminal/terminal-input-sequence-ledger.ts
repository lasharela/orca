import { randomUUID } from 'node:crypto'
import type { OrcaRuntimeService } from '../../../orca-runtime'

// Why bounded: one entry per (PTY, client input session); a session evicted here has been idle
// behind thousands of newer ones, so it holds no input still awaiting acknowledgement.
export const TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS = 4096

type SessionEntry = {
  appliedSeq: number
  writeTail: Promise<void>
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
   * connection carried it. `duplicate` is set when `seq` was already admitted; `settled` then waits
   * for the original write, so an ack never runs ahead of the bytes it covers.
   */
  admit(
    ptyId: string,
    inputSessionId: string,
    seq: number,
    write: () => Promise<void>
  ): { duplicate: boolean; settled: Promise<void> } {
    const key = `${ptyId}\u0000${inputSessionId}`
    const entry = this.sessions.get(key) ?? { appliedSeq: 0, writeTail: Promise.resolve() }
    this.sessions.delete(key)
    this.sessions.set(key, entry)
    this.evictOverflow()
    if (seq <= entry.appliedSeq) {
      return { duplicate: true, settled: entry.writeTail }
    }
    entry.appliedSeq = seq
    const run = entry.writeTail.then(write).catch(() => undefined)
    entry.writeTail = run
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
