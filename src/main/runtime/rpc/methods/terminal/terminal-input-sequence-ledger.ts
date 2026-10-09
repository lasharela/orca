import { randomUUID } from 'node:crypto'
import type { OrcaRuntimeService } from '../../../orca-runtime'

// Why bounded: one entry per (PTY, client input session); a session evicted here has been idle
// behind thousands of newer ones, so it holds no input still awaiting acknowledgement.
export const TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS = 4096
const DELIVERY_UNKNOWN_SEQS_PER_SESSION = 64

/** What one write did: `refused` wrote nothing, `delivery-unknown` threw after it may have. */
export type TerminalInputWriteOutcome = 'applied' | 'refused' | 'delivery-unknown'

/** What the client is told: `unacked` sends nothing, so the client keeps the input to replay. */
export type TerminalInputAdmission = 'applied' | 'delivery-unknown' | 'unacked'

/** The multiplex stream that brought an input frame; the ledger uses only its identity. */
export type TerminalInputCarrier = { readonly streamId: number }

type SessionEntry = {
  appliedSeq: number
  // Bumped when a refused write revokes the admissions queued behind it.
  epoch: number
  writeTail: Promise<void>
  deliveryUnknownSeqs: number[]
  // Set after a refusal: later input from carriers that predate it would land ahead of the gap.
  blocked: { seq: number; lastCarrier: number } | null
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
  private readonly carrierOrder = new WeakMap<TerminalInputCarrier, number>()
  private carrierCount = 0

  /**
   * Records `seq` and runs `write` after every earlier admitted write of the session, whichever
   * `carrier` (connection stream) brought it. Only applied input advances the session: a refused
   * write is rolled back so its replay writes it, and nothing queued behind it runs or is acked.
   * `duplicate` is set when `seq` was already admitted; `settled` then reports the original write.
   */
  admit(
    ptyId: string,
    inputSessionId: string,
    seq: number,
    carrier: TerminalInputCarrier,
    write: () => Promise<TerminalInputWriteOutcome>
  ): { duplicate: boolean; settled: Promise<TerminalInputAdmission> } {
    const key = `${ptyId}\u0000${inputSessionId}`
    const entry = this.sessions.get(key) ?? {
      appliedSeq: 0,
      epoch: 0,
      writeTail: Promise.resolve(),
      deliveryUnknownSeqs: [],
      blocked: null
    }
    this.sessions.delete(key)
    this.sessions.set(key, entry)
    this.evictOverflow()
    const carrierOrder = this.orderOf(carrier)
    if (entry.blocked) {
      // Why a newer carrier lifts it: the client replays from its oldest unacked input there.
      if (seq !== entry.blocked.seq && carrierOrder <= entry.blocked.lastCarrier) {
        return { duplicate: false, settled: Promise.resolve('unacked') }
      }
      entry.blocked = null
    }
    if (seq <= entry.appliedSeq) {
      return {
        duplicate: true,
        settled: entry.writeTail.then(() => admissionOf(entry, seq))
      }
    }
    entry.appliedSeq = seq
    const epoch = entry.epoch
    const run = entry.writeTail.then(async (): Promise<TerminalInputAdmission> => {
      if (entry.epoch !== epoch) {
        return 'unacked'
      }
      const outcome = await write().catch((): TerminalInputWriteOutcome => 'delivery-unknown')
      if (outcome === 'refused') {
        entry.epoch += 1
        entry.appliedSeq = seq - 1
        entry.blocked = { seq, lastCarrier: this.carrierCount }
        return 'unacked'
      }
      if (outcome === 'delivery-unknown') {
        entry.deliveryUnknownSeqs.push(seq)
        entry.deliveryUnknownSeqs.splice(
          0,
          entry.deliveryUnknownSeqs.length - DELIVERY_UNKNOWN_SEQS_PER_SESSION
        )
      }
      return outcome
    })
    entry.writeTail = run.then(() => undefined)
    return { duplicate: false, settled: run }
  }

  private orderOf(carrier: TerminalInputCarrier): number {
    let order = this.carrierOrder.get(carrier)
    if (order === undefined) {
      this.carrierCount += 1
      order = this.carrierCount
      this.carrierOrder.set(carrier, order)
    }
    return order
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

function admissionOf(entry: SessionEntry, seq: number): TerminalInputAdmission {
  if (seq > entry.appliedSeq) {
    return 'unacked'
  }
  return entry.deliveryUnknownSeqs.includes(seq) ? 'delivery-unknown' : 'applied'
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
