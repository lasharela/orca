import { createBrowserUuid } from '@/lib/browser-uuid'
import {
  isSameRemoteRuntimeInputEndpoint,
  type RemoteRuntimeInputEndpoint
} from './remote-runtime-recovery-input-hold'

// Why: a retention budget for input in flight across one outage, not a paste ceiling; past it the
// oldest bytes go and the gap check below stops a partial replay.
export const REMOTE_RUNTIME_INPUT_JOURNAL_MAX_CODE_UNITS = 1024 * 1024

export type SequencedRemoteRuntimeInput = {
  seq: number
  text: string
  queryReply: boolean
  // Its caller gave up on it; a replay must not deliver what was already reported failed.
  cancelled: boolean
}

/** Input sent to a remote pane but not yet acknowledged by its host, keyed by input sequence. */
export type RemoteRuntimeInputJournal = {
  readonly sessionId: string
  /**
   * Assigns the next sequence to `text`, which the caller is about to send to `endpoint` on a
   * stream whose host dedupes with `ledgerId`.
   */
  record: (
    endpoint: RemoteRuntimeInputEndpoint,
    ledgerId: string | null,
    text: string,
    queryReply?: boolean
  ) => number
  /** `applied` is false when the host's write of `seq` failed with unknown delivery. */
  acknowledge: (seq: number, applied?: boolean) => void
  /** Resolves true once the host applies `seq`, false if it fails or the journal gives it up first. */
  whenAcknowledged: (seq: number) => Promise<boolean>
  /** Gives up `seq` for its caller: settles it false and keeps it out of any later replay. */
  cancel: (seq: number) => void
  /**
   * Input still owed to `bound`, oldest first. Clears itself when it belongs elsewhere, has a gap,
   * or was sent to a host ledger other than `ledgerId` (that ledger cannot dedupe it).
   */
  unacknowledgedFor: (
    bound: RemoteRuntimeInputEndpoint,
    ledgerId: string | null
  ) => readonly SequencedRemoteRuntimeInput[]
  discard: () => void
}

export function createRemoteRuntimeInputJournal(): RemoteRuntimeInputJournal {
  const sessionId = createBrowserUuid()
  let endpoint: RemoteRuntimeInputEndpoint | null = null
  let ledger: string | null = null
  let entries: SequencedRemoteRuntimeInput[] = []
  let codeUnits = 0
  let nextSeq = 1
  let ackedSeq = 0
  const waiters = new Map<number, (acknowledged: boolean) => void>()

  const settleWaiters = (throughSeq: number, acknowledged: boolean): void => {
    for (const [seq, resolve] of waiters) {
      if (seq <= throughSeq) {
        waiters.delete(seq)
        resolve(acknowledged)
      }
    }
  }

  const discard = (): void => {
    entries = []
    codeUnits = 0
    endpoint = null
    ledger = null
    // Why: nothing before this point will be replayed, so later input starts a contiguous run.
    ackedSeq = nextSeq - 1
    settleWaiters(Number.POSITIVE_INFINITY, false)
  }

  return {
    sessionId,
    record(target, ledgerId, text, queryReply = false) {
      if (
        (endpoint && !isSameRemoteRuntimeInputEndpoint(endpoint, target)) ||
        (entries.length > 0 && ledger !== ledgerId)
      ) {
        // Why: input typed at one shell must never run in its replacement (#10065).
        discard()
      }
      endpoint = target
      ledger = ledgerId
      const seq = nextSeq
      nextSeq += 1
      entries.push({ seq, text, queryReply, cancelled: false })
      codeUnits += text.length
      while (codeUnits > REMOTE_RUNTIME_INPUT_JOURNAL_MAX_CODE_UNITS && entries.length > 0) {
        const dropped = entries.shift()
        codeUnits -= dropped?.text.length ?? 0
        if (dropped) {
          settleWaiters(dropped.seq, false)
        }
      }
      return seq
    },
    acknowledge(seq, applied = true) {
      if (seq <= ackedSeq || seq >= nextSeq) {
        return
      }
      ackedSeq = seq
      let drop = 0
      while (drop < entries.length && entries[drop].seq <= seq) {
        codeUnits -= entries[drop].text.length
        drop += 1
      }
      entries = entries.slice(drop)
      if (!applied) {
        waiters.get(seq)?.(false)
        waiters.delete(seq)
      }
      settleWaiters(seq, true)
    },
    cancel(seq) {
      const entry = entries.find((candidate) => candidate.seq === seq)
      if (entry) {
        entry.cancelled = true
      }
      waiters.get(seq)?.(false)
      waiters.delete(seq)
    },
    whenAcknowledged(seq) {
      // Why: callers ask right after record(), so a missing entry was already given up.
      if (!entries.some((entry) => entry.seq === seq)) {
        return Promise.resolve(false)
      }
      return new Promise((resolve) => {
        waiters.set(seq, resolve)
      })
    },
    unacknowledgedFor(bound, ledgerId) {
      if (entries.length === 0) {
        return entries
      }
      if (
        !endpoint ||
        !isSameRemoteRuntimeInputEndpoint(endpoint, bound) ||
        // Why: a restarted host runtime keeps the PTY but not the ledger, so it would run these again.
        ledger !== ledgerId ||
        // Why: replaying around a gap would deliver later keys without the earlier ones.
        entries[0].seq !== ackedSeq + 1
      ) {
        discard()
      }
      return entries
    },
    discard
  }
}
