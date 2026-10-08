import { createBrowserUuid } from '@/lib/browser-uuid'
import {
  isSameRemoteRuntimeInputEndpoint,
  type RemoteRuntimeInputEndpoint
} from './remote-runtime-recovery-input-hold'

// Why: a retention budget for input in flight across one outage, not a paste ceiling; past it the
// oldest bytes go and the gap check below stops a partial replay.
export const REMOTE_RUNTIME_INPUT_JOURNAL_MAX_CODE_UNITS = 1024 * 1024

export type SequencedRemoteRuntimeInput = { seq: number; text: string; queryReply: boolean }

/** Input sent to a remote pane but not yet acknowledged by its host, keyed by input sequence. */
export type RemoteRuntimeInputJournal = {
  readonly sessionId: string
  /** Assigns the next sequence to `text`, which the caller is about to send to `endpoint`. */
  record: (endpoint: RemoteRuntimeInputEndpoint, text: string, queryReply?: boolean) => number
  acknowledge: (seq: number) => void
  /** Input still owed to `bound`, oldest first. Clears itself when it belongs elsewhere or has a gap. */
  unacknowledgedFor: (bound: RemoteRuntimeInputEndpoint) => readonly SequencedRemoteRuntimeInput[]
  discard: () => void
}

export function createRemoteRuntimeInputJournal(): RemoteRuntimeInputJournal {
  const sessionId = createBrowserUuid()
  let endpoint: RemoteRuntimeInputEndpoint | null = null
  let entries: SequencedRemoteRuntimeInput[] = []
  let codeUnits = 0
  let nextSeq = 1
  let ackedSeq = 0

  const discard = (): void => {
    entries = []
    codeUnits = 0
    endpoint = null
    // Why: nothing before this point will be replayed, so later input starts a contiguous run.
    ackedSeq = nextSeq - 1
  }

  return {
    sessionId,
    record(target, text, queryReply = false) {
      if (endpoint && !isSameRemoteRuntimeInputEndpoint(endpoint, target)) {
        // Why: input typed at one shell must never run in its replacement (#10065).
        discard()
      }
      endpoint = target
      const seq = nextSeq
      nextSeq += 1
      entries.push({ seq, text, queryReply })
      codeUnits += text.length
      while (codeUnits > REMOTE_RUNTIME_INPUT_JOURNAL_MAX_CODE_UNITS && entries.length > 0) {
        codeUnits -= entries.shift()?.text.length ?? 0
      }
      return seq
    },
    acknowledge(seq) {
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
    },
    unacknowledgedFor(bound) {
      if (entries.length === 0) {
        return entries
      }
      if (
        !endpoint ||
        !isSameRemoteRuntimeInputEndpoint(endpoint, bound) ||
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
