import { describe, expect, it } from 'vitest'
import {
  TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS,
  TerminalInputSequenceLedger,
  type TerminalInputWriteOutcome
} from './terminal-input-sequence-ledger'

const applied = async (): Promise<TerminalInputWriteOutcome> => 'applied'
const APPLIED = { kind: 'applied' }
const resend = (fromSeq: number) => ({ kind: 'resend', fromSeq })

function recordingWrites() {
  const writes: string[] = []
  const write =
    (text: string, outcome: TerminalInputWriteOutcome = 'applied') =>
    async (): Promise<TerminalInputWriteOutcome> => {
      if (outcome === 'applied') {
        writes.push(text)
      }
      return outcome
    }
  return { writes, write }
}

describe('TerminalInputSequenceLedger', () => {
  it('runs admitted writes of one session in sequence order, even when an earlier write is slow', async () => {
    const ledger = new TerminalInputSequenceLedger()
    const writes: string[] = []
    let releaseFirst = (): void => {}
    const { settled: first } = ledger.admit(
      'pty-1',
      'session',
      1,
      () =>
        new Promise<TerminalInputWriteOutcome>((resolve) => {
          releaseFirst = () => {
            writes.push('a')
            resolve('applied')
          }
        })
    )
    const { settled: second } = ledger.admit('pty-1', 'session', 2, async () => {
      writes.push('b')
      return 'applied'
    })
    await Promise.resolve()
    expect(writes).toEqual([])
    releaseFirst()
    await expect(Promise.all([first, second])).resolves.toEqual([APPLIED, APPLIED])
    expect(writes).toEqual(['a', 'b'])
  })

  it('reports a thrown write as delivery-unknown, including to its replay', async () => {
    const ledger = new TerminalInputSequenceLedger()
    await expect(
      ledger.admit('pty-1', 'session', 1, async () => {
        throw new Error('write failed')
      }).settled
    ).resolves.toEqual({ kind: 'delivery-unknown' })
    const replay = ledger.admit('pty-1', 'session', 1, applied)
    expect(replay.duplicate).toBe(true)
    await expect(replay.settled).resolves.toEqual({ kind: 'delivery-unknown' })
    expect(ledger.admit('pty-1', 'session', 2, applied).duplicate).toBe(false)
  })

  it('never acks a refused write, asks for it again, and writes it when resent', async () => {
    const ledger = new TerminalInputSequenceLedger()
    const { writes, write } = recordingWrites()
    await ledger.admit('pty-1', 'session', 1, write('a')).settled
    // The SSH provider is reconnecting, so the PTY refuses seq 2; seq 3 is already queued behind it.
    const refused = ledger.admit('pty-1', 'session', 2, write('b', 'refused'))
    const queued = ledger.admit('pty-1', 'session', 3, write('c'))
    await expect(refused.settled).resolves.toEqual(resend(2))
    await expect(queued.settled).resolves.toEqual(resend(2))
    // Typed on the same stream after the refusal: running it would skip the refused bytes.
    await expect(ledger.admit('pty-1', 'session', 4, write('d')).settled).resolves.toEqual(
      resend(2)
    )
    expect(writes).toEqual(['a'])

    // The same live stream resends from seq 2, and input typed after the resend keeps flowing.
    const resent = [2, 3, 4, 5].map(
      (seq) => ledger.admit('pty-1', 'session', seq, write('bcde'[seq - 2])).settled
    )
    await expect(Promise.all(resent)).resolves.toEqual([APPLIED, APPLIED, APPLIED, APPLIED])
    expect(writes).toEqual(['a', 'b', 'c', 'd', 'e'])
  })

  it('asks a replacement stream whose replay raced the refusal to resend, instead of dropping its input', async () => {
    const ledger = new TerminalInputSequenceLedger()
    const { writes, write } = recordingWrites()
    let releaseFirst = (): void => {}
    // Old stream: seq 1 is a slow paste chunk, seq 2 queued behind it.
    ledger.admit(
      'pty-1',
      'session',
      1,
      () =>
        new Promise<TerminalInputWriteOutcome>((resolve) => {
          releaseFirst = () => {
            writes.push('1')
            resolve('applied')
          }
        })
    )
    ledger.admit('pty-1', 'session', 2, write('2', 'refused'))
    // Replacement stream replays 1 and 2 (both duplicates by now), then types seq 3.
    const replay = [1, 2].map((seq) => ledger.admit('pty-1', 'session', seq, write('dup')).settled)
    const typed = ledger.admit('pty-1', 'session', 3, write('3')).settled
    await new Promise((resolve) => setTimeout(resolve, 0))
    releaseFirst()
    await expect(Promise.all([...replay, typed])).resolves.toEqual([APPLIED, resend(2), resend(2)])

    const resent = [2, 3, 4].map(
      (seq) => ledger.admit('pty-1', 'session', seq, write(String(seq))).settled
    )
    await expect(Promise.all(resent)).resolves.toEqual([APPLIED, APPLIED, APPLIED])
    expect(writes).toEqual(['1', '2', '3', '4'])
  })

  it('never writes a dead stream frame that arrives after a gap ahead of the resend that fills it', async () => {
    const ledger = new TerminalInputSequenceLedger()
    const { writes, write } = recordingWrites()
    await ledger.admit('pty-1', 'session', 1, write('a', 'refused')).settled
    // The dead stream's later frame races the replacement's resend of seq 1.
    await expect(ledger.admit('pty-1', 'session', 3, write('c')).settled).resolves.toEqual(
      resend(1)
    )
    const resent = [1, 2, 3].map(
      (seq) => ledger.admit('pty-1', 'session', seq, write('abc'[seq - 1])).settled
    )
    await expect(Promise.all(resent)).resolves.toEqual([APPLIED, APPLIED, APPLIED])
    expect(writes).toEqual(['a', 'b', 'c'])
  })

  it('starts a new session at whatever sequence the client is up to', async () => {
    const ledger = new TerminalInputSequenceLedger()
    await expect(ledger.admit('pty-1', 'session', 40, applied).settled).resolves.toEqual(APPLIED)
    await expect(ledger.admit('pty-1', 'session', 41, applied).settled).resolves.toEqual(APPLIED)
  })

  it('scopes sequences by PTY so a session reused on another terminal is not deduped', () => {
    const ledger = new TerminalInputSequenceLedger()
    expect(ledger.admit('pty-1', 'session', 5, applied).duplicate).toBe(false)
    expect(ledger.admit('pty-2', 'session', 5, applied).duplicate).toBe(false)
  })

  it('evicts the least recently used session past its bound', () => {
    const ledger = new TerminalInputSequenceLedger()
    ledger.admit('pty-1', 'oldest', 1, applied)
    for (let index = 0; index < TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS; index += 1) {
      ledger.admit('pty-1', `session-${index}`, 1, applied)
    }
    expect(ledger.admit('pty-1', 'session-0', 1, applied).duplicate).toBe(true)
    expect(ledger.admit('pty-1', 'oldest', 1, applied).duplicate).toBe(false)
  })

  it('settles a duplicate only after the original write it repeats', async () => {
    const ledger = new TerminalInputSequenceLedger()
    let releaseFirst = (): void => {}
    ledger.admit(
      'pty-1',
      'session',
      1,
      () =>
        new Promise<TerminalInputWriteOutcome>((resolve) => {
          releaseFirst = () => resolve('applied')
        })
    )
    let duplicateSettled = false
    const duplicate = ledger.admit('pty-1', 'session', 1, applied)
    expect(duplicate.duplicate).toBe(true)
    void duplicate.settled.then(() => {
      duplicateSettled = true
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(duplicateSettled).toBe(false)
    releaseFirst()
    await expect(duplicate.settled).resolves.toEqual(APPLIED)
    expect(duplicateSettled).toBe(true)
  })

  it('names each ledger so a client can tell a restarted runtime from the one it sent to', () => {
    expect(new TerminalInputSequenceLedger().id).not.toBe(new TerminalInputSequenceLedger().id)
  })
})
