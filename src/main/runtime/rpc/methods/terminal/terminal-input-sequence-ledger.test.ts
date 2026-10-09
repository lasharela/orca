import { describe, expect, it } from 'vitest'
import {
  TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS,
  TerminalInputSequenceLedger,
  type TerminalInputWriteOutcome
} from './terminal-input-sequence-ledger'

let carriers = 0
const newCarrier = () => ({ streamId: ++carriers })
const carrier = newCarrier()
const applied = async (): Promise<TerminalInputWriteOutcome> => 'applied'

describe('TerminalInputSequenceLedger', () => {
  it('runs admitted writes of one session in sequence order, even when an earlier write is slow', async () => {
    const ledger = new TerminalInputSequenceLedger()
    const writes: string[] = []
    let releaseFirst = (): void => {}
    const { settled: first } = ledger.admit(
      'pty-1',
      'session',
      1,
      carrier,
      () =>
        new Promise<TerminalInputWriteOutcome>((resolve) => {
          releaseFirst = () => {
            writes.push('a')
            resolve('applied')
          }
        })
    )
    const { settled: second } = ledger.admit('pty-1', 'session', 2, carrier, async () => {
      writes.push('b')
      return 'applied'
    })
    await Promise.resolve()
    expect(writes).toEqual([])
    releaseFirst()
    await expect(Promise.all([first, second])).resolves.toEqual(['applied', 'applied'])
    expect(writes).toEqual(['a', 'b'])
  })

  it('reports a thrown write as delivery-unknown, including to its replay', async () => {
    const ledger = new TerminalInputSequenceLedger()
    await expect(
      ledger.admit('pty-1', 'session', 1, carrier, async () => {
        throw new Error('write failed')
      }).settled
    ).resolves.toBe('delivery-unknown')
    const replay = ledger.admit('pty-1', 'session', 1, newCarrier(), applied)
    expect(replay.duplicate).toBe(true)
    await expect(replay.settled).resolves.toBe('delivery-unknown')
    expect(ledger.admit('pty-1', 'session', 2, carrier, applied).duplicate).toBe(false)
  })

  it('never acks a refused write, and writes it when the client replays it', async () => {
    const ledger = new TerminalInputSequenceLedger()
    const writes: string[] = []
    const write =
      (text: string, outcome: TerminalInputWriteOutcome = 'applied') =>
      async (): Promise<TerminalInputWriteOutcome> => {
        if (outcome === 'applied') {
          writes.push(text)
        }
        return outcome
      }
    const dead = newCarrier()
    await ledger.admit('pty-1', 'session', 1, dead, write('a')).settled
    // The SSH provider is reconnecting, so the PTY refuses seq 2; seq 3 is already queued behind it.
    const refused = ledger.admit('pty-1', 'session', 2, dead, write('b', 'refused'))
    const queued = ledger.admit('pty-1', 'session', 3, dead, write('c'))
    await expect(refused.settled).resolves.toBe('unacked')
    await expect(queued.settled).resolves.toBe('unacked')
    // Typed on the same stream after the refusal: running it would skip the refused bytes.
    await expect(ledger.admit('pty-1', 'session', 4, dead, write('d')).settled).resolves.toBe(
      'unacked'
    )
    expect(writes).toEqual(['a'])

    const replacement = newCarrier()
    const replay = [2, 3, 4].map(
      (seq) => ledger.admit('pty-1', 'session', seq, replacement, write('bcd'[seq - 2])).settled
    )
    await expect(Promise.all(replay)).resolves.toEqual(['applied', 'applied', 'applied'])
    expect(writes).toEqual(['a', 'b', 'c', 'd'])
  })

  it('lets a new stream resume past a refused write the client chose not to replay', async () => {
    const ledger = new TerminalInputSequenceLedger()
    await ledger.admit('pty-1', 'session', 1, carrier, async () => 'refused').settled
    // Seq 1 was a terminal query reply, which clients never replay.
    await expect(ledger.admit('pty-1', 'session', 2, newCarrier(), applied).settled).resolves.toBe(
      'applied'
    )
  })

  it('scopes sequences by PTY so a session reused on another terminal is not deduped', () => {
    const ledger = new TerminalInputSequenceLedger()
    expect(ledger.admit('pty-1', 'session', 5, carrier, applied).duplicate).toBe(false)
    expect(ledger.admit('pty-2', 'session', 5, carrier, applied).duplicate).toBe(false)
  })

  it('evicts the least recently used session past its bound', () => {
    const ledger = new TerminalInputSequenceLedger()
    ledger.admit('pty-1', 'oldest', 1, carrier, applied)
    for (let index = 0; index < TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS; index += 1) {
      ledger.admit('pty-1', `session-${index}`, 1, carrier, applied)
    }
    expect(ledger.admit('pty-1', 'session-0', 1, carrier, applied).duplicate).toBe(true)
    expect(ledger.admit('pty-1', 'oldest', 1, carrier, applied).duplicate).toBe(false)
  })

  it('settles a duplicate only after the original write it repeats', async () => {
    const ledger = new TerminalInputSequenceLedger()
    let releaseFirst = (): void => {}
    ledger.admit(
      'pty-1',
      'session',
      1,
      carrier,
      () =>
        new Promise<TerminalInputWriteOutcome>((resolve) => {
          releaseFirst = () => resolve('applied')
        })
    )
    let duplicateSettled = false
    const duplicate = ledger.admit('pty-1', 'session', 1, newCarrier(), applied)
    expect(duplicate.duplicate).toBe(true)
    void duplicate.settled.then(() => {
      duplicateSettled = true
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(duplicateSettled).toBe(false)
    releaseFirst()
    await expect(duplicate.settled).resolves.toBe('applied')
    expect(duplicateSettled).toBe(true)
  })

  it('names each ledger so a client can tell a restarted runtime from the one it sent to', () => {
    expect(new TerminalInputSequenceLedger().id).not.toBe(new TerminalInputSequenceLedger().id)
  })
})
