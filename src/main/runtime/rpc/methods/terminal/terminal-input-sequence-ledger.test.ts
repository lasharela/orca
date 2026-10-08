import { describe, expect, it } from 'vitest'
import {
  TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS,
  TerminalInputSequenceLedger
} from './terminal-input-sequence-ledger'

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
        new Promise<void>((resolve) => {
          releaseFirst = () => {
            writes.push('a')
            resolve()
          }
        })
    )
    const { settled: second } = ledger.admit('pty-1', 'session', 2, async () => {
      writes.push('b')
    })
    await Promise.resolve()
    expect(writes).toEqual([])
    releaseFirst()
    await Promise.all([first, second])
    expect(writes).toEqual(['a', 'b'])
  })

  it('refuses a sequence it already applied and keeps the session after a failed write', async () => {
    const ledger = new TerminalInputSequenceLedger()
    await ledger.admit('pty-1', 'session', 1, async () => {
      throw new Error('write failed')
    }).settled
    expect(ledger.admit('pty-1', 'session', 1, async () => {}).duplicate).toBe(true)
    expect(ledger.admit('pty-1', 'session', 2, async () => {}).duplicate).toBe(false)
  })

  it('scopes sequences by PTY so a session reused on another terminal is not deduped', () => {
    const ledger = new TerminalInputSequenceLedger()
    expect(ledger.admit('pty-1', 'session', 5, async () => {}).duplicate).toBe(false)
    expect(ledger.admit('pty-2', 'session', 5, async () => {}).duplicate).toBe(false)
  })

  it('evicts the least recently used session past its bound', () => {
    const ledger = new TerminalInputSequenceLedger()
    ledger.admit('pty-1', 'oldest', 1, async () => {})
    for (let index = 0; index < TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS; index += 1) {
      ledger.admit('pty-1', `session-${index}`, 1, async () => {})
    }
    expect(ledger.admit('pty-1', 'session-0', 1, async () => {}).duplicate).toBe(true)
    expect(ledger.admit('pty-1', 'oldest', 1, async () => {}).duplicate).toBe(false)
  })

  it('settles a duplicate only after the original write it repeats', async () => {
    const ledger = new TerminalInputSequenceLedger()
    let releaseFirst = (): void => {}
    ledger.admit(
      'pty-1',
      'session',
      1,
      () =>
        new Promise<void>((resolve) => {
          releaseFirst = resolve
        })
    )
    let duplicateSettled = false
    const duplicate = ledger.admit('pty-1', 'session', 1, async () => {})
    expect(duplicate.duplicate).toBe(true)
    void duplicate.settled.then(() => {
      duplicateSettled = true
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(duplicateSettled).toBe(false)
    releaseFirst()
    await duplicate.settled
    expect(duplicateSettled).toBe(true)
  })

  it('names each ledger so a client can tell a restarted runtime from the one it sent to', () => {
    expect(new TerminalInputSequenceLedger().id).not.toBe(new TerminalInputSequenceLedger().id)
  })
})
