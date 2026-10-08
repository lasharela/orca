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
    const first = ledger.admit(
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
    const second = ledger.admit('pty-1', 'session', 2, async () => {
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
    })
    expect(ledger.admit('pty-1', 'session', 1, async () => {})).toBeNull()
    expect(ledger.admit('pty-1', 'session', 2, async () => {})).not.toBeNull()
  })

  it('scopes sequences by PTY so a session reused on another terminal is not deduped', () => {
    const ledger = new TerminalInputSequenceLedger()
    expect(ledger.admit('pty-1', 'session', 5, async () => {})).not.toBeNull()
    expect(ledger.admit('pty-2', 'session', 5, async () => {})).not.toBeNull()
  })

  it('evicts the least recently used session past its bound', () => {
    const ledger = new TerminalInputSequenceLedger()
    ledger.admit('pty-1', 'oldest', 1, async () => {})
    for (let index = 0; index < TERMINAL_INPUT_SEQUENCE_LEDGER_MAX_SESSIONS; index += 1) {
      ledger.admit('pty-1', `session-${index}`, 1, async () => {})
    }
    expect(ledger.admit('pty-1', 'session-0', 1, async () => {})).toBeNull()
    expect(ledger.admit('pty-1', 'oldest', 1, async () => {})).not.toBeNull()
  })
})
