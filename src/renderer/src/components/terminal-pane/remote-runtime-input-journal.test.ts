import { describe, expect, it } from 'vitest'
import { createRemoteRuntimeInputJournal } from './remote-runtime-input-journal'

const endpoint = { handle: 'terminal-1', incarnationId: 'incarnation-1' }

describe('remote runtime input journal resend', () => {
  it('resends from the sequence the host asked for, filling given-up input with empty text', () => {
    const journal = createRemoteRuntimeInputJournal()
    for (const text of ['a', 'b', 'c', 'd']) {
      journal.record(endpoint, 'ledger-1', text)
    }
    journal.record(endpoint, 'ledger-1', '\x1b[1;1R', true)
    journal.acknowledge(1)
    journal.cancel(3)

    expect(journal.resendFrom(endpoint, 'ledger-1', 2)).toEqual([
      { seq: 2, text: 'b' },
      { seq: 3, text: '' },
      { seq: 4, text: 'd' },
      { seq: 5, text: '' }
    ])
  })

  it('still fills every slot the host lacks after the journal discarded its input', () => {
    const journal = createRemoteRuntimeInputJournal()
    journal.record(endpoint, 'ledger-1', 'a')
    journal.record(endpoint, 'ledger-1', 'b')
    journal.acknowledge(1)
    journal.discard()
    journal.record(endpoint, 'ledger-1', 'c')

    expect(journal.resendFrom(endpoint, 'ledger-1', 2)).toEqual([
      { seq: 2, text: '' },
      { seq: 3, text: 'c' }
    ])
  })

  it('never resends what the host already acked', () => {
    const journal = createRemoteRuntimeInputJournal()
    journal.record(endpoint, 'ledger-1', 'a')
    journal.record(endpoint, 'ledger-1', 'b')
    journal.acknowledge(1)

    expect(journal.resendFrom(endpoint, 'ledger-1', 1)).toEqual([{ seq: 2, text: 'b' }])
  })
})
