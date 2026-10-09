import {
  decodeTerminalStreamText,
  type TerminalStreamFrame
} from '../../../../../shared/terminal-stream-protocol'
import { isTerminalInputLockedForClient, sendTerminalStreamInput } from './terminal-input-delivery'
import type {
  TerminalInputSequenceLedger,
  TerminalInputWriteOutcome
} from './terminal-input-sequence-ledger'
import type { TerminalMultiplexConnection } from './terminal-multiplex-connection'
import type { TerminalMultiplexStream } from './terminal-stream-types'

export function handleMultiplexInputFrame(
  state: TerminalMultiplexConnection,
  inputSequenceLedger: TerminalInputSequenceLedger,
  stream: TerminalMultiplexStream,
  frame: TerminalStreamFrame
): void {
  const { runtime } = state
  const text = decodeTerminalStreamText(frame.payload)
  // Mobile already has the higher-priority floor, so a rejected desktop claim must not suppress later phone input.
  const inputClaimTail = stream.isMobile ? Promise.resolve(true) : stream.desktopClaimTail
  // Why 'applied' for locked or unclaimed input: it is dropped by policy, and a replay must not run it later.
  const deliver = async (): Promise<TerminalInputWriteOutcome> => {
    if (!text || isTerminalInputLockedForClient(runtime, stream.ptyId, stream.client)) {
      return 'applied'
    }
    const claimed = await inputClaimTail
    if (!claimed || isTerminalInputLockedForClient(runtime, stream.ptyId, stream.client)) {
      return 'applied'
    }
    const outcome = await sendTerminalStreamInput(runtime, {
      terminal: stream.terminal,
      text,
      client: stream.client,
      isMobile: stream.isMobile
    })
    state.notifyStreamWriteUnavailable(stream, outcome)
    return outcome === 'delivered'
      ? 'applied'
      : outcome === 'rejected'
        ? 'refused'
        : 'delivery-unknown'
  }
  if (stream.inputSessionId === null || frame.seq <= 0) {
    void deliver()
    return
  }
  const inputSeq = frame.seq
  const { settled } = inputSequenceLedger.admit(
    stream.ptyId,
    stream.inputSessionId,
    inputSeq,
    stream,
    deliver
  )
  // Why ack a duplicate too: the client replays until acked, and the first copy's ack may have died with its connection.
  void settled.then((admission) => {
    if (admission !== 'unacked') {
      state.sendInputAck(stream, inputSeq, admission === 'delivery-unknown')
    }
  })
}
