import {
  decodeTerminalStreamText,
  type TerminalStreamFrame
} from '../../../../../shared/terminal-stream-protocol'
import { isTerminalInputLockedForClient, sendTerminalStreamInput } from './terminal-input-delivery'
import type { TerminalInputSequenceLedger } from './terminal-input-sequence-ledger'
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
  const deliver = async (): Promise<void> => {
    if (!text || isTerminalInputLockedForClient(runtime, stream.ptyId, stream.client)) {
      return
    }
    const claimed = await inputClaimTail
    if (!claimed || isTerminalInputLockedForClient(runtime, stream.ptyId, stream.client)) {
      return
    }
    const outcome = await sendTerminalStreamInput(runtime, {
      terminal: stream.terminal,
      text,
      client: stream.client,
      isMobile: stream.isMobile
    })
    state.notifyStreamWriteUnavailable(stream, outcome)
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
    deliver
  )
  // Why ack a duplicate too: the client replays until acked, and the first copy's ack may have died with its connection.
  void settled.then(() => state.sendInputAck(stream, inputSeq))
}
