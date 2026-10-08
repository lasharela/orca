import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  TerminalStreamOpcode,
  decodeTerminalStreamFrame,
  decodeTerminalStreamText,
  encodeTerminalStreamFrame
} from '../../../../shared/terminal-stream-protocol'
import {
  createRemoteRuntimeTransportMocks,
  type MultiplexSubscriptionCallbacks
} from './remote-runtime-pty-transport-test-harness'
import { REMOTE_RUNTIME_AUTO_RECOVERY_TIMEOUT_MS } from './remote-runtime-pty-recovery-state'

let subscriptionCallbacks: MultiplexSubscriptionCallbacks = null
let resolvedPaneHandle = 'terminal-1'

const {
  runtimeSubscribe,
  subscriptionSendBinary,
  emitMultiplexReady,
  latestSubscribePayload,
  emitSnapshot,
  subscribeFrameCount,
  resetRemoteRuntimeTransport
} = createRemoteRuntimeTransportMocks({
  getCallbacks: () => subscriptionCallbacks,
  setCallbacks: (callbacks) => {
    subscriptionCallbacks = callbacks
  },
  getResolvedPaneHandle: () => resolvedPaneHandle,
  setResolvedPaneHandle: (handle) => {
    resolvedPaneHandle = handle
  }
})

type SentInput = { seq: number; text: string }

/** Input frames sent after the `subscribeCount`-th Subscribe frame (stream ids restart per connection). */
function sentInputs(subscribeCount = 0): SentInput[] {
  let subscribes = 0
  return subscriptionSendBinary.mock.calls.flatMap(([bytes]) => {
    const frame = decodeTerminalStreamFrame(bytes)
    if (frame?.opcode === TerminalStreamOpcode.Subscribe) {
      subscribes += 1
    }
    return frame?.opcode === TerminalStreamOpcode.Input && subscribes >= subscribeCount
      ? [{ seq: frame.seq, text: decodeTerminalStreamText(frame.payload) }]
      : []
  })
}

function sentText(): string {
  return sentInputs()
    .map((input) => input.text)
    .join('')
}

/** Real hosts publish `subscribed` before the snapshot that completes the attach. */
function attachStream(
  streamId: number,
  capabilities: Record<string, 1>,
  inputLedgerId = 'ledger-1'
): void {
  subscriptionCallbacks?.onResponse({
    ok: true,
    result: {
      type: 'subscribed',
      streamId,
      capabilities,
      ...(capabilities.inputAck ? { inputLedgerId } : {})
    }
  })
  emitSnapshot(streamId, 'prompt$ ')
}

function emitInputAck(streamId: number, seq: number): void {
  subscriptionCallbacks?.onBinary?.(
    encodeTerminalStreamFrame({
      opcode: TerminalStreamOpcode.InputAck,
      streamId,
      seq,
      payload: new Uint8Array()
    })
  )
}

async function connectPane(capabilities: Record<string, 1>) {
  const { createRemoteRuntimePtyTransport } = await import('./remote-runtime-pty-transport')
  const transport = createRemoteRuntimePtyTransport('env-1', {
    worktreeId: 'wt-1',
    tabId: 'tab-1',
    leafId: 'pane:1'
  })
  transport.attach({ existingPtyId: 'remote:terminal-1', cols: 80, rows: 24, callbacks: {} })
  await vi.waitFor(() => expect(subscribeFrameCount()).toBe(1))
  const streamId = latestSubscribePayload().streamId
  attachStream(streamId, capabilities)
  await vi.waitFor(() => expect(transport.isConnected()).toBe(true))
  return { transport, streamId }
}

async function reconnect(
  capabilities: Record<string, 1>,
  attempt: number,
  inputLedgerId?: string
): Promise<void> {
  await vi.waitFor(() => expect(subscribeFrameCount()).toBe(attempt))
  attachStream(latestSubscribePayload().streamId, capabilities, inputLedgerId)
}

describe('remote pane input across a silent outage', () => {
  beforeEach(() => {
    resetRemoteRuntimeTransport()
  })

  it('names an input session and negotiates acks on subscribe', async () => {
    const { transport } = await connectPane({ inputAck: 1 })
    const payload = latestSubscribePayload()
    expect(payload.capabilities).toMatchObject({ inputAck: 1 })
    expect(payload.inputSessionId).toEqual(expect.any(String))
    transport.destroy?.()
  })

  it('replays input the dead stream never acknowledged onto the replacement stream (P1-1)', async () => {
    const { transport, streamId } = await connectPane({ inputAck: 1 })
    transport.sendInput('echo 1\r', 'driving')
    await vi.waitFor(() => expect(sentInputs()).toHaveLength(1))
    emitInputAck(streamId, sentInputs()[0].seq)
    // Typed after the link went silent but before liveness noticed: handed to the dead socket.
    transport.sendInput('echo 2\r', 'driving')
    await vi.waitFor(() => expect(sentInputs()).toHaveLength(2))
    const lostSeq = sentInputs()[1].seq

    subscriptionCallbacks?.onClose?.()
    expect(transport.sendInput('echo 3\r', 'driving')).toBe(true)
    await vi.waitFor(() => expect(runtimeSubscribe).toHaveBeenCalledTimes(2))
    await reconnect({ inputAck: 1 }, 2)

    await vi.waitFor(() =>
      expect(sentInputs(2)).toEqual([
        { seq: lostSeq, text: 'echo 2\r' },
        { seq: lostSeq + 1, text: 'echo 3\r' }
      ])
    )
    transport.destroy?.()
  })

  it('drops unacknowledged input instead of replaying it into a restarted host runtime', async () => {
    const { transport } = await connectPane({ inputAck: 1 })
    // Applied by the old runtime, but its ack died with the link.
    transport.sendInput('make deploy\r', 'driving')
    await vi.waitFor(() => expect(sentInputs()).toHaveLength(1))

    subscriptionCallbacks?.onClose?.()
    await vi.waitFor(() => expect(runtimeSubscribe).toHaveBeenCalledTimes(2))
    // Same handle and PTY, but the runtime restarted, so its dedupe ledger is new.
    await reconnect({ inputAck: 1 }, 2, 'ledger-after-restart')
    transport.sendInput('ls\r', 'driving')

    await vi.waitFor(() => expect(sentInputs(2).map((input) => input.text)).toEqual(['ls\r']))
    transport.destroy?.()
  })

  it('keeps Ctrl+C in sequence with typing across a silent outage', async () => {
    const { transport } = await connectPane({ inputAck: 1 })
    transport.sendInput('echo canceled', 'driving')
    const interrupted = transport.sendInputAccepted?.('\x03', 'driving')
    await vi.waitFor(() => expect(sentText()).toBe('echo canceled\x03'))
    transport.sendInput('echo next\r', 'driving')
    await vi.waitFor(() => expect(sentText()).toBe('echo canceled\x03echo next\r'))

    subscriptionCallbacks?.onClose?.()
    await vi.waitFor(() => expect(runtimeSubscribe).toHaveBeenCalledTimes(2))
    await reconnect({ inputAck: 1 }, 2)

    await vi.waitFor(() =>
      expect(
        sentInputs(2)
          .map((input) => input.text)
          .join('')
      ).toBe('echo canceled\x03echo next\r')
    )
    emitInputAck(latestSubscribePayload().streamId, sentInputs(2).at(-1)?.seq ?? 0)
    await expect(interrupted).resolves.toBe(true)
    transport.destroy?.()
  })

  it('keeps a paste still in size validation ahead of keys typed after the outage', async () => {
    const { transport } = await connectPane({ inputAck: 1 })
    const paste = 'A'.repeat(300 * 1024)
    transport.sendInput(paste, 'driving')
    subscriptionCallbacks?.onError?.({
      code: 'remote_runtime_unavailable',
      message: 'Could not connect to the remote Orca runtime.'
    })
    transport.sendInput('B\r', 'driving')

    await vi.waitFor(() => expect(runtimeSubscribe).toHaveBeenCalledTimes(2))
    await reconnect({ inputAck: 1 }, 2)

    // Why the long timeout: 300 KiB byte-length validation yields and is slow on a loaded runner.
    await vi.waitFor(() => expect(sentText()).toBe(`${paste}B\r`), { timeout: 10_000 })
    transport.destroy?.()
  })

  it('never replays toward a host that does not acknowledge input', async () => {
    const { transport } = await connectPane({ outputPause: 1 })
    transport.sendInput('echo 1\r', 'driving')
    await vi.waitFor(() => expect(sentInputs()).toHaveLength(1))
    expect(sentInputs()[0].seq).toBe(0)

    subscriptionCallbacks?.onClose?.()
    transport.sendInput('echo 2\r', 'driving')
    await vi.waitFor(() => expect(runtimeSubscribe).toHaveBeenCalledTimes(2))
    await reconnect({ outputPause: 1 }, 2)

    await vi.waitFor(() => expect(sentInputs(2)).toEqual([{ seq: 0, text: 'echo 2\r' }]))
    transport.destroy?.()
  })

  it('keeps debounced keys pending at detection instead of splitting the line (P1-2)', async () => {
    const { transport } = await connectPane({ outputPause: 1 })
    for (const ch of 'PZ600-0') {
      transport.sendInput(ch, 'driving')
    }
    await vi.waitFor(() => expect(sentText()).toBe('PZ600-0'))
    // Still inside the 8 ms debounce when the outage is detected.
    transport.sendInput('1', 'driving')
    transport.sendInput('2', 'driving')
    subscriptionCallbacks?.onError?.({
      code: 'remote_runtime_unavailable',
      message: 'Could not connect to the remote Orca runtime.'
    })
    transport.sendInput('3', 'driving')
    transport.sendInput('\r', 'driving')

    await vi.waitFor(() => expect(runtimeSubscribe).toHaveBeenCalledTimes(2))
    await reconnect({ outputPause: 1 }, 2)

    await vi.waitFor(() => expect(sentText()).toBe('PZ600-0123\r'))
    transport.destroy?.()
  })

  it('keeps held input past the auto-recovery window and delivers it when the same terminal returns (P1-3)', async () => {
    vi.useFakeTimers()
    try {
      const { transport } = await connectPane({ inputAck: 1 })
      let hostReachable = false
      runtimeSubscribe.mockImplementation(
        async (_args: unknown, callbacks: NonNullable<MultiplexSubscriptionCallbacks>) => {
          if (!hostReachable) {
            throw Object.assign(new Error('Could not connect to the remote Orca runtime.'), {
              code: 'remote_runtime_unavailable'
            })
          }
          subscriptionCallbacks = callbacks
          queueMicrotask(emitMultiplexReady)
          return { unsubscribe: vi.fn(), sendBinary: subscriptionSendBinary }
        }
      )

      subscriptionCallbacks?.onClose?.()
      expect(transport.sendInput('HELD-1\r', 'driving')).toBe(true)
      await vi.advanceTimersByTimeAsync(REMOTE_RUNTIME_AUTO_RECOVERY_TIMEOUT_MS + 1_000)
      expect(transport.getRecoveryState?.().phase).toBe('disconnected')
      // Typed under the "disconnected" banner: still held for the same terminal.
      expect(transport.sendInput('LATE-2\r', 'driving')).toBe(true)

      hostReachable = true
      expect(transport.retryRecovery?.()).toBe(true)
      await vi.waitFor(() => expect(subscribeFrameCount()).toBe(2))
      attachStream(latestSubscribePayload().streamId, { inputAck: 1 })
      await vi.advanceTimersByTimeAsync(50)

      expect(
        sentInputs(2)
          .map((input) => input.text)
          .join('')
      ).toBe('HELD-1\rLATE-2\r')
      transport.destroy?.()
    } finally {
      vi.useRealTimers()
    }
  })
})
