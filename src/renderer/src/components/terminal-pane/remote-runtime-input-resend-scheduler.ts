// Why backoff: a host refuses input mostly while its PTY is briefly unwritable (an SSH provider
// reconnecting), and each resend that is refused again costs a round trip.
export const REMOTE_RUNTIME_INPUT_RESEND_DELAYS_MS: readonly number[] = [
  100, 250, 500, 1000, 2000, 4000, 8000, 8000, 8000
]

export type RemoteRuntimeInputResendScheduler = {
  /** Coalesces host requests into one pending resend from the oldest sequence asked for. */
  request: (fromSeq: number) => void
  /** The host applied input, so a later refusal starts a fresh backoff. */
  noteProgress: () => void
  cancel: () => void
}

/**
 * Resends input a host asked for again, on the stream that asked, so the journal survives a
 * refusal. Only refusals that outlast every delay give up (the host lost the terminal).
 */
export function createRemoteRuntimeInputResendScheduler(deps: {
  resend: (fromSeq: number) => void
  giveUp: () => void
}): RemoteRuntimeInputResendScheduler {
  let timer: ReturnType<typeof setTimeout> | null = null
  let fromSeq = Number.POSITIVE_INFINITY
  let attempts = 0

  const cancel = (): void => {
    if (timer !== null) {
      clearTimeout(timer)
      timer = null
    }
    fromSeq = Number.POSITIVE_INFINITY
  }

  return {
    request(seq) {
      fromSeq = Math.min(fromSeq, seq)
      if (timer !== null) {
        return
      }
      const delay = REMOTE_RUNTIME_INPUT_RESEND_DELAYS_MS[attempts]
      if (delay === undefined) {
        cancel()
        attempts = 0
        deps.giveUp()
        return
      }
      attempts += 1
      timer = setTimeout(() => {
        timer = null
        const resendFrom = fromSeq
        fromSeq = Number.POSITIVE_INFINITY
        deps.resend(resendFrom)
      }, delay)
    },
    noteProgress() {
      attempts = 0
    },
    cancel
  }
}
