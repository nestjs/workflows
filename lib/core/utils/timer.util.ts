/**
 * The longest delay `setTimeout` and `setInterval` support: 2^31 - 1 ms, about 24.8 days. Node runs a longer one
 * after 1 ms instead (with a `TimeoutOverflowWarning`), so a step `timeout: '30d'` would time out every attempt at
 * once, and a `heartbeatInterval` of '30d' would renew the lease every millisecond.
 *
 * @internal
 */
export const MAX_TIMER_DELAY = 2 ** 31 - 1;

/** @internal `setTimeout`, with delays beyond what timers support treated as the longest one. */
export function setTimer(callback: () => void, ms: number): NodeJS.Timeout {
  return setTimeout(callback, Math.min(ms, MAX_TIMER_DELAY));
}

/** @internal `setInterval`, capped like `setTimer()`. */
export function setRepeatingTimer(callback: () => void, ms: number): NodeJS.Timeout {
  return setInterval(callback, Math.min(ms, MAX_TIMER_DELAY));
}
