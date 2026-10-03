import type { Duration } from '../time/duration.js';

/**
 * How something ended, for `ResultWaiter`: its value, or the error its waits reject with (a failure, or "not
 * found").
 *
 * ```ts
 * const outcome: ResultOutcome<Report> = row.state === 'completed' ? { value: row.result } : { error: new JobFailedError(row) };
 * ```
 */
export type ResultOutcome<T> = { value: T } | { error: unknown };

/**
 * What `ResultWaiter` takes: how to read an outcome from the store, and the errors its waits reject with.
 *
 * ```ts
 * const results = new ResultWaiter<unknown>({
 *   read: async (id) => {
 *     const job = await store.get(id);
 *     return !job ? { error: new JobNotFoundError(id) } : job.state === 'completed' ? { value: job.result } : null;
 *   },
 *   timeoutError: (id, timeoutMs) => new JobResultTimeoutError(id, timeoutMs),
 *   closedError: (id) => new Error(`The application shut down while waiting for the result of job "${id}".`),
 * });
 * ```
 */
export interface ResultWaiterOptions<T> {
  /**
   * Reads the outcome from the store: `null` while it hasn't ended. Called when a wait starts, then after a backoff
   * (25 ms at first, doubling up to a second), until the wait ends; a rejection rejects the wait. A wait's `timeout`
   * and `signal` are only checked between reads, so a read that never answers holds the wait open: give it a timeout
   * of its own.
   */
  read(id: string): Promise<ResultOutcome<T> | null>;
  /** The error a wait rejects with past its `timeout`. Default: an `Error` that names `id`. */
  timeoutError?(id: string, timeoutMs: number): Error;
  /** The error a wait rejects with once the waiter closed (the application shuts down). Default: an `Error` that names `id`. */
  closedError?(id: string): Error;
}

/**
 * What `ResultWaiter.wait()` takes.
 *
 * ```ts
 * await results.wait(job.id, { timeout: '30s', signal: request.signal });
 * ```
 */
export interface ResultWaitOptions {
  /** Stop waiting after this long, in wall-clock time (not a `Clock`'s). Default: no limit. */
  timeout?: Duration;
  /** Stop waiting when it aborts: the wait rejects with its reason. */
  signal?: AbortSignal;
}
