import { setImmediate as nextMacrotask } from 'node:timers/promises';
import type { ResultOutcome, ResultWaiterOptions, ResultWaitOptions } from '../interfaces/result-waiter-options.interface.js';
import { parseDuration } from '../time/duration.js';

/** What a read in flight gives way to: an outcome settled locally, or the waiter closing. */
const SETTLED = Symbol('settled');

/** One wait in progress. */
interface Wait<T> {
  outcome?: ResultOutcome<T>;
  closed: boolean;
  /** Resolves the promise the read in flight races, so a local outcome or the close needn't wait for the store. Each read replaces it. */
  settle(): void;
  /** Ends the backoff's sleep early. */
  wake?: () => void;
}

/**
 * Waits for the outcome of something another part of the application runs (a workflow instance, a job): at once
 * when this process ends it and says so (`settle()`), else by reading the store with a backoff (every 25 ms at
 * first, doubling up to every second), which sees what other processes ended. A wait rejects past its `timeout`
 * (the work goes on), when its `signal` aborts, and as soon as the waiter closes, without reading the store again:
 * close it in `beforeApplicationShutdown`, which Nest runs after `onModuleDestroy` (where workers drain, so work that
 * ends during the drain still answers) and before the HTTP server closes.
 *
 * ```ts
 * @Injectable()
 * export class JobResults implements BeforeApplicationShutdown {
 *   private readonly results = new ResultWaiter<unknown>({ read: (id) => this.readOutcome(id) });
 *
 *   constructor(events: JobEvents) {
 *     events.completed$.subscribe((job) => this.results.settle(job.id, { value: job.result }));
 *   }
 *
 *   result(id: string, options?: ResultWaitOptions) {
 *     return this.results.wait(id, options);
 *   }
 *
 *   beforeApplicationShutdown() {
 *     return this.results.close();
 *   }
 * }
 * ```
 */
export class ResultWaiter<T = unknown> {
  private readonly waits = new Map<string, Set<Wait<T>>>();
  private readonly closing = new AbortController();

  constructor(private readonly options: ResultWaiterOptions<T>) {}

  /** Whether `close()` was called: every wait since rejects at once. */
  get closed(): boolean {
    return this.closing.signal.aborted;
  }

  /**
   * Resolves with the value `id` ended with, or rejects with its outcome's error; past `timeout` with the
   * `timeoutError`, when `signal` aborts with its reason, and once the waiter closed with the `closedError`.
   */
  async wait(id: string, options: ResultWaitOptions = {}): Promise<T> {
    const timeoutMs = options.timeout === undefined ? Infinity : parseDuration(options.timeout);
    const deadline = performance.now() + timeoutMs;
    options.signal?.throwIfAborted();

    const wait: Wait<T> = { closed: this.closed, settle: () => undefined };
    const nudge = () => wait.wake?.();
    const close = () => {
      wait.closed = true;
      wait.settle();
      nudge();
    };
    this.add(id, wait);
    this.closing.signal.addEventListener('abort', close);
    options.signal?.addEventListener('abort', nudge);

    try {
      for (let delay = 25; ; delay = Math.min(delay * 2, 1_000)) {
        const read = wait.outcome || wait.closed ? SETTLED : await this.readOrGiveWay(id, wait);
        if (wait.outcome) {
          return unwrap(wait.outcome);
        }
        if (read === SETTLED) {
          throw this.options.closedError?.(id) ?? new Error(`The waiter closed while waiting for the result of "${id}".`);
        }
        if (read) {
          return unwrap(read);
        }

        options.signal?.throwIfAborted();
        const remaining = deadline - performance.now();
        if (remaining <= 0) {
          throw this.options.timeoutError?.(id, timeoutMs) ?? new Error(`"${id}" didn't end within ${timeoutMs}ms.`);
        }
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, Math.min(delay, remaining));
          wait.wake = () => {
            clearTimeout(timer);
            resolve();
          };
        });
        wait.wake = undefined;
      }
    } finally {
      this.remove(id, wait);
      this.closing.signal.removeEventListener('abort', close);
      options.signal?.removeEventListener('abort', nudge);
    }
  }

  /**
   * Settles the waits for `id` with its outcome at once, without a read, and a read in flight gives way to it: for
   * what this process saw end. A function is only called when something waits for `id`. The first outcome wins.
   */
  settle(id: string, outcome: ResultOutcome<T> | (() => ResultOutcome<T>)): void {
    const waits = this.waits.get(id);
    if (!waits) {
      return;
    }

    const settled = typeof outcome === 'function' ? outcome() : outcome;
    for (const wait of waits) {
      wait.outcome ??= settled;
      wait.settle();
      wait.wake?.();
    }
  }

  /**
   * Ends every wait (they reject with the `closedError`), and makes later ones reject at once. Resolves after a
   * macrotask, so the callers that stopped waiting have answered by then: Node's `server.close()` closes only the
   * connections idle at that moment, and one that answers later stays open until its keep-alive timeout.
   */
  async close(): Promise<void> {
    this.closing.abort();
    await nextMacrotask();
  }

  /**
   * Reads `id`, or gives way when `wait` settles or closes first. Each read races a promise of its own: a race stays
   * subscribed to a promise until that settles, so one promise for the whole wait would keep every read's race.
   * Not `read`: a private member of that name would stop a subclass declaring one of its own.
   */
  private readOrGiveWay(id: string, wait: Wait<T>): Promise<ResultOutcome<T> | null | typeof SETTLED> {
    const givenWay = new Promise<typeof SETTLED>((resolve) => (wait.settle = () => resolve(SETTLED)));
    return Promise.race([this.options.read(id), givenWay]);
  }

  private add(id: string, wait: Wait<T>): void {
    let waits = this.waits.get(id);
    if (!waits) {
      this.waits.set(id, (waits = new Set()));
    }
    waits.add(wait);
  }

  private remove(id: string, wait: Wait<T>): void {
    const waits = this.waits.get(id);
    waits?.delete(wait);
    if (waits?.size === 0) {
      this.waits.delete(id);
    }
  }
}

function unwrap<T>(outcome: ResultOutcome<T>): T {
  if ('error' in outcome) {
    throw outcome.error;
  }
  return outcome.value;
}
