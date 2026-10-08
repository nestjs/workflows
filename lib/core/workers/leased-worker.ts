import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { Logger } from '@nestjs/common';
import type { Clock } from '../interfaces/clock.interface.js';
import type { LeasedRun, LeasedWorkerOptions, LeaseRequest } from '../interfaces/leased-worker-options.interface.js';
import { systemClock } from '../time/clock.js';
import { parseDuration } from '../time/duration.js';

/**
 * A claim-execute loop under leases, any number of processes sharing a store: it claims due items (as many as it has
 * free slots), executes them, renews each one's lease every `heartbeatInterval` (a third of the lease by default), and
 * aborts an item's `signal` when a renewal finds its lease gone. It looks for work every `pollInterval`, and at once
 * after `kick()` (a local write made something due) or when an item ends. `drain()` runs due items on demand, for
 * tests and scripts.
 *
 * Shutdown is graceful: call `shutdown()` from `onModuleDestroy`. It stops claiming, waits for the claims in flight
 * (what they claim starts, told to stop at once), aborts every running item's `signal`, and waits up to
 * `shutdownTimeout` for them; the ones still running are then detached (no more renewals) and keep their lease until
 * it expires, as their work may still be going on.
 *
 * ```ts
 * @Injectable()
 * export class JobWorker implements OnApplicationBootstrap, OnModuleDestroy {
 *   private readonly worker = new LeasedWorker<Job>({
 *     claim: (lease, limit) => this.store.claim(lease, limit),
 *     renew: (job, until) => this.store.renew(job.id, job.token, until),
 *     execute: (job, run) => this.run(job, run),
 *   });
 *
 *   constructor(private readonly store: JobStore) {}
 *
 *   onApplicationBootstrap() {
 *     this.worker.start();
 *   }
 *
 *   onModuleDestroy() {
 *     return this.worker.shutdown();
 *   }
 * }
 * ```
 */
export class LeasedWorker<T> {
  /** The worker's id, shown as the lease owner. */
  readonly owner: string;
  /** The lease duration, in milliseconds. */
  readonly leaseMs: number;
  private readonly logger: Logger;
  private readonly clock: Clock;
  private readonly concurrency: number;
  private readonly pollMs: number;
  private readonly heartbeatMs: number;
  private readonly shutdownMs: number;
  private readonly produceMs: number;
  private readonly runs = new Set<Run<T>>();
  /** Claims in flight: shutdown waits for them, so nothing they claim starts unobserved. */
  private readonly claiming = new Set<Promise<unknown>>();
  private stopped = false;
  /** Ends the `drain()` rounds waiting now, at shutdown. */
  private readonly stopping = new Set<() => void>();
  private wake?: () => void;
  private kicked = false;
  private loop?: Promise<void>;
  /** When the loop next calls `produce()` (`performance.now()`). */
  private produceAt = 0;

  constructor(private readonly options: LeasedWorkerOptions<T>) {
    const name = options.name ?? 'worker';
    this.logger = new Logger(name);
    this.clock = options.clock ?? systemClock;
    this.owner = options.owner ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    this.leaseMs = parseDuration(options.leaseDuration ?? '30s');
    this.concurrency = options.concurrency ?? 10;
    this.pollMs = parseDuration(options.pollInterval ?? '1s');
    this.heartbeatMs = parseDuration(options.heartbeatInterval ?? Math.floor(this.leaseMs / 3));
    this.shutdownMs = parseDuration(options.shutdownTimeout ?? '10s');
    this.produceMs = parseDuration(options.produceInterval ?? 0);

    if (!Number.isInteger(this.concurrency) || this.concurrency < 1) {
      throw new TypeError(`${name}.concurrency (${this.concurrency}) must be a positive integer.`);
    }
    if (this.pollMs <= 0 || this.heartbeatMs <= 0) {
      throw new TypeError(`${name}.pollInterval and ${name}.heartbeatInterval must be positive, such as "1s".`);
    }
    if (this.heartbeatMs >= this.leaseMs) {
      throw new TypeError(`${name}.heartbeatInterval (${this.heartbeatMs}ms) must be shorter than ${name}.leaseDuration (${this.leaseMs}ms).`);
    }
  }

  /** The items it executes now. */
  get running(): T[] {
    return [...this.runs].map((run) => run.item);
  }

  /** Starts the polling loop (once). Without it, the worker only runs what `drain()` asks for. */
  start(): void {
    if (!this.loop && !this.stopped) {
      this.loop = this.poll();
    }
  }

  /** Look for work now instead of at the next poll (after a write in this process made something due). */
  kick(): void {
    this.kicked = true;
    this.wake?.();
  }

  /**
   * Executes every due item, round after round (`produce()`, then a claim of `concurrency` items, awaited), until a
   * round finds nothing: for tests (with a `ManualClock`), scripts, and workers a cron drives. Resolves to the
   * number of items it executed. A claim or production that fails rejects it.
   */
  async drain(options: { maxRounds?: number } = {}): Promise<number> {
    let total = 0;
    for (let round = 0; round < (options.maxRounds ?? 1_000) && !this.stopped; round++) {
      const produced = this.options.produce ? await this.options.produce() : 0;
      const executions = await this.claim(this.concurrency);
      if (executions.length === 0 && produced === 0) {
        break;
      }

      total += executions.length;
      await this.untilStopped(Promise.all(executions));
    }

    return total;
  }

  /**
   * Stops for good: no more claims, the claims in flight awaited, every running item's `signal` aborted, and at most
   * `shutdownTimeout` spent waiting for them before the rest are detached. Call it from `onModuleDestroy`.
   */
  async shutdown(): Promise<void> {
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    for (const stop of this.stopping) {
      stop();
    }
    this.wake?.();

    // What a claim in flight claims starts now, told to stop at once, so it is handed back before the store closes.
    await Promise.allSettled(this.claiming);
    for (const run of this.runs) {
      run.halt();
    }

    let timer: NodeJS.Timeout | undefined;
    await Promise.race([
      Promise.allSettled([...this.runs].map((run) => run.done)),
      new Promise((resolve) => (timer = setTimeout(resolve, this.shutdownMs))),
    ]);
    clearTimeout(timer);

    for (const run of this.runs) {
      run.detach();
    }
    await this.loop;
  }

  /**
   * Waits for `work`, or until shutdown. It races a promise of its own, not one the worker keeps: each race leaves a
   * reaction on its inputs until they settle, so a long-lived one would keep one per round.
   */
  private async untilStopped(work: Promise<unknown>): Promise<void> {
    let stop!: () => void;
    const stopped = new Promise<void>((resolve) => (stop = resolve));
    if (this.stopped) {
      stop();
    }
    this.stopping.add(stop);
    try {
      await Promise.race([work, stopped]);
    } finally {
      this.stopping.delete(stop);
    }
  }

  private async poll(): Promise<void> {
    while (!this.stopped) {
      if (this.options.produce && performance.now() >= this.produceAt) {
        this.produceAt = performance.now() + this.produceMs;
        try {
          await this.options.produce();
        } catch (error) {
          this.report(error, 'produce');
        }
      }

      const free = this.concurrency - this.runs.size;
      if (free > 0) {
        try {
          const executions = await this.claim(free);
          for (const execution of executions) {
            void execution.finally(() => this.kick());
          }
        } catch (error) {
          this.report(error, 'claim');
        }
      }

      await this.idle();
    }
  }

  private idle(): Promise<void> {
    if (this.kicked || this.stopped) {
      this.kicked = false;
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(timer);
        this.wake = undefined;
        resolve();
      };
      const timer = setTimeout(done, this.pollMs);
      this.wake = () => {
        this.kicked = false;
        done();
      };
    });
  }

  /** Claims due items and starts executing them (before any caller sees them). */
  private async claim(limit: number): Promise<Promise<void>[]> {
    if (limit <= 0) {
      return [];
    }

    const now = this.clock.now();
    const lease: LeaseRequest = { owner: this.owner, token: randomUUID(), now, until: now + this.leaseMs };
    const claim = this.options.claim(lease, limit);
    this.claiming.add(claim);
    try {
      const items = await claim;
      return items.map((item) => this.launch(item));
    } finally {
      this.claiming.delete(claim);
    }
  }

  private launch(item: T): Promise<void> {
    const run: Run<T> = new Run(item, () => this.renew(run));
    this.runs.add(run);
    if (this.stopped) {
      run.halt();
    }

    run.heartbeat = setInterval(() => void run.renew(), this.heartbeatMs);
    run.heartbeat.unref();
    let executing: Promise<void>;
    try {
      executing = this.options.execute(item, run);
    } catch (error) {
      executing = Promise.reject(error);
    }
    run.done = executing
      .catch((error: unknown) => this.report(error, 'execute'))
      .finally(() => {
        clearInterval(run.heartbeat);
        this.runs.delete(run);
      });
    return run.done;
  }

  private renew(run: Run<T>): Promise<boolean> {
    return this.options.renew(run.item, this.clock.now() + this.leaseMs);
  }

  private report(error: unknown, stage: 'claim' | 'produce' | 'execute'): void {
    if (this.options.onError) {
      this.options.onError(error, stage);
    } else {
      this.logger.error(`${stage === 'claim' ? 'Claiming' : stage === 'produce' ? 'Producing' : 'Executing'} failed; the worker carries on.`, error as Error);
    }
  }
}

/** A claimed item's run: its signal, its heartbeat, and what became of its lease. */
class Run<T> implements LeasedRun {
  heartbeat?: NodeJS.Timeout;
  done!: Promise<void>;
  private readonly controller = new AbortController();
  private lost = false;
  private isDetached = false;

  constructor(
    readonly item: T,
    private readonly renewer: () => Promise<boolean>,
  ) {}

  get signal(): AbortSignal {
    return this.controller.signal;
  }

  get leaseLost(): boolean {
    return this.lost;
  }

  get detached(): boolean {
    return this.isDetached;
  }

  async renew(): Promise<boolean> {
    if (this.lost || this.isDetached) {
      return !this.lost;
    }

    let held: boolean;
    try {
      held = await this.renewer();
    } catch {
      // Transient: the next heartbeat tries again before the lease runs out.
      return true;
    }
    if (!held) {
      this.loseLease();
    }
    return held;
  }

  loseLease(): void {
    if (this.lost) {
      return;
    }
    this.lost = true;
    clearInterval(this.heartbeat);
    this.controller.abort(new Error('The lease was lost: another worker may run this item now.'));
  }

  /** The worker shuts down: its work should stop. */
  halt(): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort(new Error('The worker is shutting down.'));
    }
  }

  /** The worker stopped waiting for it: no more renewals. */
  detach(): void {
    this.isDetached = true;
    clearInterval(this.heartbeat);
  }
}
