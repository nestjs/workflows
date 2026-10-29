import type { Duration } from '../time/duration.js';
import type { Clock } from './clock.interface.js';

/**
 * The lease `LeasedWorker` asks its `claim` to take: every item the claim returns is leased to `owner` under `token`
 * until `until`.
 *
 * ```ts
 * claim: ({ owner, token, now, until }, limit) => store.claim({ owner, token, now, leaseUntil: until, limit }),
 * ```
 */
export interface LeaseRequest {
  /** The worker's id (`LeasedWorker.owner`), for the store's `leaseOwner`. */
  owner: string;
  /** A new token for this claim: the store fences every write under the lease with it. */
  token: string;
  /** The clock's time of the claim. */
  now: number;
  /** When the lease ends unless renewed: `now` plus the lease duration. */
  until: number;
}

/**
 * One claimed item's run, which `execute` receives: its `signal` aborts when the lease is lost (a renewal found it
 * gone) or the worker shuts down, and the worker renews its lease every `heartbeatInterval` until `execute` settles.
 *
 * ```ts
 * execute: async (job, run) => {
 *   const result = await handler(job.data, { signal: run.signal });
 *   if (!(await store.complete(job.id, job.token, result))) {
 *     run.loseLease(); // another worker took the job over
 *   }
 * },
 * ```
 */
export interface LeasedRun {
  /** Aborts when the lease is lost or the worker shuts down: `leaseLost` tells which. */
  readonly signal: AbortSignal;
  /** The lease is gone: another worker may run the item now, so write nothing more under it. */
  readonly leaseLost: boolean;
  /** The worker stopped waiting for this run at shutdown (`shutdownTimeout`): it no longer renews, and the run should write nothing more. */
  readonly detached: boolean;
  /**
   * Renews the lease now, as the heartbeat does (a long step that proves it's alive): `false` once it's lost. A renewal
   * that fails (the store is unreachable) keeps the lease, which the next heartbeat renews before it runs out. One
   * renewal is in flight at a time: called while one is (a heartbeat's held up by the store), it waits for that one, then
   * for one more that goes out as soon as it answered, or beside it once it went a heartbeat interval unanswered, and
   * resolves with that one's answer. None goes once the lease was lost, the run ended or was detached, before the call
   * or meanwhile: it then resolves `false` if the lease was lost, else `true`.
   */
  renew(): Promise<boolean>;
  /** Marks the lease lost, when a fenced write found it gone: aborts `signal` and stops renewing. */
  loseLease(): void;
}

/**
 * What `LeasedWorker` takes: how to claim, renew and execute items (the store's side), and the loop's settings, which
 * default to workflows' (10 at once, a poll a second, 30 s leases renewed every third of that, a 10 s drain).
 *
 * ```ts
 * const worker = new LeasedWorker<Job>({
 *   concurrency: 5,
 *   claim: (lease, limit) => store.claimJobs(lease, limit),
 *   renew: (job, until) => store.renewJob(job.id, job.token, until),
 *   execute: (job, run) => runJob(job, run),
 * });
 * ```
 */
export interface LeasedWorkerOptions<T> {
  /** Leases up to `limit` due items under `lease`; `[]` when none is due. */
  claim(lease: LeaseRequest, limit: number): Promise<T[]>;
  /** Extends the lease of `item` to `until`; `false` when it is gone (its token isn't the item's any more). */
  renew(item: T, until: number): Promise<boolean>;
  /** Runs one claimed item. It may find `run.signal` already aborted (claimed as the worker shut down): hand it back. */
  execute(item: T, run: LeasedRun): Promise<void>;
  /**
   * Called before claiming, in each poll (at most every `produceInterval`) and each round of `drain()`: work that makes
   * items due (a schedule's occurrences). Resolves to how many it made: `drain()` goes on while it makes some.
   */
  produce?(): Promise<number>;
  /** Names the worker in errors about its options, as their prefix: `worker.concurrency`. Default `'worker'`. */
  name?: string;
  /** The worker's id, shown as the lease owner. Default `hostname:pid:random`. */
  owner?: string;
  /** Items executed at once by this worker. Default 10. */
  concurrency?: number;
  /** How often to look for due items when not kicked. Default `'1s'`. */
  pollInterval?: Duration;
  /** How long a claim is valid without a renewal. Default `'30s'`. */
  leaseDuration?: Duration;
  /** How often a running item's lease is renewed. Default a third of `leaseDuration`. */
  heartbeatInterval?: Duration;
  /** How long `shutdown()` waits for running items before detaching them. Default `'10s'`. */
  shutdownTimeout?: Duration;
  /** The least time between two `produce()` calls of the polling loop (wall-clock). Default 0: every poll. */
  produceInterval?: Duration;
  /** Where lease times come from. Default `systemClock`. */
  clock?: Clock;
  /** Where the loop reports what failed; it carries on. Default: logged. `drain()` throws instead. */
  onError?(error: unknown, stage: 'claim' | 'produce' | 'execute'): void;
}
