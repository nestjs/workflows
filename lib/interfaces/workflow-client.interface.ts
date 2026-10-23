import type { Duration } from '../core/time/duration.js';
import type { WorkflowInstance, WorkflowStatus } from './workflow-instance.interface.js';

export interface StartWorkflowOptions {
  /**
   * Instance id; default a random UUID. Starting twice with the same id (same
   * workflow and input) returns the existing instance instead of a second one,
   * so derive it from the business key: `order-${orderId}`. Inside a workflow
   * step, a start without an id gets one derived from the step's
   * `idempotencyKey`, the workflow's name and how many instances of it the step
   * started before, so a retried step gets its instances back (with the input
   * they were first started with) instead of starting new ones.
   */
  id?: string;
  /**
   * Pin a version (a positive integer). Default: the highest version of the
   * workflow's name that this application registers, also when you pass a class.
   * Starting by name a workflow this process does not register requires it.
   */
  version?: number;
  /**
   * How long the instance may run, overriding the workflow's `@Workflow(name, { timeout })`:
   * past it, the instance compensates and ends as `failed` with a `WorkflowTimeoutError`.
   * Counted from this call; ignored when the instance already exists.
   */
  timeout?: Duration;
  /**
   * The key the workflow's per-key concurrency limit counts the instance under, instead of the
   * one its `concurrency.key` computes. Needs a limit with a `key`.
   */
  concurrencyKey?: string;
  /**
   * The key the workflow's per-key rate limit counts the instance under, instead of the one its
   * `rateLimit.key` computes. Needs a rate limit with a `key`.
   */
  rateLimitKey?: string;
  /**
   * Lower runs first: an integer from 1 to 2,097,151. Due instances are claimed by priority, then most
   * overdue first; instances without one go before every prioritized one. The instance keeps it for every
   * execution (each resumption waits its turn by it), and its children inherit it. Default: none.
   */
  priority?: number;
  /**
   * Your ORM's transaction: the `tx` (or `EntityManager`, `Transaction`...) your transaction
   * callback receives. The instance is created in it, so it exists if and only if your
   * transaction commits, together with the rows you wrote. The worker picks it up after the
   * commit, at its next poll. Needs a store on your database that implements
   * `createInTransaction()`.
   */
  transaction?: unknown;
}

export interface SignalWorkflowOptions {
  /** Correlation key: only waits with the same key take the signal. */
  key?: string;
  /**
   * Deduplication id, unique per signal name, such as the id of the message or event that
   * causes the signal. A signal with the same name and id stored earlier makes this call a
   * no-op that returns that signal's `signalId` with `created: false`: a sender that runs
   * again (a redelivered message, a retried job) stores its signal once, and the first payload
   * wins. The same name and id with a different key throws `WorkflowIdConflictError`.
   * Inside a workflow step, a signal sent without an id gets one derived from the step's
   * `idempotencyKey`, so a retried step doesn't send it again.
   */
  id?: string;
  /**
   * Your ORM's transaction, as for `start()`: the signal, and the wake-up of the instances
   * waiting for it, commit with your writes. Signals queue behind it until it ends, so keep
   * it short. On PostgreSQL it must be READ COMMITTED (the default).
   */
  transaction?: unknown;
}

/** What `result()` and `startAndWait()` take. */
export interface WorkflowResultOptions {
  /**
   * How long to wait. Past it, the call rejects with `WorkflowResultTimeoutError`, and the
   * instance keeps running. Wall-clock time, not the module's clock. Default: no limit. A hard
   * deadline: a store read still in flight when it passes isn't awaited, so a timeout shorter than
   * one round trip to the store (0 included) rejects, also for an instance that has ended. To
   * check an instance without waiting, use `getStatus()`.
   */
  timeout?: Duration;
  /** Stops waiting once aborted (a client that went away), rejecting with the signal's reason. */
  signal?: AbortSignal;
}

/** What `signal()` returns. */
export interface WorkflowSignalSendResult {
  /** The stored signal's id: the new one, or with `created: false` the one stored earlier with the same `id`. */
  signalId: number;
  /** Instances parked on a matching wait that this call woke. */
  woken: number;
  /** `false` if a signal with the same name and `id` was stored earlier: nothing was written. */
  created: boolean;
}

/** What `start()` returns: safe to send to a client (no input, no lease details). */
export interface WorkflowStartResult {
  id: string;
  workflow: string;
  version: number;
  /** False if an instance with this id already existed. */
  created: boolean;
  status: WorkflowStatus;
}

export interface WorkflowCancelResult extends WorkflowInstance {
  /**
   * True if this call requested the cancellation. False if the instance was
   * already compensating or had finished, or a cancel was already requested
   * (the first reason is kept). For `terminate()`: false if the instance had
   * finished or a terminate was already requested.
   */
  accepted: boolean;
}

export interface WorkflowListFilter {
  status?: WorkflowStatus | WorkflowStatus[];
  /** Workflow name. */
  workflow?: string;
  /** With `workflow`: only instances on this version (to see whether an old version has drained). */
  version?: number;
  /** Only the children this instance started with `ctx.startChild()`. */
  parentId?: string;
  /** Only the instances this schedule started. */
  scheduleId?: string;
  /** Default 100. */
  limit?: number;
  offset?: number;
}

/** What `purge()` takes. */
export interface WorkflowPurgeOptions {
  /**
   * How long finished instances are kept, such as `'30d'`: instances that finished longer ago
   * go, with their journals, and so do signals older than this that no unfinished instance can
   * take, and rate-limit windows that ended that long ago. A signal's `id` deduplicates until its
   * signal is purged, so keep this longer than any sender's redelivery window, and longer than
   * your longest transaction.
   */
  olderThan: Duration;
  /**
   * The finished statuses to purge. Default `completed`, `failed` and `cancelled`:
   * `compensation_failed` instances wait for a person, so they go only when listed.
   */
  status?: WorkflowStatus | WorkflowStatus[];
  /** Instances, and signals, deleted per statement. Default 500. */
  batchSize?: number;
}

/** What `retry()` takes. */
export interface WorkflowRetryInstanceOptions {
  /**
   * A new run timeout, counted from now; `false` removes it. Required to retry a `failed`
   * instance whose deadline has passed, which would otherwise time out again at once. Default:
   * the deadline it has.
   */
  timeout?: Duration | false;
}

/** What `delete()` takes. */
export interface WorkflowDeleteOptions {
  /**
   * Delete an unfinished instance too, without running its compensations. A worker running it
   * stops recording at its next write. Default `false`: only finished instances.
   */
  force?: boolean;
}
