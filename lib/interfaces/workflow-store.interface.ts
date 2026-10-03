import type { SerializedWorkflowError } from './serialized-workflow-error.interface.js';
import type { WorkflowRateWindow } from './workflow-decorator-options.interface.js';
import type {
  WorkflowInstance,
  WorkflowJournalEntry,
  WorkflowParentClose,
  WorkflowStatus,
  WorkflowWait,
} from './workflow-instance.interface.js';

/**
 * Where workflow instances, their journals, their waits and the signals sent to them live.
 * The package ships `InMemoryWorkflowStore` (the default, for development and tests); in
 * production, write a provider on your database that implements this interface and registers
 * itself with `WorkflowStorage.registerSource(this)` in its constructor.
 *
 * "The store contract" (https://docs.nestjs.com/reliability/workflows#the-store-contract) sums up
 * every method; each one's JSDoc here says what it must do, what must be atomic, and the race
 * each rule prevents. `workflowStoreContract()` from `@nestjs/workflows/testing` checks an
 * implementation against it, races included.
 *
 * A few methods need more than a plain read or write: `claim` and `claimSchedules` (a lock that
 * skips rows other claims hold; `claim` also counts limits under locks), `write`, `renew` and
 * `writeSchedule` (fenced by the lease token), `signal` together with `write` when it registers
 * waits (a lock that orders them, see `signal`), and `saveSchedule` (conditional on a revision).
 * `reopen` and `purge` re-check their conditions on the rows they change. Everything else is safe
 * to implement naively.
 */
export interface WorkflowStore {
  // ---------------------------------------------------------------- instances

  /**
   * Inserts a `pending` instance, due at `now`, unless one with the same id exists. Returns the
   * stored instance: the new one (`created: true`) or the existing one, unchanged
   * (`created: false`). Must be atomic per id: of two concurrent calls, one creates.
   */
  create(instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }>;
  /**
   * Optional: `create()` through the application's transaction, for
   * `start(workflow, input, { transaction })`. `transaction` is what the application's ORM
   * hands its transaction callback (a Drizzle `tx`, a TypeORM `EntityManager`, a Sequelize
   * `transaction`...). Write only through it, so the instance commits or rolls back with the
   * application's rows, and never catch a database error inside it (on PostgreSQL that aborts
   * the transaction: use insert-or-ignore). Without this method, `start()` with a transaction
   * throws.
   */
  createInTransaction?(transaction: unknown, instance: NewWorkflowInstance): Promise<{ instance: WorkflowInstance; created: boolean }>;
  /**
   * The instance with the waits its last suspension registered (`[]` when none), and its
   * journal when `options.journal` is set, or `null` for an unknown id.
   */
  get(id: string, options?: { journal?: boolean }): Promise<WorkflowInstanceDetails | null>;
  /** Instances matching every given filter, ordered by `createdAt`, then `id`; a page of them. */
  list(query: WorkflowListQuery): Promise<WorkflowInstance[]>;
  /**
   * A cancel: sets `cancelRequested` and `cancelReason` on a `pending`, `running` or
   * `suspended` instance whose `cancelRequested` is still false.
   *
   * A terminate (`request.terminate`): sets `cancelRequested`, `terminateRequested` and
   * `cancelReason` on a `pending`, `running`, `suspended` or `compensating` instance whose
   * `terminateRequested` is still false, whether or not a cancel was requested before.
   *
   * Either way it makes the instance due now (`wakeAt` becomes `now` unless it is already due)
   * and sets `updatedAt`, and returns whether it changed anything: `false` for an unknown id, a
   * status it doesn't apply to, or a repeated request. One conditional update: of two
   * concurrent requests of the same kind, one is accepted.
   */
  requestCancel(id: string, request: WorkflowCancelRequest): Promise<boolean>;
  /**
   * An operator's retry of a finished instance (`WorkflowClient.retry()`), as one conditional
   * write: if the instance holds no lease, its status is `expect.status` and its `runs` is
   * `expect.runs` (nothing changed it since the engine read it), upsert `entries` as `write()`
   * does, set `status`, `error` and, when given, `deadline`, make it due (`wakeAt = now`), set
   * `updatedAt = now`, and return `true`. Otherwise change nothing and return `false`. One
   * transaction that locks the instance row: of two concurrent retries, one is accepted.
   */
  reopen(id: string, reopen: WorkflowReopen): Promise<boolean>;
  /**
   * Deletes the instance, with its journal and waits, if its status is one of `statuses`, and
   * returns whether it did. One statement: a worker that holds its lease finds it gone at its
   * next write or renewal.
   */
  delete(id: string, statuses: WorkflowStatus[]): Promise<boolean>;

  // ---------------------------------------------------------------- signals

  /**
   * Records a signal under the next signal id and makes due now (`wakeAt = now` unless already
   * due, and `updatedAt`) every `suspended` instance with a wait for the same name and exactly
   * the same key. Returns the id and how many instances it woke.
   *
   * Signals must be serialized with each other and with `write()`s that register waits: take
   * an exclusive lock before choosing the id and hold it until commit (the `write()` side takes
   * it shared). That gives the two guarantees the engine relies on: signal ids become visible
   * in id order, and a signal and a suspension of the same instance never interleave.
   *
   * With a `dedupeId`, a signal stored earlier with the same name and `dedupeId` makes the call
   * a no-op: write and wake nothing, and return that signal's id and key with `created: false`.
   * Enforce it with a unique constraint on `(name, dedupeId)` and insert-or-ignore, never a read
   * followed by a write, and never by catching the duplicate-key error (in the application's
   * transaction, on PostgreSQL, that aborts it). Signals without a `dedupeId` never conflict.
   */
  signal(signal: NewWorkflowSignal): Promise<WorkflowSignalResult>;
  /**
   * Optional: `signal()` through the application's transaction, for
   * `signal(signal, payload, { transaction })`, under the same rules as `createInTransaction()`.
   * The lock is then held until the application's transaction ends. On PostgreSQL the
   * transaction must be READ COMMITTED, or the wake-up can miss waits committed after its
   * snapshot: refuse other isolation levels.
   */
  signalInTransaction?(transaction: unknown, signal: NewWorkflowSignal): Promise<WorkflowSignalResult>;
  /** Signals named `name` with exactly `key` and `afterId < id <= upToId`, by id. */
  signals(query: WorkflowSignalQuery): Promise<WorkflowSignalRecord[]>;

  // ---------------------------------------------------------------- retention

  /**
   * Deletes, oldest `updatedAt` first, up to `limit` instances whose status is one of
   * `statuses` (finished ones only) and whose `updatedAt` is below `before`, with their
   * journals and waits. Re-check the status and `updatedAt` on the rows it deletes (in the
   * `DELETE`'s own `WHERE`, not only in a subquery), so an instance that an operator reopened in
   * the meantime stays.
   *
   * Also deletes, lowest id first, up to `limit` signals no instance can take any more: an
   * instance only takes signals with an id above its `signalCursor`, and a new one starts at the
   * last signal id. So a signal can go when its id is at or below the lowest `signalCursor` of
   * the unfinished instances (`pending`, `running`, `suspended`, `compensating`), its
   * `createdAt` is below `before` (its `dedupeId` keeps deduplicating until then), and it isn't
   * the newest signal (which keeps the last signal id from going back).
   *
   * And deletes up to `limit` rate-limit windows (see `claim()`) that ended before `before`: a window that
   * ended is the same as none. Re-check `windowEnd` on the rows it deletes, as for instances: a claim may
   * have started a new window in one meanwhile; and pass over the windows a claim is locking (`SKIP
   * LOCKED`) rather than wait for them, since claims lock them in another order. Returns how many
   * instances, signals and windows it deleted. Each delete is one statement; nothing else is atomic.
   */
  purge(query: WorkflowPurgeQuery): Promise<WorkflowPurgeResult>;

  // ---------------------------------------------------------------- schedules

  /**
   * Stores a schedule, as one conditional write, and returns it as stored, or `null` when the condition fails.
   * With `expectRevision: null`, inserts it unless a schedule with the id exists (revision 1, `createdAt = now`);
   * with a number, replaces every field of the one whose `revision` is that number (revision + 1). Either way
   * sets `updatedAt = now`; with `releaseLease`, clears its lease, so the worker that holds it can't
   * `writeSchedule()` any more. Of two concurrent saves expecting the same revision, one lands.
   */
  saveSchedule(schedule: WorkflowScheduleSave): Promise<WorkflowScheduleRecord | null>;
  /** The schedule, or `null` for an unknown id. */
  getSchedule(id: string): Promise<WorkflowScheduleRecord | null>;
  /** Schedules matching the filter, ordered by `id`; a page of them. */
  listSchedules(query: WorkflowScheduleQuery): Promise<WorkflowScheduleRecord[]>;
  /** Deletes the schedule (if its `revision` is still `revision`, when given), and returns whether it did. */
  deleteSchedule(id: string, revision?: number): Promise<boolean>;
  /**
   * Leases up to `limit` due schedules to a worker, most overdue first (`wakeAt`, then `id`). Due: not paused,
   * `wakeAt <= now`, no lease or an expired one (`leaseUntil < now`), and `workflow` in `workflows`. Each gets
   * `leaseToken = token`, `leaseOwner = owner` and `leaseUntil`; nothing else changes (not `revision`). Two
   * concurrent claims never return the same schedule: lock the candidates and skip those another claim holds.
   */
  claimSchedules(request: WorkflowScheduleClaimRequest): Promise<WorkflowScheduleRecord[]>;
  /**
   * The lease holder's write: sets `state` and `wakeAt`, `revision + 1` and `updatedAt = now`, and with `release`
   * clears the lease (`leaseToken`, `leaseUntil`), only while `token` is the schedule's lease token; otherwise
   * writes nothing and returns `false`. One conditional update.
   */
  writeSchedule(id: string, token: string, write: WorkflowScheduleWrite): Promise<boolean>;

  // ---------------------------------------------------------------- the worker

  /**
   * Leases up to `limit` due instances to a worker: lowest `priority` first, then most overdue (`wakeAt`),
   * then `createdAt`, then `id`. Due: status `pending`, `running`, `suspended` or `compensating`,
   * `wakeAt <= now`, no lease or an expired one (`leaseUntil < now`), and a `workflow`/`version` pair in
   * `workflows`. Each claimed instance gets `leaseToken = token`, `leaseOwner = owner`, `leaseUntil`,
   * `runs + 1`, `updatedAt = now` and status `running` (a `compensating` one keeps its status).
   *
   * Two concurrent claims must never return the same instance: lock the candidates and skip
   * those another claim holds (`FOR UPDATE SKIP LOCKED`). Also returns the last signal id,
   * read after the claim, as the execution's signal cursor.
   *
   * `limits`: an instance holds a slot while its lease is live (`leaseUntil >= now`): at most `limit` of a
   * listed workflow's instances (any version) hold one, and at most `perKey` of those with the same
   * `concurrencyKey` (instances without one count only toward `limit`).
   *
   * `rateLimits`: each claim of an instance of a listed workflow starts an execution, which takes room in its
   * workflow's windows: `limit` (the workflow's own) and `perKey` (one per `rateLimitKey`; instances without one
   * count only toward `limit`). A window holds at most `max` claims; it starts with the first claim after the
   * previous one ended (`windowEnd <= now`) and ends `duration` later.
   *
   * Under limits, take the candidates (in the order above) in stages, so one busy key never holds back the
   * others: pass over every candidate with no room at all (a full slot count or window of its workflow or its
   * key); then keep each concurrency key's first candidates, as many as it has free slots; of those, each rate
   * key's first, as many as its window has room for; of those, each workflow's first, as many as both its free
   * slots and its window allow; then the first `limit`. (With both kinds of keys, a claim can take fewer than
   * fit: a key's candidate dropped by the other kind of key isn't replaced until the next claim.)
   *
   * Count and lease as one step, so two claims never take the last room: take a transaction-scoped lock per
   * workflow with concurrency limits, in a fixed order, before counting; and lock each rate-limit window the
   * picked instances take room in (an insert-or-lock of its row, in a fixed order, so there is always a row to
   * lock), re-count under the lock, and lease only what still fits. Claims that take no room in a window never
   * wait for its lock.
   */
  claim(request: WorkflowClaimRequest): Promise<WorkflowClaim>;
  /**
   * Extends the lease to `leaseUntil` if `token` is still the instance's lease token, and
   * returns its `cancelRequested` and `terminateRequested`, or `null` (changing nothing) if the
   * lease is gone. One conditional update.
   */
  renew(id: string, token: string, leaseUntil: number): Promise<{ cancelRequested: boolean; terminateRequested: boolean } | null>;
  /**
   * Every write by the worker that holds the lease: journal entries, a status change, the
   * outcome, and handing the instance back. All or nothing, in one transaction, and only while
   * `token` is the instance's lease token: otherwise write nothing and return `false`.
   * See `WorkflowWrite` for what each field does.
   */
  write(id: string, token: string, write: WorkflowWrite): Promise<boolean>;
}

/** `WorkflowStore.get()`'s result, and `WorkflowClient.getStatus()`'s. */
export interface WorkflowInstanceDetails extends WorkflowInstance {
  /** Signals the instance waits for, if suspended in `waitForSignal()`. */
  waits: WorkflowWait[];
  /** `WorkflowClient.getStatus(id, { children: true })` only: the instances it started with `ctx.startChild()`, oldest first. */
  children?: WorkflowInstance[];
  /** With `{ journal: true }`: every step, sleep, wait and compensation, in first-write order. */
  journal?: WorkflowJournalEntry[];
}

export interface NewWorkflowInstance {
  id: string;
  workflow: string;
  version: number;
  input: unknown;
  /** For a child (`ctx.startChild()`): its parent's id, stored as `parentId`. Absent: `null`. */
  parentId?: string | null;
  /** For a child: stored as `parentClose`. Absent: `null`. */
  parentClose?: WorkflowParentClose | null;
  /** Stored as `concurrencyKey`: the key its workflow's per-key limit counts it under. Absent: `null`. */
  concurrencyKey?: string | null;
  /** Stored as `rateLimitKey`: the key its workflow's per-key rate limit counts it under. Absent: `null`. */
  rateLimitKey?: string | null;
  /** Stored as `priority`: lower is claimed first. Absent: `0`, which goes before every other priority. */
  priority?: number;
  /** For an instance a schedule started: the schedule's id, stored as `scheduleId`. Absent: `null`. */
  scheduleId?: string | null;
  /** For an instance a schedule started: the occurrence's time, stored as `scheduledAt`. Absent: `null`. */
  scheduledAt?: number | null;
  /** The instance's `deadline`: when its run timeout passes, or `null`. Stored as is. */
  deadline: number | null;
  /**
   * `createdAt`, `updatedAt` and `wakeAt`. The new instance also gets `signalCursor` = the
   * last signal id (signals sent after it started can match its waits), `runs: 0`, no lease,
   * no cancel or terminate request and `customStatus: null`.
   */
  now: number;
}

/** What `WorkflowStore.requestCancel()` receives. */
export interface WorkflowCancelRequest {
  reason: string | null;
  now: number;
  /** `WorkflowClient.terminate()`: stop without compensating. */
  terminate: boolean;
}

/** What `WorkflowStore.list()` receives: validated and defaulted by the engine. */
export interface WorkflowListQuery {
  /** Any of these statuses (never empty). */
  status?: WorkflowStatus[];
  workflow?: string;
  version?: number;
  /** Children of this instance. */
  parentId?: string;
  /** Instances this schedule started. */
  scheduleId?: string;
  limit: number;
  offset: number;
}

export interface NewWorkflowSignal {
  name: string;
  /** Correlation key; `null` for a signal sent without one. Matches waits with exactly this key. */
  key: string | null;
  /**
   * The sender's id for this signal (`WorkflowClient.signal()`'s `id` option), unique per signal
   * name: a signal with the same name and `dedupeId` is stored once. `null`: never deduplicated.
   */
  dedupeId: string | null;
  payload: unknown;
  now: number;
}

/** What `WorkflowStore.signal()` and `signalInTransaction()` return. */
export interface WorkflowSignalResult {
  /** The stored signal's id: the new one, or with `created: false` the one stored earlier. */
  id: number;
  /** Instances made due; `0` with `created: false`. */
  woken: number;
  /** `false` when a signal with the same name and `dedupeId` was stored earlier. */
  created: boolean;
  /** The stored signal's key: the given one, or with `created: false` the earlier signal's. */
  key: string | null;
}

/** What `WorkflowStore.reopen()` receives. */
export interface WorkflowReopen {
  /** What the engine read: the write applies only while the instance is still like this. */
  expect: { status: WorkflowStatus; runs: number };
  /** `pending` to run again, `compensating` to retry its compensations. */
  status: 'pending' | 'compensating';
  error: SerializedWorkflowError | null;
  /** A new `deadline`; `undefined` leaves it as it is. */
  deadline?: number | null;
  /** Journal entries to upsert by name, as in `WorkflowWrite.entries`. */
  entries: WorkflowJournalEntry[];
  now: number;
}

/** What `WorkflowStore.purge()` receives. */
export interface WorkflowPurgeQuery {
  /** Finished statuses to delete (at least one). */
  statuses: WorkflowStatus[];
  /** Instances whose `updatedAt` (when they finished), and signals whose `createdAt`, is below this. */
  before: number;
  /** At least 1: the most instances, the most signals, and the most rate-limit windows one call deletes. */
  limit: number;
}

/** How many instances, signals and rate-limit windows a purge deleted. */
export interface WorkflowPurgeResult {
  instances: number;
  signals: number;
  /** Rate-limit windows that had ended. */
  rateLimits: number;
}

export interface WorkflowSignalQuery {
  name: string;
  /** Exactly this key: `null` matches only signals sent without one. */
  key: string | null;
  afterId: number;
  upToId: number;
}

/** A signal as stored. */
export interface WorkflowSignalRecord {
  id: number;
  name: string;
  key: string | null;
  payload: unknown;
  createdAt: number;
}

export interface WorkflowClaimRequest {
  /** The worker's id, for `leaseOwner`. */
  owner: string;
  /** A new token for this claim; every write under the lease presents it. */
  token: string;
  now: number;
  leaseUntil: number;
  /** At least 1. */
  limit: number;
  /** The workflow versions this worker runs (at least one). Leave the others to other workers. */
  workflows: Array<{ name: string; version: number }>;
  /** Concurrency limits of some of those workflows (by name, every version). Absent or empty: none. */
  limits?: WorkflowConcurrencyLimit[];
  /** Rate limits of some of those workflows (by name, every version). Absent or empty: none. */
  rateLimits?: WorkflowRateLimitRule[];
}

/** A workflow's rate limits, as a claim applies them: at most `max` claims per window of `duration` ms. */
export interface WorkflowRateLimitRule {
  workflow: string;
  /** The workflow's own window (every instance counts), or `null`. */
  limit: WorkflowRateWindow | null;
  /** One window per non-null `rateLimitKey`, or `null`. */
  perKey: WorkflowRateWindow | null;
}

/** A workflow's concurrency limits, as a claim applies them. */
export interface WorkflowConcurrencyLimit {
  workflow: string;
  /** At most this many of its instances hold a slot, or `null`. */
  limit: number | null;
  /** At most this many of its instances with the same non-null `concurrencyKey` hold a slot, or `null`. */
  perKey: number | null;
}

export interface WorkflowClaim {
  /** The claimed instances, as updated by the claim. */
  instances: WorkflowInstance[];
  /** The last signal id: the highest id `signal()` has returned, or 0. */
  lastSignalId: number;
}

/**
 * One write by the lease holder (`WorkflowStore.write()`). In one transaction:
 *
 * 1. With a `signal`, take the signal lock (see `WorkflowStore.signal()`) exclusively; else, if
 *    `release.waits` is not empty, in shared mode. Before anything else.
 * 2. Lock the instance row if its lease token is `token`; if not, write nothing, return `false`.
 * 3. Upsert `entries` by name.
 * 4. Set `status`, `output`, `error` and `customStatus` when present (`undefined` leaves them as
 *    they are), and record `signal` when present.
 * 5. With `release`: replace the instance's waits with `release.waits`, clear `leaseToken` and
 *    `leaseUntil` (keep `leaseOwner`), and set `wakeAt`: `now` if a signal with an id above
 *    `release.signalCursor` matches one of the new waits (name and exact key), or if the
 *    instance has `cancelRequested` and the new status is `suspended`; otherwise
 *    `release.wakeAt`.
 * 6. Set `updatedAt = now` when anything besides the journal changed.
 */
export interface WorkflowWrite {
  now: number;
  /**
   * Journal entries, each with a name unique within the write. An entry replaces the stored
   * entry with the same name entirely; a new name goes after every name the instance already
   * has, in array order. `get(id, { journal: true })` returns them in that first-write order,
   * with `null` and `undefined` fields kept apart (store each entry as one JSON document).
   */
  entries: WorkflowJournalEntry[];
  status?: WorkflowStatus;
  output?: unknown;
  error?: SerializedWorkflowError | null;
  /** The instance's `customStatus` (`null` clears it). */
  customStatus?: unknown;
  /**
   * A signal to record as `signal()` records it (dedupe and wake-ups included), in this write's
   * transaction and only if the write lands: how a child that ends tells its parent. The signal
   * lock is then taken exclusively in step 1.
   */
  signal?: NewWorkflowSignal;
  /** Hand the instance back: parked (`suspended`), finished, or due again at once. */
  release?: WorkflowRelease;
}

export interface WorkflowRelease {
  /** When the instance is next due; `null`: only a signal or a cancel can wake it. */
  wakeAt: number | null;
  /** The waits to register, replacing the previous ones (`[]` clears them). */
  waits: WorkflowWait[];
  /** The execution's signal cursor (`WorkflowClaim.lastSignalId`): signals above it weren't seen. */
  signalCursor: number;
}

/** A schedule as stored: `WorkflowStore.getSchedule()`, `listSchedules()` and `claimSchedules()` return it. */
export interface WorkflowScheduleRecord {
  id: string;
  /** The workflow it starts (claims filter on it). */
  workflow: string;
  /** Declared with `@Workflow(name, { schedules })`, rather than saved with `WorkflowSchedules.upsert()`. */
  declared: boolean;
  /** The engine's JSON: when it runs and how. Store it as it is. */
  spec: unknown;
  /** The input of the instances it starts (JSON: a string with a codec), or `null`. */
  input: unknown;
  paused: boolean;
  /** When a worker next has something to do for it (`claimSchedules()` looks for `wakeAt <= now`), or `null`. */
  wakeAt: number | null;
  /** The engine's JSON bookkeeping. Store it as it is. */
  state: unknown;
  /** Bumped by every `saveSchedule()` and `writeSchedule()`. */
  revision: number;
  leaseOwner: string | null;
  leaseUntil: number | null;
  createdAt: number;
  updatedAt: number;
}

/** What `WorkflowStore.saveSchedule()` receives. */
export interface WorkflowScheduleSave {
  id: string;
  workflow: string;
  declared: boolean;
  spec: unknown;
  input: unknown;
  paused: boolean;
  wakeAt: number | null;
  state: unknown;
  /** `null`: insert it, if no schedule has the id. A number: replace the one whose `revision` is still this. */
  expectRevision: number | null;
  /** Clear the schedule's lease (a change to when it runs: the lease holder's work is outdated). */
  releaseLease: boolean;
  now: number;
}

/** What `WorkflowStore.listSchedules()` receives. */
export interface WorkflowScheduleQuery {
  workflow?: string;
  /** Only the declared ones (`true`), or only the others (`false`). */
  declared?: boolean;
  limit: number;
  offset: number;
}

/** What `WorkflowStore.claimSchedules()` receives. */
export interface WorkflowScheduleClaimRequest {
  owner: string;
  token: string;
  now: number;
  leaseUntil: number;
  /** At least 1. */
  limit: number;
  /** The workflow names this worker runs (at least one): it starts only their schedules' instances. */
  workflows: string[];
}

/** What `WorkflowStore.writeSchedule()` receives. */
export interface WorkflowScheduleWrite {
  now: number;
  state: unknown;
  wakeAt: number | null;
  /** Hand the schedule back: clear its lease. */
  release: boolean;
}
