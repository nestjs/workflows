import { AsyncLocalStorage } from 'node:async_hooks';
import { Logger, type Type } from '@nestjs/common';
import { createHash, randomUUID } from 'node:crypto';
import { setImmediate as nextMacrotask } from 'node:timers/promises';
import type { LeasedRun } from '../core/interfaces/leased-worker-options.interface.js';
import type { WorkflowClock } from '../interfaces/workflow-clock.interface.js';
import { parseDuration, type Duration } from '../core/time/duration.js';
import { WorkflowNonDeterminismError } from '../errors/workflow-non-determinism.error.js';
import { WorkflowDefinitionError } from '../errors/workflow-definition.error.js';
import { StepTimeoutError } from '../errors/step-timeout.error.js';
import { WorkflowFailedError } from '../errors/workflow-failed.error.js';
import { StepFailedError } from '../errors/step-failed.error.js';
import { NonRetryableStepError } from '../errors/non-retryable-step.error.js';
import {
  isWorkflowInterrupt,
  WorkflowInterrupt,
  type InterruptReason,
} from '../errors/workflow-interrupt.error.js';
import { serializeError } from '../utils/serialize-error.util.js';
import { runInStepScope } from '../utils/step-scope.util.js';
import type { SerializedWorkflowError } from '../interfaces/serialized-workflow-error.interface.js';
import type {
  Journaled,
  StartChildWorkflowOptions,
  WaitForSignalOptions,
  WorkflowCompensationContext,
  WorkflowCondition,
  WorkflowContext,
  WorkflowStepContext,
  WorkflowStepOptions,
} from '../interfaces/workflow-context.interface.js';
import type {
  WorkflowInstance,
  WorkflowJournalEntry,
  WorkflowParentClose,
  WorkflowStatus,
  WorkflowWait,
} from '../interfaces/workflow-instance.interface.js';
import type { WorkflowMetadata } from '../interfaces/workflow-decorator-options.interface.js';
import { ChildWorkflowFailedError } from '../errors/child-workflow-failed.error.js';
import type { WorkflowFailureStatus } from '../errors/workflow-failed.error.js';
import { WorkflowIdConflictError } from '../errors/workflow-id-conflict.error.js';
import { CHILD_ENDED_SIGNAL } from '../workflows.constants.js';
import { newInstance } from '../utils/new-instance.util.js';
import type { WorkflowRetryOptions } from '../interfaces/workflow-retry-options.interface.js';
import { nextRetry, resolveRetry } from '../core/retries/retry.js';
import type { ResolvedRetry } from '../core/interfaces/retry-settings.interface.js';
import { entryBytes } from '../utils/journal-limits.util.js';
import { canonical } from '../core/utils/canonical.util.js';
import { normalize } from '../utils/normalize.util.js';
import type { WorkflowJournalLimits } from '../interfaces/workflows-module-options.interface.js';
import type { WorkflowEvents } from '../events/workflow-events.service.js';
import type { WorkflowEvent } from '../events/workflow-events.interface.js';
import { signalName } from '../signals/workflow.signal.js';
import type { NewWorkflowInstance, WorkflowSignalRecord } from '../interfaces/workflow-store.interface.js';
import type { WorkflowInstanceStore } from '../storage/encoded-workflow.store.js';

/** An instance under a worker's lease. */
export interface ClaimedWorkflowInstance extends WorkflowInstance {
  leaseToken: string;
}

/** One entry per name, as the store expects: the last version of each, in the order the names first appear. */
export function uniqueEntries<T extends { name: string }>(entries: T[]): T[] {
  const latest = new Map<string, T>();
  for (const entry of entries) {
    latest.set(entry.name, entry);
  }
  return [...latest.values()];
}


type EventBody = WorkflowEvent extends infer E
  ? E extends WorkflowEvent
    ? Omit<E, 'id' | 'workflow' | 'version' | 'at'>
    : never
  : never;

type JournalKind = WorkflowJournalEntry['kind'];
type RetrySetting = number | false | WorkflowRetryOptions | undefined;

export interface ExecutionDeps {
  store: WorkflowInstanceStore;
  /** The workflow `ctx.startChild()` starts: its name, version and run timeout (`WorkflowRegistry.resolve()`). */
  resolve(workflow: Type<unknown> | string, version?: number): WorkflowMetadata;
  /**
   * Creates a child, or finds the one an interrupted execution created, and throws
   * `WorkflowIdConflictError` if the id holds another instance.
   */
  createChild(instance: NewWorkflowInstance): Promise<WorkflowInstance>;
  clock: WorkflowClock;
  events: WorkflowEvents;
  defaultRetry: ResolvedRetry;
  journalLimits: Required<WorkflowJournalLimits>;
}

/** A step's compensation, reserved when the step is called and armed when it completes. */
interface Compensation {
  step: string;
  completed: boolean;
  result?: unknown;
  fn: (result: any, ctx: WorkflowCompensationContext) => unknown;
  retry: RetrySetting;
}

export type RunOutcome = { ok: true; value: unknown } | { ok: false; error: unknown };

export type CompensationOutcome =
  | { state: 'done'; count: number }
  | { state: 'suspended'; wakeAt: number | null }
  | { state: 'failed'; error: SerializedWorkflowError }
  | { state: 'interrupted' }
  | { state: 'terminated' };

/**
 * What `ctx.signalWait()` and `ctx.timer()` make for `ctx.waitForAny()`: a description of what to
 * wait for, and how the winner's journaled payload becomes its value.
 */
export class Condition {
  private constructor(
    readonly wait: WorkflowWait | null,
    readonly match: ((payload: any) => boolean) | undefined,
    readonly timer: Duration | { until: Date | number } | undefined,
    readonly settle: (payload: unknown) => unknown,
  ) {}

  static signal(wait: WorkflowWait, match?: (payload: any) => boolean, settle: (payload: unknown) => unknown = (payload) => payload): Condition {
    return new Condition(wait, match, undefined, settle);
  }

  static timer(when: Duration | { until: Date | number }): Condition {
    // Checked now, where the mistake is: the deadline itself is fixed when the wait is reached.
    wakeTime('timer', when, 0);
    return new Condition(null, undefined, when, () => null);
  }
}

/** What a child sends its parent when it ends (`CHILD_ENDED_SIGNAL`). */
export interface ChildEnded {
  status: WorkflowStatus;
  output?: unknown;
  error?: SerializedWorkflowError | null;
}

/** A `$child:<id>` journal entry's `result`: the child that was started. */
interface ChildStart {
  id: string;
  workflow: string;
  version: number;
}

/** A `$child:<id>` journal entry's `data`: what was asked for, to tell a replay that asks for something else. */
interface ChildRequest {
  id: string;
  workflow: string;
  /** SHA-256 of the input's canonical JSON. */
  input: string;
}

/** `ChildWorkflowHandle`: waits for the child's end, once per execution, as `result()` or as a `waitForAny()` condition. */
export class ChildHandle {
  readonly condition: Condition;
  private outcome?: { value: unknown } | { error: ChildWorkflowFailedError };
  private waiting?: Promise<unknown>;

  constructor(
    readonly id: string,
    readonly workflow: string,
    readonly version: number,
    private readonly wait: (handle: ChildHandle) => Promise<unknown>,
  ) {
    this.condition = Condition.signal({ signal: CHILD_ENDED_SIGNAL, key: id }, undefined, (payload) => this.settle(payload));
  }

  result(): Promise<unknown> {
    if (this.outcome) {
      return 'error' in this.outcome ? Promise.reject(this.outcome.error) : Promise.resolve(this.outcome.value);
    }
    return (this.waiting ??= this.wait(this));
  }

  /** The child's output, from its journaled end, or its failure, thrown. Kept, so a later `result()` returns it. */
  settle(payload: unknown): unknown {
    const ended = payload as ChildEnded;
    if (ended.status === 'completed') {
      this.outcome = { value: ended.output };
      return ended.output;
    }

    const error = new ChildWorkflowFailedError(
      this.workflow,
      this.id,
      ended.status as WorkflowFailureStatus,
      ended.error ?? { name: 'Error', message: 'Unknown failure.' },
    );
    this.outcome = { error };
    throw error;
  }
}

/** A `waitForAny()` journal entry's `data`: its conditions, with each timer's deadline. */
interface AnyData {
  waits: Record<string, WorkflowWait>;
  timers: Record<string, number>;
}

/** A `waitForAny()` journal entry's `result`: the winner. */
interface AnyOutcome {
  key: string;
  signalId: number | null;
  payload: unknown;
}

/** Step or compensation options, validated before the attempt is recorded. */
interface AttemptPlan {
  policy: ResolvedRetry;
  timeoutMs?: number;
  heartbeatTimeoutMs?: number;
}

/**
 * One execution of a workflow instance: runs `run()` from the top against the
 * journal loaded at claim time.
 *
 * - A journaled step returns its stored result (or re-throws its stored
 *   failure) without running.
 * - The first step that is not journaled is the frontier. Before it runs, the
 *   execution lets the replay settle and checks that every journaled entry was
 *   reached. If one was not, the code changed under a running instance and the
 *   instance fails instead of re-running a renamed side effect.
 * - A pending sleep, wait or retry backoff records a suspension and throws
 *   `WorkflowInterrupt`. After that no new step starts and nothing new is
 *   journaled except other sleeps and waits of the same `Promise.all`; the
 *   engine parks the instance once in-flight steps settle.
 * - A cancel, shutdown, lost lease or store failure also throws
 *   `WorkflowInterrupt`, and decides the outcome whatever `run()` does with it:
 *   swallowing or wrapping it can't complete the instance.
 * - In replay-only mode (compensating or cancelling after a restart) nothing
 *   new runs: the replay only rebuilds the list of compensations.
 */
export class WorkflowExecution {
  readonly context: WorkflowContext;
  readonly journal: Map<string, WorkflowJournalEntry>;
  readonly abort = new AbortController();

  suspension: { wakeAt: number | null; waits: WorkflowWait[] } | null = null;
  fatal: Error | null = null;
  /** Why the journal limit stopped the run, once it did. */
  journalLimitError: SerializedWorkflowError | null = null;
  leaseLost = false;
  storeError: unknown = null;
  shuttingDown = false;
  cancelRequested = false;
  /** A terminate was requested: stop compensating too. */
  terminateRequested = false;
  /** Attempt counters to restore when a shutdown interrupts an attempt. */
  readonly rollbacks: WorkflowJournalEntry[] = [];

  private mode: 'run' | 'replay' | 'closed' | 'compensate';
  private userSettled = false;
  private buffer: WorkflowJournalEntry[] = [];
  private frontier?: Promise<void>;
  private readonly visited = new Set<string>();
  private readonly compensations: Compensation[] = [];
  private readonly inflight = new Set<Promise<void>>();
  private readonly consumed = new Set<number>();
  /** The last journal write issued; the next one waits for it (see `write()`). */
  private writes: Promise<void> = Promise.resolve();
  private readonly counters = { now: 0, random: 0, uuid: 0 };
  /** Children started so far in this run, per workflow name: the default child ids. */
  private readonly childCounters = new Map<string, number>();
  /** The first interrupt that stops this execution (not a suspension). */
  private stoppedBy: WorkflowInterrupt | null = null;
  /** Each journal entry's size in bytes, by name, and their sum. */
  private readonly entrySizes = new Map<string, number>();
  private journalBytes = 0;
  /** Whether the journal is past the warning line: it warns once, when it crosses it. */
  private journalLarge: boolean;
  /** The last `ctx.setStatus()` value, and the stored one (canonical JSON). */
  private customStatus: unknown;
  private storedStatus: string;
  private static readonly logger = new Logger('Workflows');

  constructor(
    readonly instance: ClaimedWorkflowInstance,
    journal: WorkflowJournalEntry[],
    /** Highest signal id visible to this execution's waits. */
    readonly signalCursor: number,
    private readonly deps: ExecutionDeps,
    /** The worker's run of this instance: its lease, renewed on a heartbeat, and a signal for a lost lease or a shutdown. */
    private readonly lease: LeasedRun,
    replayOnly: boolean,
  ) {
    const stop = () => (lease.leaseLost ? this.loseLease() : this.requestShutdown());
    if (lease.signal.aborted) {
      stop();
    } else {
      lease.signal.addEventListener('abort', stop, { once: true });
    }

    this.mode = replayOnly ? 'replay' : 'run';
    this.journal = new Map(journal.map((entry) => [entry.name, entry]));
    for (const entry of journal) {
      this.measure(entry);
    }
    // Crossed in an earlier execution, which warned then.
    this.journalLarge = this.pastWarning();
    this.customStatus = instance.customStatus ?? null;
    this.storedStatus = canonical(this.customStatus);

    for (const entry of journal) {
      const signalId = (entry.result as { signalId?: unknown } | undefined)?.signalId;
      if ((entry.kind === 'signal' || entry.kind === 'any') && typeof signalId === 'number') {
        this.consumed.add(signalId);
      }
    }

    this.context = {
      workflowId: instance.id,
      workflowName: instance.workflow,
      version: instance.version,
      schedule: instance.scheduleId === null ? null : { id: instance.scheduleId, at: instance.scheduledAt! },
      step: (name, fn, options) => this.track(this.step(name, fn, options)),
      sleep: (name, duration) => this.track(this.sleep(name, duration)),
      waitForSignal: (name, signal, options = {}) =>
        this.track(this.receive(name, { signal: signalName(signal), key: options.key ?? null }, options, false)) as Promise<any>,
      startChild: (workflow, input, options) => this.track(this.startChild(workflow, input, options)) as Promise<any>,
      executeChild: (workflow, input, options) =>
        this.track(this.startChild(workflow, input, options).then((handle) => handle.result())) as Promise<any>,
      waitForAny: (name, conditions) => this.track(this.waitForAny(name, conditions)) as Promise<any>,
      signalWait: (signal, options = {}) => Condition.signal({ signal: signalName(signal), key: options.key ?? null }, options.match) as WorkflowCondition<any>,
      timer: (when) => Condition.timer(when) as WorkflowCondition<null>,
      now: () => this.helper('now', () => this.deps.clock.now()),
      random: () => this.helper('random', () => Math.random()),
      uuid: () => this.helper('uuid', () => randomUUID()),
      commit: (name) => this.commit(name),
      setStatus: (status) => this.setStatus(status),
      fail: (message) => {
        this.assertNotInStep('fail()', 'Throw a NonRetryableStepError from the step instead.');
        throw new WorkflowFailedError(message);
      },
    };
  }

  /** Runs the user's `run()` and waits for every step it started. */
  async run(fn: () => Promise<unknown>): Promise<RunOutcome> {
    let outcome: RunOutcome;
    try {
      outcome = { ok: true, value: await fn() };
    } catch (error) {
      outcome = { ok: false, error };
    }

    this.userSettled = true;
    while (this.inflight.size) {
      await Promise.all(this.inflight);
    }

    this.mode = 'closed';
    // A cancel, shutdown, lost lease or store failure reached run() as an
    // interrupt. Whether run() rethrew, wrapped or swallowed it, it decides the
    // outcome: a run that carried on anyway never completes the instance.
    return this.stoppedBy ? { ok: false, error: this.stoppedBy } : outcome;
  }

  /** Journal entries that this execution never reached (compensations and retry records excluded). */
  unvisited(): string[] {
    return [...this.journal.values()]
      .filter((entry) => entry.kind !== 'compensation' && entry.kind !== 'retry' && !this.visited.has(entry.name))
      .map((entry) => entry.name);
  }

  /** Staged entries (sleeps, helpers) not yet written; the engine commits them. */
  drainBuffer(): WorkflowJournalEntry[] {
    const now = this.deps.clock.now();
    return this.buffer.splice(0).map((entry) => ({ ...entry, updatedAt: now }));
  }

  requestShutdown(): void {
    if (this.shuttingDown) {
      return;
    }
    this.shuttingDown = true;
    this.abort.abort(new WorkflowInterrupt('shutdown'));
  }

  /** Past the shutdown timeout, the worker stopped waiting for it: never touch the store again. */
  get detached(): boolean {
    return this.lease.detached;
  }

  /**
   * Extends the lease, for `WorkflowStepContext.heartbeat()` (the worker renews it on its own heartbeat too). The
   * worker hands the execution the cancel and terminate requests the renewal reads.
   */
  async renew(): Promise<void> {
    if (this.detached || this.leaseLost) {
      return;
    }
    await this.lease.renew();
  }

  /** Runs registered compensations in reverse order, each as a journaled, retried step. */
  async compensate(reason: SerializedWorkflowError): Promise<CompensationOutcome> {
    this.mode = 'compensate';
    this.suspension = null;

    let count = 0;
    for (const compensation of [...this.compensations].reverse()) {
      if (!compensation.completed) {
        continue;
      }
      // A running compensation finishes; the next one doesn't start.
      if (this.terminateRequested) {
        return { state: 'terminated' };
      }

      const name = `${COMPENSATE}${compensation.step}`;
      const entry = this.journal.get(name);
      if (entry?.status === 'completed') {
        continue;
      }
      if (entry?.status === 'failed') {
        return { state: 'failed', error: entry.error! };
      }

      try {
        const attempt = await this.attempt(
          name,
          'compensation',
          // Whatever the undo returns (a provider's response, say) is discarded: nothing
          // reads it, so it can't fail the undo by being unserializable.
          async (ctx) => {
            await compensation.fn(compensation.result, { ...ctx, reason });
          },
          this.plan(name, { retry: compensation.retry }),
          entry,
        );
        count++;
        this.emit({ type: 'step-compensated', step: compensation.step, attempt: attempt.attempt });
      } catch (error) {
        if (error instanceof StepFailedError) {
          return { state: 'failed', error: { ...error.cause, message: `${error.message}` } };
        }

        // A compensation that calls ctx can never run: it needs a person.
        if (this.fatal) {
          return { state: 'failed', error: serializeError(this.fatal) };
        }

        if (isWorkflowInterrupt(error)) {
          return error.reason === 'suspend' ? { state: 'suspended', wakeAt: this.suspension!.wakeAt } : { state: 'interrupted' };
        }

        // Invalid options: retrying the same code would fail the same way.
        return { state: 'failed', error: serializeError(error) };
      }
    }

    return { state: 'done', count };
  }

  // ---------------------------------------------------------------------------
  // ctx operations

  private async step<T, P>(
    name: string,
    fn: (ctx: WorkflowStepContext<P>) => T | Promise<T>,
    options: WorkflowStepOptions<T> = {},
  ): Promise<Journaled<T>> {
    this.assertNotInStep(`step("${name}")`);
    this.visit(name, 'step');
    const entry = this.journal.get(name);
    if (entry?.status === 'completed') {
      this.reserveCompensation(name, options)?.arm(entry.result);
      return entry.result as Journaled<T>;
    }
    if (entry?.status === 'failed') {
      throw new StepFailedError(name, entry.attempts, entry.error!);
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertCanStart();
    const plan = this.plan(name, options);
    // Reserved in call order, so parallel steps compensate in the same order
    // on the first run and on a replay, whichever finished first.
    const compensation = this.reserveCompensation(name, options);

    await this.reachFrontier(name);
    this.assertCanStart();
    const { value } = await this.attempt<Journaled<T>>(name, 'step', fn as (ctx: WorkflowStepContext) => unknown, plan, entry);

    compensation?.arm(value);
    return value;
  }

  private async sleep(name: string, duration: Duration | { until: Date | number }): Promise<void> {
    this.assertNotInStep(`sleep("${name}")`);
    this.visit(name, 'sleep');
    const entry = unlessCancelled(this.journal.get(name));
    if (entry?.status === 'completed') {
      return;
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertAlive();
    const now = this.deps.clock.now();
    const wakeAt = entry?.wakeAt ?? wakeTime(name, duration, now);
    if (now >= wakeAt) {
      this.stage({ name, kind: 'sleep', status: 'completed', attempts: 0, wakeAt });
      return;
    }

    if (!entry) {
      this.stage({ name, kind: 'sleep', status: 'pending', attempts: 0, wakeAt });
    }
    throw this.suspendUntil(wakeAt);
  }

  /** `waitForSignal()`, and (`internal`) a child handle's `result()`, whose wait is named `$result:<id>`. */
  private async receive(name: string, wait: WorkflowWait, options: WaitForSignalOptions<any>, internal: boolean): Promise<unknown> {
    this.assertNotInStep(internal ? 'startChild() handle result()' : `waitForSignal("${name}")`);
    this.visit(name, 'signal', internal);
    const entry = unlessCancelled(this.journal.get(name));
    if (entry?.status === 'completed') {
      return (entry.result as { payload: unknown }).payload;
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertAlive();
    const deadline = entry
      ? (entry.wakeAt ?? null)
      : options.timeout !== undefined
        ? this.deps.clock.now() + parseDuration(options.timeout)
        : null;
    if (!entry) {
      this.stage({ name, kind: 'signal', status: 'pending', attempts: 0, wakeAt: deadline, data: wait });
    }

    const candidates = await this.signalsFor(wait);
    this.assertAlive();

    const candidate = this.take(candidates, deadline, options.match);
    if (candidate) {
      await this.write([
        {
          name,
          kind: 'signal',
          status: 'completed',
          attempts: 0,
          wakeAt: deadline,
          data: wait,
          result: { signalId: candidate.id, payload: candidate.payload },
        },
      ]);
      if (!internal) {
        this.emit({ type: 'signal-received', wait: name, signal: wait.signal, signalId: candidate.id });
      }
      return candidate.payload;
    }

    if (deadline !== null && this.deps.clock.now() >= deadline) {
      await this.write([
        { name, kind: 'signal', status: 'completed', attempts: 0, wakeAt: deadline, data: wait, result: { signalId: null, payload: null } },
      ]);
      this.emit({ type: 'signal-timed-out', wait: name, signal: wait.signal });
      return null;
    }

    throw this.suspendUntil(deadline, [wait]);
  }

  private async startChild(workflow: Type<unknown> | string, input: unknown, options: StartChildWorkflowOptions = {}): Promise<ChildHandle> {
    this.assertNotInStep('startChild()', 'Start the child from run(), and pass it what the step returned.');
    if (this.fatal) {
      throw this.fatal;
    }

    const parentClose = options.parentClose ?? 'cancel';
    if (!PARENT_CLOSE.includes(parentClose)) {
      throw new TypeError(`Invalid parentClose ${JSON.stringify(parentClose)} for startChild(). Use 'cancel', 'terminate' or 'abandon'.`);
    }
    const resolved = this.deps.resolve(workflow, options.version);
    const n = (this.childCounters.get(resolved.name) ?? 0) + 1;
    this.childCounters.set(resolved.name, n);
    const id = options.id ?? `${this.instance.id}/${resolved.name}#${n}`;

    // The journal first: a replay returns the child it started, whatever the keys of its workflow's limits
    // compute today (a deploy may change them); they are computed only to start it.
    const name = `$child:${id}`;
    this.visit(name, 'child', true);
    const request: ChildRequest = { id, workflow: resolved.name, input: createHash('sha256').update(canonical(normalize(input) ?? null)).digest('base64url') };
    const entry = this.journal.get(name);
    if (entry && entry.status !== 'pending') {
      const recorded = entry.data as ChildRequest;
      if (recorded.workflow !== request.workflow || recorded.input !== request.input) {
        throw this.setFatal(
          new WorkflowNonDeterminismError(
            `${this.describe()} does not match its journal: child "${id}" was started as "${recorded.workflow}"` +
              `${recorded.workflow === request.workflow ? ' with another input' : ''}, but the code now starts "${request.workflow}". ` +
              'Children started from parallel branches need ids of their own ({ id }). ' +
              ADVICE,
          ),
        );
      }
      if (entry.status === 'failed') {
        throw new WorkflowIdConflictError(entry.error!.message);
      }
      return this.handle(entry.result as ChildStart);
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertCanStart();
    const child = newInstance(resolved, id, input, {
      caller: 'startChild()',
      now: this.deps.clock.now(),
      timeout: options.timeout,
      concurrencyKey: options.concurrencyKey,
      rateLimitKey: options.rateLimitKey,
      priority: options.priority ?? this.instance.priority,
      parentId: this.instance.id,
      parentClose,
    });
    await this.reachFrontier(name);
    this.assertCanStart();
    // Recorded before the child exists, so a parent that crashes right after creating it still
    // knows it has children to close when it ends.
    if (!entry) {
      await this.write([{ name, kind: 'child', status: 'pending', attempts: 0, data: request }]);
    }

    let created: WorkflowInstance;
    try {
      created = await this.deps.createChild(child);
    } catch (error) {
      if (!(error instanceof WorkflowIdConflictError)) {
        this.storeError = error;
        this.abort.abort(new WorkflowInterrupt('store-error'));
        throw this.interrupt('store-error');
      }

      await this.write([{ name, kind: 'child', status: 'failed', attempts: 1, data: request, error: serializeError(error) }]);
      throw error;
    }

    const started: ChildStart = { id: child.id, workflow: child.workflow, version: created.version };
    await this.write([{ name, kind: 'child', status: 'completed', attempts: 1, data: request, result: started }]);
    this.emit({ type: 'child-started', child: started.id, childWorkflow: started.workflow, childVersion: started.version });
    return this.handle(started);
  }

  private handle(started: ChildStart): ChildHandle {
    return new ChildHandle(started.id, started.workflow, started.version, (handle) =>
      this.track(this.receive(`$result:${handle.id}`, handle.condition.wait!, {}, true).then((payload) => handle.settle(payload))),
    );
  }

  /** Whether this instance started children, whose `parentClose` applies when it ends. */
  hasChildren(): boolean {
    return [...this.journal.values()].some((entry) => entry.kind === 'child');
  }

  private async waitForAny(name: string, conditions: Record<string, unknown>): Promise<{ key: string; value: unknown }> {
    this.assertNotInStep(`waitForAny("${name}")`);
    const branches = conditionsOf(name, conditions);
    this.visit(name, 'any');
    const entry = unlessCancelled(this.journal.get(name));
    if (entry?.status === 'completed') {
      const won = entry.result as AnyOutcome;
      const branch = branches.get(won.key);
      const type = won.signalId === null ? 'timer' : 'signal';
      if (!branch || (branch.wait === null) !== (type === 'timer')) {
        throw this.setFatal(
          new WorkflowNonDeterminismError(
            `${this.describe()} does not match its journal: waitForAny("${name}") was won by its ${type} "${won.key}", which the code no ` +
              `longer has. ${ADVICE}`,
          ),
        );
      }
      return { key: won.key, value: branch.settle(won.payload) };
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertAlive();
    // Timers count from when the wait was first reached: a replay keeps their deadlines.
    const now = this.deps.clock.now();
    const reached = entry?.data as AnyData | undefined;
    const data: AnyData = { waits: {}, timers: {} };
    for (const [key, branch] of branches) {
      if (branch.wait) {
        data.waits[key] = branch.wait;
      } else {
        data.timers[key] = reached?.timers[key] ?? wakeTime(`${name}: ${key}`, branch.timer!, now);
      }
    }
    const deadlines = Object.values(data.timers);
    const deadline = deadlines.length > 0 ? Math.min(...deadlines) : null;
    const pending: WorkflowJournalEntry = { name, kind: 'any', status: 'pending', attempts: 0, wakeAt: deadline, data };
    if (!entry) {
      this.stage(pending);
    }

    // Read every signal condition's candidates first, then pick without awaiting in between, so
    // a parallel wait can't take the same signal.
    const reads = [...branches].filter(([, branch]) => branch.wait !== null);
    const candidates = await Promise.all(reads.map(([, branch]) => this.signalsFor(branch.wait!)));
    this.assertAlive();

    let winner: { key: string; branch: Condition; signal: WorkflowSignalRecord } | undefined;
    for (const [i, [key, branch]] of reads.entries()) {
      const signal = this.peek(candidates[i]!, deadline, branch.match);
      if (signal && (!winner || signal.id < winner.signal.id)) {
        winner = { key, branch, signal };
      }
    }

    if (winner) {
      this.consumed.add(winner.signal.id);
      const outcome: AnyOutcome = { key: winner.key, signalId: winner.signal.id, payload: winner.signal.payload };
      await this.write([{ ...pending, status: 'completed', result: outcome }]);
      if (!winner.branch.wait!.signal.startsWith('$')) {
        this.emit({ type: 'signal-received', wait: name, signal: winner.branch.wait!.signal, signalId: winner.signal.id });
      }
      return { key: winner.key, value: winner.branch.settle(winner.signal.payload) };
    }

    if (deadline !== null && this.deps.clock.now() >= deadline) {
      const key = Object.keys(data.timers).find((timer) => data.timers[timer] === deadline)!;
      await this.write([{ ...pending, status: 'completed', result: { key, signalId: null, payload: null } satisfies AnyOutcome }]);
      return { key, value: null };
    }

    throw this.suspendUntil(deadline, Object.values(data.waits));
  }

  /** Signals sent since the instance started, up to this execution's cursor: one that arrived before the wait was reached still counts. */
  private signalsFor(wait: WorkflowWait): Promise<WorkflowSignalRecord[]> {
    return this.deps.store.signals({ name: wait.signal, key: wait.key, afterId: this.instance.signalCursor, upToId: this.signalCursor });
  }

  /** The first of `candidates` a wait can take: not taken by another wait, sent by `deadline`, and matching. */
  private peek(candidates: WorkflowSignalRecord[], deadline: number | null, match: ((payload: any) => boolean) | undefined): WorkflowSignalRecord | undefined {
    return candidates.find(
      (candidate) => !this.consumed.has(candidate.id) && (deadline === null || candidate.createdAt <= deadline) && (!match || match(candidate.payload)),
    );
  }

  /** `peek()`, and marks the signal taken. Call it with no `await` between the read and the take. */
  private take(candidates: WorkflowSignalRecord[], deadline: number | null, match: ((payload: any) => boolean) | undefined): WorkflowSignalRecord | undefined {
    const candidate = this.peek(candidates, deadline, match);
    if (candidate) {
      this.consumed.add(candidate.id);
    }
    return candidate;
  }

  private commit(name: string): void {
    this.assertNotInStep(`commit("${name}")`);
    this.visit(name, 'commit');
    const entry = this.journal.get(name);
    if (entry?.status !== 'completed') {
      // Not reached by the run being compensated: its compensations stand.
      if (this.mode !== 'run') {
        throw this.interrupt('halt');
      }

      // Not after a suspension either: code that swallowed the interrupt of a
      // wait must not pass the point of no return on the wait's behalf.
      this.assertCanStart();

      // Written with the next journal write, which always happens before the
      // next side effect, a suspension, or the switch to compensating.
      this.stage({ name, kind: 'commit', status: 'completed', attempts: 0 });
    }

    this.compensations.splice(0);
  }

  private setStatus(status: unknown): void {
    this.assertNotInStep('setStatus()', 'Set it from run(), before or after the step.');
    if (this.fatal) {
      throw this.fatal;
    }

    let json: string | undefined;
    try {
      json = JSON.stringify(status);
    } catch (error) {
      throw new TypeError(`ctx.setStatus() takes a JSON-serializable value: ${(error as Error).message}`);
    }

    const bytes = Buffer.byteLength(json ?? 'null', 'utf8');
    if (bytes > MAX_CUSTOM_STATUS_BYTES) {
      throw new TypeError(
        `ctx.setStatus() got ${bytes} bytes of JSON, over the ${MAX_CUSTOM_STATUS_BYTES}-byte limit. Keep the status a summary; ` +
          'return large data from the workflow, or keep it in your own tables.',
      );
    }
    this.customStatus = json === undefined ? null : JSON.parse(json);
  }

  /** `{ customStatus }` when `ctx.setStatus()` changed it since it was last written, else `{}`: spread into a write. */
  statusChange(): { customStatus?: unknown } {
    return canonical(this.customStatus) === this.storedStatus ? {} : { customStatus: this.customStatus };
  }

  /** After a write that carried `change` landed. */
  statusWritten(change: { customStatus?: unknown }): void {
    if (!('customStatus' in change)) {
      return;
    }

    this.storedStatus = canonical(change.customStatus);
    this.emit({ type: 'custom-status', status: change.customStatus });
  }

  /**
   * Entries still pending when the instance ends without completing: a sleep
   * or wait that will never resolve, a retry that will never run.
   */
  abandoned(): WorkflowJournalEntry[] {
    return [...this.journal.values()]
      .filter((entry) => entry.status === 'pending' && entry.kind !== 'compensation')
      .map((entry) => ({ ...entry, status: 'cancelled', wakeAt: null }));
  }

  private helper<T>(kind: 'now' | 'random' | 'uuid', produce: () => T): T {
    this.assertNotInStep(`${kind}()`, 'Inside a step, read the real value directly: the step result is journaled.');
    const name = `$${kind}:${++this.counters[kind]}`;
    this.visit(name, kind);
    const entry = this.journal.get(name);
    if (entry?.status === 'completed') {
      return entry.result as T;
    }
    if (this.mode !== 'run') {
      throw this.interrupt('halt');
    }

    this.assertCanStart();
    const value = produce();
    // Written with the next journal write, which always happens before the
    // next side effect: a value can never influence an effect without being
    // persisted first.
    this.stage({ name, kind, status: 'completed', attempts: 0, result: value });
    return value;
  }

  // ---------------------------------------------------------------------------
  // attempts

  /**
   * Resolves a step's (or compensation's) retry and timeouts. Throws a
   * `TypeError` for invalid options before an attempt is recorded, instead of
   * spending the step's attempts on it.
   */
  private plan(name: string, options: { retry?: RetrySetting; timeout?: Duration; heartbeatTimeout?: Duration }): AttemptPlan {
    try {
      return {
        policy: resolveRetry(options.retry, this.deps.defaultRetry),
        timeoutMs: options.timeout === undefined ? undefined : parseDuration(options.timeout),
        heartbeatTimeoutMs: options.heartbeatTimeout === undefined ? undefined : parseDuration(options.heartbeatTimeout),
      };
    } catch (error) {
      throw new TypeError(`Invalid options for "${name}": ${(error as Error).message}`);
    }
  }

  private async attempt<T>(
    name: string,
    kind: 'step' | 'compensation',
    fn: (ctx: WorkflowStepContext) => unknown,
    plan: AttemptPlan,
    entry: WorkflowJournalEntry | undefined,
  ): Promise<{ value: T; attempt: number }> {
    const { policy } = plan;
    const previous = entry?.status === 'pending' ? entry.attempts : 0;
    if (entry?.status === 'pending' && previous > 0) {
      if (entry.wakeAt != null && this.deps.clock.now() < entry.wakeAt) {
        throw this.suspendUntil(entry.wakeAt);
      }
      if (entry.wakeAt == null && previous >= policy.attempts) {
        throw await this.giveUp(entry, previous, {
          name: 'StepInterruptedError',
          message:
            `Attempt ${previous} did not finish: the worker stopped, lost its lease, or could not write the ` +
            'result to the store (the worker logged it).',
        });
      }
    }

    const attempt = previous + 1;
    // Recorded before the side effect, so a step that crashes the process
    // still uses up its attempts instead of crash-looping forever.
    let current: WorkflowJournalEntry = {
      name,
      kind,
      status: 'pending',
      attempts: attempt,
      wakeAt: null,
      progress: entry?.progress,
      error: entry?.error,
    };
    await this.write([current]);

    const startedAt = performance.now();
    let value: T;
    try {
      const raw = await this.invoke(name, fn, plan, attempt, current, (next) => (current = next));
      try {
        value = normalize(raw) as T;
      } catch (error) {
        throw new NonRetryableStepError(`Result of "${name}" is not JSON-serializable: ${(error as Error).message}`);
      }
    } catch (error) {
      if (this.abort.signal.aborted) {
        // Shutdown, lost lease or store failure: not the step's fault, and
        // nothing about this attempt is recorded. A shutdown gives the attempt back.
        if (this.shuttingDown && !this.leaseLost && !this.storeError) {
          this.rollbacks.push({ ...current, attempts: attempt - 1 });
        }
        throw this.abortInterrupt();
      }

      // A definition error raised inside the step (a nested ctx call) fails
      // the instance; retrying the same code would hit it again.
      if (this.fatal) {
        throw this.fatal;
      }
      if (isWorkflowInterrupt(error)) {
        throw error;
      }

      let serialized = serializeError(error);
      let retryAt: number | null = null;
      if (!(error instanceof NonRetryableStepError)) {
        const next = nextRetry(policy, attempt, error);
        if (next.retry) {
          retryAt = this.deps.clock.now() + next.delay;
        } else if (next.reason === 'threw') {
          // A throwing retryIf or backoff gives up, journaled like any failure,
          // so a replay sees the same StepFailedError instead of re-running the step.
          const thrown = serializeError(next.error);
          serialized = {
            ...thrown,
            message: `The retry options of "${name}" threw ${thrown.name}: ${thrown.message} (handling ${serialized.name}: ${serialized.message})`,
          };
        }
      }

      if (retryAt === null) {
        throw await this.giveUp(current, attempt, serialized);
      }

      await this.write([{ ...current, error: serialized, wakeAt: retryAt }]);
      this.emit({ type: 'step-failed', step: name, attempt, error: serialized, retryAt });
      throw this.suspendUntil(retryAt);
    }

    await this.write([{ ...current, status: 'completed', result: value, error: undefined, progress: undefined }]);
    if (kind === 'step') {
      this.emit({ type: 'step-completed', step: name, attempt, durationMs: Math.round(performance.now() - startedAt) });
    }
    return { value, attempt };
  }

  private invoke(
    name: string,
    fn: (ctx: WorkflowStepContext) => unknown,
    { timeoutMs: overallMs, heartbeatTimeoutMs: idleMs }: AttemptPlan,
    attempt: number,
    entry: WorkflowJournalEntry,
    update: (entry: WorkflowJournalEntry) => void,
  ): Promise<unknown> {
    const controller = new AbortController();
    const forward = () => controller.abort(this.abort.signal.reason);
    if (this.abort.signal.aborted) {
      forward();
    } else {
      this.abort.signal.addEventListener('abort', forward, { once: true });
    }

    let expire!: (error: StepTimeoutError) => void;
    const watchdog = new Promise<never>((_, reject) => {
      expire = (error) => {
        controller.abort(error);
        reject(error);
      };
    });

    const overall =
      overallMs === undefined
        ? undefined
        : setTimeout(() => expire(new StepTimeoutError(`Step "${name}" timed out after ${overallMs}ms.`)), overallMs).unref();
    let idle: NodeJS.Timeout | undefined;
    const armIdle = () => {
      if (idleMs === undefined) {
        return;
      }
      clearTimeout(idle);
      idle = setTimeout(() => expire(new StepTimeoutError(`Step "${name}" sent no heartbeat for ${idleMs}ms.`)), idleMs).unref();
    };
    armIdle();

    let current = entry;
    let settled = false;
    const ctx: WorkflowStepContext = {
      idempotencyKey: `${this.instance.id}:${name}`,
      attempt,
      signal: controller.signal,
      progress: entry.progress,
      heartbeat: async (progress?: unknown) => {
        // After the attempt ended (a timeout, or a heartbeat left running),
        // nothing may touch its journal entry any more.
        if (settled) {
          controller.signal.throwIfAborted();
          return;
        }

        armIdle();
        if (progress === undefined) {
          await this.renew();
          if (this.leaseLost) {
            throw this.interrupt('lease-lost');
          }
          return;
        }

        current = { ...current, progress: normalize(progress) };
        update(current);
        await this.write([current]);
      },
    };

    const run = Promise.resolve().then(() => runInStepScope(ctx.idempotencyKey, () => insideStep.run({ execution: this, name, outer: insideStep.getStore() }, () => fn(ctx))));
    return Promise.race([run, watchdog]).finally(() => {
      settled = true;
      clearTimeout(overall);
      clearTimeout(idle);
      this.abort.signal.removeEventListener('abort', forward);
    });
  }

  private async giveUp(
    entry: WorkflowJournalEntry,
    attempts: number,
    error: SerializedWorkflowError,
  ): Promise<StepFailedError> {
    await this.write([{ ...entry, status: 'failed', attempts, error, wakeAt: null, progress: undefined }]);
    if (entry.kind === 'step') {
      this.emit({ type: 'step-failed', step: entry.name, attempt: attempts, error, retryAt: null });
    }
    return new StepFailedError(entry.name, attempts, error);
  }

  // ---------------------------------------------------------------------------
  // bookkeeping

  private track<T>(promise: Promise<T>): Promise<T> {
    const settled = promise.then(
      () => undefined,
      () => undefined,
    );
    this.inflight.add(settled);
    void settled.then(() => this.inflight.delete(settled));
    return promise;
  }

  /**
   * A step's function runs once; its result is all a replay sees. A `ctx` call
   * made inside it would be journaled on the first run and missing on every
   * replay, so it is a definition error, reported where it happens.
   */
  private assertNotInStep(call: string, advice = 'Call ctx methods from run() and pass the values the step needs into it.'): void {
    let frame = insideStep.getStore();
    while (frame && frame.execution !== this) {
      frame = frame.outer;
    }
    if (!frame) {
      return;
    }

    const step = frame.name;

    const where = step.startsWith(COMPENSATE) ? `the compensation of step "${step.slice(COMPENSATE.length)}"` : `step "${step}"`;
    throw this.setFatal(
      new WorkflowDefinitionError(
        `${this.describe()}: ${where} called ctx.${call}. A step's function runs once and replays return its ` +
          `result, so it can't use ctx. ${advice}`,
      ),
    );
  }

  private visit(name: string, kind: JournalKind, internal = false): void {
    if (this.fatal) {
      throw this.fatal;
    }

    const helper = kind === 'now' || kind === 'random' || kind === 'uuid';
    if (typeof name !== 'string' || name.length === 0 || (!helper && !internal && name.startsWith('$'))) {
      throw this.setFatal(new WorkflowDefinitionError(`Invalid ${kind} name "${name}". Names cannot be empty or start with "$".`));
    }

    if (this.visited.has(name)) {
      throw this.setFatal(
        new WorkflowDefinitionError(
          `"${name}" is used twice in one run of workflow "${this.instance.workflow}@${this.instance.version}". ` +
            'Step, sleep and wait names must be unique per run; inside a loop, add the index (`remind-${i}`).',
        ),
      );
    }
    this.visited.add(name);

    const entry = this.journal.get(name);
    if (!entry && this.mode === 'run') {
      this.assertJournalRoom(name);
    }
    if (entry && entry.kind !== kind) {
      throw this.setFatal(
        new WorkflowNonDeterminismError(
          `${this.describe()} does not match its journal: "${name}" was recorded as a ${entry.kind}, but the code now calls it as a ${kind}. ${ADVICE}`,
        ),
      );
    }
  }

  /**
   * Before the first new step runs, let every replayable call happen (replayed
   * results resolve in microtasks, so one macrotask is enough), then require
   * that the whole journal was reached.
   */
  private reachFrontier(step: string): Promise<void> {
    this.frontier ??= (async () => {
      await nextMacrotask();
      const missing = this.unvisited();
      if (missing.length) {
        throw this.setFatal(
          new WorkflowNonDeterminismError(
            `${this.describe()} does not match its journal: ${missing.map((n) => `"${n}"`).join(', ')} ` +
              `${missing.length === 1 ? 'was' : 'were'} recorded by an earlier run but not reached before the new step "${step}". ${ADVICE}`,
          ),
        );
      }
    })();
    return this.frontier;
  }

  /** Reserves the step's compensation in call order; `arm()` it with the result once the step completed. */
  private reserveCompensation<T>(step: string, options: WorkflowStepOptions<T>): { arm(result: unknown): void } | undefined {
    if (!options.compensate) {
      return undefined;
    }

    const compensation: Compensation = {
      step,
      completed: false,
      fn: options.compensate as Compensation['fn'],
      retry: options.compensateRetry ?? options.retry,
    };
    this.compensations.push(compensation);

    return {
      arm: (result) => {
        compensation.result = result;
        compensation.completed = true;
      },
    };
  }

  private assertAlive(): void {
    if (this.fatal) {
      throw this.fatal;
    }
    if (this.leaseLost) {
      throw this.interrupt('lease-lost');
    }
    if (this.storeError) {
      throw this.interrupt('store-error');
    }
    if (this.shuttingDown) {
      throw this.interrupt('shutdown');
    }
    if (this.cancelRequested) {
      throw this.interrupt('cancel');
    }
    // Only new work stops: a replay to compensate never gets here.
    if (this.pastDeadline()) {
      throw this.interrupt('timeout');
    }
  }

  /** Whether the instance's run timeout has passed. */
  pastDeadline(): boolean {
    return this.instance.deadline !== null && this.deps.clock.now() >= this.instance.deadline;
  }

  /** No new step starts once the execution is suspending or `run()` has settled. */
  private assertCanStart(): void {
    this.assertAlive();
    if (this.suspension) {
      throw this.interrupt('suspend');
    }
    if (this.userSettled) {
      throw this.interrupt('halt');
    }
  }

  private suspendUntil(wakeAt: number | null, waits: WorkflowWait[] = []): WorkflowInterrupt {
    this.suspension ??= { wakeAt: null, waits: [] };
    if (wakeAt !== null) {
      this.suspension.wakeAt = this.suspension.wakeAt === null ? wakeAt : Math.min(this.suspension.wakeAt, wakeAt);
    }
    this.suspension.waits.push(...waits);

    return this.interrupt('suspend');
  }

  private stage(entry: WorkflowJournalEntry): void {
    this.record(entry);
    this.buffer.push(entry);
  }

  /** Keeps `entry` as the journal's entry of its name, and tracks the journal's size. */
  private record(entry: WorkflowJournalEntry): void {
    this.journal.set(entry.name, entry);
    this.measure(entry);
    if (this.journalLarge || !this.pastWarning()) {
      return;
    }

    this.journalLarge = true;
    const { warnEntries, warnBytes } = this.deps.journalLimits;
    WorkflowExecution.logger.warn(
      `${this.describe()} has ${this.journal.size} journal entries (${this.journalBytes} bytes), past the warning line of ` +
        `${warnEntries} entries or ${warnBytes} bytes: every execution loads and replays all of it. Bound the loop that grows it, ` +
        'or continue in a new instance (see https://docs.nestjs.com/reliability/workflows#keep-journals-short).',
    );
    this.emit({ type: 'journal-large', entries: this.journal.size, bytes: this.journalBytes });
  }

  private measure(entry: WorkflowJournalEntry): void {
    const bytes = entryBytes(entry);
    this.journalBytes += bytes - (this.entrySizes.get(entry.name) ?? 0);
    this.entrySizes.set(entry.name, bytes);
  }

  private pastWarning(): boolean {
    const { warnEntries, warnBytes } = this.deps.journalLimits;
    return this.journal.size >= warnEntries || this.journalBytes >= warnBytes;
  }

  /**
   * Before a new name is journaled in run mode: a journal at its limit stops the run, which then
   * compensates (compensations may still record their entries) and fails.
   */
  private assertJournalRoom(name: string): void {
    const { maxEntries, maxBytes } = this.deps.journalLimits;
    if (this.journal.size < maxEntries && this.journalBytes < maxBytes) {
      return;
    }

    this.journalLimitError ??= {
      name: 'WorkflowJournalLimitError',
      message:
        `${this.describe()} reached its journal limit before "${name}": ${this.journal.size} entries, ${this.journalBytes} bytes ` +
        `(journal.maxEntries ${maxEntries}, journal.maxBytes ${maxBytes}). Continue a long loop in a new instance instead ` +
        '(see https://docs.nestjs.com/reliability/workflows#keep-journals-short).',
    };
    throw this.interrupt('journal-limit');
  }

  /** Fenced write of staged entries plus `entries`. */
  private async write(entries: WorkflowJournalEntry[]): Promise<void> {
    if (this.detached) {
      throw this.interrupt('shutdown');
    }

    for (const entry of entries) {
      this.record(entry);
    }

    const now = this.deps.clock.now();
    const batch = [...this.buffer.splice(0), ...entries].map((entry) => ({ ...entry, updatedAt: now }));

    // Writes reach the store one at a time, in the order they were issued. On a store with
    // a connection pool, two writes in flight can commit in either order, and the later one
    // wins: a checkpoint the step didn't await, landing after the step's completion, would
    // turn the completed step back into a pending one.
    const previous = this.writes;
    let done!: () => void;
    this.writes = new Promise<void>((resolve) => (done = resolve));
    try {
      await previous;
      if (this.detached) {
        throw this.interrupt('shutdown');
      }

      let ok: boolean;
      const change = this.statusChange();
      try {
        ok = await this.deps.store.write(this.instance.id, this.instance.leaseToken, { now, entries: uniqueEntries(batch), ...change });
      } catch (error) {
        this.storeError = error;
        this.abort.abort(new WorkflowInterrupt('store-error'));
        throw this.interrupt('store-error');
      }

      if (!ok) {
        this.loseLease();
        throw this.interrupt('lease-lost');
      }
      this.statusWritten(change);
    } finally {
      done();
    }
  }

  private loseLease(): void {
    if (this.leaseLost) {
      return;
    }
    this.leaseLost = true;
    this.abort.abort(new WorkflowInterrupt('lease-lost'));
    this.lease.loseLease();
  }

  private abortInterrupt(): WorkflowInterrupt {
    return this.interrupt(this.leaseLost ? 'lease-lost' : this.storeError ? 'store-error' : 'shutdown');
  }

  /** Every interrupt `ctx` throws goes through here; the first one that stops the run is kept. */
  private interrupt(reason: InterruptReason): WorkflowInterrupt {
    const interrupt = new WorkflowInterrupt(reason);
    if (reason !== 'suspend' && reason !== 'halt') {
      this.stoppedBy ??= interrupt;
    }
    return interrupt;
  }

  private setFatal(error: Error): Error {
    this.fatal ??= error;
    return this.fatal;
  }

  private describe(): string {
    return `Instance "${this.instance.id}" of workflow "${this.instance.workflow}@${this.instance.version}"`;
  }

  private emit(body: EventBody): void {
    this.deps.events.emit({
      id: this.instance.id,
      workflow: this.instance.workflow,
      version: this.instance.version,
      at: this.deps.clock.now(),
      ...body,
    } as WorkflowEvent);
  }
}

/**
 * A sleep or wait the instance abandoned when it ended (`cancelled`) starts over when an
 * operator retries the instance: a new deadline from now, as when it was first reached.
 */
function unlessCancelled(entry: WorkflowJournalEntry | undefined): WorkflowJournalEntry | undefined {
  return entry?.status === 'cancelled' ? undefined : entry;
}

function wakeTime(name: string, when: Duration | { until: Date | number }, now: number): number {
  if (typeof when !== 'object') {
    return now + parseDuration(when);
  }

  const until = when.until instanceof Date ? when.until.getTime() : when.until;
  if (typeof until !== 'number' || !Number.isFinite(until)) {
    throw new TypeError(`Invalid deadline for sleep "${name}": ${String(when.until)}. Pass a valid Date or a timestamp in milliseconds.`);
  }
  return until;
}

/** `waitForAny()`'s conditions, checked before anything is journaled. */
function conditionsOf(name: string, conditions: Record<string, unknown>): Map<string, Condition> {
  const entries = conditions !== null && typeof conditions === 'object' ? Object.entries(conditions) : [];
  if (entries.length === 0) {
    throw new TypeError(`waitForAny("${name}") takes an object with at least one condition, such as { delivered: ctx.signalWait(shipmentDelivered) }.`);
  }

  const branches = new Map<string, Condition>();
  for (const [key, condition] of entries) {
    const branch = condition instanceof ChildHandle ? condition.condition : condition;
    if (!(branch instanceof Condition)) {
      throw new TypeError(`waitForAny("${name}"): "${key}" is not a condition. Make each one with ctx.signalWait() or ctx.timer(), or pass a child's handle.`);
    }
    branches.set(key, branch);
  }
  return branches;
}

const COMPENSATE = '$compensate:';

/** A step whose function is running, and the one it runs inside of (a step of another execution), if any. */
interface StepFrame {
  execution: WorkflowExecution;
  name: string;
  outer?: StepFrame;
}

/**
 * The steps whose functions are running, in their async context. One store for every execution, not one each: on
 * Node 20 and 22, a store that ran stays in a process-wide list every async resource walks, until `disable()`.
 */
const insideStep = new AsyncLocalStorage<StepFrame>();
const PARENT_CLOSE: WorkflowParentClose[] = ['cancel', 'terminate', 'abandon'];

/** The most a custom status (`ctx.setStatus()`) may take as JSON. */
const MAX_CUSTOM_STATUS_BYTES = 16_384;

const ADVICE =
  'A deployed change renamed, removed or reordered steps. Ship such changes as a new version ' +
  '(@Workflow(name, { version })) and keep the old class registered until its instances finish.';
