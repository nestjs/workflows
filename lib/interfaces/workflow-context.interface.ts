import type { Type } from '@nestjs/common';
import type { Duration } from '../core/time/duration.js';
import type { WorkflowParentClose } from './workflow-instance.interface.js';
import type { WorkflowInput, WorkflowOutput } from './workflow-runner.interface.js';
import type { SerializedWorkflowError } from './serialized-workflow-error.interface.js';
import type { WorkflowSignal } from '../signals/workflow.signal.js';
import type { WorkflowRetryOptions } from './workflow-retry-options.interface.js';
import type { WorkflowScheduleOccurrence } from './workflow-schedule.interface.js';

/**
 * What a journaled value looks like after its JSON round-trip: step results,
 * signal payloads and checkpoints come back this way on the first run and on
 * every replay (a `Date` is a string, methods and `undefined` fields are gone,
 * `undefined` in an array is `null`, a `Map` or `Set` is `{}`).
 */
export type Journaled<T> = unknown extends T
  ? T
  : T extends { toJSON(...args: any[]): infer R }
    ? Journaled<R>
    : T extends string | number | boolean | null | undefined | void
      ? T
      : T extends bigint | symbol | ((...args: any[]) => unknown)
        ? never
        : T extends ReadonlyMap<unknown, unknown> | ReadonlySet<unknown>
          ? Record<string, never>
          : T extends readonly unknown[]
            ? { [K in keyof T]: JournaledElement<T[K]> }
            : { [K in keyof T as T[K] extends (...args: any[]) => unknown ? never : K]: Journaled<T[K]> };

/** In arrays, `JSON.stringify` writes `undefined`, functions and symbols as `null`. */
type JournaledElement<T> = T extends undefined | void | symbol | ((...args: any[]) => unknown) ? null : Journaled<T>;

/** Handed to every step (and compensation) attempt. */
export interface WorkflowStepContext<P = unknown> {
  /**
   * `${workflowId}:${stepName}`. Identical for every attempt and every worker
   * that runs this step, so pass it to the API you call (`Idempotency-Key`,
   * a unique column) to make the side effect exactly-once.
   */
  readonly idempotencyKey: string;
  /** 1 for the first attempt. Interrupted attempts (crash, lost lease) count. */
  readonly attempt: number;
  /** Aborted on shutdown, on a lost lease, and on `timeout`/`heartbeatTimeout`. */
  readonly signal: AbortSignal;
  /**
   * The last value passed to `heartbeat(progress)` by an earlier attempt of
   * this step, or `undefined`. Type it by annotating the step function's
   * parameter: `async ({ progress }: WorkflowStepContext<Checkpoint>) => ...`.
   */
  readonly progress: Journaled<P> | undefined;
  /**
   * Proves the step is alive (resets `heartbeatTimeout`). With an argument,
   * the value is also checkpointed to the journal and handed to the next
   * attempt as `progress` if this one dies.
   */
  heartbeat(progress?: P): Promise<void>;
}

export interface WorkflowCompensationContext extends WorkflowStepContext {
  /** Why the workflow is being compensated. */
  readonly reason: SerializedWorkflowError;
}

export interface WorkflowStepOptions<T> {
  /** Default: the module's `retry` (3 attempts, 1s doubling up to 5m). */
  retry?: number | false | WorkflowRetryOptions;
  /** Fail the attempt (retryably) after this long. Capped at about 24.8 days, the longest timer Node supports. */
  timeout?: Duration;
  /** Fail the attempt (retryably) if `heartbeat()` is not called this often. Capped like `timeout`. */
  heartbeatTimeout?: Duration;
  /**
   * Undo this step, once it completed, if the workflow later fails or is
   * cancelled. Compensations run in reverse order of the `ctx.step()` calls
   * (parallel steps included), each one journaled and retried like a step. Its
   * return value is discarded. A later `ctx.commit()` discards it.
   */
  compensate?: (result: Journaled<T>, ctx: WorkflowCompensationContext) => unknown;
  /** How `compensate` retries. Default: the step's `retry`, else the module's. */
  compensateRetry?: number | false | WorkflowRetryOptions;
}

export interface WaitForSignalOptions<T> {
  /**
   * Correlation key: only signals sent with the same key match (see
   * `WorkflowClient.signal()`). Without one, the wait only matches signals sent
   * without a key.
   */
  key?: string;
  /** Extra filter over the payload. Must be a pure function of it. */
  match?: (payload: Journaled<T>) => boolean;
  /** Resolve with `null` if no matching signal was sent before this deadline. */
  timeout?: Duration;
}

declare const conditionValue: unique symbol;

/**
 * Something `ctx.waitForAny()` waits for, made by `ctx.signalWait()` or `ctx.timer()`, or a
 * child's handle (`ctx.startChild()`). Only a
 * description: nothing is waited for, and nothing journaled, until it is passed to `waitForAny()`.
 */
export interface WorkflowCondition<T = unknown> {
  /** Type-only: the `value` `waitForAny()` returns when this condition wins. Never set at runtime. */
  readonly [conditionValue]?: T;
}

/** What `ctx.startChild()` and `ctx.executeChild()` take. */
export interface StartChildWorkflowOptions {
  /**
   * The child's id. Default: `${parentId}/${workflow}#${n}`, where `n` counts the children of
   * that workflow this run started before it, so a replay gets the same child back. The id also
   * names the journal entry: pass one (derived from your data) for children started from
   * parallel branches, which can reach `startChild()` in another order on a replay.
   */
  id?: string;
  /** Pin a version, as in `WorkflowClient.start()`. Default: the highest this application registers. */
  version?: number;
  /** The child's run timeout, overriding its `@Workflow(name, { timeout })`. */
  timeout?: Duration;
  /**
   * What happens to the child if it is still running when this instance ends (completes,
   * fails, is cancelled or terminated), or starts compensating: `'cancel'` (the default)
   * cancels it, and its compensations run; `'terminate'` stops it without compensating;
   * `'abandon'` leaves it running on its own.
   */
  parentClose?: WorkflowParentClose;
  /** As in `WorkflowClient.start()`: the child's key for its workflow's per-key concurrency limit. */
  concurrencyKey?: string;
  /** As in `WorkflowClient.start()`: the child's key for its workflow's per-key rate limit. */
  rateLimitKey?: string;
  /** As in `WorkflowClient.start()`. Default: this instance's priority. */
  priority?: number;
}

/** A started child workflow (`ctx.startChild()`). Also a condition for `ctx.waitForAny()`. */
export interface ChildWorkflowHandle<O = unknown> extends WorkflowCondition<Journaled<O>> {
  readonly id: string;
  readonly workflow: string;
  readonly version: number;
  /**
   * Durable wait for the child to end: resolves with its output, or throws a
   * `ChildWorkflowFailedError` (with the child's status and error) if it failed, was cancelled
   * or terminated. Journaled, like a signal wait. Calling it again returns the same promise.
   */
  result(): Promise<Journaled<O>>;
}

/** What `ctx.signalWait()` takes: `waitForSignal()`'s options, without a timeout (race a `ctx.timer()` instead). */
export type SignalWaitOptions<T> = Omit<WaitForSignalOptions<T>, 'timeout'>;

/**
 * What `ctx.waitForAny(name, conditions)` resolves with: the key of the condition that won, and
 * its value, typed per key (check `key` to narrow `value`).
 */
export type WorkflowAnyResult<C extends Record<string, WorkflowCondition<unknown>>> = {
  [K in keyof C & string]: { key: K; value: C[K] extends WorkflowCondition<infer T> ? T : never };
}[keyof C & string];

/**
 * The API a workflow's `run()` receives. `run()` executes again from the top every time an instance
 * resumes: https://docs.nestjs.com/reliability/workflows#rules-for-workflow-code.
 */
export interface WorkflowContext {
  readonly workflowId: string;
  readonly workflowName: string;
  readonly version: number;
  /** The schedule occurrence that started this instance (`{ id, at }`: the schedule's id and the occurrence's time), or `null`. */
  readonly schedule: WorkflowScheduleOccurrence | null;

  /**
   * Runs `fn` once and journals its result; replays return the journaled
   * result without running it. `name` is unique per run and part of the
   * contract with running instances. Don't call `ctx` methods inside `fn`.
   */
  step<T, P = unknown>(
    name: string,
    fn: (ctx: WorkflowStepContext<P>) => T | Promise<T>,
    options?: WorkflowStepOptions<T>,
  ): Promise<Journaled<T>>;
  /** Durable timer. The instance is parked and resumed by any worker after the deadline. */
  sleep(name: string, duration: Duration | { until: Date | number }): Promise<void>;
  /**
   * Durable wait for a signal sent with `WorkflowClient.signal()`, including
   * one sent after the instance started but before it reached this wait.
   * Resolves with the payload, or `null` once `timeout` passes (compare with
   * `=== null` when the payload itself can be falsy).
   */
  waitForSignal<T>(
    name: string,
    signal: WorkflowSignal<T> | string,
    options?: WaitForSignalOptions<T>,
  ): Promise<Journaled<T> | null>;
  /**
   * Durable wait for whichever of several conditions happens first: a signal
   * (`ctx.signalWait()`), a timer (`ctx.timer()`) or a child's end (its handle). Resolves with the
   * key of the winner and its value (a signal's payload, `null` for a timer, a child's output; a
   * child that failed throws its `ChildWorkflowFailedError`), journaled under `name` as one entry, so a
   * replay returns the same winner. Only the winning signal is taken: a signal that matched
   * another condition stays for a later wait. When several are ready at once, the earliest
   * signal (lowest id) wins, then the earliest timer; a signal sent after a timer's deadline
   * doesn't count. Timers count from when the wait is first reached.
   *
   * ```ts
   * const outcome = await ctx.waitForAny('delivery-or-cancel', {
   *   delivered: ctx.signalWait(shipmentDelivered, { key: order.id }),
   *   cancelled: ctx.signalWait(orderCancelled, { key: order.id }),
   *   timeout: ctx.timer('3d'),
   * });
   * if (outcome.key === 'delivered') {
   *   await ctx.step('thank', () => this.mail.thank(order, outcome.value));
   * }
   * ```
   *
   * To wait for all of several signals, await several `waitForSignal()` calls with
   * `Promise.all()`: each is journaled on its own, and the instance parks once for all of them.
   */
  waitForAny<C extends Record<string, WorkflowCondition<unknown>>>(name: string, conditions: C): Promise<WorkflowAnyResult<C>>;
  /**
   * Starts a child instance of another workflow, linked to this one: `getStatus()` shows the
   * link, `list({ parentId })` finds the children, and `parentClose` decides what happens to a
   * child still running when this instance ends. Journaled: a replay returns the same child
   * instead of starting another. Resolves with a handle to wait for the child's result.
   * Starting a child with an id that another workflow, another parent or another input already
   * uses throws `WorkflowIdConflictError`.
   */
  startChild<W>(workflow: Type<W> | string, input: WorkflowInput<W>, options?: StartChildWorkflowOptions): Promise<ChildWorkflowHandle<WorkflowOutput<W>>>;
  /** `startChild()`, then its handle's `result()`. */
  executeChild<W>(workflow: Type<W> | string, input: WorkflowInput<W>, options?: StartChildWorkflowOptions): Promise<Journaled<WorkflowOutput<W>>>;
  /** A signal for `waitForAny()`: its value is the payload. */
  signalWait<T>(signal: WorkflowSignal<T> | string, options?: SignalWaitOptions<T>): WorkflowCondition<Journaled<T>>;
  /** A durable timer for `waitForAny()`: `duration` after the wait is first reached, or at `until`. Its value is `null`. */
  timer(duration: Duration | { until: Date | number }): WorkflowCondition<null>;
  /**
   * Journaled `Date.now()`: the same value on every replay. Helpers are
   * numbered in call order, so call them from sequential code, not from
   * parallel branches that reach them in a different order on a replay.
   */
  now(): number;
  /** Journaled `Math.random()`. */
  random(): number;
  /** Journaled `crypto.randomUUID()`. */
  uuid(): string;
  /**
   * Point of no return. Compensations of the steps called before it are
   * discarded: a later failure or cancel runs only the compensations of steps
   * called after it. Journaled under `name`, so it replays like a step and
   * shows in `getStatus(id, { journal: true })`.
   */
  commit(name: string): void;
  /**
   * Sets the instance's custom status, a JSON value of up to 16 KiB that the outside world
   * reads as `WorkflowClient.getStatus(id).customStatus` (a progress report, the stage an
   * order is in). It is written with the instance's next journal write, suspension or end,
   * and emitted as a `custom-status` event then. Every execution replays `run()`, and with it
   * the `setStatus()` calls, so the status needs no journal entry of its own and is never
   * read back by the workflow. `undefined` clears it (`null`).
   */
  setStatus(status: unknown): void;
  /** Fail the instance on purpose. Compensations run (only those after a `commit()`). */
  fail(message: string): never;
}
